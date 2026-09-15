//! Acceptance tests for cloud node fsync, authentication, and gateway wiring.

use std::sync::Arc;

use axum::http::{Request, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use tokio::net::TcpListener;
use walleye_bitr::{
    AppendRecord, ENCRYPTED_RECORD_CONTENT_TYPE, EncryptedRecord, HttpReplica, QuorumWriter,
    Replica, ReplicaGateway,
};
use walleye_bitr_server::{
    DiskReplica, INTERNAL_AUTH_HEADER, OpaqueArchive, ReplicaNode, gateway_router, node_router,
    router,
};

const VERSION: &str = "lakeday-cloud/deployment-identity/v1";
const INTERNAL_TOKEN: &str = "node-internal-test-token";

fn archive() -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "replica",
        64,
    )?))
}

fn opaque_record() -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": "tenant-a/catalog",
        "writer_epoch": 3,
        "lsn": 1,
        "committed_lsn": 0,
        "nonce": vec![17_u8; 24],
        "ciphertext": vec![17_u8; 12],
        "authentication": vec![17_u8; 32],
    }))
    .expect("opaque test envelope")
}

fn opaque_record_at(lsn: u64, writer_epoch: u64) -> EncryptedRecord {
    opaque_record_for("tenant-a/catalog", lsn, writer_epoch)
}

fn opaque_record_for(stream: &str, lsn: u64, writer_epoch: u64) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": writer_epoch,
        "lsn": lsn,
        "committed_lsn": lsn - 1,
        "nonce": vec![lsn as u8; 24],
        "ciphertext": vec![lsn as u8; 12],
        "authentication": vec![lsn as u8; 32],
    }))
    .expect("opaque test envelope")
}

fn tenant_token(root: &[u8; 32], tenant: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(root).expect("HMAC key");
    mac.update(format!("{VERSION}\0{tenant}\0replica-gateway-authentication").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn start_node(
    path: std::path::PathBuf,
    data_dir: &std::path::Path,
    root: &str,
) -> Result<
    (
        ReplicaNode,
        Arc<DiskReplica>,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
    ),
    Box<dyn std::error::Error>,
> {
    let node = Arc::new(DiskReplica::open_with_config(
        path,
        "test-node",
        "hot",
        data_dir,
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = node_router(Arc::clone(&node), root, Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((
        ReplicaNode::new(format!("node-{address}"), format!("http://{address}")),
        node,
        task,
    ))
}

#[tokio::test]
async fn three_nodes_form_an_authenticated_fsync_quorum_behind_one_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [23_u8; 32];
    let encoded_root = STANDARD.encode(root_key);
    let token = tenant_token(&root_key, "tenant-a");
    let directory = tempfile::tempdir()?;
    let mut members = Vec::new();
    let mut paths = Vec::new();
    let mut tasks = Vec::new();
    for index in 0..3 {
        let (mut member, _, task) = start_node(
            directory.path().join(format!("node-{index}.log")),
            directory.path(),
            &encoded_root,
        )
        .await?;
        member.id = format!("node-{index}");
        members.push(member);
        paths.push(directory.path().join(format!("node-{index}.log")));
        tasks.push(task);
    }
    let gateway = Arc::new(walleye_bitr_server::ReplicaGateway::new(
        members.clone(),
        2,
        &encoded_root,
        INTERNAL_TOKEN,
        archive()?,
    )?);
    let gateway_listener = TcpListener::bind("127.0.0.1:0").await?;
    let gateway_address = gateway_listener.local_addr()?;
    let gateway_task =
        tokio::spawn(async move { axum::serve(gateway_listener, gateway_router(gateway)).await });

    let client = Arc::new(HttpReplica::new(
        format!("http://{gateway_address}"),
        &token,
    ));
    let gateway_handle: Arc<dyn ReplicaGateway> = client;
    let writer = QuorumWriter::new(gateway_handle, [9; 32]);
    writer
        .append(AppendRecord::new(
            "tenant-a/catalog",
            2,
            1,
            0,
            b"secret delta",
        ))
        .await?;
    let recovered = writer.recover("tenant-a/catalog", 0).await?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].payload(), b"secret delta");
    for path in paths {
        let bytes = std::fs::read(path)?;
        assert!(
            !bytes
                .windows(b"secret delta".len())
                .any(|window| window == b"secret delta")
        );
    }

    gateway_task.abort();
    for task in tasks {
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn node_append_and_commit_routes_accept_bounded_binary_records()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [29_u8; 32];
    let encoded_root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let node = Arc::new(DiskReplica::open_with_config(
        directory.path().join("node.log"),
        "test-node",
        "hot",
        directory.path(),
    )?);
    let app = node_router(Arc::clone(&node), &encoded_root, Some(INTERNAL_TOKEN))?;
    let record = opaque_record();
    let body = record.encode_binary()?;

    let response = tower::ServiceExt::oneshot(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/internal/v1/append")
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(axum::body::Body::from(body.clone()))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = tower::ServiceExt::oneshot(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/internal/v1/commit")
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(axum::body::Body::from(body.clone()))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = tower::ServiceExt::oneshot(
        app.clone(),
        Request::builder()
            .method("POST")
            .uri("/v1/append")
            .header(
                "authorization",
                format!("Bearer {}", tenant_token(&root_key, "tenant-a")),
            )
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(axum::body::Body::from(body))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(node.snapshot().committed, vec![record]);

    let response = tower::ServiceExt::oneshot(
        app,
        Request::builder()
            .method("POST")
            .uri("/internal/v1/append")
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(axum::body::Body::from([0_u8; 4].as_slice()))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn node_append_many_route_accepts_one_framed_binary_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [37_u8; 32];
    let encoded_root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let node = Arc::new(DiskReplica::open_with_config(
        directory.path().join("node.log"),
        "test-node",
        "hot",
        directory.path(),
    )?);
    let app = node_router(Arc::clone(&node), &encoded_root, Some(INTERNAL_TOKEN))?;
    let records = (1..=3)
        .map(|lsn| opaque_record_at(lsn, 8))
        .collect::<Vec<_>>();
    let body = EncryptedRecord::encode_binary_batch(&records)?;

    let response = tower::ServiceExt::oneshot(
        app,
        Request::builder()
            .method("POST")
            .uri("/internal/v1/append-many")
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(axum::body::Body::from(body))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(node.snapshot().committed, records);
    Ok(())
}

#[tokio::test]
async fn one_tenant_token_cannot_read_another_tenant_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [31_u8; 32];
    let encoded_root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let node = Arc::new(DiskReplica::open_with_config(
        directory.path().join("node.log"),
        "test-node",
        "hot",
        directory.path(),
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = router(node, &encoded_root)?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });

    let response = reqwest::Client::new()
        .get(format!("http://{address}/v1/records"))
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .query(&[("stream", "tenant-b/catalog")])
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    task.abort();
    Ok(())
}

/// Compaction replaces archived records with one durable prefix fence. A
/// restart must retain the writer epoch and accept only the exact successor.
#[tokio::test]
async fn archived_prefix_compaction_survives_restart_and_reclaims_log_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("compact.log");
    let first = opaque_record_at(1, 7);
    let second = opaque_record_at(2, 7);
    let node = DiskReplica::open_with_config(&path, "test-node", "hot", directory.path())?;
    node.append(first.clone()).await?;
    node.commit(first).await?;
    node.append(second.clone()).await?;
    node.commit(second).await?;
    let before = std::fs::metadata(&path)?.len();

    assert_eq!(node.compact_archived("tenant-a/catalog", 2, 7)?, 2);
    assert!(std::fs::metadata(&path)?.len() < before);
    drop(node);

    let reopened = DiskReplica::open_with_config(&path, "test-node", "hot", directory.path())?;
    assert!(reopened.snapshot().records.is_empty());
    assert_eq!(reopened.snapshot().trimmed[0].archived_lsn, 2);
    let third = opaque_record_at(3, 7);
    reopened.append(third.clone()).await?;
    reopened.commit(third).await?;
    let stale = opaque_record_at(4, 6);
    assert_eq!(
        reopened.append(stale).await,
        Err(walleye_bitr::ReplicaError::WriterFenced)
    );
    Ok(())
}

/// One archival pass rewrites every newly published stream together while
/// retaining the unarchived suffix of each stream across restart.
#[tokio::test]
async fn archival_pass_compacts_multiple_streams_in_one_atomic_rewrite()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("multi-stream-compact.log");
    let node = DiskReplica::open_with_config(&path, "test-node", "hot", directory.path())?;
    for record in [
        opaque_record_for("tenant-a/catalog", 1, 7),
        opaque_record_for("tenant-a/catalog", 2, 7),
        opaque_record_for("tenant-a/catalog", 3, 7),
        opaque_record_for("tenant-a/events", 1, 9),
        opaque_record_for("tenant-a/events", 2, 9),
    ] {
        node.append(record.clone()).await?;
        node.commit(record).await?;
    }

    assert_eq!(
        node.compact_archived_batch(&[
            ("tenant-a/catalog".to_owned(), 2, 7),
            ("tenant-a/events".to_owned(), 1, 9),
        ])?,
        3
    );
    let snapshot = node.snapshot();
    assert_eq!(snapshot.records.len(), 2);
    assert!(
        snapshot
            .records
            .iter()
            .any(|record| record.stream() == "tenant-a/catalog" && record.lsn() == 3)
    );
    assert!(
        snapshot
            .records
            .iter()
            .any(|record| record.stream() == "tenant-a/events" && record.lsn() == 2)
    );
    drop(node);

    let reopened = DiskReplica::open_with_config(&path, "test-node", "hot", directory.path())?;
    let snapshot = reopened.snapshot();
    assert_eq!(snapshot.records.len(), 2);
    assert!(
        snapshot
            .trimmed
            .iter()
            .any(|prefix| prefix.stream == "tenant-a/catalog"
                && prefix.archived_lsn == 2
                && prefix.writer_epoch == 7)
    );
    assert!(
        snapshot
            .trimmed
            .iter()
            .any(|prefix| prefix.stream == "tenant-a/events"
                && prefix.archived_lsn == 1
                && prefix.writer_epoch == 9)
    );
    Ok(())
}

#[test]
fn open_truncates_only_an_incomplete_final_json_line() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("torn.log");
    let record = opaque_record();
    let mut complete = serde_json::to_vec(&record)?;
    complete.push(b'\n');
    let mut torn = complete.clone();
    torn.extend_from_slice(
        br#"{"stream":"tenant-a/catalog","writer_epoch":3,"lsn":2,"committed_lsn":"#,
    );
    std::fs::write(&path, &torn)?;

    let node = DiskReplica::open_with_config(&path, "test-node", "hot", directory.path())?;
    assert_eq!(node.snapshot().records, vec![record]);
    assert_eq!(std::fs::read(&path)?, complete);
    Ok(())
}

#[test]
fn open_rejects_a_malformed_interior_json_line() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("malformed.log");
    let encoded = serde_json::to_vec(&opaque_record())?;
    let mut bytes = b"not-json\n".to_vec();
    bytes.extend_from_slice(&encoded);
    bytes.push(b'\n');
    std::fs::write(&path, bytes)?;

    assert!(DiskReplica::open_with_config(&path, "test-node", "hot", directory.path()).is_err());
    Ok(())
}

#[tokio::test]
async fn missing_internal_token_never_authorizes_private_node_routes()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([39_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node = Arc::new(DiskReplica::open_with_config(
        directory.path().join("node.log"),
        "test-node",
        "hot",
        directory.path(),
    )?);
    let app = node_router(node, &root, None)?;
    let response = tower::ServiceExt::oneshot(
        app,
        Request::builder()
            .method("GET")
            .uri("/internal/v1/healthz")
            .body(axum::body::Body::empty())?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}
