//! Integration tests for dynamic membership, quorum fan-out, and safe repair.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;
use sha2::Sha256;
use tokio::net::TcpListener;
use walleye_bitr::{ENCRYPTED_RECORD_CONTENT_TYPE, EncryptedRecord, Replica, ReplicaError};
use walleye_bitr_server::{
    ADMIN_AUTH_HEADER, COMMIT_CERTIFICATE_HEADER, DiskReplica, GatewayStatus, INTERNAL_AUTH_HEADER,
    MAINTENANCE_AUTH_HEADER, OpaqueArchive, RebalanceReport, ReplicaGateway, ReplicaNode,
    gateway_router, node_router,
};

const VERSION: &str = "lakeday-cloud/deployment-identity/v1";
const INTERNAL_TOKEN: &str = "gateway-internal-test-token";

fn archive() -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "replica",
        64,
    )?))
}

fn tenant_token(root: &[u8; 32], tenant: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(root).expect("HMAC key");
    mac.update(format!("{VERSION}\0{tenant}\0replica-gateway-authentication").as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn opaque(
    stream: &str,
    writer_epoch: u64,
    lsn: u64,
    committed_lsn: u64,
    marker: u8,
) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": writer_epoch,
        "lsn": lsn,
        "committed_lsn": committed_lsn,
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 12],
        "authentication": vec![marker; 32],
    }))
    .expect("opaque test envelope")
}

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn running_node(
    directory: &tempfile::TempDir,
    name: &str,
    root: &str,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let disk = Arc::new(DiskReplica::open_with_config(
        directory.path().join(format!("{name}.log")),
        name,
        "hot",
        directory.path(),
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = node_router(Arc::clone(&disk), root, Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode {
        member: ReplicaNode::new(name, format!("http://{address}")),
        disk,
        task,
    })
}

async fn atomic_only_append(State(disk): State<Arc<DiskReplica>>, body: Bytes) -> StatusCode {
    let Ok(records) = EncryptedRecord::decode_binary_batch(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    match disk.append_and_commit_many(&records).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(ReplicaError::LsnConflict | ReplicaError::WriterFenced) => StatusCode::CONFLICT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn atomic_only_snapshot(
    State(disk): State<Arc<DiskReplica>>,
) -> Json<walleye_bitr_server::NodeSnapshot> {
    Json(disk.snapshot())
}

/// Starts a node exposing only the atomic append-and-commit operation.  A
/// gateway that regresses to the legacy data-then-marker requests cannot pass
/// this fixture.
async fn running_atomic_only_node(
    directory: &tempfile::TempDir,
    name: &str,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let disk = Arc::new(DiskReplica::open_with_config(
        directory.path().join(format!("{name}.log")),
        name,
        "hot",
        directory.path(),
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new()
        .route("/internal/v1/append-many", post(atomic_only_append))
        .route("/internal/v1/records", get(atomic_only_snapshot))
        .with_state(Arc::clone(&disk));
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode {
        member: ReplicaNode::new(name, format!("http://{address}")),
        disk,
        task,
    })
}

fn unreachable(name: &str) -> ReplicaNode {
    let port = 9_u16 + name.bytes().map(u16::from).sum::<u16>() % 1000;
    ReplicaNode::new(name, format!("http://127.0.0.1:{port}"))
}

fn members(nodes: &[&RunningNode]) -> Vec<ReplicaNode> {
    nodes.iter().map(|node| node.member.clone()).collect()
}

async fn gateway(
    nodes: Vec<ReplicaNode>,
    root: &str,
) -> Result<ReplicaGateway, Box<dyn std::error::Error>> {
    Ok(ReplicaGateway::new(
        nodes,
        2,
        root,
        INTERNAL_TOKEN,
        archive()?,
    )?)
}

#[tokio::test]
async fn append_acknowledges_two_of_three_when_one_node_is_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([41_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let gateway = gateway(
        vec![
            node0.member.clone(),
            node1.member.clone(),
            unreachable("node-2"),
        ],
        &root,
    )
    .await?;

    let record = opaque("tenant-a/catalog", 3, 1, 0, 7);
    assert_eq!(gateway.append(record.clone()).await?, 2);
    assert_eq!(node0.disk.records("tenant-a/catalog").await.len(), 1);
    assert_eq!(node1.disk.records("tenant-a/catalog").await.len(), 1);

    node0.task.abort();
    node1.task.abort();
    Ok(())
}

#[tokio::test]
async fn single_record_gateway_append_uses_atomic_node_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([71_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_atomic_only_node(&directory, "atomic-0").await?;
    let node1 = running_atomic_only_node(&directory, "atomic-1").await?;
    let node2 = running_atomic_only_node(&directory, "atomic-2").await?;
    let gateway = gateway(members(&[&node0, &node1, &node2]), &root).await?;
    let record = opaque("tenant-a/atomic", 4, 1, 0, 72);

    assert_eq!(gateway.append(record.clone()).await?, 2);
    let committed = [&node0, &node1, &node2]
        .iter()
        .filter(|node| node.disk.snapshot().committed == vec![record.clone()])
        .count();
    assert!(
        committed >= 2,
        "the acknowledgement requires two atomic commits"
    );

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn gateway_append_many_route_fans_out_one_framed_batch()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [42_u8; 32];
    let root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "batch-0", &root).await?;
    let node1 = running_node(&directory, "batch-1", &root).await?;
    let node2 = running_node(&directory, "batch-2", &root).await?;
    let gateway = Arc::new(ReplicaGateway::new(
        members(&[&node0, &node1, &node2]),
        2,
        &root,
        INTERNAL_TOKEN,
        archive()?,
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let gateway_task =
        tokio::spawn(async move { axum::serve(listener, gateway_router(gateway)).await });
    let records = (1..=3)
        .map(|lsn| opaque("tenant-a/catalog", 8, lsn, lsn - 1, lsn as u8))
        .collect::<Vec<_>>();
    let body = EncryptedRecord::encode_binary_batch(&records)?;
    let response = reqwest::Client::new()
        .post(format!("http://{address}/v1/append-many"))
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
        .body(body)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(response.headers().get(COMMIT_CERTIFICATE_HEADER).is_some());
    for node in [&node0, &node1, &node2] {
        assert_eq!(node.disk.snapshot().committed, records);
    }

    gateway_task.abort();
    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

/// A restarted replica process discovers committed local streams, publishes
/// opaque batches, and recovery joins that cold prefix to the hot quorum tail.
#[tokio::test]
async fn replica_archiver_recovers_and_resumes_committed_streams_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([81_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "archive-0", &root).await?;
    let node1 = running_node(&directory, "archive-1", &root).await?;
    let node2 = running_node(&directory, "archive-2", &root).await?;
    let configured = members(&[&node0, &node1, &node2]);
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(object_store, "replica", 2)?);
    let first = ReplicaGateway::new(
        configured.clone(),
        2,
        &root,
        INTERNAL_TOKEN,
        Arc::clone(&archive),
    )?;
    let records = (1..=3)
        .map(|lsn| opaque("tenant-a/catalog", 4, lsn, lsn - 1, lsn as u8))
        .collect::<Vec<_>>();
    for record in &records {
        first.append(record.clone()).await?;
    }
    assert_eq!(first.archive_local_commits(&node0.disk).await?, 3);
    assert_eq!(first.archive_local_commits(&node1.disk).await?, 0);
    assert_eq!(first.archive_local_commits(&node2.disk).await?, 0);
    assert_eq!(archive.archived_lsn("tenant-a/catalog").await?, 3);
    for node in [&node0, &node1, &node2] {
        let snapshot = node.disk.snapshot();
        assert!(snapshot.records.is_empty());
        assert_eq!(snapshot.trimmed[0].archived_lsn, 3);
    }
    assert_eq!(first.recover("tenant-a/catalog", 0).await?, records);

    let fourth = opaque("tenant-a/catalog", 4, 4, 3, 44);
    first.append(fourth.clone()).await?;
    let mut expected = records;
    expected.push(fourth);
    assert_eq!(first.recover("tenant-a/catalog", 0).await?, expected);

    assert_eq!(first.archive_local_commits(&node0.disk).await?, 1);
    assert_eq!(first.archive_local_commits(&node1.disk).await?, 0);
    assert_eq!(first.archive_local_commits(&node2.disk).await?, 0);
    assert_eq!(archive.archived_lsn("tenant-a/catalog").await?, 4);

    let restarted =
        ReplicaGateway::new(configured, 2, &root, INTERNAL_TOKEN, Arc::clone(&archive))?;
    let fifth = opaque("tenant-a/catalog", 4, 5, 4, 45);
    restarted.append(fifth.clone()).await?;
    expected.push(fifth);
    assert_eq!(restarted.recover("tenant-a/catalog", 0).await?, expected);

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

/// A stale hot copy at or below the immutable archive watermark must not make
/// recovery reject an otherwise valid cold prefix for missing marker quorum.
#[tokio::test]
async fn archived_prefix_ignores_stale_hot_marker_quorum() -> Result<(), Box<dyn std::error::Error>>
{
    let root = STANDARD.encode([83_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "stale-0", &root).await?;
    let node1 = running_node(&directory, "stale-1", &root).await?;
    let node2 = running_node(&directory, "stale-2", &root).await?;
    let configured = members(&[&node0, &node1, &node2]);
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(object_store, "replica", 2)?);
    let gateway = ReplicaGateway::new(configured, 2, &root, INTERNAL_TOKEN, Arc::clone(&archive))?;
    let records = (1..=4)
        .map(|lsn| opaque("tenant-a/catalog", 8, lsn, lsn - 1, (90 + lsn) as u8))
        .collect::<Vec<_>>();

    // Model the state while an already-published archive prefix is racing with
    // node compaction: node 0 has compacted through LSN 4, nodes 1 and 2
    // retain stale LSN 4 data, and only node 1 has its LSN 4 marker. Earlier
    // records still have two durable markers and are not ambiguous.
    for record in &records {
        node0.disk.append(record.clone()).await?;
        node0.disk.commit(record.clone()).await?;
        node1.disk.append(record.clone()).await?;
        node1.disk.commit(record.clone()).await?;
        node2.disk.append(record.clone()).await?;
        if record.lsn() < 4 {
            node2.disk.commit(record.clone()).await?;
        }
    }
    archive.archive_committed(&records).await?;
    node0.disk.compact_archived("tenant-a/catalog", 4, 8)?;

    assert_eq!(gateway.recover("tenant-a/catalog", 0).await?, records);

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

/// A published head is not enough to discard the hot copy: the node verifies
/// every newly covered immutable segment before its atomic rewrite.
#[tokio::test]
async fn missing_archive_segment_blocks_compaction_and_preserves_hot_records()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([82_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "verify-0", &root).await?;
    let node1 = running_node(&directory, "verify-1", &root).await?;
    let node2 = running_node(&directory, "verify-2", &root).await?;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(Arc::clone(&store), "replica", 2)?);
    let gateway = ReplicaGateway::new(
        members(&[&node0, &node1, &node2]),
        2,
        &root,
        INTERNAL_TOKEN,
        archive,
    )?;
    let record = opaque("tenant-a/catalog", 4, 1, 0, 83);
    gateway.append(record.clone()).await?;
    gateway.archive_local_commits(&node0.disk).await?;
    let objects = store
        .list(Some(&object_store::path::Path::from("replica")))
        .collect::<Vec<_>>()
        .await;
    let segment = objects
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|meta| meta.location.as_ref().contains("/segments/"))
        .ok_or("archive segment")?;
    store.delete(&segment.location).await?;

    assert!(gateway.archive_local_commits(&node1.disk).await.is_err());
    assert_eq!(node1.disk.records("tenant-a/catalog").await, vec![record]);

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn ordinary_recovery_uses_two_of_three_when_one_owner_is_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([68_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let configured = members(&[&node0, &node1, &node2]);
    let writer = gateway(configured.clone(), &root).await?;
    let record = opaque("tenant-a/catalog", 3, 1, 0, 69);
    assert_eq!(writer.append(record.clone()).await?, 2);

    // The third fan-out is allowed to finish before the owner is taken down.
    // A fresh coordinator must recover from the two live quorum copies.
    for _ in 0..100 {
        if node2.disk.records("tenant-a/catalog").await == vec![record.clone()] {
            break;
        }
        tokio::task::yield_now().await;
    }
    node2.task.abort();
    let restarted = gateway(configured, &root).await?;
    assert_eq!(
        restarted.recover("tenant-a/catalog", 0).await?,
        vec![record]
    );

    node0.task.abort();
    node1.task.abort();
    Ok(())
}

#[tokio::test]
async fn commit_certificate_recovers_one_markerless_copy_and_repairs_it()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [62_u8; 32];
    let root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let old0 = running_node(&directory, "old-0", &root).await?;
    let old1 = running_node(&directory, "old-1", &root).await?;
    let old2 = running_node(&directory, "old-2", &root).await?;
    let old_gateway = Arc::new(gateway(members(&[&old0, &old1, &old2]), &root).await?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task =
        tokio::spawn(async move { axum::serve(listener, gateway_router(old_gateway)).await });

    let record = opaque("tenant-a/catalog", 4, 1, 0, 63);
    let response = reqwest::Client::new()
        .post(format!("http://{address}/v1/append"))
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
        .body(record.encode_binary()?)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let certificate = response
        .headers()
        .get(COMMIT_CERTIFICATE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or("missing commit certificate")?
        .to_owned();

    // Recreate the handoff with a fresh disk containing only the fsynced data
    // record. It deliberately has no local commit marker, matching a gateway
    // crash immediately after its quorum response.
    let source = running_node(&directory, "certificate-source", &root).await?;
    source.disk.append(record.clone()).await?;
    let target0 = running_node(&directory, "certificate-target-0", &root).await?;
    let target1 = running_node(&directory, "certificate-target-1", &root).await?;
    let handoff = gateway(members(&[&source, &target0, &target1]), &root).await?;

    assert_eq!(
        handoff
            .recover_with_watermark("tenant-a/catalog", 0, Some(1))
            .await,
        Err(ReplicaError::CommitCertificateMissing)
    );
    assert_eq!(
        handoff
            .recover_with_watermark_certificate("tenant-a/catalog", 0, Some(1), Some(&certificate),)
            .await?,
        vec![record.clone()]
    );
    let report = handoff
        .repair_with_certificate("tenant-a/catalog", 1, Some(&certificate))
        .await?;
    assert_eq!(report.repaired, 2);
    assert_eq!(
        target0.disk.records("tenant-a/catalog").await,
        vec![record.clone()]
    );
    assert_eq!(target1.disk.records("tenant-a/catalog").await, vec![record]);

    task.abort();
    old0.task.abort();
    old1.task.abort();
    old2.task.abort();
    source.task.abort();
    target0.task.abort();
    target1.task.abort();
    Ok(())
}

#[tokio::test]
async fn fresh_gateway_recovers_from_all_three_disk_replicas()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([48_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let configured = members(&[&node0, &node1, &node2]);
    let first_gateway = gateway(configured.clone(), &root).await?;
    let second_gateway = gateway(configured, &root).await?;
    let record = opaque("tenant-a/catalog", 3, 1, 0, 49);

    first_gateway.append(record.clone()).await?;
    for node in [&node0, &node1, &node2] {
        let snapshot = node.disk.snapshot();
        assert_eq!(snapshot.records, vec![record.clone()]);
        assert_eq!(snapshot.committed, vec![record.clone()]);
    }
    assert_eq!(
        second_gateway.recover("tenant-a/catalog", 0).await?,
        vec![record]
    );

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn quorum_failure_does_not_acknowledge_one_partial_copy()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([42_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let gateway = gateway(
        vec![
            node0.member.clone(),
            unreachable("node-1"),
            unreachable("node-2"),
        ],
        &root,
    )
    .await?;

    let record = opaque("tenant-a/catalog", 3, 1, 0, 8);
    assert_eq!(
        gateway.append(record).await,
        Err(ReplicaError::QuorumUnavailable)
    );
    // An unavailable cell is not equivalent to a healthy empty stream.
    assert_eq!(
        gateway.recover("tenant-a/catalog", 0).await,
        Err(ReplicaError::QuorumUnavailable)
    );
    assert!(node0.disk.records("tenant-a/catalog").await.is_empty());

    node0.task.abort();
    Ok(())
}

#[tokio::test]
async fn ordinary_recovery_does_not_treat_an_all_node_outage_as_empty()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([70_u8; 32]);
    let gateway = gateway(
        vec![
            unreachable("node-0"),
            unreachable("node-1"),
            unreachable("node-2"),
        ],
        &root,
    )
    .await?;

    assert_eq!(
        gateway.recover("tenant-a/catalog", 0).await,
        Err(ReplicaError::QuorumUnavailable)
    );
    Ok(())
}

#[tokio::test]
async fn replacement_is_repaired_from_a_single_survivor_using_authenticated_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([43_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let replacement = running_node(&directory, "node-replacement", &root).await?;
    let gateway = gateway(
        vec![
            node0.member.clone(),
            node1.member.clone(),
            unreachable("node-2"),
        ],
        &root,
    )
    .await?;

    let record = opaque("tenant-a/catalog", 3, 1, 0, 9);
    assert_eq!(gateway.append(record.clone()).await?, 2);
    // The surviving node also has the asynchronous marker in this handoff;
    // the authenticated watermark bounds which contiguous prefix to copy.
    node0.disk.commit(record.clone()).await?;
    gateway.remove_node("node-1")?;
    gateway.add_node(replacement.member.clone())?;
    let report = gateway.repair("tenant-a/catalog", 1).await?;
    assert_eq!(report.repaired, 1);
    gateway.remove_node("node-2")?;
    assert_eq!(
        replacement.disk.records("tenant-a/catalog").await,
        vec![record.clone()]
    );
    assert_eq!(gateway.recover("tenant-a/catalog", 0).await?, vec![record]);
    assert!(gateway.nodes()?.iter().all(|node| node.id != "node-1"));

    node0.task.abort();
    node1.task.abort();
    replacement.task.abort();
    Ok(())
}

#[tokio::test]
async fn two_data_copies_without_markers_are_not_resurrected()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([44_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let gateway = gateway(members(&[&node0, &node1, &node2]), &root).await?;
    let partial = opaque("tenant-a/catalog", 3, 1, 0, 10);
    node0.disk.append(partial.clone()).await?;
    node1.disk.append(partial.clone()).await?;

    assert_eq!(
        gateway.recover("tenant-a/catalog", 0).await,
        Err(ReplicaError::RecoveryAmbiguous {
            lsn: 1,
            committed_nodes: 0,
            quorum: 2,
        })
    );
    assert_eq!(node1.disk.records("tenant-a/catalog").await, vec![partial]);
    assert!(node2.disk.records("tenant-a/catalog").await.is_empty());

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn watermark_recovery_fails_when_the_prefix_stops_before_the_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([60_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let gateway = gateway(members(&[&node0, &node1, &node2]), &root).await?;
    let first = opaque("tenant-a/catalog", 3, 1, 0, 61);
    node0.disk.append(first.clone()).await?;
    node1.disk.append(first).await?;

    assert_eq!(
        gateway
            .recover_with_watermark("tenant-a/catalog", 0, Some(2))
            .await,
        Err(ReplicaError::RecoveryIncomplete {
            expected_lsn: 2,
            committed_lsn: 2,
        })
    );

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn conflicting_records_are_not_selected_even_when_each_has_one_copy()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([45_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let gateway = gateway(members(&[&node0, &node1, &node2]), &root).await?;
    let first = opaque("tenant-a/catalog", 3, 1, 0, 11);
    let second = opaque("tenant-a/catalog", 3, 1, 0, 12);
    node0.disk.append(first).await?;
    node1.disk.append(second).await?;

    assert!(gateway.recover("tenant-a/catalog", 0).await?.is_empty());
    let report = gateway.rebalance(None).await?;
    assert_eq!(report.committed_records, 0);
    assert_eq!(report.unsafe_records_skipped, 1);
    assert!(node2.disk.records("tenant-a/catalog").await.is_empty());

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn authenticated_watermark_repair_copies_only_the_requested_stream_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [46_u8; 32];
    let root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let gateway = Arc::new(gateway(members(&[&node0, &node1, &node2]), &root).await?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, gateway_router(gateway)).await });

    let first = opaque("tenant-a/catalog", 3, 1, 0, 13);
    let second = opaque("tenant-a/catalog", 3, 2, 1, 14);
    node0.disk.append(first.clone()).await?;
    node0.disk.append(second).await?;
    node0.disk.commit(first.clone()).await?;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{address}/v1/admin/repair"))
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .json(&json!({"stream": "tenant-a/catalog", "committed_lsn": 1}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let report: RebalanceReport = response.json().await?;
    assert_eq!(report.committed_records, 1);
    assert_eq!(
        node1.disk.records("tenant-a/catalog").await,
        vec![first.clone()]
    );
    assert_eq!(node2.disk.records("tenant-a/catalog").await, vec![first]);

    let unauthorized = client
        .post(format!("http://{address}/v1/admin/repair"))
        .json(&json!({"stream": "tenant-a/catalog", "committed_lsn": 2}))
        .send()
        .await?;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    task.abort();
    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn gateway_readiness_requires_two_healthy_nodes_and_internal_api_is_private()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([47_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let joining = Arc::new(
        gateway(
            vec![
                node0.member.clone(),
                node1.member.clone(),
                unreachable("node-2"),
            ],
            &root,
        )
        .await?
        .with_local_member_id("joining-node"),
    );
    let response = tower::ServiceExt::oneshot(
        gateway_router(joining),
        axum::http::Request::builder()
            .uri("/controlz")
            .body(axum::body::Body::empty())?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let gateway = Arc::new(
        gateway(
            vec![
                node0.member.clone(),
                node1.member.clone(),
                unreachable("node-2"),
            ],
            &root,
        )
        .await?
        .with_admin_token("operator-admin-token"),
    );
    let app = gateway_router(Arc::clone(&gateway));
    let response = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::builder()
            .uri("/readyz")
            .body(axum::body::Body::empty())?,
    )
    .await?;
    // A ready answer now names the members that are serving.
    assert_eq!(response.status(), StatusCode::OK);
    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .method("GET")
            .uri("/internal/v1/nodes")
            .header(INTERNAL_AUTH_HEADER, "wrong")
            .body(axum::body::Body::empty())?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let app = gateway_router(gateway);
    let response = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::builder()
            .method("GET")
            .uri("/v1/admin/status")
            .header(ADMIN_AUTH_HEADER, "operator-admin-token")
            .body(axum::body::Body::empty())?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let status: GatewayStatus = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map(|body| serde_json::from_slice(&body))??;
    assert_eq!(status.quorum, 2);
    assert_eq!(status.storage_nodes, 3);
    assert_eq!(status.healthy_storage, 2);
    assert_eq!(status.active_requests, 0);

    let response = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .method("POST")
            .uri("/internal/v1/nodes")
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(serde_json::to_vec(
                &ReplicaNode::new("new-node", "http://127.0.0.1:1"),
            )?))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

    node0.task.abort();
    node1.task.abort();
    Ok(())
}

#[tokio::test]
async fn maintenance_fence_blocks_writes_and_requires_release_after_rebalance()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [65_u8; 32];
    let root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let gateway = Arc::new(
        gateway(members(&[&node0, &node1, &node2]), &root)
            .await?
            .with_admin_token("operator-admin-token")
            .with_local_member_id("node-0"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, gateway_router(gateway)).await });
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{address}/internal/v1/maintenance/fence"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let lease: serde_json::Value = response.json().await?;
    let token = lease["token"].as_str().ok_or("missing maintenance token")?;
    assert!(!token.is_empty());

    let status = client
        .get(format!("http://{address}/v1/admin/status"))
        .header(ADMIN_AUTH_HEADER, "operator-admin-token")
        .send()
        .await?;
    assert_eq!(status.status(), StatusCode::OK);
    let status: GatewayStatus = status.json().await?;
    assert!(status.maintenance);
    assert_eq!(status.active_requests, 0);

    let data_readiness = client
        .get(format!("http://{address}/readyz"))
        .send()
        .await?;
    assert_eq!(data_readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
    let control_readiness = client
        .get(format!("http://{address}/controlz"))
        .send()
        .await?;
    assert_eq!(control_readiness.status(), StatusCode::NO_CONTENT);

    let blocked = client
        .post(format!("http://{address}/v1/append"))
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .json(&opaque("tenant-a/catalog", 3, 1, 0, 66))
        .send()
        .await?;
    assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(node0.disk.records("tenant-a/catalog").await.is_empty());
    assert!(node1.disk.records("tenant-a/catalog").await.is_empty());
    assert!(node2.disk.records("tenant-a/catalog").await.is_empty());

    let blocked_read = client
        .get(format!(
            "http://{address}/v1/records?stream=tenant-a%2Fcatalog&after_lsn=0"
        ))
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .send()
        .await?;
    assert_eq!(blocked_read.status(), StatusCode::SERVICE_UNAVAILABLE);

    let rebalance = client
        .post(format!("http://{address}/internal/v1/rebalance"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .header(MAINTENANCE_AUTH_HEADER, token)
        .json(&json!({}))
        .send()
        .await?;
    assert_eq!(rebalance.status(), StatusCode::OK);

    let second_fence = client
        .post(format!("http://{address}/internal/v1/maintenance/fence"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .send()
        .await?;
    assert_eq!(second_fence.status(), StatusCode::CONFLICT);

    let wrong_release = client
        .delete(format!(
            "http://{address}/internal/v1/maintenance/fence/999"
        ))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .send()
        .await?;
    assert_eq!(wrong_release.status(), StatusCode::UNAUTHORIZED);

    let released = client
        .delete(format!(
            "http://{address}/internal/v1/maintenance/fence/{token}"
        ))
        .header(ADMIN_AUTH_HEADER, "operator-admin-token")
        .send()
        .await?;
    assert_eq!(released.status(), StatusCode::NO_CONTENT);

    let admitted = client
        .post(format!("http://{address}/v1/append"))
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .json(&opaque("tenant-a/catalog", 3, 1, 0, 67))
        .send()
        .await?;
    assert_eq!(admitted.status(), StatusCode::NO_CONTENT);

    task.abort();
    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn maintenance_fence_resumes_by_operation_id_after_gateway_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([66_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let configured = members(&[&node0, &node1, &node2]);
    let first = Arc::new(gateway(configured.clone(), &root).await?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let first_task =
        tokio::spawn(async move { axum::serve(listener, gateway_router(first)).await });
    let client = reqwest::Client::new();
    let operation_id = "cohort-fence-restart";
    let first_lease = client
        .post(format!("http://{address}/internal/v1/maintenance/fence"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .json(&json!({"operation_id": operation_id}))
        .send()
        .await?;
    assert_eq!(first_lease.status(), StatusCode::OK);
    let first_token = first_lease.json::<serde_json::Value>().await?["token"]
        .as_str()
        .ok_or("missing first maintenance token")?
        .to_owned();

    // A new gateway process has no local admission state. It reconstructs the
    // same signed owner/generation from the durable node markers and can
    // continue the operation without acquiring a competing fence.
    let second = Arc::new(gateway(configured, &root).await?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let second_address = listener.local_addr()?;
    let second_task =
        tokio::spawn(async move { axum::serve(listener, gateway_router(second)).await });
    let second_lease = client
        .post(format!(
            "http://{second_address}/internal/v1/maintenance/fence"
        ))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .json(&json!({"operation_id": operation_id}))
        .send()
        .await?;
    assert_eq!(second_lease.status(), StatusCode::OK);
    let second_token = second_lease.json::<serde_json::Value>().await?["token"]
        .as_str()
        .ok_or("missing resumed maintenance token")?
        .to_owned();
    assert_eq!(second_token, first_token);

    let release = client
        .delete(format!(
            "http://{second_address}/internal/v1/maintenance/fence/{second_token}"
        ))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .header(MAINTENANCE_AUTH_HEADER, &second_token)
        .send()
        .await?;
    assert_eq!(release.status(), StatusCode::NO_CONTENT);

    first_task.abort();
    second_task.abort();
    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn five_node_gateway_reconstructs_epoch_and_rejects_stale_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([51_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let node3 = running_node(&directory, "node-3", &root).await?;
    let node4 = running_node(&directory, "node-4", &root).await?;
    let configured = members(&[&node0, &node1, &node2, &node3, &node4]);
    let first_gateway = gateway(configured.clone(), &root).await?;

    let first = opaque("tenant-a/catalog", 7, 1, 0, 52);
    let second = opaque("tenant-a/catalog", 8, 2, 1, 53);
    assert_eq!(first_gateway.append(first.clone()).await?, 2);
    assert_eq!(first_gateway.append(second.clone()).await?, 2);

    // A stale writer must be rejected before it can obtain two acknowledgments
    // from the five-node set, even though quorum is intentionally only two.
    let stale = opaque("tenant-a/catalog", 7, 3, 2, 54);
    assert_eq!(
        first_gateway.append(stale).await,
        Err(ReplicaError::WriterFenced)
    );

    // Restarting a second gateway reconstructs the fence from node evidence;
    // no process-local commit journal is needed to reject the same stale epoch.
    let restarted = gateway(configured, &root).await?;
    let stale_after_restart = opaque("tenant-a/catalog", 7, 3, 2, 55);
    assert_eq!(
        restarted.append(stale_after_restart).await,
        Err(ReplicaError::WriterFenced)
    );
    assert_eq!(
        restarted.recover("tenant-a/catalog", 0).await?,
        vec![first, second]
    );

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    node3.task.abort();
    node4.task.abort();
    Ok(())
}

#[tokio::test]
async fn five_node_gateway_serializes_concurrent_same_lsn_writers()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([56_u8; 32]);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0", &root).await?;
    let node1 = running_node(&directory, "node-1", &root).await?;
    let node2 = running_node(&directory, "node-2", &root).await?;
    let node3 = running_node(&directory, "node-3", &root).await?;
    let node4 = running_node(&directory, "node-4", &root).await?;
    let gateway =
        Arc::new(gateway(members(&[&node0, &node1, &node2, &node3, &node4]), &root).await?);
    let first = opaque("tenant-a/catalog", 9, 1, 0, 57);
    assert_eq!(gateway.append(first).await?, 2);
    let candidate_a = opaque("tenant-a/catalog", 9, 2, 1, 58);
    let candidate_b = opaque("tenant-a/catalog", 9, 2, 1, 59);
    let (result_a, result_b) =
        tokio::join!(gateway.append(candidate_a), gateway.append(candidate_b));
    assert_eq!([result_a.is_ok(), result_b.is_ok()], [true, false]);
    let rejected = if result_a.is_err() {
        result_a
    } else {
        result_b
    };
    assert_eq!(rejected, Err(ReplicaError::LsnConflict));

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    node3.task.abort();
    node4.task.abort();
    Ok(())
}

#[tokio::test]
async fn durable_fence_survives_gateway_and_storage_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([81_u8; 32]);
    let directory = tempfile::tempdir()?;
    let first = running_node(&directory, "node-0", &root).await?;
    let second = running_node(&directory, "node-1", &root).await?;
    let configured = members(&[&first, &second]);
    let first_gateway = gateway(configured, &root).await?;
    let token = first_gateway.begin_maintenance().await?;
    assert!(first.disk.maintenance_status()?.active);
    assert!(second.disk.maintenance_status()?.active);
    assert!(
        std::fs::metadata(walleye_bitr_server::maintenance_marker_path(
            directory.path().join("node-0.log")
        ))
        .is_ok()
    );

    first.task.abort();
    second.task.abort();
    drop(first);
    drop(second);
    let restarted_first = running_node(&directory, "node-0", &root).await?;
    let restarted_second = running_node(&directory, "node-1", &root).await?;
    let restarted = gateway(members(&[&restarted_first, &restarted_second]), &root).await?;
    let status = restarted.metrics().await?;
    assert!(status.maintenance);
    assert_eq!(status.fenced_storage, 2);
    assert_eq!(
        restarted.maintenance_token().await.as_deref(),
        Some(token.as_str())
    );
    assert_eq!(
        restarted
            .append(opaque("tenant-a/catalog", 1, 1, 0, 82))
            .await,
        Err(walleye_bitr::ReplicaError::QuorumUnavailable)
    );
    let fenced_record = opaque("tenant-a/catalog", 1, 1, 0, 84);
    assert_eq!(
        restarted_first.disk.append(fenced_record.clone()).await,
        Err(walleye_bitr::ReplicaError::WriterFenced)
    );
    assert_eq!(
        restarted_first.disk.commit(fenced_record).await,
        Err(walleye_bitr::ReplicaError::WriterFenced)
    );

    restarted.end_maintenance(&token).await?;
    assert!(!restarted_first.disk.maintenance_status()?.active);
    assert!(!restarted_second.disk.maintenance_status()?.active);
    assert_eq!(
        restarted
            .append(opaque("tenant-a/catalog", 1, 1, 0, 83))
            .await?,
        2
    );

    restarted_first.task.abort();
    restarted_second.task.abort();
    Ok(())
}

#[tokio::test]
async fn partial_acquisition_and_release_remain_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([84_u8; 32]);
    let directory = tempfile::tempdir()?;
    let first = running_node(&directory, "node-0", &root).await?;
    let second = running_node(&directory, "node-1", &root).await?;
    let gateway = gateway(
        vec![
            first.member.clone(),
            second.member.clone(),
            unreachable("node-2"),
        ],
        &root,
    )
    .await?;

    assert_eq!(
        gateway.begin_maintenance().await,
        Err(walleye_bitr::ReplicaError::QuorumUnavailable)
    );
    assert!(first.disk.maintenance_status()?.active);
    assert!(second.disk.maintenance_status()?.active);
    let token = gateway
        .maintenance_token()
        .await
        .ok_or("partial acquisition lost its recovery token")?;
    assert_eq!(
        gateway
            .append(opaque("tenant-a/catalog", 1, 1, 0, 85))
            .await,
        Err(walleye_bitr::ReplicaError::QuorumUnavailable)
    );

    // Remove the unavailable endpoint, but keep the durable markers until a
    // complete release is acknowledged by every remaining node.
    gateway.remove_node("node-2")?;
    gateway.end_maintenance(&token).await?;
    assert!(!first.disk.maintenance_status()?.active);
    assert!(!second.disk.maintenance_status()?.active);

    first.task.abort();
    second.task.abort();
    Ok(())
}

#[tokio::test]
async fn stale_owner_generation_cannot_release_a_new_fence()
-> Result<(), Box<dyn std::error::Error>> {
    let root = STANDARD.encode([86_u8; 32]);
    let directory = tempfile::tempdir()?;
    let first = running_node(&directory, "node-0", &root).await?;
    let second = running_node(&directory, "node-1", &root).await?;
    let gateway = gateway(members(&[&first, &second]), &root).await?;
    let old_token = gateway.begin_maintenance().await?;
    gateway.end_maintenance(&old_token).await?;
    let new_token = gateway.begin_maintenance().await?;
    assert_ne!(old_token, new_token);
    assert_eq!(
        gateway.end_maintenance(&old_token).await,
        Err(walleye_bitr::ReplicaError::GatewayUnauthorized)
    );
    assert!(first.disk.maintenance_status()?.active);
    assert!(second.disk.maintenance_status()?.active);
    gateway.end_maintenance(&new_token).await?;

    first.task.abort();
    second.task.abort();
    Ok(())
}

/// Authenticated recovery failures expose their cause while unauthorized callers
/// still receive no storage details.
#[tokio::test]
async fn recovery_http_failure_retains_diagnostic_without_bypassing_auth()
-> Result<(), Box<dyn std::error::Error>> {
    let root_key = [62_u8; 32];
    let root = STANDARD.encode(root_key);
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "diagnostic-0", &root).await?;
    let node1 = running_node(&directory, "diagnostic-1", &root).await?;
    let node2 = running_node(&directory, "diagnostic-2", &root).await?;
    let gateway = Arc::new(gateway(members(&[&node0, &node1, &node2]), &root).await?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move { axum::serve(listener, gateway_router(gateway)).await });
    let url =
        format!("http://{address}/v1/records?stream=tenant-a/catalog&after_lsn=2&committed_lsn=1");
    let client = reqwest::Client::new();
    let unauthorized = client.get(&url).send().await?;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert!(unauthorized.text().await?.is_empty());
    let response = client
        .get(url)
        .bearer_auth(tenant_token(&root_key, "tenant-a"))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response.text().await?;
    task.abort();
    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    let diagnostic: serde_json::Value =
        serde_json::from_str(&body).expect("recovery failure must retain a structured diagnostic");
    assert_eq!(diagnostic["code"], "recovery_failed");
    assert!(
        diagnostic["error"]
            .as_str()
            .is_some_and(|value| value.contains("3") && value.contains("1"))
    );
    Ok(())
}
