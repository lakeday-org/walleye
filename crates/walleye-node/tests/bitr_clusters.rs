//! Two Bitr clusters, two daemons, one stream, end to end.
//!
//! This is the case the object-store WAL cannot stand in for. Nodes writing
//! that WAL share it, so each can always replay what the other left. Two Bitr
//! clusters share nothing but the bucket, so the tail one of them holds is
//! genuinely unreadable by the other. That used to lose the rows silently.
//!
//! Ownership is what keeps it from happening now: the second daemon does not
//! take a table whose owner is alive, it forwards to it, and the owner hands
//! the table over by flushing before it releases, which puts the tail in the
//! bucket where the second can read it.
//!
//! Each cluster here is what a real one is: three replica nodes behind a
//! gateway, spoken to over HTTP. The daemons are configured exactly as
//! deployment configures them, through `WALLEYE_BITR_URL` and the two keys, so
//! nothing about the path under test is stubbed.
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceExt;
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaNode, gateway_router, node_router};
use walleye_lance::{LanceDurability, LanceStorageOptions, Table, TableConfig};
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";
const INTERNAL: &str = "node-internal-test-token";
const STREAM: &str = "probe";
/// The deployment root key both clusters are built from. Sharing it is right:
/// what differs between the two is which replicas hold the log, not who the
/// tenant is, and that is exactly the confusion being tested.
const ROOT: [u8; 32] = [23; 32];

/// One Bitr cluster: three replicas that fsync, behind the gateway a client
/// talks to. Returns its address and the tasks holding it up.
async fn bitr_cluster(
    dir: &std::path::Path,
    name: &str,
) -> (String, Vec<tokio::task::JoinHandle<()>>) {
    let encoded = STANDARD.encode(ROOT);
    let mut tasks = Vec::new();
    let mut members = Vec::new();
    for index in 0..3 {
        let node = Arc::new(
            DiskReplica::open_with_config(
                dir.join(format!("{name}-{index}.log")),
                "test-node",
                "hot",
                dir,
            )
            .expect("replica storage"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = node_router(Arc::clone(&node), &encoded, Some(INTERNAL)).expect("node router");
        tasks.push(tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        }));
        members.push(ReplicaNode::new(
            format!("{name}-{index}"),
            format!("http://{address}"),
        ));
    }
    let archive = Arc::new(
        OpaqueArchive::new(
            Arc::new(object_store_bitr::memory::InMemory::new()),
            "replica",
            64,
        )
        .expect("archive"),
    );
    let gateway = Arc::new(
        walleye_bitr_server::ReplicaGateway::new(members, 2, &encoded, INTERNAL, archive)
            .expect("gateway"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tasks.push(tokio::spawn(async move {
        let _ = axum::serve(listener, gateway_router(gateway)).await;
    }));
    (format!("http://{address}"), tasks)
}

fn config(
    cache: &std::path::Path,
    node: &str,
    root: &str,
    bitr: &str,
    members: Vec<Node>,
) -> Config {
    Config {
        node_id: node.into(),
        listen: "127.0.0.1:0".into(),
        directory: cache.join(node),
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: TOKEN.into(),
        bitr: true,
        members,
        kubernetes: None,
        processor: None,
        lease: Default::default(),
        api: Some(ApiConfig {
            root_uri: root.to_owned(),
            bitr_url: Some(bitr.to_owned()),
        }),
    }
}

async fn send(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// A daemon backed by Bitr refuses writes until it has seen its quorum, which
/// a background probe establishes after `open` returns. Waiting for that is
/// what any client does, and skipping it just races the probe.
async fn define(app: &axum::Router) {
    for attempt in 0..60 {
        let (status, body) = send(
            app,
            "/v1/streams",
            json!({"name": STREAM, "primary_key": ["id"],
                   "columns": [{"name":"id","type":"int64"},{"name":"at","type":"int64"}]}),
        )
        .await;
        if status == StatusCode::OK || body.contains("already exists") {
            return;
        }
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "define: {status} {body}"
        );
        assert!(attempt < 59, "the quorum never became writable: {body}");
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

async fn write_row(app: &axum::Router, id: i64) -> (StatusCode, String) {
    send(
        app,
        &format!("/v1/streams/{STREAM}/events"),
        json!({"rows": [{"id": id, "at": id}]}),
    )
    .await
}

/// Every id in the stream, read from shared storage rather than through either
/// daemon, so neither one's memory can flatter the answer.
async fn ids_in_storage(root: &str) -> Vec<i64> {
    let uri = format!("{}/data/{STREAM}", root.trim_end_matches('/'));
    let stamped = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("at", DataType::Int64, false),
        Field::new("_walleye_seq", DataType::UInt64, false),
    ]));
    let config = TableConfig::new(STREAM, uri, stamped, vec!["id".into()]).unwrap();
    let mut table = Table::open(
        config,
        LanceStorageOptions::default(),
        LanceDurability::ObjectStore,
    )
    .await
    .expect("read the stream back");
    let scanned = table.scan(None, 10_000).await.expect("scan");
    let mut found: Vec<i64> = scanned
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name("id").expect("id column");
            let values = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is int64");
            (0..values.len())
                .map(|i| values.value(i))
                .collect::<Vec<_>>()
        })
        .collect();
    let _ = table.close().await;
    found.sort_unstable();
    found
}

/// A daemon on one Bitr cluster hands a stream to a daemon on another, with
/// rows acknowledged and unflushed until the moment of the handover.
///
/// The second daemon cannot read the first one's log. While the first owns
/// the stream, the second sends it the writes; when the first stops, it
/// flushes and releases, and the second claims a stream whose tail is in the
/// bucket where anyone can reach it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_stream_moves_between_two_bitr_clusters_without_losing_a_row() {
    // SAFETY: the daemon reads its deployment keys from the environment, and
    // this is the only test in this binary.
    unsafe {
        std::env::set_var("LAKEDAY_DATAPLANE_ROOT_KEY", STANDARD.encode(ROOT));
        std::env::set_var("WALLEYE_DATA_KEY", STANDARD.encode([9_u8; 32]));
    }

    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let logs = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());

    let (first_bitr, first_tasks) = bitr_cluster(logs.path(), "east").await;
    let (second_bitr, second_tasks) = bitr_cluster(logs.path(), "west").await;
    assert_ne!(first_bitr, second_bitr, "two clusters, two gateways");

    // The first daemon, serving for real so the second can reach it. Two
    // deployments, not one cluster: the only thing they share is the bucket.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_at = format!("http://{}", listener.local_addr().unwrap());
    let first = Service::open(config(
        cache.path(),
        "east",
        &root,
        &first_bitr,
        vec![Node::new("east", first_at.clone(), 1.0).unwrap()],
    ))
    .await
    .expect("the first daemon opens");
    let first_app = router(first.clone());
    define(&first_app).await;
    let serving = tokio::spawn({
        let first_app = first_app.clone();
        async move {
            let _ = axum::serve(listener, first_app).await;
        }
    });

    // Acknowledged, and deliberately not flushed. This row exists only in the
    // first cluster's quorum.
    let (status, body) = write_row(&first_app, 1).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the first daemon stores a row: {body}"
    );

    // The second daemon, on the other cluster. The first owns the stream, so
    // the second's write goes there.
    let second = Service::open(config(
        cache.path(),
        "west",
        &root,
        &second_bitr,
        vec![Node::new("west", "http://127.0.0.1:2", 1.0).unwrap()],
    ))
    .await
    .expect("the second daemon opens");
    let second_app = router(second.clone());
    define(&second_app).await;
    let (status, body) = write_row(&second_app, 2).await;
    assert_eq!(status, StatusCode::OK, "forwarded to the owner: {body}");

    // The first stops, handing the stream over, and the second takes it -
    // once its own quorum is writable, which a client waits out.
    first.release().await;
    let mut taken = write_row(&second_app, 3).await;
    for _ in 0..60 {
        if taken.0 != StatusCode::SERVICE_UNAVAILABLE {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        taken = write_row(&second_app, 3).await;
    }
    let (status, body) = taken;
    assert_eq!(
        status,
        StatusCode::OK,
        "the second daemon takes the stream: {body}"
    );
    first.close().await;
    second.close().await;
    serving.abort();
    for task in first_tasks.into_iter().chain(second_tasks) {
        task.abort();
    }

    // The whole point. Row 1 was acknowledged by a daemon on a cluster the
    // second one cannot read, and it is still here.
    assert_eq!(
        ids_in_storage(&root).await,
        vec![1, 2, 3],
        "no acknowledged row was lost moving between clusters"
    );
}
