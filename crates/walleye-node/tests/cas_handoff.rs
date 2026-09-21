//! Passing the writer lock back and forth between two processes.
//!
//! The MemWAL writer is claimed by a compare-and-swap on the shard manifest:
//! a claimant reads the current writer epoch and commits `epoch + 1`, and the
//! loser of a race is fenced. Nothing in that scheme says a claim is final, so
//! two processes writing the same stream on the same bucket should be able to
//! hand the lock between them indefinitely — each one claiming, writing, and
//! losing it again to the other without either corrupting the stream or
//! dropping an acknowledged row.
//!
//! That is the property these tests pin down, at the two layers where it can
//! break: the claim itself, and the daemon's policy about re-claiming.
//!
//! Both layers hold. A fenced process discards the dead handle and takes the
//! writer back on its next write, so the lock is a rota rather than a
//! one-way door, and the price of a turn is one open and a WAL replay.
//!
//! Two `Service`s over one root are two processes as far as the lock is
//! concerned. They share no memory, no catalog, no cache and no writer handle;
//! the manifest in the object store is the only thing between them, which is
//! exactly the situation of two hosts against one bucket.
//!
//! Set `WALLEYE_TEST_S3_URI` (say `s3://walleye/cas-handoff`) to run the same
//! tests against a real bucket, where the CAS is S3's conditional put rather
//! than a file rename. Without it the S3 tests skip.
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;
use walleye_lance::{LanceDurability, LanceStorageOptions, Table, TableConfig};
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";
const STREAM: &str = "probe";
/// How many times the lock changes hands. Three is the smallest number that
/// tells the two failure modes apart: a lock that can be taken once but never
/// given back fails on the third, not the second.
const ROUNDS: usize = 6;

/// A bucket prefix nothing else is using, so a failed run cannot poison the
/// next one. Only used when `WALLEYE_TEST_S3_URI` is set.
fn bucket_root() -> Option<String> {
    let base = std::env::var("WALLEYE_TEST_S3_URI").ok()?;
    let base = base.trim().trim_end_matches('/');
    if base.is_empty() {
        return None;
    }
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Some(format!("{base}/run-{unique}"))
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("at", DataType::Int64, false),
    ]))
}

fn config(cache: &std::path::Path, node: &str, root: &str) -> Config {
    Config {
        node_id: node.into(),
        listen: "127.0.0.1:0".into(),
        directory: cache.join(node),
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: TOKEN.into(),
        bitr: false,
        // Each process is a cluster of one, so ownership never routes a write
        // elsewhere and the only thing arbitrating is the writer claim.
        members: vec![Node::new(node, format!("http://{node}"), 1.0).unwrap()],
        kubernetes: None,
        processor: None,
        api: Some(ApiConfig {
            root_uri: root.to_owned(),
            bitr_url: None,
        }),
    }
}

async fn send(app: &axum::Router, method: &str, uri: &str, body: Value) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Bring a process up. The first one to get there defines the stream; the
/// second finds it in the catalog, which is what a second host would do.
async fn process(cache: &std::path::Path, node: &str, root: &str) -> (Arc<Service>, axum::Router) {
    let service = Service::open(config(cache, node, root)).await.unwrap();
    let app = router(service.clone());
    let (status, body) = send(
        &app,
        "POST",
        "/v1/streams",
        json!({"name": STREAM, "primary_key": ["id"],
               "columns": [{"name":"id","type":"int64"},{"name":"at","type":"int64"}]}),
    )
    .await;
    assert!(
        status == StatusCode::OK || body.contains("already exists"),
        "define {STREAM} on {node}: {status} {body}"
    );
    (service, app)
}

/// Write one row, retrying while the answer is "another writer holds this".
///
/// A fenced write is a 503 with `retry-after`, which is the engine telling the
/// caller to come back — so a caller that gives up on the first one has not
/// tested the contract. Returns how many attempts it took, or the last refusal.
async fn write_row(app: &axum::Router, id: i64) -> Result<usize, String> {
    let mut last = String::new();
    for attempt in 1..=5 {
        let (status, body) = send(
            app,
            "POST",
            &format!("/v1/streams/{STREAM}/events"),
            json!({"rows": [{"id": id, "at": id}]}),
        )
        .await;
        if status == StatusCode::OK {
            return Ok(attempt);
        }
        if status != StatusCode::SERVICE_UNAVAILABLE {
            return Err(format!("{status}: {body}"));
        }
        last = format!("{status}: {body}");
        tokio::time::sleep(std::time::Duration::from_millis(200 * attempt as u64)).await;
    }
    Err(format!("refused five times, last {last}"))
}

/// Every id in the stream, read straight from storage rather than through
/// either process, so a claim made by the reader cannot be mistaken for one of
/// theirs.
async fn ids_in_storage(root: &str, stored: Arc<Schema>) -> Vec<i64> {
    let uri = format!("{}/data/{STREAM}", root.trim_end_matches('/'));
    let config = TableConfig::new(STREAM, uri, stored, vec!["id".into()]).unwrap();
    let mut table = Table::open(
        config,
        LanceStorageOptions::default(),
        LanceDurability::ObjectStore,
    )
    .await
    .expect("read the stream back");
    let scanned = table.scan(None, 10_000).await.expect("scan");
    let mut ids: Vec<i64> = scanned
        .iter()
        .flat_map(|batch: &RecordBatch| {
            let column = batch.column_by_name("id").expect("id column");
            let values = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is int64");
            (0..values.len()).map(|i| values.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let _ = table.close().await;
    ids.sort_unstable();
    ids
}

/// The claim itself, with no daemon in the way: alternate two independent
/// table handles over one root and check the epoch climbs by one each time.
///
/// This is the layer the user-visible property rests on. If it holds here and
/// not above, the limit is policy rather than the lock.
async fn claims_alternate(root: &str) {
    let mut epochs = Vec::new();
    for round in 0..ROUNDS {
        let config = TableConfig::new(
            STREAM,
            format!("{}/data/{STREAM}", root.trim_end_matches('/')),
            schema(),
            vec!["id".into()],
        )
        .unwrap();
        // A fresh storage session per claim: two processes share no cache.
        let mut table = Table::open(
            config,
            LanceStorageOptions::default(),
            LanceDurability::ObjectStore,
        )
        .await
        .unwrap_or_else(|e| panic!("round {round} could not claim the writer: {e}"));
        epochs.push(table.writer_epoch());
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![round as i64])),
                Arc::new(Int64Array::from(vec![round as i64])),
            ],
        )
        .unwrap();
        table
            .append(vec![batch])
            .await
            .unwrap_or_else(|e| panic!("round {round} claimed the writer but could not write: {e}"));
        table.checkpoint().await.expect("checkpoint");
        table.close().await.expect("close");
    }
    assert_eq!(epochs.len(), ROUNDS);
    for pair in epochs.windows(2) {
        assert!(
            pair[1] > pair[0],
            "each claim takes a higher epoch than the last, got {epochs:?}"
        );
    }
    let ids = ids_in_storage(root, schema()).await;
    assert_eq!(
        ids,
        (0..ROUNDS as i64).collect::<Vec<_>>(),
        "every round's row survived the handovers"
    );
}

/// Two daemons, one stream, one bucket: the lock goes back and forth while
/// both stay up, and nothing acknowledged is lost.
async fn processes_alternate(root: &str) {
    let cache = tempfile::tempdir().unwrap();
    let (a_service, a) = process(cache.path(), "a", root).await;
    let (b_service, b) = process(cache.path(), "b", root).await;

    let mut written = Vec::new();
    let mut attempts = Vec::new();
    let mut refused: Option<(usize, String)> = None;
    for round in 0..ROUNDS {
        let (who, app) = if round % 2 == 0 { ("a", &a) } else { ("b", &b) };
        match write_row(app, round as i64).await {
            Ok(tries) => {
                written.push(round as i64);
                attempts.push((who, tries));
            }
            Err(why) => {
                refused = Some((round, format!("{who} was refused: {why}")));
                break;
            }
        }
    }
    println!("  handovers that succeeded: {attempts:?}");

    a_service.close().await;
    b_service.close().await;

    if let Some((round, why)) = refused {
        panic!(
            "the writer lock stopped moving at round {round} of {ROUNDS}: {why}\n  \
             rounds that did land: {written:?}"
        );
    }

    // A stream the daemon defined carries the arrival number it stamps on
    // every row, so reading it back needs that column too.
    let stamped = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("at", DataType::Int64, false),
        Field::new("_walleye_seq", DataType::UInt64, false),
    ]));
    let ids = ids_in_storage(root, stamped).await;
    assert_eq!(
        ids, written,
        "every acknowledged row is in the stream after {ROUNDS} handovers"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_can_be_taken_and_retaken_on_a_local_store() {
    let dir = tempfile::tempdir().unwrap();
    claims_alternate(&format!("file://{}/store", dir.path().display())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claim_can_be_taken_and_retaken_on_a_bucket() {
    let Some(root) = bucket_root() else {
        eprintln!("skipping: set WALLEYE_TEST_S3_URI to run this against a bucket");
        return;
    };
    claims_alternate(&root).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_pass_the_writer_back_and_forth_on_a_local_store() {
    let dir = tempfile::tempdir().unwrap();
    processes_alternate(&format!("file://{}/store", dir.path().display())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_pass_the_writer_back_and_forth_on_a_bucket() {
    let Some(root) = bucket_root() else {
        eprintln!("skipping: set WALLEYE_TEST_S3_URI to run this against a bucket");
        return;
    };
    processes_alternate(&root).await;
}
