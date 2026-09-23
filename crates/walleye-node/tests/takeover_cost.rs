//! What a successor pays to open a table a live process is writing to.
//!
//! Opening replays every write-ahead entry the incumbent has not flushed, and
//! each one is object-store round trips. On a local disk that is milliseconds;
//! against a bucket it is the difference between a handover nobody notices and
//! twenty seconds of a stream not answering.
//!
//! Flushing the incumbent first advances the replay cursor, so the successor
//! replays nothing and opens in constant time however far behind the flush
//! threshold the stream had drifted.
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;
use walleye_lance::{LanceDurability, LanceStorageOptions, Table, TableConfig};
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";

fn config(path: &std::path::Path) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes: 1024 * 1024 * 1024,
        disk_bytes: 256 * 1024 * 1024,
        token: TOKEN.into(),
        bitr: false,
        members: vec![Node::new("n", "http://n", 1.0).unwrap()],
        kubernetes: None,
        processor: None,
        lease: Default::default(),
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", path.display()),
            bitr_url: None,
        }),
    }
}
async fn ingest(app: &axum::Router, n: i64) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/streams/probe/events")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"rows":[{"id":n,"at":n}]}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn open_after(
    app: &axum::Router,
    dir: &std::path::Path,
    entries: i64,
    flush: bool,
) -> std::time::Duration {
    for n in 0..entries {
        assert_eq!(ingest(app, n).await, StatusCode::OK);
    }
    if flush {
        let flushed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/table/probe/flush_lsm/")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status();
        assert_eq!(flushed, StatusCode::OK, "flush before handover");
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("at", DataType::Int64, false),
        Field::new("_walleye_seq", DataType::UInt64, false),
    ]));
    let successor = TableConfig::new(
        "probe",
        format!("file://{}/store/data/probe", dir.display()),
        schema,
        vec!["id".into()],
    )
    .unwrap();
    let started = std::time::Instant::now();
    let opened = Table::open(
        successor,
        LanceStorageOptions::default(),
        LanceDurability::ObjectStore,
    )
    .await;
    let took = started.elapsed();
    assert!(opened.is_ok(), "the successor opened the table");
    drop(opened);
    took
}

async fn started(dir: &std::path::Path) -> (Arc<Service>, axum::Router) {
    let service = Service::open(config(dir)).await.unwrap();
    let app = router(service.clone());
    let defined = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/streams")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"name":"probe","primary_key":["id"],
                           "columns":[{"name":"id","type":"int64"},{"name":"at","type":"int64"}]})
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
        .status();
    assert_eq!(defined, StatusCode::OK);
    (service, app)
}

/// A handover costs what the incumbent has not flushed. Flushing first makes
/// it cost nothing, and that is the difference an operator can act on today.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flushing_before_a_handover_makes_the_successor_open_at_once() {
    const BACKLOG: i64 = 2048;

    let unflushed = {
        let d = tempfile::tempdir().unwrap();
        let (service, app) = started(d.path()).await;
        let took = open_after(&app, d.path(), BACKLOG, false).await;
        drop(app);
        service.close().await;
        took
    };
    let flushed = {
        let d = tempfile::tempdir().unwrap();
        let (service, app) = started(d.path()).await;
        let took = open_after(&app, d.path(), BACKLOG, true).await;
        drop(app);
        service.close().await;
        took
    };

    println!(
        "  {BACKLOG} unflushed entries: successor open {:.0} ms; after a flush: {:.0} ms",
        unflushed.as_secs_f64() * 1000.0,
        flushed.as_secs_f64() * 1000.0
    );
    // Locally the gap is around sixtyfold. Against a bucket every replayed
    // entry is round trips rather than page-cache reads, so the same ratio is
    // seconds rather than milliseconds. Two is a floor that will not flake.
    assert!(
        flushed * 2 < unflushed,
        "flushing first must make a handover cheap: {flushed:?} against {unflushed:?}"
    );
}
