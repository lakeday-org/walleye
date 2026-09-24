//! Tiers built by maintaining one stream from another.
//!
//! A view reads the rows that arrived after its cursor, runs its query over
//! only those, writes the result, and remembers how far it reached. Chaining
//! that gives the bronze, silver and gold shape: each tier is the same
//! primitive applied again, not a different mechanism.
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";
const ARROW: &str = "application/vnd.apache.arrow.stream";
const JSON: &str = "application/json";

mod common;

fn config(path: &std::path::Path) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: TOKEN.into(),
        bitr: false,
        members: vec![Node::new("n", "http://n", 1.0).unwrap()],
        kubernetes: None,
        processor: None,
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", path.display()),
            bitr_url: None,
        }),
    }
}

fn tickets(rows: &[&str]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(rows.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buffer, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    buffer
}

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, value)
}
async fn post(
    app: &axum::Router,
    uri: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    call(app, "POST", uri, content_type, body).await
}
async fn sql(app: &axum::Router, statement: &str) -> Value {
    let (status, body) = post(
        app,
        "/v1/query",
        JSON,
        json!({ "sql": statement }).to_string().into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{statement}: {body}");
    body
}

async fn seed(path: &std::path::Path, rows: &[&str]) -> axum::Router {
    let app = router(Service::open(config(path)).await.unwrap());
    let (status, body) = post(
        &app,
        "/v1/table/bronze/create/?mode=create",
        ARROW,
        tickets(rows),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create bronze: {body}");
    app
}
async fn add(app: &axum::Router, rows: &[&str]) {
    let (status, body) = post(app, "/v1/table/bronze/insert/", ARROW, tickets(rows)).await;
    assert_eq!(status, StatusCode::OK, "insert: {body}");
}
async fn define(app: &axum::Router, name: &str, definition: Value) {
    let (status, body) = post(
        app,
        &format!("/v1/view/{name}/create/"),
        JSON,
        definition.to_string().into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create view {name}: {body}");
}
async fn refresh(app: &axum::Router, name: &str) -> Value {
    let (status, body) = post(
        app,
        &format!("/v1/view/{name}/refresh/?passes=50"),
        JSON,
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refresh {name}: {body}");
    body
}

/// A tier is maintained forward. The first pass takes what is there, a later
/// pass takes only what arrived since, and a pass with nothing to do says so
/// instead of redoing the tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_processes_each_row_once_and_resumes_where_it_stopped() {
    let d = tempfile::tempdir().unwrap();
    let app = seed(d.path(), &["first", "second"]).await;
    define(
        &app,
        "silver",
        json!({
            "source": "bronze",
            "target": "silver_rows",
            "sql": "SELECT upper(body) AS shouted FROM bronze"
        }),
    )
    .await;

    let first = refresh(&app, "silver").await;
    assert_eq!(first["rows"], 2, "{first}");
    assert_eq!(first["written"], 2, "{first}");
    assert_eq!(first["caught_up"], true, "{first}");

    let again = refresh(&app, "silver").await;
    assert_eq!(again["rows"], 0, "a caught up view does no work: {again}");

    add(&app, &["third"]).await;
    let third = refresh(&app, "silver").await;
    assert_eq!(third["rows"], 1, "only the new row: {third}");

    let rows = sql(&app, "SELECT shouted FROM silver_rows ORDER BY shouted").await;
    let shouted: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["shouted"].as_str().unwrap())
        .collect();
    assert_eq!(shouted, vec!["FIRST", "SECOND", "THIRD"], "{rows}");
}

/// The cursor is the durable part. A view that has already consumed a row
/// must not consume it again after the process restarts, or every restart
/// would rebuild and repay for the whole tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_survives_a_restart() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path())).await.unwrap();
    {
        let app = router(service.clone());
        let (status, body) = post(
            &app,
            "/v1/table/bronze/create/?mode=create",
            ARROW,
            tickets(&["alpha", "beta"]),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "create bronze: {body}");
        define(
            &app,
            "silver",
            json!({"source": "bronze", "target": "silver_rows",
                   "sql": "SELECT body FROM bronze"}),
        )
        .await;
        assert_eq!(refresh(&app, "silver").await["rows"], 2);
        drop(app);
    }
    service.close().await;
    drop(service);
    // A second process over the same store, with nothing in memory.
    let app = router(Service::open(config(d.path())).await.unwrap());
    let described = post(&app, "/v1/view/silver/describe/", JSON, b"{}".to_vec()).await;
    assert_eq!(described.0, StatusCode::OK, "{:?}", described.1);
    assert!(
        described.1["cursor"].as_u64().unwrap() > 0,
        "the cursor was remembered: {}",
        described.1
    );
    let after = refresh(&app, "silver").await;
    assert_eq!(after["rows"], 0, "nothing is reprocessed: {after}");

    add(&app, &["gamma"]).await;
    let fresh = refresh(&app, "silver").await;
    assert_eq!(fresh["rows"], 1, "and new rows still arrive: {fresh}");
}

/// Repeating a pass is safe. The rows a replay produces are identical to the
/// ones already written, and the target's content key collapses them, so a
/// crash between writing and recording the cursor costs work but not
/// correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replaying_a_pass_does_not_duplicate_rows() {
    let d = tempfile::tempdir().unwrap();
    let app = seed(d.path(), &["one", "two", "three"]).await;
    define(
        &app,
        "silver",
        json!({"source": "bronze", "target": "silver_rows",
               "sql": "SELECT body FROM bronze"}),
    )
    .await;
    refresh(&app, "silver").await;

    // Stand in for a crash after the write and before the cursor moved: the
    // same source rows are processed a second time.
    define(
        &app,
        "replay",
        json!({"source": "bronze", "target": "silver_rows",
               "sql": "SELECT body FROM bronze"}),
    )
    .await;
    refresh(&app, "replay").await;

    let counted = sql(&app, "SELECT count(*) AS n FROM silver_rows").await;
    assert_eq!(
        counted.as_array().unwrap()[0]["n"],
        3,
        "the replayed rows collapsed: {counted}"
    );
}

/// A view cannot write into the stream it reads, because that would feed its
/// own output back to itself for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_may_not_write_back_into_its_source() {
    let d = tempfile::tempdir().unwrap();
    let app = seed(d.path(), &["anything"]).await;
    let (status, body) = post(
        &app,
        "/v1/view/loop/create/",
        JSON,
        json!({"source": "bronze", "target": "bronze", "sql": "SELECT body FROM bronze"})
            .to_string()
            .into_bytes(),
    )
    .await;
    assert_ne!(status, StatusCode::OK, "a view that eats itself");
    assert!(body.to_string().contains("own source"), "{body}");
}

/// The whole shape: raw rows become a classified tier, and a further tier
/// keeps only the labels the model was sure of, rolling the rest up rather
/// than discarding them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiers_refine_raw_rows_into_confident_labels() {
    // The decision service is a stand-in: which team a ticket goes to is not
    // what this tests, how the tiers carry and gate the answer is. One ticket
    // is answered unsure on purpose, so the gold tier's gate has something
    // to hold back.
    common::start(common::JEV);
    let sure =
        |label: &str, confidence: f64| common::choice(label, confidence, &[(label, confidence)]);
    common::answer("wrong size", "team", sure("returns", 0.95));
    common::answer("tracking number", "team", sure("shipping", 0.97));
    common::answer("charged twice", "team", sure("billing", 0.6));
    common::answer("two weeks late", "team", sure("shipping", 0.93));
    let d = tempfile::tempdir().unwrap();
    let app = seed(
        d.path(),
        &[
            "My running shoes arrived in the wrong size. Can I swap them?",
            "The tracking number has not moved in nine days.",
            "I was charged twice for the same order.",
        ],
    )
    .await;

    let spec = json!({
        "team": {
            "type": "choice",
            "instructions": "Which team should handle this?",
            "criteria": {
                "returns": "Exchanges, refunds, wrong or damaged items",
                "shipping": "Delivery status, delays, lost packages",
                "billing": "Charges, invoices, payment problems"
            }
        }
    })
    .to_string()
    .replace('\'', "''");

    // Silver asks the decision service once per row and keeps the answer.
    define(
        &app,
        "silver",
        json!({
            "source": "bronze",
            "target": "silver_tickets",
            "sql": format!(
                "SELECT body, d['team']['answer'] AS team, d['team']['confidence'] AS sure \
                 FROM (SELECT body, prompt_jev(body, '{spec}') AS d FROM bronze)"
            )
        }),
    )
    .await;
    let silver = refresh(&app, "silver").await;
    assert_eq!(silver["written"], 3, "{silver}");

    // Gold keeps a narrow label only where the model was sure, and rolls the
    // rest up to a label that is still true.
    define(
        &app,
        "gold",
        json!({
            "source": "silver_tickets",
            "target": "gold_tickets",
            "sql": "SELECT body, CASE WHEN sure >= 0.9 THEN team ELSE 'needs_triage' END AS routed \
                    FROM silver_tickets"
        }),
    )
    .await;
    let gold = refresh(&app, "gold").await;
    assert_eq!(gold["written"], 3, "{gold}");

    let rows = sql(&app, "SELECT routed FROM gold_tickets ORDER BY routed").await;
    let routed: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["routed"].as_str().unwrap())
        .collect();
    assert_eq!(
        routed,
        ["needs_triage", "returns", "shipping"],
        "the sure answers route and the unsure one is held for triage: {rows}"
    );

    // And the tiers advance independently as new rows arrive.
    add(&app, &["Where is my parcel? It is two weeks late."]).await;
    assert_eq!(refresh(&app, "silver").await["rows"], 1);
    assert_eq!(refresh(&app, "gold").await["rows"], 1);
    let counted = sql(&app, "SELECT count(*) AS n FROM gold_tickets").await;
    assert_eq!(counted.as_array().unwrap()[0]["n"], 4, "{counted}");
}
