//! Typed decisions inside SQL, over rows the node is holding.
//!
//! These tests call the real decision service, because the thing worth
//! proving is that a column of text becomes a column of labels end to end.
//! Without a key they skip rather than fail: a checkout with no credentials
//! is not a broken build.
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

fn keyed() -> bool {
    ["WALLEYE_TYPESAFE_API_KEY", "TYPESAFE_API_KEY"]
        .iter()
        .any(|name| std::env::var(name).is_ok_and(|key| !key.trim().is_empty()))
}

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

fn tickets_ipc(rows: &[&str]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
    ]));
    let ids: Vec<String> = (0..rows.len()).map(|n| format!("t{n}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(rows.to_vec())) as ArrayRef,
        ],
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

async fn post(
    app: &axum::Router,
    uri: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
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

async fn sql(app: &axum::Router, statement: &str) -> (StatusCode, Value) {
    post(
        app,
        "/v1/query",
        "application/json",
        json!({ "sql": statement }).to_string().into_bytes(),
    )
    .await
}

const ARROW: &str = "application/vnd.apache.arrow.stream";
const TAXONOMY: &str = "returns=Exchanges, refunds, wrong or damaged items; \
     shipping=Delivery status, delays, lost packages; \
     billing=Charges, invoices, payment problems";

async fn seeded(path: &std::path::Path, rows: &[&str]) -> axum::Router {
    let service = Service::open(config(path)).await.unwrap();
    let app = router(service);
    let (status, body) = post(
        &app,
        "/v1/table/tickets/create/?mode=create",
        ARROW,
        tickets_ipc(rows),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create: {body}");
    app
}

/// A column of text becomes a column of labels, chosen from a taxonomy the
/// query writes inline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_classifies_a_column_against_an_inline_taxonomy() {
    if !keyed() {
        eprintln!("skipping: no decision service key configured");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let app = seeded(
        d.path(),
        &[
            "My running shoes arrived in the wrong size. Can I swap them for a size 10?",
            "The tracking number has not moved in nine days.",
            "I was charged twice for the same order.",
        ],
    )
    .await;

    let (status, body) = sql(
        &app,
        &format!(
            "SELECT id, classify(body, 'Which team should handle this?', '{TAXONOMY}') AS team \
             FROM tickets ORDER BY id"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body.as_array().expect("rows");
    assert_eq!(rows.len(), 3, "{body}");
    assert_eq!(rows[0]["team"], "returns", "{body}");
    assert_eq!(rows[1]["team"], "shipping", "{body}");
    assert_eq!(rows[2]["team"], "billing", "{body}");
}

/// Confidence is the second axis. It comes back as a number the query can
/// compare against, which is what makes a tier able to hold a row back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confidence_is_queryable_so_a_tier_can_gate_on_it() {
    if !keyed() {
        eprintln!("skipping: no decision service key configured");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["I was charged twice for the same order."]).await;

    let (status, body) = sql(
        &app,
        &format!(
            "SELECT classify(body, 'Which team should handle this?', '{TAXONOMY}') AS team, \
                    classify_confidence(body, 'Which team should handle this?', '{TAXONOMY}') AS sure, \
                    holds(body, 'The customer is asking for money back') AS refund \
             FROM tickets"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let row = &body.as_array().expect("rows")[0];
    assert_eq!(row["team"], "billing", "{body}");
    let sure = row["sure"].as_f64().expect("a confidence");
    assert!(
        (0.0..=1.0).contains(&sure),
        "confidence in range, got {sure}"
    );
    assert!(
        sure > 0.5,
        "an unambiguous billing complaint is not a coin flip, got {sure}"
    );
    let refund = row["refund"].as_f64().expect("a probability");
    assert!(
        (0.0..=1.0).contains(&refund),
        "probability in range, got {refund}"
    );
}

/// A taxonomy the service would refuse is refused as a query error naming the
/// problem, rather than as a column of nulls the caller has to interpret.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_taxonomy_fails_the_query_and_says_why() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["anything at all"]).await;

    let (status, body) = sql(
        &app,
        "SELECT classify(body, 'pick one', 'no-equals-sign-here') AS team FROM tickets",
    )
    .await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a taxonomy with no meanings is not usable"
    );
    assert!(
        body.to_string().contains("option=meaning"),
        "names what the taxonomy should look like, got {body}"
    );

    let (status, body) = sql(
        &app,
        "SELECT classify(body, 'pick one', 'only=the sole option') AS team FROM tickets",
    )
    .await;
    assert_ne!(status, StatusCode::OK, "a choice of one is not a choice");
    assert!(
        body.to_string().contains("at least two options"),
        "names the reason, got {body}"
    );
}

/// Rows repeat, and an answer about identical text is the same answer. The
/// query must not pay for the same question twice, which is what makes
/// materialising a tier over a large table affordable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_text_is_asked_about_once() {
    if !keyed() {
        eprintln!("skipping: no decision service key configured");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let repeated = "The tracking number has not moved in nine days.";
    let app = seeded(d.path(), &[repeated; 24]).await;

    let started = std::time::Instant::now();
    let (status, body) = sql(
        &app,
        &format!(
            "SELECT classify(body, 'Which team should handle this?', '{TAXONOMY}') AS team \
             FROM tickets"
        ),
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body.as_array().expect("rows");
    assert_eq!(rows.len(), 24, "{body}");
    assert!(
        rows.iter().all(|row| row["team"] == "shipping"),
        "every copy gets the same label, got {body}"
    );
    // Twenty-four separate calls take several seconds; one takes a fraction
    // of one. The margin is wide enough not to be timing sensitive.
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "identical rows collapsed to one question, took {elapsed:?}"
    );
}
