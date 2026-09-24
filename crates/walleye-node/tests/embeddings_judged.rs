//! Text that might or might not be worth searching by meaning: the judge's
//! half of the embedding decision, with the judge and the embedder as
//! stand-ins so it runs in CI.
//!
//! A one-line sentence is too short for code to call it prose on its own, so
//! it is asked about; a product name is not prose at all and never is.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

mod common;

const TOKEN: &str = "deployment-secret-token";

async fn ingest(app: &Router, source: &str, records: Value) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/ingest/{source}"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(records.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

fn changed(report: &Value, what: &str) -> Option<Value> {
    report["changes"]
        .as_array()?
        .iter()
        .find(|c| c["what"].as_str() == Some(what))
        .cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sentence_that_might_be_a_label_is_asked_about() {
    common::start(common::Services {
        jev: true,
        model: false,
        embeddings: true,
    });
    common::answer_to(
        "stream \"laptop_tickets\"",
        "\"issue\"",
        "embed_",
        common::noul(0.9),
    );
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: d.path().join("cache"),
        memory_bytes: 1024 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: TOKEN.into(),
        bitr: false,
        members: vec![Node::new("n", "http://n", 1.0).unwrap()],
        kubernetes: None,
        processor: None,
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", d.path().display()),
            bitr_url: None,
        }),
    })
    .await
    .unwrap();
    let app = router(service.clone());
    let issues = [
        (
            "Dell XPS 13 Plus",
            "screen flickers when the laptop wakes up",
        ),
        (
            "ThinkPad X1 Carbon",
            "battery drains overnight with the lid closed",
        ),
        (
            "MacBook Air M3",
            "trackpad stops clicking after an hour of use",
        ),
        (
            "Surface Laptop 6",
            "fans run loudly even when nothing is open",
        ),
        (
            "Framework 13",
            "wifi drops every few minutes on the office network",
        ),
    ];
    let records: Vec<Value> = issues
        .iter()
        .enumerate()
        .map(|(n, (product, issue))| {
            json!({"ticket": format!("T-{n}"), "product": product, "issue": issue})
        })
        .collect();
    let report = ingest(&app, "laptop_tickets", Value::Array(records)).await;
    let issue = changed(&report, "embed issue").expect("the issue column is decided");
    assert_eq!(issue["by"], "jev", "a sentence goes to the judge: {report}");
    assert_eq!(issue["chose"], "true", "{report}");
    assert_eq!(report["embedded"], 5, "{report}");
    assert!(
        changed(&report, "embed product").is_none(),
        "a product name is never considered prose, so nobody is asked: {report}"
    );
    assert!(
        common::jev_requests("laptop_tickets")
            .iter()
            .all(|r| !r["questions"]
                .to_string()
                .contains("\"product\" holds free text")),
        "the judge was never asked about the product name"
    );
    service.close().await;
}
