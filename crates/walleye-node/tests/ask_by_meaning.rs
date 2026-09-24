//! A question asked in words, answered by what text means, end to end - with
//! the model and the embedder as stand-ins, so this runs in CI.
//!
//! What it proves is the plumbing: ingest embeds the reviews, the model is
//! told which vector holds the meaning of which text, and the statement it
//! drafts runs `embed` and `cosine_distance` and comes back ranked. The draft
//! is scripted against the hint itself, so a model that was not told about
//! the vectors gets no statement and the test fails. How well a real model
//! uses the hint is `live.rs`'s question, run on demand.
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

async fn post(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
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
    let bytes = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_question_about_meaning_is_answered_by_meaning() {
    common::start(common::Services {
        jev: false,
        model: true,
        embeddings: true,
    });
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: d.path().join("cache"),
        // Four tables open at once - two of them the ingest path's own bronze
        // and quarantine - and a vector graph: more than 512 MiB allows.
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

    let reviews = [
        "The courier left it in the rain and the cardboard had turned to mush, the corner of the case was cracked.",
        "Great product, fast shipping, and support answered my question within an hour of asking.",
        "Stopped working after a week, and the replacement they sent had the very same fault again.",
        "Exactly as described, well made, and cheaper than anywhere else I looked at this month.",
        "The colour was nothing like the photos on the site, so I sent it straight back for a refund.",
    ];
    let records: Vec<Value> = reviews
        .iter()
        .enumerate()
        .map(|(n, r)| json!({"order": format!("o{n}"), "stars": (n % 5) + 1, "review": r}))
        .collect();
    let (status, report) = post(&app, "/v1/ingest/shop_reviews", Value::Array(records)).await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["embedded"], 5, "the reviews were embedded: {report}");

    // Only a prompt that carries the hint gets a statement back.
    let hint = r#"\"review_embedding\" holds the meaning of \"review\""#;
    common::draft(
        hint,
        "SELECT \"review\" FROM \"shop_reviews\" \
         ORDER BY cosine_distance(\"review_embedding\", embed('courier left it in the rain')) \
         LIMIT 1",
    );
    let (status, answer) = post(
        &app,
        "/v1/query",
        json!({"text": "which review is about something damaged in delivery?"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(
        common::model_prompts(hint).len(),
        1,
        "the model was told which vector holds the meaning of which text"
    );
    let sql = answer["sql"].as_str().unwrap_or_default();
    assert!(sql.contains("cosine_distance"), "{sql}");
    assert_eq!(
        answer["rows"][0]["review"].as_str().unwrap_or_default(),
        reviews[0],
        "embed and cosine_distance ran, and ranked: {answer}"
    );
    service.close().await;
}
