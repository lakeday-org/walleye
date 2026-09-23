//! A question asked in words, answered by what text means rather than the
//! words it uses, end to end: real embeddings on the way in, and the real
//! text-to-SQL model on the way out.
//!
//! Skips without an OpenAI key. It is the one test that shows the pieces
//! meet: ingest embeds the reviews, the model is told the vectors exist and
//! how to use them, and the SQL it writes finds a review that shares no words
//! with the question.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

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
    let Some(key) = std::env::var("OPENAI_API_KEY")
        .ok()
        .filter(|k| !k.trim().is_empty())
    else {
        eprintln!("skipping: no OpenAI key configured");
        return;
    };
    // SAFETY: the only test in this binary.
    unsafe {
        std::env::set_var("WALLEYE_EMBEDDING_KEY", &key);
        std::env::remove_var("WALLEYE_EMBEDDING_URL");
        std::env::set_var("WALLEYE_EMBEDDING_MODEL", "text-embedding-3-small");
        if std::env::var("WALLEYE_MODEL_KEY").is_err() {
            std::env::set_var("WALLEYE_MODEL_KEY", &key);
        }
    }
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

    // No review says "damaged" or "packaging". The first one means it.
    let (status, answer) = post(
        &app,
        "/v1/query",
        json!({"text": "which review is about something damaged in delivery? just the top one"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let sql = answer["sql"].as_str().unwrap_or_default();
    eprintln!("SQL {sql}");
    assert!(
        sql.contains("cosine_distance") && sql.contains("embed("),
        "the model searched by meaning rather than by words: {sql}"
    );
    let first = answer["rows"][0]["review"].as_str().unwrap_or_default();
    assert_eq!(first, reviews[0], "the review that means it: {answer}");
    service.close().await;
}
