//! Embedding text that might or might not be worth searching: the judge's
//! half of the decision, against the real decision service.
//!
//! The embedding model here is a stand-in that speaks OpenAI's shape, served
//! in the test, so this runs with no key and can be told to fail. Its vectors
//! are a hash of each text's words, which is enough for a text to be nearest
//! to itself - the property a search has to have.
//!
//! One test, run as a sequence, because the embedding model is configured
//! through the environment and every test in a binary shares it.
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    routing,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";
const WIDTH: usize = 16;

#[derive(Default)]
struct Model {
    down: AtomicBool,
    embedded: AtomicUsize,
}

/// A vector for a text: its words hashed into a fixed width, normalised.
fn vector(text: &str) -> Vec<f32> {
    use std::hash::{Hash, Hasher};
    let mut v = [0f32; WIDTH];
    for word in text.split_whitespace() {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        word.to_lowercase().hash(&mut h);
        v[(h.finish() as usize) % WIDTH] += 1.0;
    }
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
    v.iter().map(|x| x / norm).collect()
}

async fn embeddings(
    State(model): State<Arc<Model>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if headers.get("authorization").and_then(|h| h.to_str().ok()) != Some("Bearer test-key") {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "bad key"})));
    }
    if model.down.load(Ordering::SeqCst) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "overloaded"})),
        );
    }
    let input: Vec<String> = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t.as_str().unwrap().to_owned())
        .collect();
    model.embedded.fetch_add(input.len(), Ordering::SeqCst);
    let data: Vec<Value> = input
        .iter()
        .enumerate()
        .map(|(index, text)| json!({"object": "embedding", "index": index, "embedding": vector(text)}))
        .collect();
    (
        StatusCode::OK,
        Json(json!({"object": "list", "data": data, "model": body["model"]})),
    )
}

async fn node(dir: &std::path::Path) -> (Arc<Service>, Router) {
    let service = Service::open(Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: dir.join("cache"),
        memory_bytes: 512 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: TOKEN.into(),
        bitr: false,
        members: vec![Node::new("n", "http://n", 1.0).unwrap()],
        kubernetes: None,
        processor: None,
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", dir.display()),
            bitr_url: None,
        }),
    })
    .await
    .unwrap();
    let app = router(service.clone());
    (service, app)
}

async fn post(app: &Router, uri: &str, body: String) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn ingest(app: &Router, source: &str, records: Value) -> Value {
    let (status, body) = post(app, &format!("/v1/ingest/{source}"), records.to_string()).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

fn changed(report: &Value, what: &str) -> Option<Value> {
    report["changes"]
        .as_array()?
        .iter()
        .find(|c| c["what"].as_str() == Some(what))
        .cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sentence_that_might_be_a_label_is_judged() {
    if !std::env::var("TYPESAFE_API_KEY").is_ok_and(|k| !k.trim().is_empty()) {
        eprintln!("skipping: no decision service key configured");
        return;
    }
    let model = Arc::new(Model::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1/embeddings", listener.local_addr().unwrap());
    let server = tokio::spawn({
        let app = Router::new()
            .route("/v1/embeddings", routing::post(embeddings))
            .with_state(model.clone());
        async move {
            let _ = axum::serve(listener, app).await;
        }
    });
    // SAFETY: the only test in this binary; nothing else reads these.
    unsafe {
        std::env::set_var("WALLEYE_EMBEDDING_URL", &url);
        std::env::set_var("WALLEYE_EMBEDDING_KEY", "test-key");
        std::env::set_var("WALLEYE_EMBEDDING_MODEL", "stand-in-embedder");
    }
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    // Short enough that code cannot call either one prose on its own.
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
        .map(|(n, (product, issue))| json!({"ticket": format!("T-{n}"), "product": product, "issue": issue}))
        .collect();
    let report = ingest(&app, "laptop_tickets", Value::Array(records)).await;
    let issue = changed(&report, "embed issue").expect("the issue column is decided");
    assert_eq!(issue["by"], "jev", "a sentence goes to the judge: {report}");
    assert_eq!(
        issue["chose"], "true",
        "a problem described in words is searched by meaning: {report}"
    );
    assert_eq!(report["embedded"], 5, "{report}");
    assert!(
        changed(&report, "embed product").is_none_or(|c| c["chose"] == "false"),
        "a product name is not something to search by meaning: {report}"
    );
    server.abort();
    service.close().await;
}
