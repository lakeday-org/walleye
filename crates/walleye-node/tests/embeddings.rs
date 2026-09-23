//! Embedding the text worth searching by meaning, end to end.
//!
//! The embedding model here is a stand-in that speaks OpenAI's shape, served
//! in the test, so this runs with no key and can be told to fail. Its vectors
//! are a hash of each text's words, which is enough for a text to be nearest
//! to itself - the property a search has to have.
//!
//! One test, run as a sequence, because the embedding model is configured
//! through the environment and every test in a binary shares it.
use arrow_array::{Array, RecordBatch, StringArray};
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

async fn sql(app: &Router, statement: &str) -> Vec<Value> {
    let (status, body) = post(app, "/v1/query", json!({"sql": statement}).to_string()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{statement}: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).unwrap()
}

/// The nearest rows to a text, by the vector column, through the LanceDB
/// search every client uses.
async fn nearest(app: &Router, table: &str, column: &str, text: &str, k: usize) -> Vec<String> {
    let (status, body) = post(
        app,
        &format!("/v1/table/{table}/query/"),
        json!({"vector": vector(text), "vector_column": column, "k": k,
               "prefilter": true, "version": null})
        .to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(body), None).unwrap();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let mut found = Vec::new();
    for batch in batches {
        let reviews = batch.column_by_name("review").unwrap();
        let reviews = reviews.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..reviews.len() {
            found.push(reviews.value(i).to_owned());
        }
    }
    found
}

fn changed(report: &Value, what: &str) -> Option<Value> {
    report["changes"]
        .as_array()?
        .iter()
        .find(|c| c["what"].as_str() == Some(what))
        .cloned()
}

const REVIEWS: [&str; 5] = [
    "The package arrived three days late and the box was crushed on one side.",
    "Great product, fast shipping, and support answered my question within an hour.",
    "Stopped working after a week, and the replacement had the very same fault again.",
    "Exactly as described, well made, and cheaper than anywhere else I looked at.",
    "The colour was nothing like the photos, so I sent it straight back for a refund.",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn text_worth_searching_by_meaning_is_embedded_and_searchable() {
    // A judge is not what is being tested here, and every column in this test
    // is decided without one: paragraphs are embedded by rule and codes never.
    // SAFETY: the only test in this binary; nothing else reads these.
    unsafe {
        std::env::remove_var("TYPESAFE_API_KEY");
        std::env::remove_var("WALLEYE_EMBEDDING_KEY");
    }
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let records = |range: std::ops::Range<usize>| -> Value {
        Value::Array(
            range
                .map(|n| {
                    json!({"sku": format!("SKU-{n}"), "rating": (n % 5) + 1,
                           "review": REVIEWS[n % REVIEWS.len()]})
                })
                .collect(),
        )
    };

    // --- nothing configured, nothing embedded ----------------------------
    let plain = ingest(&app, "unembedded", records(0..5)).await;
    assert!(
        changed(&plain, "embed review").is_none(),
        "no model, no decision: {plain}"
    );
    assert_eq!(plain["embedded"], 0, "{plain}");

    // --- configured -------------------------------------------------------
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
    unsafe {
        std::env::set_var("WALLEYE_EMBEDDING_URL", &url);
        std::env::set_var("WALLEYE_EMBEDDING_KEY", "test-key");
        std::env::set_var("WALLEYE_EMBEDDING_MODEL", "stand-in-embedder");
    }

    let created = ingest(&app, "reviews", records(0..5)).await;
    let decided = changed(&created, "embed review").expect("the review column is decided");
    assert_eq!(decided["chose"], "true", "{created}");
    assert_eq!(decided["by"], "rule", "paragraphs need no judge: {created}");
    assert!(
        changed(&created, "embed sku").is_none(),
        "codes are never considered prose: {created}"
    );
    assert_eq!(created["embedded"], 5, "{created}");
    assert_eq!(created["unembedded"], 0, "{created}");

    // The vector column sits beside its text, and a search by meaning finds
    // the review nearest to what was asked.
    let found = nearest(&app, "reviews", "review_embedding", REVIEWS[2], 1).await;
    assert_eq!(
        found,
        vec![REVIEWS[2].to_owned()],
        "a text is nearest itself"
    );

    // --- the model goes down: ingestion does not wait for it ---------------
    model.down.store(true, Ordering::SeqCst);
    let during = ingest(&app, "reviews", records(5..8)).await;
    assert_eq!(during["accepted"], 3, "rows land anyway: {during}");
    assert_eq!(
        during["unembedded"], 3,
        "and say they are waiting: {during}"
    );
    assert!(
        changed(&during, "embedding")
            .is_some_and(|c| c["chose"].as_str().unwrap().starts_with("deferred")),
        "{during}"
    );
    assert_eq!(
        sql(
            &app,
            "SELECT count(*) AS n FROM reviews WHERE review_embedding IS NULL"
        )
        .await[0]["n"],
        3
    );

    // --- it comes back: the next request fills the gap ----------------------
    model.down.store(false, Ordering::SeqCst);
    let after = ingest(&app, "reviews", records(8..9)).await;
    assert!(
        after["embedded"].as_u64().unwrap() >= 4,
        "the three waiting rows and the new one: {after}"
    );
    assert_eq!(
        sql(
            &app,
            "SELECT count(*) AS n FROM reviews WHERE review_embedding IS NULL"
        )
        .await[0]["n"],
        0,
        "nothing is left waiting"
    );
    assert_eq!(
        sql(&app, "SELECT count(*) AS n FROM reviews").await[0]["n"],
        9,
        "filling vectors in replaced rows rather than adding them"
    );

    // --- a rebuild keeps the vectors it has ---------------------------------
    let before = model.embedded.load(Ordering::SeqCst);
    let widened = ingest(
        &app,
        "reviews",
        json!([{"sku": "SKU-99", "rating": 4, "review": "Brand new wording that nobody has sent before today.",
                "channel": "web"}]),
    )
    .await;
    assert_eq!(widened["rebuilt"], true, "a new column: {widened}");
    let spent = model.embedded.load(Ordering::SeqCst) - before;
    assert_eq!(
        spent, 1,
        "only the text nobody had embedded was sent, not the whole table again"
    );
    assert_eq!(
        sql(
            &app,
            "SELECT count(*) AS n FROM reviews WHERE review_embedding IS NULL"
        )
        .await[0]["n"],
        0
    );

    // --- and survives a flush to disk ---------------------------------------
    let (status, body) = post(&app, "/v1/table/reviews/flush_lsm/", "{}".into()).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let found = nearest(&app, "reviews", "review_embedding", REVIEWS[4], 1).await;
    assert_eq!(found, vec![REVIEWS[4].to_owned()], "search after a flush");

    server.abort();
    service.close().await;
}
