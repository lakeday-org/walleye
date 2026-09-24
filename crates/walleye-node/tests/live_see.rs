//! Against the real services: whether real Jev, composing through
//! json-render in the V8 worker, draws what was asked the way a person would
//! want it drawn. An evaluation, not a test of the code - `see.rs` tests the
//! code against stand-ins - so it is ignored by default and run on demand,
//! with `TYPESAFE_API_KEY` and `OPENAI_API_KEY` set:
//!
//!     cargo test -p walleye-node --test live_see -- --ignored --nocapture
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

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(body.map(|b| Body::from(b.to_string())).unwrap_or_default())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, value)
}

fn kinds(drawn: &Value) -> Vec<String> {
    let spec = &drawn["spec"];
    let mut out = Vec::new();
    let mut stack = vec![spec["root"].as_str().unwrap().to_owned()];
    while let Some(id) = stack.pop() {
        let element = &spec["elements"][&id];
        out.push(format!(
            "{}({})",
            element["type"].as_str().unwrap(),
            element["props"]["title"].as_str().unwrap_or("")
        ));
        for child in element["children"].as_array().into_iter().flatten().rev() {
            stack.push(child.as_str().unwrap().to_owned());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "calls the real Jev and OpenAI services; run with --ignored to evaluate"]
async fn real_jev_draws_what_was_asked() {
    assert!(
        std::env::var("TYPESAFE_API_KEY").is_ok(),
        "set TYPESAFE_API_KEY to run the live evaluation"
    );
    let key = std::env::var("OPENAI_API_KEY").expect("set OPENAI_API_KEY");
    // SAFETY: the only test in this binary.
    unsafe {
        if std::env::var("WALLEYE_MODEL_KEY").is_err() {
            std::env::set_var("WALLEYE_MODEL_KEY", &key);
        }
    }
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
        lease: Default::default(),
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", d.path().display()),
            bitr_url: None,
        }),
    })
    .await
    .unwrap();
    let app = router(service);

    let definition = json!({"name": "orders", "columns": [
        {"name": "id", "type": "int64"},
        {"name": "placed_at", "type": "timestamp"},
        {"name": "country", "type": "string"},
        {"name": "channel", "type": "string"},
        {"name": "total", "type": "float64"},
    ], "primary_key": ["id"]});
    assert_eq!(
        call(&app, "POST", "/v1/streams", Some(definition)).await.0,
        StatusCode::OK
    );
    let countries = ["US", "US", "US", "DE", "FR", "GB", "US", "DE"];
    let channels = ["web", "web", "app", "store"];
    let rows: Vec<Value> = (0..600u32)
        .map(|i| {
            json!({
                "id": i,
                "placed_at": format!("2026-08-{:02}T{:02}:{:02}:00Z", 1 + i / 20, i % 24, i % 60),
                "country": countries[(i * 7 % 8) as usize],
                "channel": channels[(i * 3 % 4) as usize],
                "total": 20.0 + ((i * 37) % 180) as f64,
            })
        })
        .collect();
    let (status, body) = call(
        &app,
        "POST",
        "/v1/streams/orders/events",
        Some(json!({"rows": rows})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    for question in [
        "How much revenue did we make each day?",
        "What share of orders comes from each channel?",
        "What was our total revenue?",
        "Which countries order the most?",
    ] {
        let started = std::time::Instant::now();
        let (status, drawn) =
            call(&app, "POST", "/v1/see", Some(json!({"question": question}))).await;
        assert_eq!(status, StatusCode::OK, "{drawn}");
        println!(
            "{question}\n  sql: {}\n  drawn by {} as {:?} in {} ms",
            drawn["panels"][0]["sql"].as_str().unwrap_or(""),
            drawn["composed_by"],
            kinds(&drawn),
            started.elapsed().as_millis()
        );
        assert_eq!(drawn["composed_by"], "jev", "{drawn}");
    }

    let started = std::time::Instant::now();
    let (status, drawn) = call(&app, "GET", "/v1/see/tables/orders", None).await;
    assert_eq!(status, StatusCode::OK, "{drawn}");
    println!(
        "the orders table's own dashboard, drawn by {} in {} ms:\n  {:?}",
        drawn["composed_by"],
        started.elapsed().as_millis(),
        kinds(&drawn)
    );
    assert_eq!(drawn["composed_by"], "jev", "{drawn}");

    let started = std::time::Instant::now();
    let (_, again) = call(&app, "GET", "/v1/see/tables/orders", None).await;
    println!(
        "drawn again from the saved spec in {} ms",
        started.elapsed().as_millis()
    );
    assert_eq!(again["spec"]["elements"], drawn["spec"]["elements"]);

    let (status, made) = call(
        &app,
        "PUT",
        "/v1/dashboards/sales",
        Some(json!({"questions": ["How much revenue did we make each day?"]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{made}");
    let started = std::time::Instant::now();
    let (status, changed) = call(
        &app,
        "POST",
        "/v1/dashboards/sales/chat",
        Some(json!({"message": "Also show revenue by country, and put it first"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{changed}");
    println!(
        "chat: {} in {} ms, drawn by {}: {:?}",
        changed["did"],
        started.elapsed().as_millis(),
        changed["dashboard"]["composed_by"],
        kinds(&changed["dashboard"])
    );
    assert_eq!(changed["dashboard"]["panels"].as_array().unwrap().len(), 2);
}
