//! Pipelines that run themselves, transforms written in JavaScript, and rows
//! that leave the node when something is worth telling somebody about.
use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";
const ARROW: &str = "application/vnd.apache.arrow.stream";
const JSON: &str = "application/json";

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

fn readings(names: &[&str], values: &[f64]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sensor", DataType::Utf8, true),
        Field::new("celsius", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(values.to_vec())) as ArrayRef,
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
async fn define(app: &axum::Router, name: &str, definition: Value) -> (StatusCode, Value) {
    post(
        app,
        &format!("/v1/view/{name}/create/"),
        JSON,
        definition.to_string().into_bytes(),
    )
    .await
}
async fn defined(app: &axum::Router, name: &str, definition: Value) {
    let (status, body) = define(app, name, definition).await;
    assert_eq!(status, StatusCode::OK, "create view {name}: {body}");
}
async fn refresh(app: &axum::Router, name: &str) -> Value {
    let (status, body) = post(
        app,
        &format!("/v1/view/{name}/refresh/?passes=20"),
        JSON,
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refresh {name}: {body}");
    body
}

async fn seeded(path: &std::path::Path, names: &[&str], values: &[f64]) -> axum::Router {
    let app = router(Service::open(config(path)).await.unwrap());
    let (status, body) = post(
        &app,
        "/v1/table/raw/create/?mode=create",
        ARROW,
        readings(names, values),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create raw: {body}");
    app
}

/// A tier written in JavaScript. The worker sees rows as plain objects and
/// returns the rows to write, so a transform that is awkward in SQL does not
/// have to be written in SQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_transforms_a_tier() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a", "b", "c"], &[0.0, 100.0, 37.0]).await;
    let (status, body) = define(
        &app,
        "fahrenheit",
        json!({
            "source": "raw",
            "target": "converted",
            "worker": "export default (rows) => rows.map(r => ({ \
                          sensor: r.sensor, \
                          f: Math.round(r.celsius * 9 / 5 + 32) \
                       }))"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let progress = refresh(&app, "fahrenheit").await;
    assert_eq!(progress["written"], 3, "{progress}");

    let rows = sql(&app, "SELECT sensor, f FROM converted ORDER BY sensor").await;
    let got: Vec<(&str, f64)> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (row["sensor"].as_str().unwrap(), row["f"].as_f64().unwrap()))
        .collect();
    assert_eq!(got, vec![("a", 32.0), ("b", 212.0), ("c", 99.0)], "{rows}");
}

/// A worker may drop rows, which is what makes it a filter as well as a map.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_may_keep_only_some_rows() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a", "b", "c"], &[10.0, 95.0, 20.0]).await;
    define(
        &app,
        "hot",
        json!({
            "source": "raw",
            "target": "hot_rows",
            "worker": "export default rows => rows.filter(r => r.celsius > 90)"
        }),
    )
    .await;
    let progress = refresh(&app, "hot").await;
    assert_eq!(progress["rows"], 3, "{progress}");
    assert_eq!(progress["written"], 1, "{progress}");
    let counted = sql(&app, "SELECT count(*) AS n FROM hot_rows").await;
    assert_eq!(counted.as_array().unwrap()[0]["n"], 1, "{counted}");
}

/// A worker that will not return is stopped, and the view says so instead of
/// hanging the node. The cursor does not move, so the batch is still there
/// once somebody fixes the worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_that_never_returns_fails_its_view_and_keeps_the_batch() {
    let d = tempfile::tempdir().unwrap();
    // SAFETY: this test process sets the limit before the worker runs.
    unsafe { std::env::set_var("WALLEYE_WORKER_SECONDS", "1") };
    let app = seeded(d.path(), &["a"], &[1.0]).await;
    define(
        &app,
        "stuck",
        json!({
            "source": "raw",
            "target": "never",
            "worker": "export default () => { while (true) {} }"
        }),
    )
    .await;
    let (status, body) = post(&app, "/v1/view/stuck/refresh/", JSON, b"{}".to_vec()).await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a worker that hangs is not a success"
    );
    assert!(body.to_string().contains("stopped"), "{body}");

    let described = post(&app, "/v1/view/stuck/describe/", JSON, b"{}".to_vec()).await;
    assert_eq!(
        described.1["cursor"].as_u64().unwrap(),
        0,
        "the batch was not consumed: {}",
        described.1
    );
    unsafe { std::env::remove_var("WALLEYE_WORKER_SECONDS") };
}

/// Rows worth telling somebody about leave the node. Delivery happens before
/// the cursor moves, so an endpoint that was down gets the rows on the next
/// pass rather than never.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_alert_delivers_rows_and_retries_until_it_lands() {
    let refused = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let counted = Arc::clone(&refused);
    let recorded = Arc::clone(&seen);

    // An endpoint that refuses the first delivery and accepts the second.
    let endpoint = axum::Router::new().route(
        "/fire",
        axum::routing::post(move |body: String| {
            let counted = Arc::clone(&counted);
            let recorded = Arc::clone(&recorded);
            async move {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    return StatusCode::SERVICE_UNAVAILABLE;
                }
                let rows: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                recorded.lock().unwrap().push(rows);
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, endpoint).await;
    });

    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a", "b"], &[20.0, 99.0]).await;
    define(
        &app,
        "overheating",
        json!({
            "source": "raw",
            "sql": "SELECT sensor, celsius FROM raw WHERE celsius > 90",
            "alert": {"url": format!("http://{address}/fire")}
        }),
    )
    .await;

    // The endpoint refuses, so the pass fails and nothing is consumed.
    let (status, body) = post(&app, "/v1/view/overheating/refresh/", JSON, b"{}".to_vec()).await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a refused delivery is not a success"
    );
    assert!(body.to_string().contains("503"), "{body}");
    let described = post(&app, "/v1/view/overheating/describe/", JSON, b"{}".to_vec()).await;
    assert_eq!(
        described.1["cursor"].as_u64().unwrap(),
        0,
        "nothing consumed"
    );

    // On the next pass the endpoint accepts, and the same rows arrive.
    let progress = refresh(&app, "overheating").await;
    assert_eq!(progress["delivered"], 1, "{progress}");
    assert_eq!(
        progress["written"], 0,
        "a watching view writes nothing: {progress}"
    );
    let delivered = seen.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1, "one delivery landed");
    let rows = delivered[0].as_array().expect("an array of rows");
    assert_eq!(rows.len(), 1, "only the hot sensor: {:?}", rows);
    assert_eq!(rows[0]["sensor"], "b");

    // And a caught-up view stops firing, rather than repeating itself.
    let quiet = refresh(&app, "overheating").await;
    assert_eq!(quiet["delivered"], 0, "{quiet}");
    assert_eq!(seen.lock().unwrap().len(), 1, "no repeat delivery");
}

/// A view must produce something. One that neither writes nor tells anyone is
/// a pipeline with no output.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_that_goes_nowhere_is_refused() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a"], &[1.0]).await;
    let (status, body) = define(
        &app,
        "pointless",
        json!({"source": "raw", "sql": "SELECT sensor FROM raw"}),
    )
    .await;
    assert_ne!(status, StatusCode::OK, "it produces nothing");
    assert!(body.to_string().contains("target"), "{body}");

    let (status, body) = define(&app, "empty", json!({"source": "raw", "target": "out"})).await;
    assert_ne!(status, StatusCode::OK, "it does nothing");
    assert!(body.to_string().contains("worker"), "{body}");
}

/// Nobody calls refresh. A pipeline catches up on its own, and a chain of
/// tiers drains end to end because a pass repeats while anything moved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pipeline_runs_itself() {
    let d = tempfile::tempdir().unwrap();
    // A short idle tick so the test does not wait on the production cadence.
    // SAFETY: set before the service that reads it starts.
    unsafe { std::env::set_var("WALLEYE_VIEW_IDLE_SECONDS", "1") };
    let app = seeded(d.path(), &["a", "b", "c"], &[10.0, 95.0, 99.0]).await;

    defined(
        &app,
        "converted",
        json!({
            "source": "raw",
            "target": "converted_rows",
            "worker": "export default rows => rows.map(r => ({ sensor: r.sensor, \
                       f: Math.round(r.celsius * 9 / 5 + 32) }))"
        }),
    )
    .await;
    defined(
        &app,
        "hot",
        json!({
            "source": "converted_rows",
            "target": "hot_rows",
            "sql": "SELECT sensor, f FROM converted_rows WHERE f > 200"
        }),
    )
    .await;

    // The second tier's source does not exist until the first tier writes it,
    // so this also proves a chain can be declared before it has run.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut hot = 0;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let counted = post(
            &app,
            "/v1/query",
            JSON,
            json!({"sql": "SELECT count(*) AS n FROM hot_rows"})
                .to_string()
                .into_bytes(),
        )
        .await;
        if counted.0 == StatusCode::OK
            && let Some(n) = counted.1.as_array().and_then(|rows| rows.first())
            && let Some(n) = n["n"].as_u64()
        {
            hot = n;
            if hot == 2 {
                break;
            }
        }
    }
    assert_eq!(hot, 2, "both hot sensors reached the last tier unaided");
    unsafe { std::env::remove_var("WALLEYE_VIEW_IDLE_SECONDS") };
}
