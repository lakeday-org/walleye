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
    sized(path, 512 * 1024 * 1024)
}
/// Every open stream holds a writer against the budget, so a worker that
/// fans out to several streams needs room for several writers.
fn sized(path: &std::path::Path, memory_bytes: usize) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes,
        disk_bytes: 64 * 1024 * 1024,
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
    seeded_with(config(path), names, values).await
}
async fn seeded_with(config: Config, names: &[&str], values: &[f64]) -> axum::Router {
    let app = router(Service::open(config).await.unwrap());
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

    // The node's own driver runs this view too, woken by every write, so which
    // pass meets the refusal and which delivers is not something to assert.
    // What the endpoint heard is: refused once, the same rows offered again,
    // accepted once. Were the cursor to move on a refusal, those rows would
    // never be offered again and nothing would land.
    for _ in 0..20 {
        if !seen.lock().unwrap().is_empty() {
            break;
        }
        let _ = post(&app, "/v1/view/overheating/refresh/", JSON, b"{}".to_vec()).await;
    }
    assert_eq!(
        refused.load(Ordering::SeqCst),
        2,
        "refused once, then offered again and accepted"
    );
    let delivered = seen.lock().unwrap().clone();
    assert_eq!(delivered.len(), 1, "one delivery landed");
    let rows = delivered[0].as_array().expect("an array of rows");
    assert_eq!(rows.len(), 1, "only the hot sensor: {:?}", rows);
    assert_eq!(rows[0]["sensor"], "b");
    let described = post(&app, "/v1/view/overheating/describe/", JSON, b"{}".to_vec()).await;
    assert!(
        described.1["cursor"].as_u64().unwrap() > 0,
        "consumed once delivered: {}",
        described.1
    );
    let progress = refresh(&app, "overheating").await;
    assert_eq!(
        progress["written"], 0,
        "a watching view writes nothing: {progress}"
    );

    // And a caught-up view stops firing, rather than repeating itself.
    let quiet = refresh(&app, "overheating").await;
    assert_eq!(quiet["delivered"], 0, "{quiet}");
    assert_eq!(seen.lock().unwrap().len(), 1, "no repeat delivery");
    assert_eq!(
        refused.load(Ordering::SeqCst),
        2,
        "and nothing was sent again"
    );
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

/// A worker's heap is part of the machine's memory, not extra to it. A view
/// that asks for more than the node has left is refused before the worker
/// runs, and its batch stays where it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_larger_than_the_budget_is_refused_before_it_runs() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a"], &[1.0]).await;
    defined(
        &app,
        "greedy",
        json!({
            "source": "raw",
            "target": "never",
            "worker": "export default rows => rows",
            // More than the whole node, let alone what is left of it.
            "worker_heap_mb": 4096
        }),
    )
    .await;
    let (status, body) = post(&app, "/v1/view/greedy/refresh/", JSON, b"{}".to_vec()).await;
    assert_ne!(status, StatusCode::OK, "the node cannot afford this worker");
    let said = body.to_string();
    assert!(
        said.contains("worker greedy") || said.to_lowercase().contains("resources"),
        "names the borrower and the budget, got {said}"
    );

    let described = post(&app, "/v1/view/greedy/describe/", JSON, b"{}".to_vec()).await;
    assert_eq!(
        described.1["cursor"].as_u64().unwrap(),
        0,
        "the batch was not consumed: {}",
        described.1
    );
    assert_eq!(
        described.1["worker_heap_mb"], 4096,
        "the view keeps its own sizing"
    );
}

/// A view sizes its own worker, and a modest one runs inside a budget that
/// refuses a large one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_sizes_its_own_worker() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a", "b"], &[1.0, 2.0]).await;
    defined(
        &app,
        "modest",
        json!({
            "source": "raw",
            "target": "doubled",
            "worker": "export default rows => rows.map(r => ({ sensor: r.sensor, \
                       twice: r.celsius * 2 }))",
            "worker_heap_mb": 32,
            "worker_seconds": 5
        }),
    )
    .await;
    let progress = refresh(&app, "modest").await;
    assert_eq!(progress["written"], 2, "{progress}");
    let rows = sql(&app, "SELECT twice FROM doubled ORDER BY twice").await;
    let doubled: Vec<f64> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["twice"].as_f64().unwrap())
        .collect();
    assert_eq!(doubled, vec![2.0, 4.0], "{rows}");
}

/// A worker does not have to hand its rows back to be written somewhere it did
/// not choose. It writes them itself, to whichever stream each row belongs in,
/// and the host commits the lot with the cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_routes_rows_to_the_streams_it_names() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded_with(
        sized(d.path(), 2048 * 1024 * 1024),
        &["a", "b", "c"],
        &[5.0, 95.0, 120.0],
    )
    .await;
    defined(
        &app,
        "router",
        json!({
            "source": "raw",
            "target": "normal",
            "worker": "export default (rows, ctx) => {\
                 const kept = [];\
                 for (const r of rows) {\
                   if (r.celsius > 100) ctx.write('overheating', { sensor: r.sensor, c: r.celsius });\
                   else if (r.celsius > 50) ctx.write('warm', { sensor: r.sensor, c: r.celsius });\
                   else kept.push({ sensor: r.sensor, c: r.celsius });\
                 }\
                 return kept;\
               }"
        }),
    )
    .await;

    let progress = refresh(&app, "router").await;
    assert_eq!(progress["rows"], 3, "{progress}");
    assert_eq!(
        progress["written"], 3,
        "every row landed somewhere: {progress}"
    );

    // Three streams, none of which the view declared except the target.
    for (stream, sensor) in [("normal", "a"), ("warm", "b"), ("overheating", "c")] {
        let rows = sql(&app, &format!("SELECT sensor FROM {stream}")).await;
        let got: Vec<&str> = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["sensor"].as_str().unwrap())
            .collect();
        assert_eq!(
            got,
            vec![sensor],
            "{stream} holds only its own rows: {rows}"
        );
    }
}

/// A worker that writes and returns nothing is still a worker: the rows it
/// wrote are the output, and the view needs no target of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_may_be_the_only_writer() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded_with(
        sized(d.path(), 2048 * 1024 * 1024),
        &["a", "b"],
        &[1.0, 2.0],
    )
    .await;
    defined(
        &app,
        "fanout",
        json!({
            "source": "raw",
            "target": "mirror",
            "worker": "export default (rows, ctx) => {\
                 ctx.write('by_sensor', rows.map(r => ({ sensor: r.sensor })));\
                 ctx.write('audit', { seen: rows.length });\
                 return [];\
               }"
        }),
    )
    .await;
    let progress = refresh(&app, "fanout").await;
    assert_eq!(progress["rows"], 2, "{progress}");
    assert_eq!(
        progress["written"], 3,
        "two rows and one audit line: {progress}"
    );

    let audited = sql(&app, "SELECT seen FROM audit").await;
    assert_eq!(audited.as_array().unwrap()[0]["seen"], 2, "{audited}");
    let counted = sql(&app, "SELECT count(*) AS n FROM by_sensor").await;
    assert_eq!(counted.as_array().unwrap()[0]["n"], 2, "{counted}");
}

/// Writing back into the source would feed a worker its own output for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_may_not_write_into_its_own_source() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a"], &[1.0]).await;
    defined(
        &app,
        "ouroboros",
        json!({
            "source": "raw",
            "target": "out",
            "worker": "export default (rows, ctx) => { ctx.write('raw', rows[0]); return [] }"
        }),
    )
    .await;
    let (status, body) = post(&app, "/v1/view/ouroboros/refresh/", JSON, b"{}".to_vec()).await;
    assert_ne!(status, StatusCode::OK, "a worker that eats its own output");
    assert!(body.to_string().contains("own source"), "{body}");
}

/// Ingest written as a worker. No source stream, no external script: the view
/// runs on a clock, the worker goes and gets its rows, and writes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_fetches_its_own_rows_on_a_schedule() {
    // An upstream the worker will call.
    let served = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&served);
    let upstream = axum::Router::new().route(
        "/readings",
        axum::routing::get(move || {
            let counted = Arc::clone(&counted);
            async move {
                let n = counted.fetch_add(1, Ordering::SeqCst);
                axum::Json(json!({"data": [
                    {"sensor": format!("s{n}"), "celsius": 20.0 + n as f64}
                ]}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, upstream).await;
    });

    // SAFETY: set before the service that reads it starts.
    unsafe { std::env::set_var("WALLEYE_WORKER_FETCH_ALLOW", "127.0.0.1") };
    let d = tempfile::tempdir().unwrap();
    let app = router(
        Service::open(sized(d.path(), 2048 * 1024 * 1024))
            .await
            .unwrap(),
    );

    defined(
        &app,
        "poller",
        json!({
            "every_seconds": 1,
            "target": "readings",
            "worker": format!(
                "export default (rows, ctx) => {{\
                   const answer = ctx.call({{ url: 'http://{address}/readings' }});\
                   if (answer.status !== 200) throw new Error('upstream said ' + answer.status);\
                   const body = JSON.parse(answer.body);\
                   return body.data.map(r => ({{ sensor: r.sensor, celsius: r.celsius }}));\
                 }}"
            )
        }),
    )
    .await;

    let first = refresh(&app, "poller").await;
    assert_eq!(first["written"], 1, "the worker fetched and wrote: {first}");

    // Too soon: the view runs on its own clock, not on demand.
    let soon = refresh(&app, "poller").await;
    assert_eq!(soon["written"], 0, "not due yet: {soon}");

    // Once the interval passes it runs again. Whether this call or the node's
    // own driver gets there first does not matter, and asserting on which one
    // did would be asserting on a race.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut got: Vec<String> = Vec::new();
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let _ = refresh(&app, "poller").await;
        let rows = sql(&app, "SELECT sensor FROM readings ORDER BY sensor").await;
        got = rows
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["sensor"].as_str().unwrap().to_owned())
            .collect();
        if got.len() >= 2 {
            break;
        }
    }
    assert!(
        got.len() >= 2,
        "it polled again on its own clock, got {got:?}"
    );
    assert_eq!(got[0], "s0", "and kept what it had: {got:?}");
    unsafe { std::env::remove_var("WALLEYE_WORKER_FETCH_ALLOW") };
}

/// Reaching out is a capability, not a default. A host nobody allowed is
/// refused by name, inside the worker, where it can be caught.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_may_not_call_a_host_nobody_allowed() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a"], &[1.0]).await;
    defined(
        &app,
        "sneaky",
        json!({
            "source": "raw",
            "target": "out",
            "worker": "export default (rows, ctx) => {\
                 try { ctx.call({ url: 'https://example.invalid/steal' }); }\
                 catch (e) { return [{ sensor: 'refused', celsius: 0, why: String(e.message) }]; }\
                 return [{ sensor: 'reached', celsius: 0, why: 'no' }];\
               }"
        }),
    )
    .await;
    refresh(&app, "sneaky").await;
    let rows = sql(&app, "SELECT sensor, why FROM out").await;
    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["sensor"], "refused", "{rows}");
    assert!(
        row["why"]
            .as_str()
            .unwrap()
            .contains("WALLEYE_WORKER_FETCH_ALLOW"),
        "it names how to allow it: {rows}"
    );
}

/// A webhook. The request reaches a worker, the worker writes what arrived,
/// and the answer only goes out once those rows are durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_answers_an_inbound_request_and_stores_what_it_was_sent() {
    let d = tempfile::tempdir().unwrap();
    let app = router(
        Service::open(sized(d.path(), 2048 * 1024 * 1024))
            .await
            .unwrap(),
    );
    defined(
        &app,
        "hook",
        json!({
            "every_seconds": 3600,
            "target": "ignored",
            "worker": "export default { fetch(request, ctx) {\
                 const event = JSON.parse(request.body);\
                 ctx.write('events', { kind: event.kind, at: request.headers['x-sent-at'] ?? '' });\
                 return { status: 202, headers: { 'x-handled-by': 'hook' }, body: 'stored' };\
               } }"
        }),
    )
    .await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/worker/hook/")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .header("x-sent-at", "noon")
                .body(Body::from(json!({"kind":"ping"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::ACCEPTED,
        "the worker chose 202"
    );
    assert_eq!(
        response.headers().get("x-handled-by").unwrap(),
        "hook",
        "and its own header"
    );
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&bytes), "stored");

    // The rows were durable before the answer went out.
    let rows = sql(&app, "SELECT kind, at FROM events").await;
    let row = &rows.as_array().unwrap()[0];
    assert_eq!(row["kind"], "ping", "{rows}");
    assert_eq!(row["at"], "noon", "it saw the header: {rows}");
}

/// The deployment token is the node's business. A worker that could read it
/// could use it, so it never sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_inbound_request_does_not_carry_the_token_into_the_worker() {
    let d = tempfile::tempdir().unwrap();
    let app = router(
        Service::open(sized(d.path(), 2048 * 1024 * 1024))
            .await
            .unwrap(),
    );
    defined(
        &app,
        "peek",
        json!({
            "every_seconds": 3600,
            "target": "ignored",
            "worker": "export default { fetch(request) {\
                 return { status: 200, body: JSON.stringify(Object.keys(request.headers)) };\
               } }"
        }),
    )
    .await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/worker/peek/")
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let seen = String::from_utf8_lossy(&bytes).to_lowercase();
    assert!(
        !seen.contains("authorization"),
        "the token stayed out: {seen}"
    );
    assert!(
        seen.contains("content-type"),
        "ordinary headers got through: {seen}"
    );
}

/// A cron schedule is accepted and refused on its own terms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_may_run_on_a_cron() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a"], &[1.0]).await;
    defined(
        &app,
        "hourly",
        json!({
            "cron": "0 * * * *",
            "target": "hourly_rows",
            "worker": "export default () => [{ ran: 1 }]"
        }),
    )
    .await;
    let described = post(&app, "/v1/view/hourly/describe/", JSON, b"{}".to_vec()).await;
    assert_eq!(described.1["cron"], "0 * * * *", "{:?}", described.1);

    // Declaring it does not fire it: the first run is at the next named time.
    let progress = refresh(&app, "hourly").await;
    assert_eq!(progress["written"], 0, "not due yet: {progress}");

    let (status, body) = define(
        &app,
        "nonsense",
        json!({"cron": "0 25 * * *", "target": "never", "worker": "export default () => []"}),
    )
    .await;
    assert_ne!(status, StatusCode::OK, "there is no hour 25");
    assert!(body.to_string().contains("hour"), "{body}");

    let (status, body) = define(
        &app,
        "both",
        json!({"cron": "0 * * * *", "every_seconds": 60, "target": "never",
               "worker": "export default () => []"}),
    )
    .await;
    assert_ne!(status, StatusCode::OK, "one clock or the other");
    assert!(body.to_string().contains("not both"), "{body}");
}

/// A socket the node holds open. Frames arrive, a worker reads them in
/// bounded batches, and what it writes lands. The connection outlives any one
/// worker turn, which is the point: an isolate is a turn, a stream is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_reads_a_socket_the_node_holds_open() {
    use futures::SinkExt;
    // A server that greets whoever subscribes, then sends three ticks.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut stream) = tokio_tungstenite::accept_async(socket).await else {
                    return;
                };
                use futures::StreamExt;
                // Wait for the subscription before sending anything.
                let opening = stream.next().await;
                let subscribed = matches!(
                    opening,
                    Some(Ok(tokio_tungstenite::tungstenite::Message::Text(ref t)))
                        if t.contains("ticks")
                );
                if !subscribed {
                    return;
                }
                for n in 0..3 {
                    let frame = json!({"seq": n, "px": 100.0 + n as f64}).to_string();
                    if stream
                        .send(tokio_tungstenite::tungstenite::Message::Text(frame.into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            });
        }
    });

    let d = tempfile::tempdir().unwrap();
    let app = router(
        Service::open(sized(d.path(), 2048 * 1024 * 1024))
            .await
            .unwrap(),
    );
    defined(
        &app,
        "feed",
        json!({
            "websocket": {
                "url": format!("ws://{address}/"),
                "subscribe": "{\"channel\":\"ticks\"}",
                "frames": 8,
                "window_ms": 300
            },
            "target": "ticks",
            "worker": "export default (rows) => rows.map(r => {\
                 const frame = JSON.parse(r.data);\
                 return { seq: frame.seq, px: frame.px };\
               })"
        }),
    )
    .await;

    // The node connects on its own, so all the test does is wait for rows.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(40);
    let mut got = 0u64;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let counted = post(
            &app,
            "/v1/query",
            JSON,
            json!({"sql": "SELECT count(*) AS n FROM ticks"})
                .to_string()
                .into_bytes(),
        )
        .await;
        if counted.0 == StatusCode::OK
            && let Some(row) = counted.1.as_array().and_then(|rows| rows.first())
            && let Some(n) = row["n"].as_u64()
        {
            got = n;
            if got >= 3 {
                break;
            }
        }
    }
    assert_eq!(got, 3, "every frame reached the stream");

    let rows = sql(&app, "SELECT seq, px FROM ticks ORDER BY seq").await;
    let seqs: Vec<i64> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(
        seqs,
        vec![0, 1, 2],
        "in order, parsed by the worker: {rows}"
    );
}

/// A socket view is its own source, and says so rather than accepting a
/// definition that cannot mean anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_socket_view_is_checked_when_it_is_declared() {
    let d = tempfile::tempdir().unwrap();
    let app = seeded(d.path(), &["a"], &[1.0]).await;
    for (bad, wanted) in [
        (
            json!({"websocket": {"url": "https://example.test/"}, "target": "x",
                   "worker": "export default r => r"}),
            "ws or wss",
        ),
        (
            json!({"websocket": {"url": "wss://example.test/"}, "source": "raw", "target": "x",
                   "worker": "export default r => r"}),
            "no source",
        ),
        (
            json!({"websocket": {"url": "wss://example.test/"}, "target": "x",
                   "sql": "SELECT 1"}),
            "needs a worker",
        ),
    ] {
        let (status, body) = define(&app, "bad", bad).await;
        assert_ne!(status, StatusCode::OK, "{wanted}");
        assert!(body.to_string().contains(wanted), "{wanted}: {body}");
    }
}

/// Two passes over one view at once - the node's own processor and a caller's
/// refresh, say - must not both deliver the same rows. Each read the cursor
/// before either moved it, so an alert fired once per pass rather than once
/// per row: at least once had quietly become at least twice.
///
/// The endpoint takes its time answering, which holds the window open that the
/// passes used to race through, so this fails every time rather than now and
/// then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_passes_at_once_deliver_each_row_once() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let recorded = Arc::clone(&seen);
    let endpoint = axum::Router::new().route(
        "/fire",
        axum::routing::post(move |body: String| {
            let recorded = Arc::clone(&recorded);
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
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
        "overheating_twice",
        json!({
            "source": "raw",
            "sql": "SELECT sensor, celsius FROM raw WHERE celsius > 90",
            "alert": {"url": format!("http://{address}/fire")}
        }),
    )
    .await;

    let (first, second) = tokio::join!(
        refresh(&app, "overheating_twice"),
        refresh(&app, "overheating_twice")
    );
    let delivered = first["delivered"].as_u64().unwrap() + second["delivered"].as_u64().unwrap();
    assert_eq!(
        delivered, 1,
        "one pass delivered, the other found nothing: {first} {second}"
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "the endpoint heard once");
}
