//! Seeing data, end to end, with Jev and the drafting model as stand-ins.
//!
//! What runs for real: the node, its queries, and json-render's composer in
//! a V8 worker asking the stand-in Jev its questions. So these prove that a
//! choice Jev makes is the one drawn, that the engine's own choice is used
//! when Jev gives none, that a saved dashboard is redrawn from fresh rows
//! without asking Jev anything, and that a table gets a dashboard of its own.
//! How well a real Jev chooses is `live.rs`'s question.
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

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(match body {
            Some(body) => Body::from(body.to_string()),
            None => Body::empty(),
        })
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

async fn node(dir: &std::path::Path) -> Router {
    common::start(common::Services {
        jev: true,
        model: true,
        embeddings: false,
    });
    let service = Service::open(Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: dir.join("cache"),
        memory_bytes: 1024 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: TOKEN.into(),
        bitr: false,
        members: vec![Node::new("n", "http://n", 1.0).unwrap()],
        kubernetes: None,
        lease: Default::default(),
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", dir.display()),
            bitr_url: None,
        }),
    })
    .await
    .unwrap();
    router(service)
}

/// A table of signups: when, on which plan, paying how much.
async fn signups(app: &Router, table: &str, days: u32) {
    let definition = json!({"name": table, "columns": [
        {"name": "id", "type": "int64"},
        {"name": "at", "type": "timestamp"},
        {"name": "plan", "type": "string"},
        {"name": "paid", "type": "float64"},
    ], "primary_key": ["id"]});
    let (status, body) = call(app, "POST", "/v1/streams", Some(definition)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    add(app, table, 0, days).await;
}

async fn add(app: &Router, table: &str, from: u32, days: u32) {
    let plans = ["free", "free", "free", "pro", "team"];
    let rows: Vec<Value> = (from..from + days * 5)
        .map(|i| {
            json!({
                "id": i,
                "at": format!("2026-09-{:02}T{:02}:00:00Z", 1 + i / 5, 8 + i % 5),
                "plan": plans[(i % 5) as usize],
                "paid": if plans[(i % 5) as usize] == "free" { 0.0 } else { 10.0 + i as f64 },
            })
        })
        .collect();
    let (status, body) = call(
        app,
        "POST",
        &format!("/v1/streams/{table}/events"),
        Some(json!({"rows": rows})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

fn root(drawn: &Value) -> &Value {
    let spec = &drawn["spec"];
    &spec["elements"][spec["root"].as_str().unwrap()]
}

/// The one panel a single question is drawn as, inside the page's grid.
fn only_panel(drawn: &Value) -> &Value {
    let children = root(drawn)["children"].as_array().unwrap();
    assert_eq!(children.len(), 1, "one panel: {drawn}");
    &drawn["spec"]["elements"][children[0].as_str().unwrap()]
}

/// A question is answered in SQL, and drawn the way Jev chose - not the way
/// the engine would have. Unscripted, the same question is drawn the
/// engine's way, and says so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_question_is_drawn_as_jev_chooses() {
    let d = tempfile::tempdir().unwrap();
    let app = node(d.path()).await;
    signups(&app, "chosen_signups", 5).await;
    let asked = "How many chosen signups came in each day, drawn as bars?";
    common::draft(
        "How many chosen signups came in each day",
        "SELECT date_trunc('day', at) AS day, count(*) AS signups FROM chosen_signups \
         GROUP BY 1 ORDER BY 1",
    );
    // The page is a grid; the engine's first choice for a count over time
    // is a line, and Jev, asked for bars, picks bars.
    common::answer(
        "chosen signups came in each day",
        "root",
        common::choice("grid", 0.97, &[]),
    );
    // The composer asks about each group of candidates in turn: the grid
    // again, which it does not need inside itself, then the panel.
    common::answer(
        "chosen signups came in each day",
        "select_0",
        common::choice("omit", 0.95, &[]),
    );
    common::answer(
        "chosen signups came in each day",
        "select_1",
        common::choice("use:q0-bars_over_time", 0.93, &[]),
    );
    let (status, drawn) = call(&app, "POST", "/v1/see", Some(json!({"question": asked}))).await;
    assert_eq!(status, StatusCode::OK, "{drawn}");
    let asked_jev: Vec<Vec<String>> = common::jev_requests("chosen signups came in each day")
        .iter()
        .map(|r| {
            r["questions"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect()
        })
        .collect();
    assert_eq!(drawn["composed_by"], "jev", "{asked_jev:?}");
    assert_eq!(only_panel(&drawn)["type"], "BarChart", "{drawn}");
    assert_eq!(only_panel(&drawn)["props"]["x"], "day");
    assert_eq!(
        only_panel(&drawn)["props"]["data"],
        json!({"$state": "/q0"})
    );
    let rows = drawn["spec"]["state"]["q0"].as_array().unwrap();
    assert_eq!(rows.len(), 5, "one bar a day: {drawn}");
    assert!(rows.iter().all(|r| r["signups"] == 5));
    assert!(
        !common::jev_requests("chosen signups came in each day").is_empty(),
        "the composer asked Jev"
    );

    let plain = "How many plain signups came in each day?";
    common::draft(
        "How many plain signups came in each day",
        "SELECT date_trunc('day', at) AS day, count(*) AS signups FROM chosen_signups \
         GROUP BY 1 ORDER BY 1",
    );
    let (status, drawn) = call(&app, "POST", "/v1/see", Some(json!({"question": plain}))).await;
    assert_eq!(status, StatusCode::OK, "{drawn}");
    assert_eq!(drawn["composed_by"], "rules", "{drawn}");
    assert_eq!(only_panel(&drawn)["type"], "LineChart", "{drawn}");
}

/// A saved dashboard is drawn from its queries' rows as they are now, and
/// drawing it asks Jev nothing: composing is paid once, when it is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saved_dashboard_is_redrawn_from_fresh_rows_without_jev() {
    let d = tempfile::tempdir().unwrap();
    let app = node(d.path()).await;
    signups(&app, "saved_signups", 3).await;
    let request = json!({
        "title": "Saved signups",
        "prompt": "saved signups overview",
        "panels": [
            {"title": "Signups", "sql": "SELECT count(*) AS signups FROM saved_signups"},
            {"title": "By plan", "sql": "SELECT plan, count(*) AS signups FROM saved_signups GROUP BY plan ORDER BY 2 DESC"},
        ],
    });
    let (status, made) = call(&app, "PUT", "/v1/dashboards/saved", Some(request)).await;
    assert_eq!(status, StatusCode::OK, "{made}");
    assert_eq!(root(&made)["type"], "Grid", "{made}");
    assert_eq!(made["spec"]["state"]["q0"], json!([{"signups": 15}]));
    let asked = common::jev_requests("saved signups overview").len();

    add(&app, "saved_signups", 100, 2).await;
    let (status, drawn) = call(&app, "GET", "/v1/dashboards/saved", None).await;
    assert_eq!(status, StatusCode::OK, "{drawn}");
    assert_eq!(
        drawn["spec"]["state"]["q0"],
        json!([{"signups": 25}]),
        "{drawn}"
    );
    assert_eq!(drawn["spec"]["elements"], made["spec"]["elements"]);
    assert_eq!(
        common::jev_requests("saved signups overview").len(),
        asked,
        "drawing asked Jev nothing"
    );

    let (status, listed) = call(&app, "GET", "/v1/dashboards", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        listed["dashboards"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["name"] == "saved" && d["panels"] == 2),
        "{listed}"
    );
    let (status, _) = call(&app, "DELETE", "/v1/dashboards/saved", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(&app, "GET", "/v1/dashboards/saved", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// A table has a dashboard of its own, made from its columns: how many rows,
/// how they arrive over time, how they break down by plan, what is paid,
/// and the latest rows. It is kept, and made again when asked to be.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_has_a_dashboard_of_its_own() {
    let d = tempfile::tempdir().unwrap();
    let app = node(d.path()).await;
    signups(&app, "own_signups", 4).await;
    let (status, drawn) = call(&app, "GET", "/v1/see/tables/own_signups", None).await;
    assert_eq!(status, StatusCode::OK, "{drawn}");
    assert!(
        drawn["errors"].is_null(),
        "every panel ran: {}",
        drawn["errors"]
    );
    let panels: Vec<&str> = drawn["panels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    for expected in ["rows", "over_time", "measure_0", "by_0", "latest"] {
        assert!(panels.contains(&expected), "{expected} in {panels:?}");
    }
    let state = &drawn["spec"]["state"];
    assert_eq!(state["rows"], json!([{"rows": 20}]));
    assert_eq!(
        state["over_time"].as_array().unwrap().len(),
        4,
        "four days, a point each: {state}"
    );
    assert_eq!(state["by_0"][0], json!({"plan": "free", "rows": 12}));
    assert_eq!(state["latest"].as_array().unwrap().len(), 20);
    // Drawn the engine's way, since nothing scripted Jev's picks: a number
    // for the count, a line over time, bars by plan.
    let kinds: Vec<&str> = drawn["spec"]["elements"]
        .as_object()
        .unwrap()
        .values()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    for kind in ["Grid", "Metric", "LineChart", "BarChart", "Table"] {
        assert!(kinds.contains(&kind), "{kind} in {kinds:?}");
    }

    let (_, again) = call(&app, "GET", "/v1/see/tables/own_signups", None).await;
    assert_eq!(
        again["spec"]["elements"], drawn["spec"]["elements"],
        "kept, not remade"
    );
    let (status, _) = call(&app, "GET", "/v1/see/tables/no_such_table", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// Saying what else to show adds a panel for it: Jev reads the message as
/// asking for new data, text to SQL answers it, and the dashboard keeps
/// what it had and gains the new panel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chat_adds_what_was_asked_for() {
    let d = tempfile::tempdir().unwrap();
    let app = node(d.path()).await;
    signups(&app, "chat_signups", 3).await;
    let request = json!({
        "panels": [{"title": "Signups", "sql": "SELECT count(*) AS signups FROM chat_signups"}],
    });
    let (status, made) = call(&app, "PUT", "/v1/dashboards/chatty", Some(request)).await;
    assert_eq!(status, StatusCode::OK, "{made}");

    let message = "Also show chat revenue by plan";
    common::answer(
        "Also show chat revenue by plan",
        "needs",
        common::choice("new_data", 0.97, &[]),
    );
    common::draft(
        "Also show chat revenue by plan",
        "SELECT plan, sum(paid) AS revenue FROM chat_signups GROUP BY plan ORDER BY 2 DESC",
    );
    let (status, changed) = call(
        &app,
        "POST",
        "/v1/dashboards/chatty/chat",
        Some(json!({"message": message})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{changed}");
    assert_eq!(
        changed["did"], "added Also show chat revenue by plan",
        "{changed}"
    );
    let dashboard = &changed["dashboard"];
    assert_eq!(
        dashboard["panels"].as_array().unwrap().len(),
        2,
        "{dashboard}"
    );
    let revenue = dashboard["spec"]["state"]["q1"].as_array().unwrap();
    assert_eq!(revenue[0]["plan"], "team", "{revenue:?}");
    let drawn: Vec<&Value> = dashboard["spec"]["elements"]
        .as_object()
        .unwrap()
        .values()
        .filter(|e| e["props"]["data"]["$state"] == "/q1")
        .collect();
    assert_eq!(drawn.len(), 1, "the new panel is drawn once: {dashboard}");

    // And it was saved that way.
    let (_, saved) = call(&app, "GET", "/v1/dashboards/chatty", None).await;
    assert_eq!(saved["spec"]["elements"], dashboard["spec"]["elements"]);
}
