//! Ingest anything, end to end through `POST /v1/ingest/{source}`.
//!
//! Most of these run without a judge configured, because that is what a CI
//! checkout has and because every judgement has a safe default the path must
//! work with: a lossless type, an optional field, a value kept aside rather
//! than lost. The ones that need a judge to decide something say so and skip
//! without a key.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json, value::RawValue};
use std::sync::Arc;
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";

fn keyed() -> bool {
    std::env::var("TYPESAFE_API_KEY").is_ok_and(|key| !key.trim().is_empty())
}

async fn node(dir: &std::path::Path) -> (Arc<Service>, axum::Router) {
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

async fn post(app: &axum::Router, uri: &str, body: String) -> (StatusCode, String) {
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
    let bytes = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Send records and return what the endpoint reported.
async fn ingest(app: &axum::Router, source: &str, records: Value) -> Value {
    let (status, body) = post(app, &format!("/v1/ingest/{source}"), records.to_string()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    serde_json::from_str(&body).unwrap()
}

/// Rows as JSON text, unparsed, so a number is compared by its digits.
async fn sql_text(app: &axum::Router, statement: &str) -> String {
    let (status, body) = post(app, "/v1/query", json!({"sql": statement}).to_string()).await;
    assert_eq!(status, StatusCode::OK, "{statement}: {body}");
    body
}

async fn sql(app: &axum::Router, statement: &str) -> Vec<Value> {
    serde_json::from_str(&sql_text(app, statement).await).unwrap()
}

fn changed(report: &Value, what: &str) -> Option<Value> {
    report["changes"]
        .as_array()?
        .iter()
        .find(|c| c["what"].as_str() == Some(what))
        .cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_source_nobody_has_seen_becomes_a_typed_table() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;

    let records: Vec<Value> = (1..=5)
        .map(|n| json!({"order_id": n, "zip": "02134", "price": "12.50", "shipped": n % 2 == 0}))
        .collect();
    let report = ingest(&app, "orders", Value::Array(records)).await;
    assert_eq!(report["table"], "orders", "{report}");
    assert_eq!(report["created"], true, "{report}");
    assert_eq!(report["accepted"], 5, "{report}");
    assert_eq!(report["quarantined"], 0, "{report}");

    let rows = sql(
        &app,
        "SELECT order_id, zip, CAST(price AS VARCHAR) AS price, shipped FROM orders ORDER BY order_id",
    )
    .await;
    assert_eq!(rows.len(), 5, "{rows:?}");
    // A zip code keeps its leading zero: it was never offered a number type.
    assert_eq!(rows[0]["zip"], "02134", "{rows:?}");
    // Money is exact. Unjudged, the narrowest lossless type is decimal, never
    // floating point.
    assert!(
        rows[0]["price"].as_str().unwrap().starts_with("12.5"),
        "{rows:?}"
    );
    assert_eq!(rows[1]["shipped"], true, "{rows:?}");
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bronze_keeps_the_bytes_that_arrived_and_silver_keeps_the_digits() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    // 2^53 + 1: the first integer a float cannot hold.
    let raw = r#"[{"id": "a", "big": 9007199254740993}]"#;
    let (status, body) = post(&app, "/v1/ingest/ledger", raw.to_owned()).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let bronze = sql_text(
        &app,
        "SELECT record FROM ingest_bronze WHERE source = 'ledger'",
    )
    .await;
    assert!(
        bronze.contains("9007199254740993"),
        "bronze holds the record exactly: {bronze}"
    );
    let silver = sql_text(&app, "SELECT big FROM ledger").await;
    assert!(
        silver.contains("9007199254740993"),
        "silver holds every digit: {silver}"
    );
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_known_source_goes_where_its_rule_says_without_being_judged() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    ingest(&app, "events", json!([{"kind": "click", "at": 1}])).await;
    let second = ingest(&app, "events", json!([{"kind": "view", "at": 2}])).await;
    assert_eq!(second["table"], "events");
    assert_eq!(second["created"], false);
    assert!(
        changed(&second, "route events").is_none() && changed(&second, "table name").is_none(),
        "routing a known source decides nothing: {second}"
    );
    assert_eq!(
        sql(&app, "SELECT count(*) AS n FROM events").await[0]["n"],
        2
    );
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_field_that_stops_arriving_does_not_stop_ingestion() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    ingest(
        &app,
        "people",
        json!([{"name": "ada", "email": "a@x.io"}, {"name": "bo", "email": "b@x.io"}]),
    )
    .await;
    let report = ingest(&app, "people", json!([{"name": "cy"}])).await;
    assert_eq!(report["accepted"], 1, "{report}");
    assert_eq!(report["quarantined"], 0, "{report}");
    let rows = sql(&app, "SELECT name, email FROM people ORDER BY name").await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(rows[2]["name"], "cy");
    assert!(
        rows[2]["email"].is_null(),
        "the missing field is null: {rows:?}"
    );
    service.close().await;
}

/// A field the rule says every record has, missing from new records. That is
/// decided once for the batch: either the field is optional after all, and
/// the rule says so from now on, or the records are broken and refused. It
/// never stops ingestion for the records that are fine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_required_field_that_goes_missing_is_decided_once_for_the_batch() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    ingest(
        &app,
        "members",
        json!([{"name": "ada", "email": "a@x.io"}, {"name": "bo", "email": "b@x.io"}]),
    )
    .await;
    // Mark email required, as a judge sure of it would have.
    let path = d.path().join("store/ingest/tables/members.json");
    let mut rule: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    for column in rule["columns"].as_array_mut().unwrap() {
        if column["name"] == "email" {
            column["required"] = json!(true);
        }
    }
    std::fs::write(&path, serde_json::to_vec(&rule).unwrap()).unwrap();

    let report = ingest(&app, "members", json!([{"name": "cy"}, {"name": "di"}])).await;
    let decided = changed(&report, "email missing").expect("the missing field is decided");
    let rule: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let still_required = rule["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "email")
        .unwrap()["required"]
        .as_bool()
        .unwrap();
    if decided["chose"] == "now optional" {
        assert_eq!(report["accepted"], 2, "{report}");
        assert!(!still_required, "and the rule says so from now on: {rule}");
    } else {
        // Refusing needs a judge sure of it; code never refuses on a guess.
        assert_eq!(decided["by"], "jev", "{report}");
        assert_eq!(report["quarantined"], 2, "{report}");
        assert!(still_required);
    }
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_field_gets_its_own_column_and_nothing_earlier_is_lost() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    ingest(&app, "signups", json!([{"user": "ada"}, {"user": "bo"}])).await;
    let report = ingest(&app, "signups", json!([{"user": "cy", "plan": "pro"}])).await;
    assert_eq!(
        report["rebuilt"], true,
        "a new column is a rebuild: {report}"
    );
    assert!(
        changed(&report, "new column plan").is_some(),
        "it says what it added: {report}"
    );
    let rows = sql(&app, "SELECT user, plan FROM signups ORDER BY user").await;
    assert_eq!(
        rows.len(),
        3,
        "every record from before the rebuild is still there: {rows:?}"
    );
    assert!(rows[0]["plan"].is_null());
    assert_eq!(rows[2]["plan"], "pro");
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_value_that_does_not_fit_is_kept_aside_not_lost() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    ingest(
        &app,
        "stock",
        json!([{"sku": "a", "qty": 3}, {"sku": "b", "qty": 4}]),
    )
    .await;
    let report = ingest(&app, "stock", json!([{"sku": "c", "qty": "N/A"}])).await;
    assert_eq!(report["accepted"], 1, "{report}");
    assert_eq!(report["rescued"], 1, "{report}");
    let rows = sql(&app, "SELECT qty, walleye_extra FROM stock WHERE sku = 'c'").await;
    assert!(rows[0]["qty"].is_null(), "{rows:?}");
    assert!(
        rows[0]["walleye_extra"].as_str().unwrap().contains("N/A"),
        "the value that did not fit is kept with its row: {rows:?}"
    );
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn something_that_is_not_a_record_is_refused_and_still_kept() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let report = ingest(&app, "mixed", json!([{"a": 1}, 7, "text"])).await;
    assert_eq!(report["accepted"], 1, "{report}");
    assert_eq!(report["quarantined"], 2, "{report}");
    let refused = sql(
        &app,
        "SELECT reason FROM ingest_quarantine WHERE source = 'mixed'",
    )
    .await;
    assert_eq!(refused.len(), 2);
    assert!(refused.iter().all(|r| r["reason"] == "not a JSON object"));
    assert_eq!(
        sql(
            &app,
            "SELECT count(*) AS n FROM ingest_bronze WHERE source = 'mixed'"
        )
        .await[0]["n"],
        3,
        "bronze keeps what was refused too"
    );
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_record_sent_twice_is_one_row() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let record = json!([{"event": "ping", "at": "2026-09-23T10:00:00Z"}]);
    ingest(&app, "beats", record.clone()).await;
    ingest(&app, "beats", record).await;
    assert_eq!(
        sql(&app, "SELECT count(*) AS n FROM beats").await[0]["n"],
        1
    );
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_keyed_by_ids_does_not_become_hundreds_of_columns() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let mut record = serde_json::Map::new();
    for n in 0..300 {
        record.insert(format!("user_{n}"), json!(n));
    }
    let report = ingest(&app, "presence", json!([record])).await;
    assert_eq!(report["accepted"], 1, "{report}");
    let (status, body) = post(&app, "/v1/table/presence/describe/", "{}".into()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let fields = serde_json::from_str::<Value>(&body).unwrap()["schema"]["fields"]
        .as_array()
        .map(Vec::len)
        .expect("describe lists the fields");
    assert!(
        (2..=walleye_node::ingest::MAX_COLUMNS).contains(&fields),
        "the column cap holds whatever was sent: {fields} columns"
    );
    let decided = changed(&report, "keys are data").expect("the cap's decision is reported");
    assert_eq!(decided["chose"], "true", "{report}");
    assert_eq!(
        decided["by"], "rule",
        "past the cap it is not a judgement: {report}"
    );
    let rows = sql_text(&app, "SELECT walleye_extra FROM presence").await;
    assert!(rows.contains("user_299"), "the data is kept whole: {rows}");
    service.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_body_can_be_one_record_a_list_or_a_wrapped_list() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let one = ingest(&app, "shapes", json!({"n": 1})).await;
    assert_eq!(one["accepted"], 1, "{one}");
    let wrapped = ingest(&app, "shapes", json!({"records": [{"n": 2}, {"n": 3}]})).await;
    assert_eq!(wrapped["accepted"], 2, "{wrapped}");
    assert_eq!(
        sql(&app, "SELECT count(*) AS n FROM shapes").await[0]["n"],
        3
    );
    let _ = RawValue::from_string("{}".into());
    service.close().await;
}

// --- judged ------------------------------------------------------------------

/// What a column's values mean is the judge's call, among the kinds they fit.
/// A zip code only fits text, so that one is never asked; a price, a moment
/// given as seconds, and a count each fit several and are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_judge_reads_what_the_values_mean() {
    if !keyed() {
        eprintln!("skipping: no decision service key configured");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let records: Vec<Value> = (0..6)
        .map(|n| {
            json!({
                "order_id": format!("ord_{n}"),
                "price": format!("{}.99", 10 + n),
                "created": 1_700_000_000 + n * 60,
                "quantity": n + 1,
                "zip": "02134"
            })
        })
        .collect();
    let report = ingest(&app, "shop_orders", Value::Array(records)).await;
    let kind = |field: &str| {
        changed(&report, &format!("type of {field}"))
            .map(|c| c["chose"].as_str().unwrap_or_default().to_owned())
            .unwrap_or_default()
    };
    assert_eq!(kind("price"), "decimal", "{report}");
    assert_eq!(kind("created"), "timestamp_seconds", "{report}");
    assert_eq!(kind("quantity"), "int64", "{report}");
    assert!(
        changed(&report, "type of zip").is_none(),
        "a zip code fits only text, so nobody was asked: {report}"
    );
    let key = changed(&report, "primary key").unwrap();
    assert_eq!(key["chose"], "order_id", "{report}");
    assert_eq!(key["by"], "jev", "{report}");

    // A key is never optional, whatever a judge would say about it. A record
    // without one cannot be placed, so it is refused, and kept.
    let keyless = ingest(
        &app,
        "shop_orders",
        json!([{"price": "5.00", "created": 1_700_000_900, "quantity": 1, "zip": "02134"}]),
    )
    .await;
    assert_eq!(keyless["quarantined"], 1, "{keyless}");
    assert_eq!(keyless["accepted"], 0, "{keyless}");
    let refused = sql(
        &app,
        "SELECT reason FROM ingest_quarantine WHERE source = 'shop_orders'",
    )
    .await;
    assert_eq!(
        refused[0]["reason"], "missing its key \"order_id\"",
        "{refused:?}"
    );

    // And a record with the same key replaces the one before it.
    ingest(
        &app,
        "shop_orders",
        json!([{"order_id": "ord_0", "price": "1.00", "created": 1_700_000_000, "quantity": 9, "zip": "02134"}]),
    )
    .await;
    let rows = sql(
        &app,
        "SELECT quantity FROM shop_orders WHERE order_id = 'ord_0'",
    )
    .await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["quantity"], 9, "{rows:?}");
    service.close().await;
}

/// A new source whose records are the same kind of thing an existing table
/// holds is mapped to that table once, and by the rule after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_source_of_the_same_records_joins_the_existing_table() {
    if !keyed() {
        eprintln!("skipping: no decision service key configured");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let first: Vec<Value> = (0..5)
        .map(|n| json!({"invoice_id": format!("in_{n}"), "customer": "acme", "amount": "99.00", "currency": "usd"}))
        .collect();
    ingest(&app, "billing_eu", Value::Array(first)).await;
    let second: Vec<Value> = (5..10)
        .map(|n| json!({"invoice_id": format!("in_{n}"), "customer": "globex", "amount": "15.50", "currency": "eur"}))
        .collect();
    let report = ingest(&app, "billing_us", Value::Array(second)).await;
    assert_eq!(report["created"], false, "{report}");
    assert_eq!(report["table"], "billing_eu", "{report}");
    assert_eq!(
        sql(&app, "SELECT count(*) AS n FROM billing_eu").await[0]["n"],
        10
    );
    let again = ingest(
        &app,
        "billing_us",
        json!([{"invoice_id": "in_99", "customer": "initech", "amount": "1.00", "currency": "usd"}]),
    )
    .await;
    assert!(
        changed(&again, "route billing_us").is_none(),
        "once mapped, a source is not judged again: {again}"
    );
    service.close().await;
}

/// A field that starts arriving where another used to, under the same name in
/// a different case convention, is that column: `userId` is `user_id`. That
/// is not a judgement - the names are the same once case and separators are
/// ignored - so code decides it, and it works with or without a judge.
///
/// It matters most for the key. A key renamed is still the key, and a check
/// that ran before renames were settled would refuse every record from the
/// rename on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_field_renamed_to_another_case_convention_is_the_same_column() {
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let before: Vec<Value> = (0..5)
        .map(|n| json!({"user_id": format!("u{n}"), "visits": n}))
        .collect();
    ingest(&app, "sessions", Value::Array(before)).await;
    let report = ingest(&app, "sessions", json!([{"userId": "u9", "visits": 7}])).await;
    let renamed = changed(&report, "userId is user_id renamed").expect("{report}");
    assert_eq!(renamed["by"], "rule", "{report}");
    assert_eq!(
        report["quarantined"], 0,
        "a renamed key is still a key: {report}"
    );
    let rows = sql(&app, "SELECT user_id FROM sessions WHERE visits = 7").await;
    assert_eq!(
        rows[0]["user_id"], "u9",
        "read into the column it always was: {rows:?}"
    );
    service.close().await;
}

/// A value that does not fit the column is either a mistake, kept aside, or
/// a sign the column was typed too narrowly. Widening rebuilds the table from
/// bronze, so the earlier rows come back under the wider type rather than
/// being left behind in the old one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_column_typed_too_narrowly_is_widened_and_rebuilt() {
    if !keyed() {
        eprintln!("skipping: no decision service key configured");
        return;
    }
    let d = tempfile::tempdir().unwrap();
    let (service, app) = node(d.path()).await;
    let before: Vec<Value> = (0..5)
        .map(|n| json!({"part": format!("p{n}"), "code": 100 + n}))
        .collect();
    ingest(&app, "parts", Value::Array(before)).await;
    let report = ingest(
        &app,
        "parts",
        json!([{"part": "p9", "code": "A-17"}, {"part": "p10", "code": "B-2"}]),
    )
    .await;
    let widened = changed(&report, "widen code").expect("the misfit is decided");
    assert_eq!(widened["chose"], "string", "{report}");
    assert_eq!(report["rebuilt"], true, "{report}");
    let rows = sql(&app, "SELECT part, code FROM parts ORDER BY part").await;
    assert_eq!(rows.len(), 7, "every earlier row came back: {rows:?}");
    let codes: Vec<&str> = rows.iter().filter_map(|r| r["code"].as_str()).collect();
    assert!(
        codes.contains(&"100") && codes.contains(&"A-17"),
        "{rows:?}"
    );
    service.close().await;
}
