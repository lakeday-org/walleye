//! The LanceDB remote protocol over a local root: create, insert, scan, vector
//! search, count, list, describe, index, and drop, with retried inserts
//! collapsing on the hidden content-hash key.
use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    builder::{FixedSizeListBuilder, Float32Builder},
};
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";

fn config(path: &std::path::Path) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes: 32 * 1024 * 1024,
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
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("city", DataType::Utf8, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]))
}
fn batch(rows: &[(i64, &str, [f32; 2])]) -> RecordBatch {
    let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    for (_, _, v) in rows {
        vectors.values().append_slice(v);
        vectors.append(true);
    }
    let vectors: FixedSizeListArray = vectors.finish();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(vectors),
        ],
    )
    .unwrap()
}
fn ipc(batches: &[RecordBatch]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut out, &schema()).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.finish().unwrap();
    out
}
async fn send(
    app: &axum::Router,
    method: &str,
    path: &str,
    ct: &str,
    body: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", ct)
                .header("x-api-key", TOKEN)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = r.status();
    let bytes = to_bytes(r.into_body(), 64 * 1024 * 1024).await.unwrap();
    (status, bytes.to_vec())
}
async fn post_json(app: &axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let (status, bytes) = send(
        app,
        "POST",
        path,
        "application/json",
        body.to_string().into_bytes(),
    )
    .await;
    let value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
    (status, value)
}
fn rows(bytes: &[u8]) -> Vec<RecordBatch> {
    let reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    reader.collect::<Result<Vec<_>, _>>().unwrap()
}
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b.column_by_name("id").unwrap();
        let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
        out.extend(col.iter().flatten());
    }
    out
}

#[tokio::test]
async fn lancedb_protocol_round_trip() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path())).await.unwrap();
    let app = router(service.clone());
    let arrow = "application/vnd.apache.arrow.stream";

    // Create with initial rows, then the same create must report "already exists".
    let first = batch(&[(1, "seattle", [0.0, 1.0]), (2, "seattle", [1.0, 0.0])]);
    let (status, _) = send(
        &app,
        "POST",
        "/v1/table/clicks/create/?mode=create",
        arrow,
        ipc(&[first.clone()]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(
        &app,
        "POST",
        "/v1/table/clicks/create/?mode=create",
        arrow,
        ipc(&[]),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("already exists"));
    let (status, _) = send(
        &app,
        "POST",
        "/v1/table/clicks/create/?mode=exist_ok",
        arrow,
        ipc(&[]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Describe hides the content-hash key.
    let (status, describe) =
        post_json(&app, "/v1/table/clicks/describe/", json!({"version": null})).await;
    assert_eq!(status, StatusCode::OK, "{describe}");
    let names: Vec<&str> = describe["schema"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["id", "city", "vector"]);
    assert_eq!(describe["version"], 2);

    // Insert, then retry the exact same insert: rows collapse on the hidden key.
    let more = batch(&[(3, "portland", [0.0, -1.0])]);
    for _ in 0..2 {
        let (status, body) = send(
            &app,
            "POST",
            "/v1/table/clicks/insert/",
            arrow,
            ipc(&[more.clone()]),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    }
    let (status, count) = post_json(&app, "/v1/table/clicks/count_rows/", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(count, json!(3));
    let (_, count) = post_json(
        &app,
        "/v1/table/clicks/count_rows/",
        json!({"predicate": "city = 'seattle'"}),
    )
    .await;
    assert_eq!(count, json!(2));

    // Plain scan with filter, projection and the SDK's huge k sentinel.
    let (status, bytes) = send(&app, "POST", "/v1/table/clicks/query/", "application/json",
        json!({"k": i64::MAX as u64 - 1, "filter": "id > 1", "columns": ["id", "city"], "vector": [], "prefilter": true, "version": null}).to_string().into_bytes()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let scanned = rows(&bytes);
    let mut got = ids(&scanned);
    got.sort();
    assert_eq!(got, [2, 3]);
    assert_eq!(scanned[0].schema().fields().len(), 2);

    // Vector search returns _distance and nearest first; filter applies too.
    let (status, bytes) = send(&app, "POST", "/v1/table/clicks/query/", "application/json",
        json!({"k": 2, "vector": [0.0, 0.9], "prefilter": true, "nprobes": 1, "refine_factor": null, "version": null}).to_string().into_bytes()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let nearest = rows(&bytes);
    assert_eq!(ids(&nearest), [1, 2]);
    assert!(nearest[0].schema().column_with_name("_distance").is_some());
    assert!(
        nearest[0]
            .schema()
            .column_with_name("_walleye_pk")
            .is_none()
    );
    let (status, bytes) = send(&app, "POST", "/v1/table/clicks/query/", "application/json",
        json!({"k": 5, "vector": [0.0, 0.9], "filter": "city = 'portland'", "prefilter": true, "version": null}).to_string().into_bytes()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(ids(&rows(&bytes)), [3]);

    // Vector indexes need base-table rows, which this build cannot produce yet:
    // the request is refused with a clear reason and search stays exact.
    let (status, body) = post_json(
        &app,
        "/v1/table/clicks/create_index/",
        json!({"column": "vector", "index_type": "IVF_FLAT", "metric_type": "l2"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.as_str().unwrap().contains("compaction"), "{body}");
    let (_, listed) = post_json(&app, "/v1/table/clicks/index/list/", json!({})).await;
    assert_eq!(listed["indexes"], json!([]));
    let (status, bytes) = send(
        &app,
        "POST",
        "/v1/table/clicks/query/",
        "application/json",
        json!({"k": 1, "vector": [1.0, 0.1], "prefilter": true, "version": null})
            .to_string()
            .into_bytes(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    assert_eq!(ids(&rows(&bytes)), [2]);

    // List, unknown table is 404, drop, then gone.
    let (status, bytes) = send(&app, "GET", "/v1/table/", "application/json", Vec::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["tables"],
        json!(["clicks"])
    );
    let (status, _) = post_json(&app, "/v1/table/nope/describe/", json!({"version": null})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = post_json(&app, "/v1/table/clicks/drop/", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = post_json(&app, "/v1/table/clicks/describe/", json!({"version": null})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, bytes) = send(&app, "GET", "/v1/table/", "application/json", Vec::new()).await;
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["tables"],
        json!([])
    );

    // Recreate after drop starts empty.
    let (status, _) = send(
        &app,
        "POST",
        "/v1/table/clicks/create/?mode=create",
        arrow,
        ipc(&[first]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, count) = post_json(&app, "/v1/table/clicks/count_rows/", json!({})).await;
    assert_eq!(count, json!(2));

    // Bearer auth still works and a bad key is rejected.
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/table/")
                .header("x-api-key", "wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/table/")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    service.close().await;
}
