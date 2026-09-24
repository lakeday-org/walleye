//! WALLEYE_RAM_GB is the whole process budget. Everything that allocates
//! borrows from the cache through one governor: opening a table takes its
//! memtable and vector-graph footprint, an inbound body takes its decode
//! cost, and each is refused outright when the budget cannot cover it.
use arrow_array::{ArrayRef, FixedSizeListArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use std::sync::Arc;
use tower::ServiceExt;
use walleye_node::{ApiConfig, Budget, Config, Service, router};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";
const MIB: usize = 1024 * 1024;

fn config(path: &std::path::Path, memory_bytes: usize) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes,
        disk_bytes: 256 * MIB,
        token: TOKEN.into(),
        bitr: false,
        members: vec![Node::new("n", "http://n", 1.0).unwrap()],
        kubernetes: None,
        lease: Default::default(),
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", path.display()),
            bitr_url: None,
        }),
    }
}
fn vector_ipc(rows: usize, dim: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ),
    ]));
    let flat = arrow_array::Float32Array::from(vec![0.5f32; rows * dim]);
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        dim as i32,
        Arc::new(flat),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let mut out = Vec::new();
    let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut out, &schema).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
    out
}
async fn send(
    app: &axum::Router,
    method: &str,
    path: &str,
    ct: &str,
    body: Vec<u8>,
) -> (StatusCode, String) {
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", ct)
                .header("x-api-key", TOKEN)
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = r.status();
    let bytes = to_bytes(r.into_body(), 64 * MIB).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}
async fn stats(app: &axum::Router) -> serde_json::Value {
    let (_, body) = send(
        app,
        "GET",
        "/internal/cache/stats",
        "application/json",
        Vec::new(),
    )
    .await;
    serde_json::from_str(&body).unwrap()
}

#[tokio::test]
async fn every_allocation_borrows_from_the_one_budget_and_fails_closed() {
    let d = tempfile::tempdir().unwrap();
    // 512 MiB process budget: 256 MiB runtime floor, 256 MiB for the cache
    // and everything that borrows from it.
    let service = Service::open(config(d.path(), 512 * MIB)).await.unwrap();
    let app = router(service.clone());
    let at_rest = stats(&app).await;
    let budget = at_rest["budget"]["memory_budget"].as_u64().unwrap() as usize;
    assert_eq!(budget, 512 * MIB - Budget::RUNTIME_MEMORY_FLOOR);
    assert_eq!(
        at_rest["memory_capacity"].as_u64().unwrap() as usize,
        budget
    );
    assert_eq!(at_rest["budget"]["memory_reserved"], 0);

    // Opening a vector table takes its memtable footprint out of the cache:
    // 48 MiB of memtable plus 100k x 576 bytes of graph. Not per dimension:
    // the vectors live in the memtable, and the graph measures the same at
    // 128 dimensions as at 3072.
    let arrow = "application/vnd.apache.arrow.stream";
    let (status, body) = send(
        &app,
        "POST",
        "/v1/table/v/create/?mode=create",
        arrow,
        vector_ipc(10, 128),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let open = stats(&app).await;
    let reserved = open["budget"]["memory_reserved"].as_u64().unwrap() as usize;
    let expected = 48 * MIB + 100_000 * 576;
    assert_eq!(reserved, expected, "{open}");
    let capacity = open["memory_capacity"].as_u64().unwrap() as usize;
    assert!(
        capacity <= budget - reserved && capacity >= budget - reserved - 8 * MIB,
        "{open}"
    );

    // A 62 MiB body fits (144 MiB free): it is decoded, chunked across
    // memtables, and the reservation is released afterwards.
    let (status, body) = send(
        &app,
        "POST",
        "/v1/table/v/insert/",
        arrow,
        vector_ipc(120_000, 128),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(
        &app,
        "POST",
        "/v1/table/v/count_rows/",
        "application/json",
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // The create's ten rows are identical to the first ten inserted, so the
    // content-hash key collapses them: 120,000, not 120,010.
    assert_eq!(body, "120000");
    let after = stats(&app).await;
    assert_eq!(
        after["budget"]["memory_reserved"].as_u64().unwrap() as usize,
        expected
    );
    assert_eq!(after["memory_capacity"], open["memory_capacity"]);

    // A body whose decode cost exceeds what is free is refused before it is
    // decoded, and nothing changes.
    let (status, body) = send(
        &app,
        "POST",
        "/v1/table/v/insert/",
        arrow,
        vector_ipc(150_000, 128),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert!(body.contains("memory budget"), "{body}");
    let (_, body) = send(
        &app,
        "POST",
        "/v1/table/v/count_rows/",
        "application/json",
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(body, "120000");
    let settled = stats(&app).await;
    assert_eq!(
        settled["budget"]["memory_reserved"].as_u64().unwrap() as usize,
        expected
    );

    // Refusal is transient, not terminal: closing a table nobody is using
    // returns its memory, and the next open succeeds. This is what makes the
    // client's retry meaningful rather than a restart.
    let (status, body) = send(
        &app,
        "POST",
        "/v1/table/v2/create/?mode=create",
        arrow,
        vector_ipc(10, 128),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let both = stats(&app).await;
    assert_eq!(
        both["budget"]["memory_reserved"].as_u64().unwrap() as usize,
        expected * 2,
        "{both}"
    );
    let closed = service
        .engine()
        .expect("engine")
        .close_idle(std::time::Duration::ZERO)
        .await;
    assert_eq!(closed, 2, "both tables were idle");
    let reclaimed = stats(&app).await;
    assert_eq!(reclaimed["budget"]["memory_reserved"], 0, "{reclaimed}");
    assert_eq!(
        reclaimed["memory_capacity"].as_u64().unwrap() as usize,
        budget,
        "the cache gets the whole budget back"
    );
    // The data is untouched: a closed table reopens on use.
    let (status, body) = send(
        &app,
        "POST",
        "/v1/table/v/count_rows/",
        "application/json",
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "120000");

    // A table the remaining budget cannot hold is refused at open.
    let d2 = tempfile::tempdir().unwrap();
    let small = Service::open(config(d2.path(), 160 * MIB)).await.unwrap();
    let small_app = router(small.clone());
    let (status, body) = send(
        &small_app,
        "POST",
        "/v1/table/big/create/?mode=create",
        arrow,
        vector_ipc(10, 128),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("memory budget"), "{body}");
    // A plain table without vectors fits: 48 MiB of an 80 MiB budget.
    let (status, body) = send(
        &small_app,
        "POST",
        "/v1/table/plain/create/?mode=create",
        arrow,
        {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
            )
            .unwrap();
            let mut out = Vec::new();
            let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut out, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
            out
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    service.close().await;
    small.close().await;
}

/// The JSON ingestion path borrows from the same budget as the Arrow one.
/// Parsing a body into values and copying them into Arrow costs several times
/// the bytes received, and that expansion used to happen outside the budget
/// entirely, so a large insert could exceed a budget the server had promised
/// to keep.
#[tokio::test]
async fn json_ingestion_is_bounded_by_the_budget() {
    let d = tempfile::tempdir().unwrap();
    // 160 MiB: 80 MiB of floor, 80 MiB of budget, and one open table holds
    // 48 MiB of it.
    let service = Service::open(config(d.path(), 160 * MIB)).await.unwrap();
    let app = router(service.clone());
    let (status, body) = send(
        &app,
        "POST",
        "/v1/streams",
        "application/json",
        serde_json::json!({"name":"events","columns":[{"name":"id","type":"int64"}],
                           "primary_key":["id"]})
        .to_string()
        .into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A small insert fits.
    let rows: Vec<_> = (0..64).map(|id| serde_json::json!({"id": id})).collect();
    let (status, body) = send(
        &app,
        "POST",
        "/v1/streams/events/events",
        "application/json",
        serde_json::json!({"rows": rows}).to_string().into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // One whose expansion the remaining budget cannot cover is refused, and
    // says so, rather than being parsed and hoped for.
    let rows: Vec<_> = (0..350_000)
        .map(|id| serde_json::json!({"id": id}))
        .collect();
    let big = serde_json::json!({"rows": rows}).to_string().into_bytes();
    assert!(big.len() > 3 * MIB, "a body worth refusing: {}", big.len());
    let (status, body) = send(
        &app,
        "POST",
        "/v1/streams/events/events",
        "application/json",
        big,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert!(body.contains("memory budget"), "{body}");

    // The stream is untouched by the refusal.
    let (status, body) = send(
        &app,
        "POST",
        "/v1/query",
        "application/json",
        serde_json::json!({"sql":"SELECT count(*) AS n FROM events"})
            .to_string()
            .into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("\"n\":64"), "{body}");
    service.close().await;
}
