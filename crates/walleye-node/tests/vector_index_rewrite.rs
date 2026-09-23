//! Creating a vector index rewrites every flushed generation so no query can
//! reach an index built with the previous metric. The catalog is written
//! first, so a definition that already matches proves only that the catalog
//! was updated, never that the rewrite ran.
use arrow_array::{ArrayRef, FixedSizeListArray, Int64Array, RecordBatch};
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
const MIB: usize = 1024 * 1024;
const DIM: usize = 8;

fn config(path: &std::path::Path) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes: 1024 * MIB,
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

/// `rows` vectors, numbered from `first` so separate batches stay distinct
/// rather than collapsing onto the same content hash.
fn vector_ipc(first: i64, rows: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        ),
    ]));
    let ids: Vec<i64> = (first..first + rows as i64).collect();
    let values: Vec<f32> = ids
        .iter()
        .flat_map(|i| (0..DIM).map(move |d| (*i as f32) + d as f32))
        .collect();
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        DIM as i32,
        Arc::new(arrow_array::Float32Array::from(values)) as ArrayRef,
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(vectors) as ArrayRef,
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

async fn send(
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
    let bytes = to_bytes(response.into_body(), 32 * 1024 * 1024)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, value)
}

const ARROW: &str = "application/vnd.apache.arrow.stream";
const JSON: &str = "application/json";

async fn generations(app: &axum::Router) -> usize {
    let (status, body) = send(app, "/v1/table/v/get_lsm_stats/", JSON, b"{}".to_vec()).await;
    assert_eq!(status, StatusCode::OK, "lsm stats: {body}");
    body["sstables"].as_array().map(Vec::len).unwrap_or(0)
}

/// Flush enough batches to leave more than one generation behind.
async fn fill(app: &axum::Router, batches: usize) {
    for batch in 0..batches {
        let first = 1 + (batch * 500) as i64;
        let (status, body) = send(app, "/v1/table/v/insert/", ARROW, vector_ipc(first, 500)).await;
        assert_eq!(status, StatusCode::OK, "insert: {body}");
        let (status, body) = send(app, "/v1/table/v/flush_lsm/", JSON, b"{}".to_vec()).await;
        assert_eq!(status, StatusCode::OK, "flush: {body}");
    }
}

async fn create_index(app: &axum::Router, metric: &str) -> (StatusCode, Value) {
    send(
        app,
        "/v1/table/v/create_index/",
        JSON,
        json!({"column": "vector", "index_type": "IVF_HNSW_SQ", "metric_type": metric})
            .to_string()
            .into_bytes(),
    )
    .await
}

/// A repeated create_index carries the same definition the catalog already
/// holds. That is exactly the state a retry lands in after the rewrite
/// failed, so the rewrite has to run again rather than report success on the
/// strength of the catalog alone.
#[tokio::test]
async fn repeating_create_index_still_rewrites_the_generations() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path())).await.unwrap();
    let app = router(service.clone());

    let (status, body) = send(
        &app,
        "/v1/table/v/create/?mode=create",
        ARROW,
        vector_ipc(0, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create: {body}");

    fill(&app, 3).await;
    assert!(
        generations(&app).await > 1,
        "several generations to rewrite"
    );

    let (status, body) = create_index(&app, "cosine").await;
    assert_eq!(status, StatusCode::OK, "first create_index: {body}");
    assert_eq!(
        generations(&app).await,
        1,
        "the rewrite merged every generation"
    );

    // More generations arrive, and the definition now already matches.
    fill(&app, 3).await;
    assert!(
        generations(&app).await > 1,
        "new generations after the index exists"
    );

    let (status, body) = create_index(&app, "cosine").await;
    assert_eq!(status, StatusCode::OK, "repeated create_index: {body}");
    assert_eq!(
        generations(&app).await,
        1,
        "an unchanged definition still rewrites, because the catalog alone \
         does not prove the generations carry the index"
    );
}
