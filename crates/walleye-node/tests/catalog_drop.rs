//! The catalog listing is a snapshot taken before each definition is read.
//! A table that disappears in between belongs to whoever dropped it, and
//! must not take the rest of the catalog down with it.
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::Value;
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

/// An Arrow IPC stream holding one row, enough to create a table.
fn ipc() -> Vec<u8> {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
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

async fn create_table(app: &axum::Router, name: &str) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/table/{name}/create/"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/vnd.apache.arrow.stream")
                .body(Body::from(ipc()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "create {name}");
}

async fn list_tables(app: &axum::Router) -> (StatusCode, Value) {
    let response = app
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
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// A definition object named by the listing but already gone when it is read
/// is the ordinary outcome of a concurrent drop. Reading it must be skipped,
/// not propagated: the other tables in the catalog are still there.
///
/// The window is real rather than simulated, so this races deliberately. It
/// can only fail when the bug is present: a listing that loses the race and
/// still answers is the whole point.
#[tokio::test]
async fn a_definition_that_disappears_does_not_fail_the_whole_listing() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path())).await.unwrap();
    let app = router(service.clone());

    create_table(&app, "keep").await;
    let streams = d.path().join("store/streams");
    let template = std::fs::read_to_string(streams.join("keep.json")).unwrap();

    // Each round publishes a batch of definitions and deletes them while the
    // listing is being resolved. The listing snapshots every name, so a
    // delete that lands before that name is read is the concurrent drop.
    for round in 0..40 {
        let names: Vec<String> = (0..40).map(|n| format!("ghost{round}_{n}")).collect();
        let paths: Vec<_> = names
            .iter()
            .map(|name| {
                let path = streams.join(format!("{name}.json"));
                std::fs::write(&path, template.replace("\"keep\"", &format!("\"{name}\"")))
                    .unwrap();
                path
            })
            .collect();
        let remove = tokio::task::spawn_blocking({
            let paths = paths.clone();
            move || {
                for path in paths {
                    let _ = std::fs::remove_file(&path);
                }
            }
        });
        let (status, body) = list_tables(&app).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "round {round}: a vanished definition took the listing down: {body}"
        );
        let tables = body["tables"].as_array().unwrap();
        assert!(
            tables.iter().any(|t| t == "keep"),
            "round {round}: still lists the table that exists, got {tables:?}"
        );
        remove.await.unwrap();
        for path in paths {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Dropping a table removes it from the catalog. The claim the drop holds
/// over the name must be released once the objects are gone, so the name is
/// free again rather than wedged.
#[tokio::test]
async fn a_dropped_table_leaves_the_catalog_and_frees_its_name() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path())).await.unwrap();
    let app = router(service.clone());

    create_table(&app, "t").await;

    let dropped = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/table/t/drop/")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(dropped.status(), StatusCode::OK, "drop");

    let (status, body) = list_tables(&app).await;
    assert_eq!(status, StatusCode::OK);
    let tables: Vec<&str> = body["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t.as_str().unwrap())
        .collect();
    assert!(
        tables.is_empty(),
        "dropped table is gone from the catalog, got {tables:?}"
    );
}
