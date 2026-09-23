//! Scoped access tokens through the whole router: each token reaches exactly
//! the routes its scopes name, and tokens published into the bucket after the
//! node started - or taken out of it - take effect without a restart.
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, access, router};
use walleye_ring::Node;

const SYSTEM: &str = "deployment-secret-token";

fn config(path: &std::path::Path) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes: 1024 * 1024 * 1024,
        disk_bytes: 64 * 1024 * 1024,
        token: SYSTEM.into(),
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

/// What the control plane writes: hashes and scopes, never a token.
fn publish(root: &std::path::Path, tokens: &[(&str, &[&str], Option<u64>)]) {
    let dir = root.join("store/_walleye");
    std::fs::create_dir_all(&dir).unwrap();
    let tokens: Vec<_> = tokens
        .iter()
        .map(|(token, scopes, expires_at)| {
            json!({
                "id": format!("tok_{token}"),
                "sha256": hex::encode(Sha256::digest(token.as_bytes())),
                "scopes": scopes,
                "expires_at": expires_at,
            })
        })
        .collect();
    // Write beside and rename, as an object store replaces an object whole.
    let staged = dir.join("access.json.tmp");
    std::fs::write(
        &staged,
        serde_json::to_vec(&json!({"version": 1, "tokens": tokens})).unwrap(),
    )
    .unwrap();
    std::fs::rename(staged, dir.join("access.json")).unwrap();
}

fn ipc() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let mut out = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut out, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    out
}

async fn call(
    app: &axum::Router,
    token: Option<&str>,
    method: &str,
    path: &str,
    body: Vec<u8>,
) -> StatusCode {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(token) = token {
        request = request.header("x-api-key", token);
    }
    let content_type = if body.first() == Some(&b'{') {
        "application/json"
    } else {
        "application/vnd.apache.arrow.stream"
    };
    let response = app
        .clone()
        .oneshot(
            request
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let _ = to_bytes(response.into_body(), 64 * 1024 * 1024).await;
    status
}

fn empty() -> Vec<u8> {
    b"{}".to_vec()
}

#[tokio::test]
async fn each_token_reaches_only_what_its_scopes_name() {
    let d = tempfile::tempdir().unwrap();
    publish(
        d.path(),
        &[
            ("reader", &["data:read"], None),
            ("writer", &["data:write"], None),
            ("manager", &["data:manage"], None),
            ("expired", &["data:read"], Some(1)),
        ],
    );
    let service = Service::open(config(d.path())).await.unwrap();
    let app = router(service.clone());

    // The probes need nothing; everything else needs a token it accepts.
    assert_eq!(
        call(&app, None, "GET", "/healthz", vec![]).await,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, None, "GET", "/v1/table/", vec![]).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, Some("nobody"), "GET", "/v1/table/", vec![]).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&app, Some("expired"), "GET", "/v1/table/", vec![]).await,
        StatusCode::UNAUTHORIZED
    );

    // Creating a table is management, and management alone does not read.
    assert_eq!(
        call(&app, Some("reader"), "POST", "/v1/table/t/create/", ipc()).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, Some("manager"), "POST", "/v1/table/t/create/", ipc()).await,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            Some("manager"),
            "POST",
            "/v1/table/t/count_rows/",
            empty()
        )
        .await,
        StatusCode::FORBIDDEN
    );

    // A reader queries and is refused both a write and a drop.
    assert_eq!(
        call(
            &app,
            Some("reader"),
            "POST",
            "/v1/table/t/count_rows/",
            empty()
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            Some("reader"),
            "POST",
            "/v1/table/t/query/",
            json!({"k": 10}).to_string().into_bytes()
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, Some("reader"), "POST", "/v1/table/t/insert/", ipc()).await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, Some("reader"), "POST", "/v1/table/t/drop/", empty()).await,
        StatusCode::FORBIDDEN
    );

    // A writer inserts and reads nothing.
    assert_eq!(
        call(&app, Some("writer"), "POST", "/v1/table/t/insert/", ipc()).await,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, Some("writer"), "GET", "/v1/table/", vec![]).await,
        StatusCode::FORBIDDEN
    );

    // Peer routes are the platform's alone, whatever a token holds.
    assert_eq!(
        call(
            &app,
            Some("manager"),
            "GET",
            "/internal/cache/stats",
            vec![]
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        call(&app, Some(SYSTEM), "GET", "/internal/cache/stats", vec![]).await,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            Some(SYSTEM),
            "POST",
            "/v1/table/t/count_rows/",
            empty()
        )
        .await,
        StatusCode::OK
    );
    service.close().await;
}

#[tokio::test]
async fn tokens_change_without_a_restart() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path())).await.unwrap();
    let app = router(service.clone());
    assert_eq!(
        call(&app, Some("minted"), "GET", "/v1/table/", vec![]).await,
        StatusCode::UNAUTHORIZED
    );

    // A token minted after boot is picked up the first time it is presented,
    // once the read a bad token spent has come round again.
    publish(d.path(), &[("minted", &["data:read"], None)]);
    tokio::time::sleep(access::MISS_REFRESH_INTERVAL).await;
    assert_eq!(
        call(&app, Some("minted"), "GET", "/v1/table/", vec![]).await,
        StatusCode::OK
    );

    // Revoked, it stops within one refresh interval.
    publish(d.path(), &[]);
    tokio::time::sleep(access::REFRESH_INTERVAL + std::time::Duration::from_secs(1)).await;
    assert_eq!(
        call(&app, Some("minted"), "GET", "/v1/table/", vec![]).await,
        StatusCode::UNAUTHORIZED
    );
    service.close().await;
}
