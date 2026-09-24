//! A node with an edge key serves only what the edge forwarded: a request
//! without the key is refused before its token is looked at, one with it goes
//! on to ordinary token auth, and the provider's health check needs neither.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;
use walleye_node::{
    ApiConfig, Config, Service,
    edge::{EDGE_KEY_HEADER, require},
    router,
};
use walleye_ring::Node;

const TOKEN: &str = "deployment-secret-token";
const EDGE_KEY: &str = "edge-key-for-this-environment";

fn config(path: &std::path::Path) -> Config {
    Config {
        node_id: "n".into(),
        listen: "127.0.0.1:0".into(),
        directory: path.join("cache"),
        memory_bytes: 1024 * 1024 * 1024,
        disk_bytes: 256 * 1024 * 1024,
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

async fn send(app: &Router, path: &str, headers: &[(&str, &str)]) -> (StatusCode, String) {
    let mut request = Request::builder().uri(path);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn only_the_edge_reaches_a_keyed_node() {
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(config(dir.path())).await.unwrap();
    let app = require(router(service.clone()), Some(EDGE_KEY));
    let bearer = format!("Bearer {TOKEN}");

    // A valid data token alone is refused, and refused as a forbidden
    // address rather than a bad credential: token auth never ran.
    let (status, body) = send(&app, "/v1/table/", &[("authorization", &bearer)]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("does not accept requests"), "{body}");
    let (status, _) = send(&app, "/v1/table/", &[("x-api-key", TOKEN)]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // A wrong key is no key.
    let (status, _) = send(
        &app,
        "/v1/table/",
        &[
            (EDGE_KEY_HEADER, "edge-key-for-this-environmenT"),
            ("authorization", &bearer),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // With the key the request reaches token auth, which decides.
    let (status, _) = send(&app, "/v1/table/", &[(EDGE_KEY_HEADER, EDGE_KEY)]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = send(
        &app,
        "/v1/table/",
        &[(EDGE_KEY_HEADER, EDGE_KEY), ("authorization", &bearer)],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Every other surface needs the key as well, `/readyz` included.
    let (status, _) = send(&app, "/readyz", &[]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(&app, "/internal/cache/stats", &[("authorization", &bearer)]).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The provider's health check reaches the node directly, with nothing.
    assert_eq!(
        send(&app, "/healthz", &[]).await,
        (StatusCode::OK, "ok".into())
    );
    service.close().await;
}

#[tokio::test]
async fn a_node_without_a_key_checks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let service = Service::open(config(dir.path())).await.unwrap();
    let app = require(router(service.clone()), None);
    let (status, _) = send(
        &app,
        "/v1/table/",
        &[("authorization", &format!("Bearer {TOKEN}"))],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    service.close().await;
}
