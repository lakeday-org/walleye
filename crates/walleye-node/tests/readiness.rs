//! A node must not claim readiness before it can make writes durable, and a
//! forward must wait out an owner that is still starting rather than turning
//! its refusal into a 502.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
    routing::any,
};
use bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tower::ServiceExt;
use walleye_node::{ApiConfig, Config, Service, cluster::Cluster, router};
use walleye_ring::{Membership, Node};

const TOKEN: &str = "deployment-secret-token";

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
        processor: None,
        api: Some(ApiConfig {
            root_uri: format!("file://{}/store", path.display()),
            bitr_url: None,
        }),
    }
}
async fn get(app: &Router, path: &str) -> (StatusCode, String) {
    let r = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = r.status();
    let body = to_bytes(r.into_body(), 64 * 1024).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Object-store durability needs no quorum, so a single node is write-ready
/// as soon as it opens.
#[tokio::test]
async fn a_single_node_is_ready_without_a_quorum() {
    let d = tempfile::tempdir().unwrap();
    let service = Service::open(config(d.path())).await.unwrap();
    let app = router(service.clone());
    assert_eq!(get(&app, "/healthz").await, (StatusCode::OK, "ok".into()));
    assert_eq!(get(&app, "/readyz").await, (StatusCode::OK, "ready".into()));
    service.close().await;
}

/// An owner that answers 503 while it starts is retried, not reported
/// unreachable. The body and method survive the retries.
#[tokio::test]
async fn a_forward_waits_out_an_owner_that_is_starting() {
    let refusals = Arc::new(AtomicU32::new(0));
    let seen = refusals.clone();
    let peer = Router::new().route(
        "/v1/table/t/insert/",
        any(move |request: Request<Body>| {
            let seen = seen.clone();
            async move {
                assert_eq!(request.method(), Method::POST);
                let body = to_bytes(request.into_body(), 1024).await.unwrap();
                assert_eq!(body.as_ref(), b"rows");
                if seen.fetch_add(1, Ordering::AcqRel) < 3 {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "the replica quorum is unreachable; retry",
                    )
                        .into_response()
                } else {
                    (StatusCode::OK, "written").into_response()
                }
            }
        }),
    );
    use axum::response::IntoResponse;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, peer).await.unwrap() });

    let owner = Node::new("owner", endpoint, 1.0).unwrap();
    let ring = Arc::new(Membership::new(vec![owner.clone()]).unwrap());
    let cluster = Cluster::new("me".into(), ring, TOKEN.into()).unwrap();
    let response = cluster
        .forward(
            &owner,
            Method::POST,
            "/v1/table/t/insert/",
            Some("application/vnd.apache.arrow.stream"),
            Bytes::from_static(b"rows"),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(body.as_ref(), b"written");
    assert_eq!(
        refusals.load(Ordering::Acquire),
        4,
        "three refusals then one success"
    );
}
