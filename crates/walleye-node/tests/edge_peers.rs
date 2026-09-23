//! Members of a keyed cluster apply the edge check to each other, so a node's
//! own calls to its peers have to carry the key. This binary sets the key in
//! its environment before anything reads it, as a Machine's does.
use axum::{
    Router,
    body::to_bytes,
    http::{Method, StatusCode},
    routing::post,
};
use bytes::Bytes;
use walleye_node::{
    cluster::Cluster,
    edge::{self, EDGE_KEY_HEADER},
    ownership::Peer,
};

const EDGE_KEY: &str = "edge-key-for-this-environment-0123456789";

#[tokio::test]
async fn a_forward_to_a_keyed_peer_carries_the_key() {
    // SAFETY: the only thread that reads the environment is this test's, and
    // nothing has read the key yet.
    unsafe { std::env::set_var("WALLEYE_EDGE_KEY", EDGE_KEY) };
    assert_eq!(edge::configured(), Some(EDGE_KEY));
    assert_eq!(
        edge::peer_headers().get(EDGE_KEY_HEADER).unwrap(),
        EDGE_KEY,
        "the cache's peer requests carry the key too"
    );

    let peer = edge::require(
        Router::new().route("/v1/table/t/insert/", post(|| async { "written" })),
        Some(EDGE_KEY),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, peer).await.unwrap() });

    let owner = Peer {
        node: "owner".into(),
        addr: endpoint,
    };
    let cluster = Cluster::new(
        "me".into(),
        "http://me".into(),
        "deployment-secret-token".into(),
    )
    .unwrap();
    let response = cluster
        .forward(
            &owner,
            Method::POST,
            "/v1/table/t/insert/",
            None,
            Bytes::from_static(b"rows"),
            std::time::Duration::from_secs(1),
            std::future::pending(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(body.as_ref(), b"written");
}
