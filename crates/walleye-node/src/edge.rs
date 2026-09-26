//! Edge-only ingress.
//!
//! A managed node is reachable at its provider's public address as well as
//! through the platform's edge, and only the edge is meant to be used. The
//! edge stamps every request it forwards with a per-environment key; a node
//! configured with that key (`WALLEYE_EDGE_KEY`) refuses every request that
//! does not carry it, before any token is looked at. `/healthz` is the one
//! exception: the provider's own health checks reach the node directly.
//!
//! The node's calls to its own peers carry the key too, since a peer is the
//! same kind of node and applies the same rule. A node with no key configured
//! has no edge in front of it and checks nothing.
use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::{Arc, OnceLock};

/// The header the edge adds to every request it forwards.
pub const EDGE_KEY_HEADER: &str = "x-walleye-edge-key";

/// The key this process requires, read once from `WALLEYE_EDGE_KEY`.
pub fn configured() -> Option<&'static str> {
    static KEY: OnceLock<Option<String>> = OnceLock::new();
    KEY.get_or_init(|| {
        std::env::var("WALLEYE_EDGE_KEY")
            .ok()
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty())
    })
    .as_deref()
}

/// Wraps `router` so that, with a key, only requests carrying it are served.
pub fn require(router: Router, key: Option<&str>) -> Router {
    match key {
        Some(key) => router.layer(axum::middleware::from_fn_with_state(
            Arc::<str>::from(key),
            check,
        )),
        None => router,
    }
}

async fn check(State(key): State<Arc<str>>, request: Request, next: Next) -> Response {
    let presented = request
        .headers()
        .get(EDGE_KEY_HEADER)
        .map(HeaderValue::as_bytes);
    if request.uri().path() == "/healthz" || presented.is_some_and(|p| same(p, key.as_bytes())) {
        return next.run(request).await;
    }
    (
        StatusCode::FORBIDDEN,
        "this address does not accept requests; use the instance's own hostname",
    )
        .into_response()
}

fn same(presented: &[u8], expected: &[u8]) -> bool {
    presented.len() == expected.len()
        && presented
            .iter()
            .zip(expected)
            .fold(0u8, |d, (a, b)| d | (a ^ b))
            == 0
}

/// Headers every call from this node to a peer carries.
pub fn peer_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(mut value) = configured().and_then(|key| HeaderValue::from_str(key).ok()) {
        value.set_sensitive(true);
        headers.insert(EDGE_KEY_HEADER, value);
    }
    headers
}
