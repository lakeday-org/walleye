//! Per-stream ownership across cluster members. A stream's owner is the
//! rendezvous-hash winner for its name on the membership ring, the same
//! placement the cache uses. Only the owner opens the stream's MemWAL writer;
//! every other member forwards the request to it. The MemWAL writer epoch
//! (and, in Bitr mode, the Bitr writer epoch derived from it) fences a stale
//! owner after a membership change; the membership fingerprint header catches
//! a forward that raced such a change before it reaches the WAL.
#![allow(clippy::result_large_err)]
use axum::{
    body::Bytes,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;
use walleye_ring::{Membership, Node};

/// Sent on every forwarded request: the forwarder's view of membership.
pub const MEMBERS_HEADER: &str = "x-walleye-members";
/// Marks a request as already forwarded once; a second hop is refused.
pub const FORWARDED_HEADER: &str = "x-walleye-forwarded";
/// Which member answered, for observability and tests.
pub const OWNER_HEADER: &str = "x-walleye-owner";
/// How long a forward waits out an owner that is starting up before it
/// answers 502. Nodes converge on the same quorum, so the skew between a
/// ready forwarder and its owner is seconds.
pub const FORWARD_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(15);

#[derive(Debug)]
pub struct NotOwner(pub Node);
impl std::fmt::Display for NotOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream is owned by {} at {}", self.0.id, self.0.endpoint)
    }
}
impl std::error::Error for NotOwner {}

#[derive(Clone)]
pub struct Cluster {
    pub node_id: String,
    pub ring: Arc<Membership>,
    pub token: String,
    client: reqwest::Client,
}
impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}
impl Cluster {
    pub fn new(
        node_id: String,
        ring: Arc<Membership>,
        token: String,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            node_id,
            ring,
            token,
            client: Self::client()?,
        })
    }
    /// Pooled connections are dropped after a few seconds idle, so a peer that
    /// restarted (new Machine version, new address) stops poisoning forwards
    /// quickly; a transport failure on a pooled connection also retries once
    /// on a fresh one.
    fn client() -> Result<reqwest::Client, reqwest::Error> {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(120))
            .pool_idle_timeout(std::time::Duration::from_secs(5))
            .build()
    }
    /// The member that owns `stream`, or `None` when this node does.
    pub fn owner(&self, stream: &str) -> Option<Node> {
        let ring = self.ring.snapshot();
        let owner = ring.owner(stream.as_bytes());
        (owner.id != self.node_id).then(|| owner.clone())
    }
    /// A stable digest of the member ids this node currently sees.
    pub fn fingerprint(&self) -> String {
        let ring = self.ring.snapshot();
        let mut ids: Vec<&str> = ring.members().iter().map(|n| n.id.as_str()).collect();
        ids.sort_unstable();
        let joined = ids.join("\n");
        format!("{:016x}", xxhash_rust::xxh3::xxh3_64(joined.as_bytes()))
    }
    /// Reject a forwarded request whose sender saw a different membership.
    pub fn check_fence(&self, headers: &HeaderMap) -> Result<(), Response> {
        if let Some(seen) = headers.get(MEMBERS_HEADER).and_then(|v| v.to_str().ok())
            && seen != self.fingerprint()
        {
            return Err((
                StatusCode::CONFLICT,
                "membership changed while the request was in flight; retry",
            )
                .into_response());
        }
        Ok(())
    }
    /// Fetch every row of `stream` from the member that owns it, as an Arrow
    /// IPC file, for a query that spans owners.
    pub async fn fetch_snapshot(&self, owner: &Node, stream: &str) -> Result<bytes::Bytes, String> {
        let url = format!(
            "{}/internal/snapshot/{stream}",
            owner.endpoint.trim_end_matches('/')
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| format!("owner {} is unreachable: {error}", owner.id))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!(
                "owner {} refused a snapshot of {stream} with {status}: {body}",
                owner.id
            ));
        }
        response
            .bytes()
            .await
            .map_err(|error| format!("owner {} truncated {stream}: {error}", owner.id))
    }
    /// Relay one request to `owner` and return its response verbatim.
    pub async fn forward(
        &self,
        owner: &Node,
        method: Method,
        path_and_query: &str,
        content_type: Option<&str>,
        body: Bytes,
    ) -> Response {
        let url = format!("{}{}", owner.endpoint.trim_end_matches('/'), path_and_query);
        let build = |client: &reqwest::Client| {
            let mut request = client
                .request(method.clone(), &url)
                .bearer_auth(&self.token)
                .header(MEMBERS_HEADER, self.fingerprint())
                .header(FORWARDED_HEADER, "1")
                .body(body.clone());
            if let Some(content_type) = content_type {
                request = request.header("content-type", content_type);
            }
            request
        };
        // A peer that is still starting refuses with 503, and one that just
        // restarted fails to connect. Both are safe to retry: a connect error
        // means the request never arrived, and a 503 is an explicit refusal
        // with no side effect. Wait out a peer's startup window instead of
        // turning it into a bare 502.
        let deadline = std::time::Instant::now() + FORWARD_RETRY_WINDOW;
        let mut delay = std::time::Duration::from_millis(100);
        let mut client = self.client.clone();
        let mut attempts = 0_u32;
        let outcome = loop {
            attempts += 1;
            let outcome = build(&client).send().await;
            let retryable = match &outcome {
                Err(error) => error.is_connect() || (error.is_request() && !error.is_timeout()),
                Ok(response) => response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
            };
            if !retryable || std::time::Instant::now() >= deadline {
                if attempts > 1 {
                    eprintln!(
                        "walleye.forward owner={} attempts={attempts} settled",
                        owner.id
                    );
                }
                break outcome;
            }
            // A pooled connection may belong to the peer's previous
            // incarnation; take a fresh one for the retry.
            if let Ok(fresh) = Self::client() {
                client = fresh;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(std::time::Duration::from_secs(1));
        };
        match outcome {
            Ok(response) => {
                let status = StatusCode::from_u16(response.status().as_u16())
                    .unwrap_or(StatusCode::BAD_GATEWAY);
                let content_type = response
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                match response.bytes().await {
                    Ok(bytes) => {
                        let mut reply = (status, bytes).into_response();
                        if let Some(ct) = content_type
                            && let Ok(value) = ct.parse()
                        {
                            reply.headers_mut().insert("content-type", value);
                        }
                        if let Ok(value) = owner.id.parse() {
                            reply.headers_mut().insert(OWNER_HEADER, value);
                        }
                        reply
                    }
                    Err(error) => (
                        StatusCode::BAD_GATEWAY,
                        format!("owner {}: {error}", owner.id),
                    )
                        .into_response(),
                }
            }
            Err(error) => (
                StatusCode::BAD_GATEWAY,
                format!("owner {} unreachable: {error}", owner.id),
            )
                .into_response(),
        }
    }
}

/// Axum middleware: send a request for a stream to the member that owns it.
/// Local and single-node requests pass through untouched apart from the
/// owner header on the response.
pub async fn route_to_owner(
    axum::extract::State(s): axum::extract::State<Arc<crate::Service>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Some(cluster) = s.engine.as_ref().and_then(|e| e.cluster()).cloned() else {
        return next.run(request).await;
    };
    let path = request.uri().path().to_string();
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let routed = matches!(
        segments.as_slice(),
        ["v1", "table", _, ..] | ["v1", "streams", ..] | ["v1", "query"]
    );
    if !routed {
        return next.run(request).await;
    }
    // Forwarding carries this node's token, so authenticate first.
    if crate::authorize(&s, request.headers()).is_err() {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    if let Err(response) = cluster.check_fence(request.headers()) {
        return response;
    }
    let forwarded = request.headers().contains_key(FORWARDED_HEADER);
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|p| p.to_string())
        .unwrap_or(path.clone());
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, 512 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let stream = match segments.as_slice() {
        ["v1", "table", name, ..] => Some((*name).to_string()),
        ["v1", "streams", name, ..] => Some((*name).to_string()),
        ["v1", "streams"] => serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(str::to_string)),
        ["v1", "query"] => {
            let sql = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|v| v.get("sql").and_then(|q| q.as_str()).map(str::to_string));
            match sql.map(|q| walleye_lance::sql_table_names(&q)) {
                Some(Ok(tables)) => {
                    let mut owners: Vec<Option<Node>> =
                        tables.iter().map(|t| cluster.owner(t)).collect();
                    owners
                        .sort_by(|a, b| a.as_ref().map(|n| &n.id).cmp(&b.as_ref().map(|n| &n.id)));
                    owners.dedup_by(|a, b| a.as_ref().map(|n| &n.id) == b.as_ref().map(|n| &n.id));
                    match owners.as_slice() {
                        // Every referenced table is owned here, or the query
                        // names none: run it locally.
                        [] | [None] => None,
                        [Some(owner)] => Some(format!("\u{0}{}", owner.id)),
                        // Tables spread across members: this node runs the
                        // query and gathers the rows it does not own.
                        _ => None,
                    }
                }
                Some(Err(error)) => {
                    return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
                }
                None => None,
            }
        }
        _ => None,
    };
    let owner = match stream.as_deref() {
        Some(id) if id.starts_with('\u{0}') => {
            let id = &id[1..];
            cluster
                .ring
                .snapshot()
                .members()
                .iter()
                .find(|n| n.id == id)
                .cloned()
        }
        Some(name) => cluster.owner(name),
        None => None,
    };
    if let Some(owner) = owner {
        if forwarded {
            return (
                StatusCode::CONFLICT,
                format!("ownership moved to {} while forwarding; retry", owner.id),
            )
                .into_response();
        }
        return cluster
            .forward(
                &owner,
                method,
                &path_and_query,
                content_type.as_deref(),
                bytes,
            )
            .await;
    }
    let request = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes));
    let mut response = next.run(request).await;
    if let Ok(value) = cluster.node_id.parse() {
        response.headers_mut().insert(OWNER_HEADER, value);
    }
    response
}
