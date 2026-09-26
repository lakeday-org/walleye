//! Reaching the process that owns a table. Who that is comes from
//! [`crate::ownership`]: every table has one owner with a live lease, and a
//! request that reaches any other process is forwarded to it. The fencing is
//! the owner's Lance MemWAL writer epoch, which is the epoch in the table's
//! ownership record; a forward that races an ownership change is answered
//! with 409 and a route-error header, and the forwarder asks again.
#![allow(clippy::result_large_err)]
use crate::ownership::{Peer, Route};
use axum::{
    body::Bytes,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// Marks a request as already forwarded once; a second hop is refused.
pub const FORWARDED_HEADER: &str = "x-walleye-forwarded";
/// Which process answered, for observability and tests.
pub const OWNER_HEADER: &str = "x-walleye-owner";
/// Why a request could not be served where it landed:
///
/// - `stale-owner` (409): the process it reached does not own the table and
///   did nothing with it. Send it to the owner.
/// - `lost-ownership` (409): the process owned the table when the request
///   started and not when it would have acknowledged it. The outcome is
///   unknown; the new owner replays whatever reached the log.
/// - `no-owner` (503): nobody owns the table and this process cannot take it
///   yet. `Retry-After` says when to come back.
/// - `owner-unreachable` (503): the owner's lease has not lapsed but it does
///   not accept connections. Nothing was delivered.
/// - `owner-lost` (503): the owner stopped answering with the request in
///   flight and its lease then lapsed. The outcome is unknown.
pub const ROUTE_ERROR_HEADER: &str = "x-walleye-route-error";
/// How long a forward waits out an owner that is starting up before it
/// answers 503. Nodes converge on the same quorum, so the skew between a
/// ready forwarder and its owner is seconds.
pub const FORWARD_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(15);

/// This process does not own the table, and refused before doing anything:
/// another live process owns it (named when known), or this one lost it
/// before its writer opened.
#[derive(Debug)]
pub struct NotOwner {
    pub table: String,
    pub owner: Option<Peer>,
}
impl std::fmt::Display for NotOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.owner {
            Some(peer) => write!(
                f,
                "table {} is owned by {} at {}",
                self.table, peer.node, peer.addr
            ),
            None => write!(f, "this process does not own table {}", self.table),
        }
    }
}
impl std::error::Error for NotOwner {}

/// This process stopped owning the table while the request was in flight,
/// so it does not acknowledge it. The rows may or may not have reached the
/// log; the new owner replays whatever did, and a retry of rows with a
/// primary key collapses onto them.
#[derive(Debug)]
pub struct StaleOwner(pub String);
impl std::fmt::Display for StaleOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ownership of {} moved while the request was in flight; its outcome is unknown. \
             Retry: the request reaches the new owner, and rows with a primary key are not \
             stored twice",
            self.0
        )
    }
}
impl std::error::Error for StaleOwner {}

/// Nobody owns the table and this process cannot take it yet.
#[derive(Debug)]
pub struct NoOwner {
    pub table: String,
    pub retry_after: std::time::Duration,
}
impl std::fmt::Display for NoOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "no live process owns {} and this one cannot take it yet; retry",
            self.table
        )
    }
}
impl std::error::Error for NoOwner {}

/// Marks a response refused before anything was applied, so the routing
/// layer may send the same request on to whoever owns the table now.
#[derive(Clone, Copy, Debug)]
pub struct Refused;

/// The answer for an error that says where a table is not: 409 with
/// `stale-owner`, or 503 with `Retry-After` and `no-owner`. `None` for any
/// other error.
pub fn route_error(error: &(dyn std::error::Error + 'static)) -> Option<Response> {
    if let Some(not) = error.downcast_ref::<NotOwner>() {
        let mut response = stale_owner(&not.to_string());
        response.extensions_mut().insert(Refused);
        return Some(response);
    }
    if let Some(stale) = error.downcast_ref::<StaleOwner>() {
        return Some(
            (
                StatusCode::CONFLICT,
                [(ROUTE_ERROR_HEADER, "lost-ownership")],
                stale.to_string(),
            )
                .into_response(),
        );
    }
    if let Some(none) = error.downcast_ref::<NoOwner>() {
        return Some(unavailable("no-owner", none.retry_after, &none.to_string()));
    }
    None
}

pub fn stale_owner(message: &str) -> Response {
    (
        StatusCode::CONFLICT,
        [(ROUTE_ERROR_HEADER, "stale-owner")],
        message.to_owned(),
    )
        .into_response()
}

pub fn unavailable(
    reason: &'static str,
    retry_after: std::time::Duration,
    message: &str,
) -> Response {
    let seconds = retry_after.as_millis().div_ceil(1000).max(1).to_string();
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [
            ("retry-after", seconds.as_str()),
            (ROUTE_ERROR_HEADER, reason),
        ],
        message.to_owned(),
    )
        .into_response()
}

#[derive(Clone)]
pub struct Cluster {
    /// This process's configured id; its session name is the ownership node.
    pub node_id: String,
    /// Where peers reach this process.
    pub endpoint: String,
    pub token: String,
    client: reqwest::Client,
}
impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("node_id", &self.node_id)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}
impl Cluster {
    pub fn new(node_id: String, endpoint: String, token: String) -> Result<Self, reqwest::Error> {
        Ok(Self {
            node_id,
            endpoint,
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
            .default_headers(crate::edge::peer_headers())
            .connect_timeout(std::time::Duration::from_secs(2))
            .timeout(std::time::Duration::from_secs(120))
            .pool_idle_timeout(std::time::Duration::from_secs(5))
            .build()
    }
    /// Fetch every row of `stream` from the member that owns it, as an Arrow
    /// IPC file, for a query that spans owners.
    pub async fn fetch_snapshot(&self, owner: &Peer, stream: &str) -> Result<bytes::Bytes, String> {
        let url = format!(
            "{}/internal/snapshot/{stream}",
            owner.addr.trim_end_matches('/')
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| format!("owner {} is unreachable: {error}", owner.node))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!(
                "owner {} refused a snapshot of {stream} with {status}: {body}",
                owner.node
            ));
        }
        response
            .bytes()
            .await
            .map_err(|error| format!("owner {} truncated {stream}: {error}", owner.node))
    }
    /// Run one statement on the member that owns the table it reads, and take
    /// its rows.
    ///
    /// The alternative is [`Self::fetch_snapshot`], which drags every row of
    /// the table across the network so this node can filter it. For a
    /// statement naming a single table, asking its owner to run the statement
    /// moves the answer rather than the table.
    /// Append rows to a table another process owns, through that process's
    /// own insert route. Returns the table version it reports.
    pub async fn insert(
        &self,
        owner: &Peer,
        table: &str,
        batches: &[arrow_array::RecordBatch],
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let Some(first) = batches.first() else {
            return Err("nothing to insert".into());
        };
        let mut body = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &first.schema())?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        let url = format!(
            "{}/v1/table/{table}/insert/",
            owner.addr.trim_end_matches('/')
        );
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .header(FORWARDED_HEADER, "1")
            .header("content-type", "application/vnd.apache.arrow.stream")
            .body(body)
            .send()
            .await
            .map_err(|error| format!("owner {} is unreachable: {error}", owner.node))?;
        let status = response.status();
        let route_error = response
            .headers()
            .get(ROUTE_ERROR_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let text = response.text().await.unwrap_or_default();
        if status.is_success() {
            let answer: serde_json::Value = serde_json::from_str(&text)?;
            return Ok(answer["version"].as_u64().unwrap_or(0));
        }
        if route_error.as_deref() == Some("stale-owner") {
            return Err(Box::new(NotOwner {
                table: table.to_owned(),
                owner: None,
            }));
        }
        Err(format!(
            "owner {} refused the insert into {table} with {status}: {text}",
            owner.node
        )
        .into())
    }
    pub async fn run_sql(&self, owner: &Peer, sql: &str) -> Result<bytes::Bytes, String> {
        let url = format!("{}/v1/query", owner.addr.trim_end_matches('/'));
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .header(FORWARDED_HEADER, "1")
            .json(&serde_json::json!({"sql": sql}))
            .send()
            .await
            .map_err(|error| format!("owner {} is unreachable: {error}", owner.node))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!(
                "owner {} refused the query with {status}: {body}",
                owner.node
            ));
        }
        response
            .bytes()
            .await
            .map_err(|error| format!("owner {} truncated the answer: {error}", owner.node))
    }

    /// Relay one request to `owner` and return its response verbatim.
    ///
    /// A 503 from an owner still reaching its quorum is retried for a while,
    /// and a connect failure twice, quickly: neither delivered anything. An
    /// owner that still refuses connections is answered 503 with
    /// `Retry-After` set to `verdict_in`, when its lease may be judged
    /// lapsed and the table claimed.
    ///
    /// The forward gives up when `owner_lost` resolves, which the caller ties
    /// to the owner's lease lapsing on this node's clock: an owner paused with
    /// the request in its socket buffer would otherwise hold it for the whole
    /// client timeout. By then the owner has stopped acting as one, so if it
    /// resumes it refuses rather than applies what it was sent.
    #[allow(clippy::too_many_arguments)]
    pub async fn forward(
        &self,
        owner: &Peer,
        method: Method,
        path_and_query: &str,
        content_type: Option<&str>,
        body: Bytes,
        verdict_in: std::time::Duration,
        owner_lost: impl std::future::Future<Output = ()>,
    ) -> Response {
        let url = format!("{}{}", owner.addr.trim_end_matches('/'), path_and_query);
        let build = |client: &reqwest::Client| {
            let mut request = client
                .request(method.clone(), &url)
                .bearer_auth(&self.token)
                .header(FORWARDED_HEADER, "1")
                .body(body.clone());
            if let Some(content_type) = content_type {
                request = request.header("content-type", content_type);
            }
            request
        };
        let deadline = std::time::Instant::now() + FORWARD_RETRY_WINDOW;
        let attempts = async {
            let mut delay = std::time::Duration::from_millis(100);
            let mut client = self.client.clone();
            let mut attempts = 0_u32;
            let mut failed = 0_u32;
            loop {
                attempts += 1;
                let outcome = build(&client).send().await;
                let retryable = match &outcome {
                    Err(error)
                        if error.is_connect() || (error.is_request() && !error.is_timeout()) =>
                    {
                        failed += 1;
                        failed < 3
                    }
                    Err(_) => false,
                    Ok(response) => {
                        response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE
                            && response.headers().get(ROUTE_ERROR_HEADER).is_none()
                    }
                };
                if !retryable || std::time::Instant::now() >= deadline {
                    if attempts > 1 {
                        eprintln!(
                            "walleye.forward owner={} attempts={attempts} settled",
                            owner.node
                        );
                    }
                    return outcome;
                }
                // A pooled connection may belong to the peer's previous
                // incarnation; take a fresh one for the retry.
                if let Ok(fresh) = Self::client() {
                    client = fresh;
                }
                // A connection that failed is retried at once on a fresh
                // one; only an owner that answered 503 is given time.
                if outcome.is_ok() {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(1));
                }
            }
        };
        let outcome = tokio::select! {
            outcome = attempts => outcome,
            () = owner_lost => {
                return unavailable(
                    "owner-lost",
                    std::time::Duration::from_secs(1),
                    &format!(
                        "owner {} stopped answering and its lease lapsed; the outcome of this \
                         request is unknown, and a retry reaches whoever owns the table now",
                        owner.node
                    ),
                );
            }
        };
        match outcome {
            Ok(response) => {
                let status = StatusCode::from_u16(response.status().as_u16())
                    .unwrap_or(StatusCode::BAD_GATEWAY);
                let mut headers = HeaderMap::new();
                for name in [
                    "content-type",
                    "retry-after",
                    ROUTE_ERROR_HEADER,
                    OWNER_HEADER,
                ] {
                    if let Some(value) = response.headers().get(name)
                        && let Ok(value) = value.to_str()
                        && let Ok(value) = value.parse()
                    {
                        headers.insert(name, value);
                    }
                }
                match response.bytes().await {
                    Ok(bytes) => {
                        let mut reply = (status, bytes).into_response();
                        reply.headers_mut().extend(headers);
                        if !reply.headers().contains_key(OWNER_HEADER)
                            && let Ok(value) = owner.node.parse()
                        {
                            reply.headers_mut().insert(OWNER_HEADER, value);
                        }
                        reply
                    }
                    Err(error) => (
                        StatusCode::BAD_GATEWAY,
                        format!("owner {}: {error}", owner.node),
                    )
                        .into_response(),
                }
            }
            // Nothing was delivered: the owner's lease has not lapsed yet, so
            // nobody else may take the table, and the caller should come back.
            Err(error) if error.is_connect() => unavailable(
                "owner-unreachable",
                verdict_in,
                &format!(
                    "owner {} does not accept connections and its lease has not lapsed yet: \
                     {error}",
                    owner.node
                ),
            ),
            Err(error) => (
                StatusCode::BAD_GATEWAY,
                format!("owner {} unreachable: {error}", owner.node),
            )
                .into_response(),
        }
    }
}

/// The table a request is about, when it is about exactly one.
enum Target {
    Table(String),
    /// A statement over several tables runs here and gathers the rest.
    Local,
}

/// Axum middleware: send a request for a table to the process that owns it,
/// claiming the table here if nobody does.
pub async fn route_to_owner(
    axum::extract::State(s): axum::extract::State<Arc<crate::Service>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Some(engine) = s.engine() else {
        return next.run(request).await;
    };
    let path = request.uri().path().to_string();
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let routed = matches!(
        segments.as_slice(),
        ["v1", "table", _, ..]
            | ["v1", "streams", ..]
            | ["v1", "query"]
            | ["v1", "view", _, "refresh"]
            | ["v1", "worker", _, ..]
    );
    if !routed {
        return next.run(request).await;
    }
    // Forwarding carries this node's token; the access gate in front of this
    // has already admitted the caller for this route.
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
    let target = match segments.as_slice() {
        ["v1", "table", name, ..] => Target::Table((*name).to_string()),
        // A view's refresh and its worker's requests run where its alarms
        // live: on the owner of the view's key.
        ["v1", "view", name, "refresh"] | ["v1", "worker", name, ..] => {
            match engine.view(name).await {
                Ok(view) => Target::Table(crate::engine::driver_key(&view)),
                Err(_) => Target::Local,
            }
        }
        ["v1", "streams", name, ..] => Target::Table((*name).to_string()),
        ["v1", "streams"] => match serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(str::to_string))
        {
            Some(name) => Target::Table(name),
            None => Target::Local,
        },
        ["v1", "query"] => {
            let sql = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|v| v.get("sql").and_then(|q| q.as_str()).map(str::to_string));
            match sql.map(|q| walleye_lance::sql_table_names(&q)) {
                Some(Ok(tables)) if tables.len() == 1 => {
                    Target::Table(tables.into_iter().next().expect("one table"))
                }
                Some(Ok(_)) | None => Target::Local,
                Some(Err(error)) => {
                    return (StatusCode::BAD_REQUEST, error.to_string()).into_response();
                }
            }
        }
        _ => Target::Local,
    };
    let local = |parts: axum::http::request::Parts, bytes: Bytes| {
        let next = next.clone();
        async move {
            let request = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes));
            let mut response = next.run(request).await;
            if let Ok(value) = engine.ownership().node().parse() {
                response.headers_mut().insert(OWNER_HEADER, value);
            }
            response
        }
    };
    // A name that cannot be a table is the handler's to refuse; it must not
    // become an ownership record.
    let table = match target {
        Target::Table(table) if crate::engine::valid_key(&table) => table,
        _ => return local(parts, bytes).await,
    };
    // A request that finds the table moved - a forward that reaches a former
    // owner, or a local attempt refused before it applied anything - asks the
    // bucket again and goes once more.
    let mut fresh = forwarded;
    for attempt in 0..2 {
        let last = attempt == 1;
        let routing = std::time::Instant::now();
        let route = engine.route_request(&table, fresh, forwarded).await;
        slow_step(&table, attempt, forwarded, "route", routing, None);
        match route {
            Ok(Route::Local { .. }) => {
                let serving = std::time::Instant::now();
                let response = local(parts.clone(), bytes.clone()).await;
                slow_step(
                    &table,
                    attempt,
                    forwarded,
                    "local",
                    serving,
                    Some(&response),
                );
                if !last && !forwarded && response.extensions().get::<Refused>().is_some() {
                    fresh = true;
                    continue;
                }
                return response;
            }
            Ok(Route::Unowned) => {
                return unavailable(
                    "no-owner",
                    engine.ownership().config().sample(),
                    &format!("no live process owns {table} and this one cannot take it yet"),
                );
            }
            Ok(Route::Remote { peer, .. }) if forwarded => {
                return stale_owner(&format!(
                    "this process does not own {table}; {} does",
                    peer.node
                ));
            }
            Ok(Route::Remote { peer, verdict_in }) => {
                let forwarding = std::time::Instant::now();
                let response = engine
                    .cluster()
                    .forward(
                        &peer,
                        method.clone(),
                        &path_and_query,
                        content_type.as_deref(),
                        bytes.clone(),
                        verdict_in,
                        engine.lease_lapses(peer.node.clone()),
                    )
                    .await;
                slow_step(
                    &table,
                    attempt,
                    forwarded,
                    &format!("forward to={}", peer.node),
                    forwarding,
                    Some(&response),
                );
                // Each says nothing was applied and the owner was not there:
                // it no longer owns the table, it could not be reached, or it
                // let the table go and knows nobody to send it to. Ask again;
                // this process may take the table itself.
                let moved = response.headers().get(ROUTE_ERROR_HEADER).is_some_and(|v| {
                    v == "stale-owner" || v == "owner-unreachable" || v == "no-owner"
                });
                if !last && moved {
                    fresh = true;
                    continue;
                }
                return response;
            }
            Err(error) => {
                return match route_error(&*error) {
                    Some(response) => response,
                    None => unavailable(
                        "no-owner",
                        std::time::Duration::from_secs(1),
                        &format!("could not read who owns {table}: {error}"),
                    ),
                };
            }
        }
    }
    unreachable!("the second attempt always returns")
}

/// Say so when one step of routing a request took over a second: which step,
/// and what it answered. A handover that leaves a write waiting shows here
/// as the process it waited on and why.
fn slow_step(
    table: &str,
    attempt: usize,
    forwarded: bool,
    step: &str,
    started: std::time::Instant,
    response: Option<&Response>,
) {
    let elapsed = started.elapsed();
    if elapsed < std::time::Duration::from_secs(1) {
        return;
    }
    let (status, route_error) = response.map_or((0, String::new()), |response| {
        (
            response.status().as_u16(),
            response
                .headers()
                .get(ROUTE_ERROR_HEADER)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_owned(),
        )
    });
    eprintln!(
        "walleye.route slow table={table} step={step} attempt={attempt} forwarded={forwarded} \
         status={status} route_error={route_error} elapsed_ms={}",
        elapsed.as_millis()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three ways a request finds its table is not here each answer with
    /// a status a client can act on, and say which way in a header.
    #[test]
    fn route_errors_say_what_to_do_next() {
        let refused = route_error(&NotOwner {
            table: "t".into(),
            owner: None,
        })
        .unwrap();
        assert_eq!(refused.status(), StatusCode::CONFLICT);
        assert_eq!(refused.headers()[ROUTE_ERROR_HEADER], "stale-owner");
        assert!(
            refused.extensions().get::<Refused>().is_some(),
            "nothing was applied, so the request may go on to the owner"
        );

        let lost = route_error(&StaleOwner("t".into())).unwrap();
        assert_eq!(lost.status(), StatusCode::CONFLICT);
        assert_eq!(lost.headers()[ROUTE_ERROR_HEADER], "lost-ownership");
        assert!(
            lost.extensions().get::<Refused>().is_none(),
            "its outcome is unknown, so it is not re-sent for the caller"
        );

        let none = route_error(&NoOwner {
            table: "t".into(),
            retry_after: std::time::Duration::from_millis(1_200),
        })
        .unwrap();
        assert_eq!(none.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(none.headers()[ROUTE_ERROR_HEADER], "no-owner");
        assert_eq!(none.headers()["retry-after"], "2");

        assert!(route_error(&std::io::Error::other("anything else")).is_none());
    }
}
