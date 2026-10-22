//! Who may call which route.
//!
//! Two kinds of caller hold a credential. The deployment's own token
//! (`WALLEYE_TOKEN`) belongs to the platform and to this node's peers, and may
//! call anything. Access tokens belong to customers: the control plane mints
//! them, keeps only their SHA-256, and publishes the active set as one object
//! in the instance's own bucket at [`ACCESS_OBJECT`]. Each carries some of
//! three scopes, and a route asks for exactly one.
//!
//! Every route is registered through [`Routes`], which takes the route's
//! requirement alongside its handler, and [`gate`] refuses any matched route
//! the table does not name. A route added to the router by hand, without a
//! requirement, is therefore refused rather than served unscoped.
//!
//! The published set is read at boot, polled every [`REFRESH_INTERVAL`] with a
//! conditional GET, and read again when a token arrives that the node does not
//! know, at most once per [`MISS_REFRESH_INTERVAL`] so a stream of bad tokens
//! cannot turn into a stream of object-store reads. Creating or revoking a
//! token touches nothing but that object, so neither restarts the node.
//!
//! The object looks like this; `expires_at` is Unix seconds or null:
//!
//! ```json
//! {"version":1,"tokens":[
//!   {"id":"tok_1","sha256":"<64 hex>","scopes":["data:read"],"expires_at":null}
//! ]}
//! ```
use axum::{
    Json, Router,
    extract::{MatchedPath, Request, State},
    handler::Handler,
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{MethodFilter, on},
};
use object_store::{GetOptions, ObjectStore, path::Path};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Where the control plane publishes the active token set, relative to the
/// deployment's root.
pub const ACCESS_OBJECT: &str = "_walleye/access.json";
/// How often a node asks whether the token set changed. A revoked token stops
/// working within this long.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(3);
/// The least time between two reads caused by an unrecognised token.
pub const MISS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// What a customer token may do. Each implies nothing about the others.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    /// List and describe tables and views, query, count, list indexes, stats.
    Read,
    /// Add rows.
    Write,
    /// Create and drop tables and views, build indexes, compact and flush.
    Manage,
}
impl Scope {
    pub const ALL: [Scope; 3] = [Scope::Read, Scope::Write, Scope::Manage];
    pub const fn name(self) -> &'static str {
        match self {
            Scope::Read => "data:read",
            Scope::Write => "data:write",
            Scope::Manage => "data:manage",
        }
    }
    fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|scope| scope.name() == name)
    }
    const fn bit(self) -> u8 {
        match self {
            Scope::Read => 1,
            Scope::Write => 2,
            Scope::Manage => 4,
        }
    }
}

/// A set of scopes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Scopes(u8);
impl Scopes {
    pub fn contains(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
    }
}
impl FromIterator<Scope> for Scopes {
    fn from_iter<I: IntoIterator<Item = Scope>>(scopes: I) -> Self {
        Scopes(scopes.into_iter().fold(0, |bits, scope| bits | scope.bit()))
    }
}

/// What one route asks of its caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Required {
    /// No credential: liveness and readiness probes.
    Open,
    /// The deployment's own token only: peer and operator routes.
    System,
    /// The deployment's token, or an access token holding this scope.
    Data(Scope),
}

/// One registered route and what it requires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub method: Method,
    pub path: &'static str,
    pub required: Required,
}

/// A router whose every route was registered with its requirement.
pub struct Routes<S> {
    router: Router<S>,
    rules: Vec<Rule>,
}
impl<S: Clone + Send + Sync + 'static> Default for Routes<S> {
    fn default() -> Self {
        Self {
            router: Router::new(),
            rules: Vec::new(),
        }
    }
}
impl<S: Clone + Send + Sync + 'static> Routes<S> {
    /// Register `handler` for one method on `path`, with what it requires.
    pub fn route<H, T>(
        mut self,
        method: Method,
        path: &'static str,
        required: Required,
        handler: H,
    ) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        let filter = MethodFilter::try_from(method.clone())
            .unwrap_or_else(|_| panic!("{method} {path} is not a routable method"));
        assert!(
            !self
                .rules
                .iter()
                .any(|rule| rule.method == method && rule.path == path),
            "{method} {path} is registered twice"
        );
        self.router = self.router.route(path, on(filter, handler));
        self.rules.push(Rule {
            method,
            path,
            required,
        });
        self
    }
    /// Apply something to the routes registered so far, such as a body limit.
    pub fn map(mut self, change: impl FnOnce(Router<S>) -> Router<S>) -> Self {
        self.router = change(self.router);
        self
    }
    pub fn merge(mut self, other: Routes<S>) -> Self {
        for rule in &other.rules {
            assert!(
                !self
                    .rules
                    .iter()
                    .any(|mine| mine.method == rule.method && mine.path == rule.path),
                "{} {} is registered twice",
                rule.method,
                rule.path
            );
        }
        self.router = self.router.merge(other.router);
        self.rules.extend(other.rules);
        self
    }
    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }
    pub fn into_parts(self) -> (Router<S>, Table) {
        let table = Table(
            self.rules
                .into_iter()
                .map(|rule| ((rule.method, rule.path), rule.required))
                .collect(),
        );
        (self.router, table)
    }
}

/// Every registered route's requirement, looked up by matched path.
pub struct Table(HashMap<(Method, &'static str), Required>);
impl Table {
    /// None for a route nobody gave a requirement, which the gate refuses.
    pub fn required(&self, method: &Method, path: &str) -> Option<Required> {
        // A HEAD is answered by the GET handler, so it asks what GET asks.
        let method = if method == Method::HEAD {
            &Method::GET
        } else {
            method
        };
        self.0
            .iter()
            .find(|((m, p), _)| m == method && *p == path)
            .map(|(_, required)| *required)
    }
}

/// Who presented a credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Principal {
    System,
    Token(Scopes),
}
impl Principal {
    pub fn allows(self, required: Required) -> bool {
        match (self, required) {
            (_, Required::Open) | (Principal::System, _) => true,
            (Principal::Token(_), Required::System) => false,
            (Principal::Token(scopes), Required::Data(scope)) => scopes.contains(scope),
        }
    }
}

/// Why a credential was not accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    Missing,
    Unknown,
    Expired,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Published {
    version: u32,
    tokens: Vec<PublishedToken>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedToken {
    id: String,
    sha256: String,
    scopes: Vec<String>,
    #[serde(default)]
    expires_at: Option<u64>,
}

struct Grant {
    scopes: Scopes,
    expires_at: Option<u64>,
}

type Grants = HashMap<[u8; 32], Grant>;

fn parse(bytes: &[u8]) -> Result<Grants, String> {
    let published: Published =
        serde_json::from_slice(bytes).map_err(|e| format!("unreadable: {e}"))?;
    if published.version != 1 {
        return Err(format!("version {} is not understood", published.version));
    }
    let mut grants = Grants::new();
    for token in published.tokens {
        let hash: [u8; 32] = hex::decode(&token.sha256)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| format!("token {} has no SHA-256", token.id))?;
        let scopes = token
            .scopes
            .iter()
            .map(|name| {
                Scope::parse(name).ok_or_else(|| format!("token {} names scope {name}", token.id))
            })
            .collect::<Result<Scopes, _>>()?;
        grants.insert(
            hash,
            Grant {
                scopes,
                expires_at: token.expires_at,
            },
        );
    }
    Ok(grants)
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The token a request presents: the LanceDB SDK's `x-api-key`, or a bearer.
pub fn presented(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
        })
}

/// The credentials this node accepts, kept current from the bucket.
pub struct Access {
    system: String,
    source: Option<(Arc<dyn ObjectStore>, Path)>,
    grants: std::sync::RwLock<Arc<Grants>>,
    /// The ETag of what `grants` was read from. Held across a read, so two
    /// refreshes never race each other to install an older answer.
    etag: tokio::sync::Mutex<Option<String>>,
    last_miss: std::sync::Mutex<Option<Instant>>,
}
impl Access {
    /// Accepts only the deployment's own token: a node with no bucket has
    /// nowhere to read customer tokens from.
    pub fn system_only(system: String) -> Self {
        Self {
            system,
            source: None,
            grants: Default::default(),
            etag: Default::default(),
            last_miss: Default::default(),
        }
    }
    /// Reads the published set once before answering anything. A set that
    /// has never been published is empty; one that cannot be read is an
    /// error, because a node serving with no idea who may call it is worse
    /// than a node that has not started.
    pub async fn open(
        system: String,
        store: Arc<dyn ObjectStore>,
        root: &Path,
    ) -> Result<Self, String> {
        let access = Self {
            source: Some((store, root.clone().join("_walleye").join("access.json"))),
            ..Self::system_only(system)
        };
        access.refresh().await?;
        Ok(access)
    }

    /// Read the published set again if it changed.
    pub async fn refresh(&self) -> Result<(), String> {
        let Some((store, path)) = &self.source else {
            return Ok(());
        };
        let mut etag = self.etag.lock().await;
        let options = GetOptions {
            if_none_match: etag.clone(),
            ..Default::default()
        };
        let (grants, tag) = match store.get_opts(path, options).await {
            Ok(result) => {
                let tag = result.meta.e_tag.clone();
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| format!("reading {ACCESS_OBJECT}: {e}"))?;
                (
                    parse(&bytes).map_err(|e| format!("{ACCESS_OBJECT} is {e}"))?,
                    tag,
                )
            }
            Err(object_store::Error::NotModified { .. }) => return Ok(()),
            Err(object_store::Error::NotFound { .. }) => (Grants::new(), None),
            Err(e) => return Err(format!("reading {ACCESS_OBJECT}: {e}")),
        };
        *self
            .grants
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(grants);
        *etag = tag;
        Ok(())
    }

    /// Keep the set current for the life of the process.
    pub fn spawn_refresh(self: Arc<Self>, every: Duration) {
        if self.source.is_none() {
            return;
        }
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if let Err(error) = self.refresh().await {
                    eprintln!("walleye.access stage=refresh outcome=error error={error}");
                }
            }
        });
    }

    fn lookup(&self, hash: &[u8; 32]) -> Option<Result<Principal, Refusal>> {
        let grants = self
            .grants
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        grants.get(hash).map(|grant| match grant.expires_at {
            Some(at) if now_seconds() >= at => Err(Refusal::Expired),
            _ => Ok(Principal::Token(grant.scopes)),
        })
    }

    /// Whether this unrecognised token may cost a read of the bucket now.
    fn may_refresh_on_miss(&self) -> bool {
        let mut last = self
            .last_miss
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        if last.is_some_and(|at| now.duration_since(at) < MISS_REFRESH_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }

    /// Who is calling, or why they are not let in.
    pub async fn identify(&self, headers: &HeaderMap) -> Result<Principal, Refusal> {
        let token = presented(headers).ok_or(Refusal::Missing)?;
        let system = self.system.as_bytes();
        if token.len() == system.len()
            && token
                .bytes()
                .zip(system.iter())
                .fold(0u8, |d, (a, b)| d | (a ^ b))
                == 0
        {
            return Ok(Principal::System);
        }
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        if let Some(found) = self.lookup(&hash) {
            return found;
        }
        // A token minted a moment ago is not in the last poll yet.
        if self.source.is_some() && self.may_refresh_on_miss() {
            if let Err(error) = self.refresh().await {
                eprintln!("walleye.access stage=miss outcome=error error={error}");
            }
            if let Some(found) = self.lookup(&hash) {
                return found;
            }
        }
        Err(Refusal::Unknown)
    }
}

fn refuse(status: StatusCode, message: String) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

/// The state the gate needs: the node's credentials and the route table.
#[derive(Clone)]
pub struct Gate {
    pub access: Arc<Access>,
    pub table: Arc<Table>,
}

/// Admit a request only when its route has a requirement and the caller
/// meets it. Runs before anything else looks at the request, including the
/// forward to a stream's owner, which carries this node's own token.
pub async fn gate(State(gate): State<Gate>, request: Request, next: Next) -> Response {
    // No matched path is a 404 or 405 the router answers itself.
    let Some(path) = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
    else {
        return next.run(request).await;
    };
    let method = request.method().clone();
    let required = match gate.table.required(&method, &path) {
        Some(required) => required,
        None => {
            return refuse(
                StatusCode::FORBIDDEN,
                format!("{method} {path} has no access rule, so nobody may call it"),
            );
        }
    };
    if required == Required::Open {
        return next.run(request).await;
    }
    let principal = match gate.access.identify(request.headers()).await {
        Ok(principal) => principal,
        Err(Refusal::Missing) => {
            return refuse(
                StatusCode::UNAUTHORIZED,
                "no token; send it as x-api-key or a bearer".into(),
            );
        }
        Err(Refusal::Unknown) => {
            return refuse(StatusCode::UNAUTHORIZED, "unauthorized".into());
        }
        Err(Refusal::Expired) => {
            return refuse(StatusCode::UNAUTHORIZED, "this token has expired".into());
        }
    };
    if !principal.allows(required) {
        let message = match required {
            Required::Data(scope) => format!("this token does not have {}", scope.name()),
            _ => "this route is for the platform only".into(),
        };
        return refuse(StatusCode::FORBIDDEN, message);
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, PutPayload, local::LocalFileSystem};

    const SYSTEM: &str = "deployment-secret-token";

    fn sha(token: &str) -> String {
        hex::encode(Sha256::digest(token.as_bytes()))
    }
    fn headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", token.parse().unwrap());
        headers
    }
    async fn publish(store: &LocalFileSystem, tokens: serde_json::Value) {
        store
            .put(
                &Path::from("_walleye/access.json"),
                PutPayload::from(
                    serde_json::to_vec(&serde_json::json!({"version":1,"tokens":tokens})).unwrap(),
                ),
            )
            .await
            .unwrap();
    }

    /// The whole route table, as the node serves it. Adding a route means
    /// adding its line here, which is where its scope gets decided.
    #[test]
    fn every_route_has_exactly_one_requirement() {
        use Required::*;
        use Scope::*;
        let expected: Vec<(&str, &str, Required)> = vec![
            ("GET", "/healthz", Open),
            ("GET", "/readyz", Open),
            ("GET", "/internal/cache/{key}", System),
            ("PUT", "/internal/cache/{key}", System),
            ("GET", "/internal/cache/stats", System),
            ("GET", "/internal/snapshot/{name}", System),
            ("POST", "/internal/cache/flush", System),
            ("GET", "/internal/ownership", System),
            ("GET", "/internal/alarms", System),
            ("GET", "/internal/version", System),
            ("POST", "/v1/streams", Data(Manage)),
            ("POST", "/v1/streams/{name}/events", Data(Write)),
            ("POST", "/v1/ingest/{source}", Data(Write)),
            ("POST", "/v1/query", Data(Read)),
            ("GET", "/v1/table/", Data(Read)),
            ("GET", "/v1/namespace/{namespace}/table/list", Data(Read)),
            ("POST", "/v1/table/{name}/create/", Data(Manage)),
            ("POST", "/v1/table/{name}/describe/", Data(Read)),
            ("POST", "/v1/table/{name}/drop/", Data(Manage)),
            ("POST", "/v1/table/{name}/insert/", Data(Write)),
            ("POST", "/v1/table/{name}/query/", Data(Read)),
            ("POST", "/v1/table/{name}/count_rows/", Data(Read)),
            ("POST", "/v1/table/{name}/create_index/", Data(Manage)),
            ("POST", "/v1/table/{name}/index/list/", Data(Read)),
            ("POST", "/v1/table/{name}/compact_lsm/", Data(Manage)),
            ("POST", "/v1/table/{name}/flush_lsm/", Data(Manage)),
            ("POST", "/v1/table/{name}/get_lsm_stats/", Data(Read)),
            ("GET", "/v1/view/", Data(Read)),
            ("POST", "/v1/view/{name}/create/", Data(Manage)),
            ("POST", "/v1/view/{name}/describe/", Data(Read)),
            ("POST", "/v1/view/{name}/drop/", Data(Manage)),
            ("POST", "/v1/view/{name}/refresh/", Data(Manage)),
            ("GET", "/v1/worker/{name}/", Data(Write)),
            ("POST", "/v1/worker/{name}/", Data(Write)),
            ("PUT", "/v1/worker/{name}/", Data(Write)),
            ("DELETE", "/v1/worker/{name}/", Data(Write)),
        ];
        let routes = crate::routes();
        let served: Vec<(String, &str, Required)> = routes
            .rules()
            .iter()
            .map(|rule| (rule.method.to_string(), rule.path, rule.required))
            .collect();
        let expected: Vec<(String, &str, Required)> = expected
            .into_iter()
            .map(|(m, p, r)| (m.to_string(), p, r))
            .collect();
        assert_eq!(served, expected);
    }

    #[test]
    fn a_route_without_a_requirement_is_refused() {
        let (_, table) = crate::routes().into_parts();
        assert_eq!(
            table.required(&Method::POST, "/v1/table/{name}/update/"),
            None
        );
        assert_eq!(table.required(&Method::DELETE, "/v1/query"), None);
        assert_eq!(
            table.required(&Method::HEAD, "/healthz"),
            Some(Required::Open)
        );
    }

    #[test]
    fn manage_implies_nothing_else() {
        let manage = Principal::Token([Scope::Manage].into_iter().collect());
        assert!(manage.allows(Required::Data(Scope::Manage)));
        assert!(!manage.allows(Required::Data(Scope::Read)));
        assert!(!manage.allows(Required::Data(Scope::Write)));
        assert!(!manage.allows(Required::System));
        let every = Principal::Token(Scope::ALL.into_iter().collect());
        assert!(!every.allows(Required::System));
        assert!(Principal::System.allows(Required::System));
    }

    #[tokio::test]
    async fn tokens_come_from_the_bucket_and_expire_on_their_own() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        publish(
            &store,
            serde_json::json!([
                {"id":"r","sha256":sha("reader"),"scopes":["data:read"],"expires_at":null},
                {"id":"old","sha256":sha("old"),"scopes":["data:read"],"expires_at":now_seconds() - 1},
            ]),
        )
        .await;
        let access = Access::open(SYSTEM.into(), store.clone(), &Path::default())
            .await
            .unwrap();
        assert_eq!(
            access.identify(&headers(SYSTEM)).await,
            Ok(Principal::System)
        );
        assert_eq!(
            access.identify(&headers("reader")).await,
            Ok(Principal::Token([Scope::Read].into_iter().collect()))
        );
        assert_eq!(
            access.identify(&headers("old")).await,
            Err(Refusal::Expired)
        );
        assert_eq!(
            access.identify(&HeaderMap::new()).await,
            Err(Refusal::Missing)
        );
        let mut bearer = HeaderMap::new();
        bearer.insert("authorization", "Bearer reader".parse().unwrap());
        assert!(access.identify(&bearer).await.is_ok());
    }

    #[tokio::test]
    async fn a_missing_set_admits_only_the_platform_and_a_bad_one_does_not_boot() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let access = Access::open(SYSTEM.into(), store.clone(), &Path::default())
            .await
            .unwrap();
        assert_eq!(
            access.identify(&headers(SYSTEM)).await,
            Ok(Principal::System)
        );
        assert_eq!(
            access.identify(&headers("reader")).await,
            Err(Refusal::Unknown)
        );
        publish(
            &store,
            serde_json::json!([{"id":"x","sha256":sha("x"),"scopes":["data:everything"]}]),
        )
        .await;
        assert!(
            Access::open(SYSTEM.into(), store, &Path::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_revoked_token_stops_at_the_next_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        publish(
            &store,
            serde_json::json!([{"id":"w","sha256":sha("writer"),"scopes":["data:write"]}]),
        )
        .await;
        let access = Arc::new(
            Access::open(SYSTEM.into(), store.clone(), &Path::default())
                .await
                .unwrap(),
        );
        access.clone().spawn_refresh(Duration::from_millis(50));
        assert!(access.identify(&headers("writer")).await.is_ok());
        publish(&store, serde_json::json!([])).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            access.identify(&headers("writer")).await,
            Err(Refusal::Unknown)
        );
    }

    #[tokio::test]
    async fn an_unknown_token_reads_the_bucket_at_most_once_a_second() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let access = Access::open(SYSTEM.into(), store.clone(), &Path::default())
            .await
            .unwrap();
        // A bad token spends the read allowance.
        assert_eq!(
            access.identify(&headers("nobody")).await,
            Err(Refusal::Unknown)
        );
        publish(
            &store,
            serde_json::json!([{"id":"n","sha256":sha("new"),"scopes":["data:read"]}]),
        )
        .await;
        // So a real token minted just after it waits for the next allowance.
        assert_eq!(
            access.identify(&headers("new")).await,
            Err(Refusal::Unknown)
        );
        tokio::time::sleep(MISS_REFRESH_INTERVAL + Duration::from_millis(50)).await;
        assert!(access.identify(&headers("new")).await.is_ok());
    }
}
