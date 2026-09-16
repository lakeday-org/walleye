//! Single-deployment stream API, Foyer peer service, and Bitr node composition.
pub mod cluster;
mod engine;
mod lancedb;
mod processor;
pub use processor::ProcessorConfig;
pub mod kubernetes;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
pub use engine::{
    ApiConfig, Column, HIDDEN_PK, PK_METADATA_KEY, StreamDefinition, TableExists, TableNotFound,
};
use lance_core::cache::{CacheBackend, InternalCacheKey};
use serde::Deserialize;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use walleye_cache::{LanceFoyerCacheBackend, PeerConfig};
use walleye_ring::{Membership, Node};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node_id: String,
    pub listen: String,
    pub directory: PathBuf,
    pub memory_bytes: usize,
    pub disk_bytes: usize,
    pub token: String,
    pub bitr: bool,
    pub members: Vec<Node>,
    #[serde(default)]
    pub api: Option<ApiConfig>,
    #[serde(default)]
    pub kubernetes: Option<kubernetes::DiscoveryConfig>,
    #[serde(default)]
    pub processor: Option<ProcessorConfig>,
}
impl Config {
    /// Build a single node, or a static cluster member, from environment variables.
    ///
    /// Required: `WALLEYE_BUCKET` (bucket name, optionally `bucket/prefix`) or
    /// `WALLEYE_ROOT_URI` (a full `s3://` or `file://` URI).
    ///
    /// Optional: `WALLEYE_PORT` (8080), `WALLEYE_TOKEN` (generated and printed
    /// when absent), `WALLEYE_DIR` (`./walleye-cache`), `WALLEYE_RAM_GB` (1),
    /// `WALLEYE_NVME_GB` (8), `WALLEYE_BITR_URL` (enables Bitr cluster mode),
    /// `WALLEYE_MEMBERS` (`id=http://host:8080,...`) with `WALLEYE_NODE_ID`
    /// naming this member.
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Self::from_env_with(|name| std::env::var(name).ok())
    }
    pub fn from_env_with(
        mut get: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let root_uri = match (get("WALLEYE_ROOT_URI"), get("WALLEYE_BUCKET")) {
            (Some(uri), _) => uri,
            (None, Some(bucket)) => format!("s3://{}", bucket.trim_matches('/')),
            (None, None) => return Err("set WALLEYE_BUCKET or WALLEYE_ROOT_URI".into()),
        };
        let port: u16 = get("WALLEYE_PORT").as_deref().unwrap_or("8080").parse()?;
        let gb = |name: &str, default: f64, get: &mut dyn FnMut(&str) -> Option<String>| {
            get(name)
                .map(|v| v.parse::<f64>())
                .transpose()
                .map(|v| (v.unwrap_or(default) * 1024.0 * 1024.0 * 1024.0) as usize)
        };
        let memory_bytes = gb("WALLEYE_RAM_GB", 1.0, &mut get)?;
        let disk_bytes = gb("WALLEYE_NVME_GB", 8.0, &mut get)?;
        let token = match get("WALLEYE_TOKEN") {
            Some(token) => token,
            None => {
                let token = uuid::Uuid::new_v4().simple().to_string();
                eprintln!("WALLEYE_TOKEN not set; generated token: {token}");
                token
            }
        };
        let (node_id, members) = match get("WALLEYE_MEMBERS") {
            Some(list) => {
                let node_id = get("WALLEYE_NODE_ID")
                    .ok_or("WALLEYE_NODE_ID is required with WALLEYE_MEMBERS")?;
                let members = list
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|entry| {
                        let (id, endpoint) = entry
                            .split_once('=')
                            .ok_or("WALLEYE_MEMBERS entries are id=http://host:port")?;
                        Ok(Node::new(id, endpoint, 1.0)?)
                    })
                    .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
                (node_id, members)
            }
            None => {
                let node_id = get("WALLEYE_NODE_ID").unwrap_or_else(|| "single".into());
                let node = Node::new(&node_id, format!("http://localhost:{port}"), 1.0)?;
                (node_id, vec![node])
            }
        };
        let bitr_url = get("WALLEYE_BITR_URL");
        Ok(Config {
            node_id,
            listen: format!("0.0.0.0:{port}"),
            directory: PathBuf::from(
                get("WALLEYE_DIR").unwrap_or_else(|| "./walleye-cache".into()),
            ),
            memory_bytes,
            disk_bytes,
            token,
            bitr: bitr_url.is_some(),
            members,
            api: Some(ApiConfig { root_uri, bitr_url }),
            kubernetes: None,
            processor: None,
        })
    }
}
pub struct Service {
    pub config: Config,
    pub cache: Arc<LanceFoyerCacheBackend>,
    ring: Arc<Membership>,
    engine: Option<engine::Engine>,
    hits: AtomicU64,
    misses: AtomicU64,
    stores: AtomicU64,
    pub(crate) revision: tokio::sync::Mutex<String>,
    pub(crate) changed: tokio::sync::Notify,
    quiescing: AtomicBool,
}
impl Service {
    pub async fn open(config: Config) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let ring = Arc::new(Membership::new(config.members.clone())?);
        if config.token.len() < 16
            || !ring
                .snapshot()
                .members()
                .iter()
                .any(|n| n.id == config.node_id)
        {
            return Err("invalid deployment token or node membership".into());
        }
        let cache = Arc::new(
            LanceFoyerCacheBackend::new(
                &config.directory,
                config.memory_bytes,
                config.disk_bytes,
                "walleye",
            )
            .await?,
        );
        let engine = if let Some(api) = config.api.clone() {
            let params = engine::storage_params();
            let peers =
                (config.members.len() > 1 || config.kubernetes.is_some()).then(|| PeerConfig {
                    token: config.token.clone(),
                    ring: ring.clone(),
                });
            let cached = walleye_lance::CachedStorage::from_backend(
                cache.clone(),
                &api.root_uri,
                params.clone(),
                peers,
            )
            .await?;
            let cluster = (config.members.len() > 1 || config.kubernetes.is_some())
                .then(|| {
                    cluster::Cluster::new(
                        config.node_id.clone(),
                        ring.clone(),
                        config.token.clone(),
                    )
                })
                .transpose()?;
            Some(
                engine::Engine::open(api, cached, params, cluster)
                    .await
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        let service = Arc::new(Self {
            config,
            cache,
            ring,
            engine,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            stores: AtomicU64::new(0),
            revision: tokio::sync::Mutex::new(format!("\"{}\"", uuid::Uuid::new_v4())),
            changed: tokio::sync::Notify::new(),
            quiescing: AtomicBool::new(false),
        });
        // Serve immediately; owned streams open and warm in the background so
        // the first request does not pay for it.
        let warming = service.clone();
        tokio::spawn(async move {
            if let Some(engine) = &warming.engine {
                engine.warm().await;
            }
        });
        Ok(service)
    }
    /// Follow Kubernetes cache endpoints. Failure preserves the last good ring;
    /// missing peers fall back to the query process's normal origin loading.
    pub async fn discover(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.config.kubernetes {
            Some(config) => kubernetes::follow(config, self.ring.clone()).await,
            None => std::future::pending().await,
        }
    }
    /// Stops new processor delivery while the API remains open for in-flight commits.
    pub fn quiesce(&self) {
        self.quiescing.store(true, Ordering::Release);
        self.changed.notify_one();
    }
    pub async fn close(&self) {
        if let Some(e) = &self.engine {
            e.close().await;
        }
        let _ = self.cache.close().await;
    }
}
pub fn router(service: Arc<Service>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/internal/cache/{key}", get(read).put(write))
        .route("/internal/cache/stats", get(stats))
        .route("/internal/cache/flush", post(flush))
        .route("/v1/streams", post(define))
        .route("/v1/streams/{name}/events", post(ingest))
        .route("/v1/query", post(query))
        .layer(DefaultBodyLimit::max(8 * 1024 * 1024))
        .merge(lancedb::routes())
        .layer(axum::middleware::from_fn_with_state(
            service.clone(),
            cluster::route_to_owner,
        ))
        .with_state(service)
}
/// Accepts either `Authorization: Bearer <token>` or the LanceDB SDK's `x-api-key`.
pub(crate) fn authorize(s: &Service, headers: &HeaderMap) -> Result<(), StatusCode> {
    let presented = match headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        Some(key) => key.to_string(),
        None => headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or(StatusCode::UNAUTHORIZED)?
            .to_string(),
    };
    let expected = &s.config.token;
    if presented.len() != expected.len()
        || presented
            .bytes()
            .zip(expected.bytes())
            .fold(0u8, |d, (a, b)| d | (a ^ b))
            != 0
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}
fn key(value: &str) -> Result<InternalCacheKey, StatusCode> {
    Ok(InternalCacheKey::from_bytes(
        hex::decode(value)
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .try_into()
            .map_err(|_| StatusCode::BAD_REQUEST)?,
    ))
}
async fn read(
    State(s): State<Arc<Service>>,
    Path(k): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    authorize(&s, &headers)?;
    if let Some(bytes) = s.cache.export_entry(&key(&k)?).await {
        s.hits.fetch_add(1, Ordering::Relaxed);
        Ok(bytes.into_response())
    } else {
        s.misses.fetch_add(1, Ordering::Relaxed);
        Err(StatusCode::NOT_FOUND)
    }
}
async fn write(
    State(s): State<Arc<Service>>,
    Path(k): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    authorize(&s, &headers)?;
    let key = key(&k)?;
    if s.ring.snapshot().owner(key.as_bytes()).id != s.config.node_id {
        return Err(StatusCode::CONFLICT);
    }
    if !s.cache.import_entry(&key, body).await {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    s.stores.fetch_add(1, Ordering::Relaxed);
    Ok(StatusCode::NO_CONTENT)
}
async fn stats(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    authorize(&s, &headers)?;
    Ok(Json(
        serde_json::json!({"node":s.config.node_id,"members":s.ring.snapshot().members(),"membership_epoch":s.ring.snapshot().epoch(),"hits":s.hits.load(Ordering::Relaxed),"misses":s.misses.load(Ordering::Relaxed),"stores":s.stores.load(Ordering::Relaxed),"entries":s.cache.num_entries().await,"memory_usage":s.cache.memory_usage(),"memory_capacity":s.cache.memory_capacity(),"disk_capacity":s.cache.persistent_capacity()}),
    ))
}
async fn flush(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    authorize(&s, &headers)?;
    s.cache.flush().await;
    Ok(StatusCode::NO_CONTENT)
}
type ApiError = (StatusCode, Json<serde_json::Value>);
fn failure(e: impl std::fmt::Display) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"error":e.to_string()})),
    )
}
pub(crate) fn api<'a>(s: &'a Service, h: &HeaderMap) -> Result<&'a engine::Engine, ApiError> {
    authorize(s, h).map_err(|code| (code, Json(serde_json::json!({"error":"unauthorized"}))))?;
    s.engine.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"error":"use the configured stream ingress"})),
    ))
}
async fn define(
    State(s): State<Arc<Service>>,
    h: HeaderMap,
    Json(def): Json<StreamDefinition>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let name = def.name.clone();
    let engine = api(&s, &h)?;
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    engine.define(def).await.map_err(failure)?;
    Ok(Json(serde_json::json!({"stream":name})))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ingest {
    rows: Vec<serde_json::Value>,
}
async fn ingest(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    Json(input): Json<Ingest>,
) -> Result<Response, ApiError> {
    let engine = api(&s, &h)?;
    let mut revision = s.revision.lock().await;
    if let Some(expected) = h.get("if-match") {
        if expected.to_str().ok() != Some(revision.as_str()) {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                Json(serde_json::json!({"error":"stream snapshot changed"})),
            ));
        }
        // Conditional processor commits fit one atomic Lance WAL batch.
        if input.rows.len() > 1024 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error":"conditional ingestion is limited to 1024 rows"})),
            ));
        }
    }
    // Invalidate snapshots before attempting ingestion, including a partial failure.
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    let count = engine.ingest(&name, input.rows).await.map_err(failure)?;
    s.changed.notify_one();
    Ok((
        [("etag", revision.as_str())],
        Json(serde_json::json!({"stream":name,"ingested":count})),
    )
        .into_response())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    sql: String,
}
async fn query(
    State(s): State<Arc<Service>>,
    h: HeaderMap,
    Json(input): Json<Query>,
) -> Result<Response, ApiError> {
    let engine = api(&s, &h)?;
    let revision = s.revision.lock().await;
    let bytes = engine.query(&input.sql).await.map_err(failure)?;
    Ok((
        [
            ("content-type", "application/json"),
            ("etag", revision.as_str()),
        ],
        bytes,
    )
        .into_response())
}

#[cfg(test)]
mod config_tests {
    use super::Config;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl FnMut(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn bucket_alone_is_enough() {
        let c = Config::from_env_with(env(&[("WALLEYE_BUCKET", "walleye")])).unwrap();
        assert_eq!(c.listen, "0.0.0.0:8080");
        assert_eq!(c.node_id, "single");
        assert_eq!(c.members.len(), 1);
        assert_eq!(c.members[0].endpoint, "http://localhost:8080");
        assert_eq!(c.memory_bytes, 1 << 30);
        assert_eq!(c.disk_bytes, 8 << 30);
        assert!(!c.bitr);
        assert!(c.token.len() >= 16);
        let api = c.api.unwrap();
        assert_eq!(api.root_uri, "s3://walleye");
        assert!(api.bitr_url.is_none());
    }

    #[test]
    fn overrides_and_cluster() {
        let c = Config::from_env_with(env(&[
            ("WALLEYE_BUCKET", "walleye/prod/"),
            ("WALLEYE_PORT", "9000"),
            ("WALLEYE_RAM_GB", "0.5"),
            ("WALLEYE_NVME_GB", "20"),
            ("WALLEYE_TOKEN", "sixteen-char-token!"),
            ("WALLEYE_DIR", "/data/cache"),
            ("WALLEYE_BITR_URL", "http://127.0.0.1:30080"),
            ("WALLEYE_NODE_ID", "b"),
            (
                "WALLEYE_MEMBERS",
                "a=http://a:8080, b=http://b:8080,c=http://c:8080",
            ),
        ]))
        .unwrap();
        assert_eq!(c.listen, "0.0.0.0:9000");
        assert_eq!(c.memory_bytes, 512 << 20);
        assert_eq!(c.disk_bytes, 20 << 30);
        assert_eq!(c.token, "sixteen-char-token!");
        assert_eq!(c.directory.to_str().unwrap(), "/data/cache");
        assert!(c.bitr);
        assert_eq!(c.node_id, "b");
        assert_eq!(c.members.len(), 3);
        let api = c.api.unwrap();
        assert_eq!(api.root_uri, "s3://walleye/prod");
        assert_eq!(api.bitr_url.as_deref(), Some("http://127.0.0.1:30080"));
    }

    #[test]
    fn missing_bucket_and_bad_members_fail() {
        assert!(Config::from_env_with(env(&[])).is_err());
        assert!(
            Config::from_env_with(env(&[
                ("WALLEYE_BUCKET", "walleye"),
                ("WALLEYE_MEMBERS", "a=http://a:8080"),
            ]))
            .is_err()
        );
        assert!(
            Config::from_env_with(env(&[
                ("WALLEYE_BUCKET", "walleye"),
                ("WALLEYE_NODE_ID", "a"),
                ("WALLEYE_MEMBERS", "http://a:8080"),
            ]))
            .is_err()
        );
    }
}
