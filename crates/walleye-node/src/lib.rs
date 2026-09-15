//! Single-deployment stream API, Foyer peer service, and Bitr node composition.
mod engine;
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
pub use engine::{ApiConfig, Column, StreamDefinition};
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
pub struct Service {
    pub config: Config,
    pub cache: Arc<LanceFoyerCacheBackend>,
    ring: Arc<Membership>,
    engine: Option<engine::Engine>,
    hits: AtomicU64,
    misses: AtomicU64,
    stores: AtomicU64,
    revision: tokio::sync::Mutex<String>,
    changed: tokio::sync::Notify,
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
            Some(
                engine::Engine::open(api, cached, params)
                    .await
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        Ok(Arc::new(Self {
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
        }))
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
        .with_state(service)
}
fn authorize(s: &Service, headers: &HeaderMap) -> Result<(), StatusCode> {
    let expected = format!("Bearer {}", s.config.token);
    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if value.len() != expected.len()
        || value
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
fn api<'a>(s: &'a Service, h: &HeaderMap) -> Result<&'a engine::Engine, ApiError> {
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
