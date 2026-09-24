//! Single-deployment stream API, Foyer peer service, and Bitr node composition.
// The futures that route a request, fire an alarm and forward to an owner
// nest deeply enough that proving them Send walks past the default limit.
#![recursion_limit = "256"]
pub mod access;
pub(crate) mod alarms;
pub mod ask;
pub mod cluster;
pub mod cron;
mod engine;
pub mod ingest;
pub mod kubernetes;
mod lancedb;
pub mod ownership;
pub mod reach;
pub mod see;
pub mod values;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
pub use engine::{
    ApiConfig, Column, HIDDEN_PK, PK_METADATA_KEY, StreamDefinition, StreamRequest, TableExists,
    TableNotFound,
};
use lance_core::cache::{CacheBackend, InternalCacheKey};
pub use ownership::LeaseConfig;
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
    /// Lease timing for table ownership.
    #[serde(default)]
    pub lease: LeaseConfig,
}
impl Config {
    /// Build a single node, or a static cluster member, from environment variables.
    ///
    /// Required: `WALLEYE_BUCKET` (bucket name, optionally `bucket/prefix`) or
    /// `WALLEYE_ROOT_URI` (a full `s3://` or `file://` URI).
    ///
    /// Optional: `WALLEYE_PORT` (8080), `WALLEYE_BIND` (`[::]`, dual-stack),
    /// `WALLEYE_TOKEN` (the deployment's own credential, which may call every
    /// route; generated and printed when absent), `WALLEYE_DIR`
    /// (`./walleye-cache`), `WALLEYE_RAM_GB` (1), `WALLEYE_NVME_GB` (8), `WALLEYE_BITR_URL` (enables Bitr cluster mode),
    /// `WALLEYE_MEMBERS` (`id=http://host:8080,...`) with `WALLEYE_NODE_ID`
    /// naming this member, `WALLEYE_ADVERTISE_URL` (where peers reach a node
    /// started without `WALLEYE_MEMBERS`; `http://localhost:<port>`), and the
    /// lease timings `WALLEYE_LEASE_TTL_MS` (10000), `WALLEYE_LEASE_SKEW_MS`
    /// (2000) and `WALLEYE_OWNERSHIP_SAMPLE_MS` (2000).
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
                // A replacement started beside this process forwards to it
                // until it hands its tables over, so it needs an address the
                // replacement can reach.
                let advertise = get("WALLEYE_ADVERTISE_URL")
                    .unwrap_or_else(|| format!("http://localhost:{port}"));
                let node = Node::new(&node_id, advertise, 1.0)?;
                (node_id, vec![node])
            }
        };
        let ms = |name: &str, default: u64, get: &mut dyn FnMut(&str) -> Option<String>| {
            get(name)
                .map(|v| v.parse::<u64>())
                .transpose()
                .map(|v| v.unwrap_or(default))
        };
        let defaults = LeaseConfig::default();
        let lease = LeaseConfig {
            ttl_ms: ms("WALLEYE_LEASE_TTL_MS", defaults.ttl_ms, &mut get)?,
            skew_ms: ms("WALLEYE_LEASE_SKEW_MS", defaults.skew_ms, &mut get)?,
            sample_ms: ms("WALLEYE_OWNERSHIP_SAMPLE_MS", defaults.sample_ms, &mut get)?,
        };
        lease.validate()?;
        let bitr_url = get("WALLEYE_BITR_URL");
        Ok(Config {
            node_id,
            // Dual-stack by default: IPv6-only private networks (Fly 6PN) reach
            // `[::]`, and IPv4 clients map in. `WALLEYE_BIND` overrides the address.
            listen: format!(
                "{}:{port}",
                get("WALLEYE_BIND").unwrap_or_else(|| "[::]".into())
            ),
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
            lease,
        })
    }
}
/// A table untouched for this long is closed, returning its memory to the
/// budget. It reopens on its next use, so the cost is a reopen and the
/// benefit is that a table refused for want of memory can be opened once an
/// idle one has gone.
pub const IDLE_TABLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Fixed reservations the engine keeps out of the budgets. Set the budgets to
/// the machine's memory and the volume's size; nothing else is held back.
pub struct Budget {
    /// Memory the cache may use at rest.
    pub cache_memory: usize,
    /// Disk the cache may use at rest.
    pub cache_disk: usize,
    /// Cap on the Bitr node log, handed to the embedded daemon.
    pub bitr_log_max: usize,
}
impl Budget {
    /// Binary, tokio, connection buffers, allocator slack.
    pub const RUNTIME_MEMORY_FLOOR: usize = 256 * 1024 * 1024;
    /// In-flight replication buffers of the embedded Bitr daemon.
    pub const BITR_MEMORY_RESERVE: usize = 128 * 1024 * 1024;
    /// Filesystem metadata, journal, and the cache's own bookkeeping.
    pub const FILESYSTEM_DISK_FLOOR: usize = 512 * 1024 * 1024;
    pub fn for_config(config: &Config) -> Self {
        // Below 512 MiB (test configurations) the floor scales down rather
        // than swallowing the whole budget.
        let memory_floor = Self::RUNTIME_MEMORY_FLOOR.min(config.memory_bytes / 2)
            + if config.bitr {
                Self::BITR_MEMORY_RESERVE.min(config.memory_bytes / 8)
            } else {
                0
            };
        let disk_floor = Self::FILESYSTEM_DISK_FLOOR.min(config.disk_bytes / 2);
        let cache_disk = config.disk_bytes.saturating_sub(disk_floor);
        Self {
            cache_memory: config.memory_bytes.saturating_sub(memory_floor).max(1),
            cache_disk,
            // Half, not all: compacting the log writes a replacement beside
            // it and renames, so a log allowed to fill the budget could not
            // be compacted without overrunning the volume.
            bitr_log_max: cache_disk / 2,
        }
    }
}
/// Bytes under `dir`, excluding anything inside `skip` (the cache's own
/// directory, which the governor accounts for separately).
fn directory_bytes(dir: &str, skip: &std::path::Path) -> usize {
    fn walk(path: &std::path::Path, skip: &std::path::Path, total: &mut usize) {
        if path == skip {
            return;
        }
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                walk(&entry.path(), skip, total);
            } else {
                *total += meta.len() as usize;
            }
        }
    }
    let mut total = 0;
    let skip = skip.canonicalize().unwrap_or_else(|_| skip.to_path_buf());
    walk(std::path::Path::new(dir), &skip, &mut total);
    total
}
pub struct Service {
    pub config: Config,
    pub cache: Arc<LanceFoyerCacheBackend>,
    /// Who may call what: the deployment token and the published access tokens.
    access: Arc<access::Access>,
    ring: Arc<Membership>,
    engine: Option<engine::Engine>,
    /// Socket views this process holds a connection for.
    sockets: tokio::sync::Mutex<std::collections::HashSet<String>>,
    hits: AtomicU64,
    misses: AtomicU64,
    stores: AtomicU64,
    pub(crate) revision: tokio::sync::Mutex<String>,
    pub(crate) changed: tokio::sync::Notify,
    quiescing: AtomicBool,
    /// Writes can be made durable right now: the engine has warmed and, with
    /// Bitr, the local replica quorum is reachable.
    write_ready: AtomicBool,
    /// This node has been write-ready at least once. Sticky, so a quorum lost
    /// later does not pull a node that still serves reads out of rotation.
    served: AtomicBool,
    /// What `/readyz` reports: ready, or why not.
    readiness: std::sync::Mutex<serde_json::Value>,
}

/// The local Bitr gateway's write readiness, with its explanation when it is
/// not ready. `/readyz` there fails while the daemon is initializing, fenced
/// for maintenance, or short of `quorum` healthy members, and its body names
/// the members that did not answer.
async fn quorum_state(client: &reqwest::Client, gateway: &str) -> (bool, serde_json::Value) {
    match client
        .get(format!("{}/readyz", gateway.trim_end_matches('/')))
        // Generous: the gateway probes every member before answering, and a
        // member busy serving writes is not a member that has gone away.
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(response) if response.status().is_success() => {
            let body = response.text().await.unwrap_or_default();
            // The gateway names its members on a ready answer too, so the
            // detail carries who is serving, not only who is not.
            let detail = serde_json::from_str::<serde_json::Value>(&body)
                .unwrap_or_else(|_| serde_json::json!({"ready": true}));
            (true, detail)
        }
        Ok(response) => {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            let detail = serde_json::from_str::<serde_json::Value>(&body).unwrap_or_else(|_| {
                serde_json::json!({
                    "ready": false,
                    "reason": format!("the local replica gateway answered {status}"),
                })
            });
            (false, detail)
        }
        Err(error) => (
            false,
            serde_json::json!({
                "ready": false,
                "reason": "the local replica gateway is unreachable",
                "error": error.to_string(),
            }),
        ),
    }
}

impl Service {
    pub async fn open(config: Config) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        use_tls();
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
        // WALLEYE_RAM_GB and WALLEYE_NVME_GB are whole-process budgets. The
        // cache takes what is left after the fixed floors; everything else
        // that allocates (memtables, request bodies, index builds, queries,
        // the Bitr log) borrows from the cache through the governor.
        let bitr_gateway = config
            .api
            .as_ref()
            .is_some_and(|api| api.bitr_url.is_some());
        let budget = Budget::for_config(&config);
        let log_max = std::env::var("LAKEDAY_REPLICA_LOG_MAX_BYTES")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| budget.bitr_log_max.to_string());
        // SAFETY: runs during startup, before the cache, the S3 client, and
        // the embedded Bitr daemon exist; nothing else reads the environment
        // concurrently at this point.
        unsafe { std::env::set_var("LAKEDAY_REPLICA_LOG_MAX_BYTES", log_max) };
        let cache = Arc::new(
            LanceFoyerCacheBackend::new(
                &config.directory,
                budget.cache_memory,
                budget.cache_disk,
                "walleye",
            )
            .await?,
        );
        // Customer access tokens are published into the deployment's own
        // storage, so a node with storage reads them from there.
        let access = Arc::new(match config.api.as_ref() {
            Some(api) => {
                let (store, root) = lance_io::object_store::ObjectStore::from_uri_and_params(
                    Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
                    &api.root_uri,
                    &engine::storage_params(),
                )
                .await?;
                access::Access::open(config.token.clone(), store.inner.clone(), &root).await?
            }
            None => access::Access::system_only(config.token.clone()),
        });
        access.clone().spawn_refresh(access::REFRESH_INTERVAL);
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
            // The ring places cache entries and nothing else. Who owns a table
            // is the ownership record in the bucket; this node's entry in the
            // member list is only where it tells peers to reach it.
            let endpoint = config
                .members
                .iter()
                .find(|member| member.id == config.node_id)
                .map(|member| member.endpoint.clone())
                .ok_or("this node is not in its own member list")?;
            let cluster =
                cluster::Cluster::new(config.node_id.clone(), endpoint, config.token.clone())?;
            Some(
                engine::Engine::open(api, cached, params, cluster, config.lease.clone())
                    .await
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };
        let service = Arc::new(Self {
            config,
            cache,
            access,
            ring,
            engine,
            sockets: tokio::sync::Mutex::new(std::collections::HashSet::new()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            stores: AtomicU64::new(0),
            revision: tokio::sync::Mutex::new(format!("\"{}\"", uuid::Uuid::new_v4())),
            changed: tokio::sync::Notify::new(),
            quiescing: AtomicBool::new(false),
            // Without Bitr there is no quorum to wait for: durability is the
            // object store, which the engine already opened.
            write_ready: AtomicBool::new(!bitr_gateway),
            served: AtomicBool::new(!bitr_gateway),
            readiness: std::sync::Mutex::new(if bitr_gateway {
                serde_json::json!({"ready": false, "reason": "starting"})
            } else {
                serde_json::json!({"ready": true})
            }),
        });
        // Three things run for the life of the node: the disk the Bitr log
        // takes is reported so the cache's ceiling tracks it, tables nobody
        // is using are closed so their memory returns to the budget, and
        // readiness is established and then kept current.
        service.clone().spawn_disk_sampler();
        service.clone().spawn_idle_sweeper();
        service.clone().spawn_view_driver();
        service.clone().spawn_alarms();
        service.clone().spawn_readiness();
        service.clone().spawn_ownership();
        Ok(service)
    }

    /// Fire alarms on the keys this process owns, each when it comes due.
    ///
    /// This is the one scheduler. It sleeps until the earliest pending alarm
    /// or until an alarm or an ownership changes, and fires each due alarm
    /// once; the firing itself proves ownership by writing to the key's
    /// record, so a process that has lost a key cannot fire its alarms.
    fn spawn_alarms(self: Arc<Self>) {
        let Some(engine) = &self.engine else { return };
        let _ = engine;
        tokio::spawn(async move {
            let in_flight: Arc<std::sync::Mutex<std::collections::HashSet<(String, String)>>> =
                Arc::default();
            loop {
                if self.quiescing.load(Ordering::Acquire) {
                    return;
                }
                let Some(engine) = &self.engine else { return };
                let owners = engine.ownership();
                let changed = owners.alarms_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let mut next: Option<u64> = None;
                for (key, name, alarm) in owners.held_alarms() {
                    let slot = (key.clone(), name.clone());
                    if in_flight.lock().expect("in-flight set").contains(&slot) {
                        continue;
                    }
                    if alarm.at_ms > now {
                        next = Some(next.map_or(alarm.at_ms, |next| next.min(alarm.at_ms)));
                        continue;
                    }
                    in_flight
                        .lock()
                        .expect("in-flight set")
                        .insert(slot.clone());
                    let service = Arc::clone(&self);
                    let in_flight = Arc::clone(&in_flight);
                    tokio::spawn(async move {
                        if let Some(engine) = &service.engine {
                            if let Err(error) = engine.fire(&key, &name).await {
                                eprintln!(
                                    "walleye.alarm fire key={key} alarm={name} outcome=refused error={error}"
                                );
                            }
                            in_flight.lock().expect("in-flight set").remove(&slot);
                            engine.ownership().alarms_changed.notify_waiters();
                        }
                    });
                }
                // A wall clock that jumps is caught within a minute.
                let wait = next
                    .map(|next| next.saturating_sub(now))
                    .unwrap_or(60_000)
                    .min(60_000);
                tokio::select! {
                    _ = &mut changed => {}
                    _ = tokio::time::sleep(std::time::Duration::from_millis(wait)) => {}
                }
            }
        });
    }

    /// Claim what dead or departed owners left, spread across the live
    /// processes, and close writers for tables this process lost.
    fn spawn_ownership(self: Arc<Self>) {
        let Some(engine) = &self.engine else { return };
        let every = engine.ownership().config().sample();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                if self.quiescing.load(Ordering::Acquire) {
                    return;
                }
                if let Some(engine) = &self.engine {
                    let claimed = engine.sweep().await;
                    if !claimed.is_empty() {
                        eprintln!("walleye.ownership sweep claimed={}", claimed.join(","));
                        // Views over what was just taken may be behind.
                        self.changed.notify_one();
                    }
                }
                self.reconcile_sockets().await;
            }
        });
    }

    /// Report the disk the Bitr log occupies beside the cache file, so the
    /// cache's ceiling is the budget minus what the log holds.
    fn spawn_disk_sampler(self: Arc<Self>) {
        let Some(engine) = self.engine.as_ref().filter(|_| self.config.bitr) else {
            return;
        };
        let resources = engine.resources().clone();
        let data_dir = std::env::var("LAKEDAY_REPLICA_DATA_DIR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "/data".into());
        // The cache usually lives on the same volume; counting it as
        // "elsewhere" would charge it twice and starve its own ceiling.
        let cache_dir = self.config.directory.clone();
        tokio::spawn(async move {
            loop {
                let dir = data_dir.clone();
                let cache_dir = cache_dir.clone();
                let used = tokio::task::spawn_blocking(move || directory_bytes(&dir, &cache_dir))
                    .await
                    .unwrap_or(0);
                if let Err(error) = resources.set_disk_usage("bitr", used).await {
                    eprintln!("walleye.budget stage=disk_sample outcome=error error={error}");
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }

    /// Return the memory of tables nobody is using, so a node whose budget is
    /// full can still open a new one.
    fn spawn_idle_sweeper(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                if let Some(engine) = &self.engine {
                    engine.close_idle(IDLE_TABLE_TIMEOUT).await;
                }
            }
        });
    }

    /// Keep every view whose source this node owns caught up, without
    /// anybody asking.
    ///
    /// The loop advances views until a whole pass moves nothing, then waits
    /// to be woken: by an append, or by this process taking over a source
    /// whose views may be behind. There is no timer. Rows reach a source only
    /// through its owner, so the owner always hears of them, and a pass that
    /// fails is retried by an alarm rather than by the next tick.
    fn spawn_view_driver(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                if self.quiescing.load(Ordering::Acquire) {
                    return;
                }
                if let Some(engine) = &self.engine {
                    for (name, outcome) in engine.advance_views(64).await {
                        match outcome {
                            Ok(progress)
                                if progress.rows > 0
                                    || progress.written > 0
                                    || progress.delivered > 0 =>
                            {
                                eprintln!(
                                    "walleye.view view={name} rows={} written={} \
                                     delivered={} through={}",
                                    progress.rows,
                                    progress.written,
                                    progress.delivered,
                                    progress.through
                                )
                            }
                            Ok(_) => {}
                            Err(error) => {
                                eprintln!("walleye.view view={name} outcome=error error={error}")
                            }
                        }
                    }
                }
                self.changed.notified().await;
            }
        });
    }

    /// Hold open a socket for every socket view whose key this process owns,
    /// and hand what arrives to that view's worker. Called from the ownership
    /// sweep, so a socket follows its view's owner rather than being held by
    /// every node.
    ///
    /// The connection lives here rather than in the isolate, because an
    /// isolate is a bounded turn and a socket is not. A worker stays a
    /// stoppable batch of frames while the stream itself keeps running, and
    /// a worker that fails costs its batch rather than the connection.
    async fn reconcile_sockets(self: &Arc<Self>) {
        let Some(engine) = &self.engine else { return };
        for name in engine.view_names().await.unwrap_or_default() {
            if self.sockets.lock().await.contains(&name) {
                continue;
            }
            let Ok(view) = engine.view(&name).await else {
                continue;
            };
            let Some(socket) = view.websocket.clone() else {
                continue;
            };
            if engine
                .ownership()
                .holds(&engine::driver_key(&view))
                .is_none()
            {
                continue;
            }
            self.sockets.lock().await.insert(name.clone());
            tokio::spawn(Arc::clone(self).hold_socket(name, view, socket));
        }
    }

    /// One socket, reconnected for as long as the view exists.
    async fn hold_socket(
        self: Arc<Self>,
        name: String,
        view: engine::ViewDefinition,
        socket: engine::Socket,
    ) {
        let key = engine::driver_key(&view);
        let mut backoff = std::time::Duration::from_secs(1);
        loop {
            // A view that has been dropped takes its socket with it, and one
            // this process no longer owns is its new owner's to hold.
            let Some(engine) = &self.engine else { return };
            let gone = engine.view(&name).await.is_err();
            if self.quiescing.load(Ordering::Acquire)
                || gone
                || engine.ownership().holds(&key).is_none()
            {
                eprintln!("walleye.socket view={name} outcome=released gone={gone}");
                self.sockets.lock().await.remove(&name);
                return;
            }
            match self.read_socket(&name, &key, &socket).await {
                Ok(frames) => {
                    eprintln!("walleye.socket view={name} outcome=closed frames={frames}");
                    backoff = std::time::Duration::from_secs(1);
                }
                Err(error) => {
                    eprintln!("walleye.socket view={name} outcome=error error={error}");
                }
            }
            tokio::time::sleep(backoff).await;
            // Back off to a minute, so a socket that refuses is not hammered.
            backoff = (backoff * 2).min(std::time::Duration::from_secs(60));
        }
    }

    /// Connect once and read until the far end stops. Returns how many frames
    /// were handled.
    async fn read_socket(
        &self,
        name: &str,
        key: &str,
        socket: &engine::Socket,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        use futures::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::{
            Message, client::IntoClientRequest, http::HeaderValue,
        };
        let mut request = socket.url.as_str().into_client_request()?;
        for (header, value) in &socket.headers {
            let value = crate::reach::substitute_public(value)?;
            request.headers_mut().insert(
                tokio_tungstenite::tungstenite::http::HeaderName::try_from(header.as_str())?,
                HeaderValue::from_str(&value)?,
            );
        }
        use_tls();
        let (mut stream, _) = tokio_tungstenite::connect_async(request).await?;
        eprintln!("walleye.socket view={name} outcome=open");
        if let Some(opening) = &socket.subscribe {
            let opening = crate::reach::substitute_public(opening)?;
            stream.send(Message::Text(opening.into())).await?;
        }

        let mut handled = 0usize;
        let mut batch: Vec<String> = Vec::with_capacity(socket.frames);
        let window = std::time::Duration::from_millis(socket.window_ms.max(1));
        loop {
            let deadline = tokio::time::sleep(window);
            tokio::pin!(deadline);
            let full = loop {
                tokio::select! {
                    frame = stream.next() => match frame {
                        Some(Ok(Message::Text(text))) => {
                            batch.push(text.to_string());
                            if batch.len() >= socket.frames {
                                break false;
                            }
                        }
                        Some(Ok(Message::Binary(_))) => {}
                        Some(Ok(Message::Ping(payload))) => {
                            stream.send(Message::Pong(payload)).await?;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(error.into()),
                        None => break true,
                    },
                    () = &mut deadline => break false,
                }
            };
            if !batch.is_empty() {
                let frames = std::mem::take(&mut batch);
                handled += frames.len();
                if let Some(engine) = &self.engine
                    && let Err(error) = engine.handle_frames(name, frames).await
                {
                    // A bad batch costs itself, not the connection.
                    eprintln!("walleye.socket view={name} outcome=batch_error error={error}");
                }
            }
            let owned = self
                .engine
                .as_ref()
                .is_some_and(|engine| engine.ownership().holds(key).is_some());
            if full || !owned || self.quiescing.load(Ordering::Acquire) {
                return Ok(handled);
            }
        }
    }

    /// Wait until writes can be durable, say so, warm the streams this node
    /// owns, then keep readiness current for as long as the node runs.
    fn spawn_readiness(self: Arc<Self>) {
        tokio::spawn(async move {
            let gateway = self
                .config
                .api
                .as_ref()
                .and_then(|api| api.bitr_url.clone());
            if let Some(gateway) = &gateway {
                self.await_quorum(gateway).await;
            }
            // Ready once writes can be durable. Warming opens owned streams
            // ahead of their first request, which is an optimization: holding
            // readiness until it finishes would keep a restarted node out of
            // rotation for as long as it takes to open every table it owns,
            // and a stream that is not warm yet simply opens on use.
            self.write_ready.store(true, Ordering::Release);
            self.served.store(true, Ordering::Release);
            *self.readiness() = serde_json::json!({"ready": true});
            if let Some(engine) = &self.engine {
                engine.warm().await;
            }
            // Keep write readiness current: a quorum lost later stops this
            // node from accepting writes it could not make durable, while
            // reads and `/healthz` continue.
            if let Some(gateway) = gateway {
                let client = reqwest::Client::new();
                // Withdrawing write readiness costs every client a refusal, so
                // it takes several failed polls in a row, while restoring it
                // takes one. A probe that times out because a member is busy
                // is not a quorum that has gone away, and the append itself
                // still refuses a write it cannot make durable. `/readyz`
                // meanwhile reports what the last poll saw, without waiting.
                const WITHDRAW_AFTER: u32 = 3;
                let mut consecutive_failures = 0_u32;
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    let (ready, detail) = quorum_state(&client, &gateway).await;
                    *self.readiness() = detail.clone();
                    consecutive_failures = if ready {
                        0
                    } else {
                        consecutive_failures.saturating_add(1)
                    };
                    let writable = ready || consecutive_failures < WITHDRAW_AFTER;
                    if self.write_ready.swap(writable, Ordering::AcqRel) != writable {
                        eprintln!(
                            "walleye.ready stage=quorum write_ready={writable} \
                             consecutive_failures={consecutive_failures} detail={detail}"
                        );
                    }
                }
            }
        });
    }

    /// Block until the local Bitr gateway reports a reachable quorum. The
    /// embedded daemon seeds archived prefixes and reaches its quorum before
    /// it reports ready, and a writer opened before then fails recovery.
    /// There is no deadline: a node that never reaches a quorum cannot write,
    /// and saying otherwise would only move the failure to the first request.
    async fn await_quorum(&self, gateway: &str) {
        let client = reqwest::Client::new();
        let started = std::time::Instant::now();
        let mut polls = 0_u32;
        loop {
            let (ready, detail) = quorum_state(&client, gateway).await;
            *self.readiness() = detail.clone();
            if ready {
                eprintln!(
                    "walleye.ready stage=quorum outcome=ready elapsed_ms={}",
                    started.elapsed().as_millis()
                );
                return;
            }
            // Every 15s while waiting, so a stuck boot is diagnosable.
            if polls.is_multiple_of(60) {
                eprintln!(
                    "walleye.ready stage=quorum outcome=waiting elapsed_ms={} detail={detail}",
                    started.elapsed().as_millis()
                );
            }
            polls += 1;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    /// Follow Kubernetes cache endpoints. Failure preserves the last good ring;
    /// missing peers fall back to the query process's normal origin loading.
    pub async fn discover(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.config.kubernetes {
            Some(config) => kubernetes::follow(config, self.ring.clone()).await,
            None => std::future::pending().await,
        }
    }
    /// The stream engine, when this node serves the API.
    pub fn engine(&self) -> Option<&engine::Engine> {
        self.engine.as_ref()
    }
    fn readiness(&self) -> std::sync::MutexGuard<'_, serde_json::Value> {
        self.readiness
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Hand every table this process owns to whoever claims it next, while
    /// the API keeps answering: a request for a table already handed over is
    /// forwarded to its new owner.
    pub async fn release(&self) {
        if let Some(engine) = &self.engine {
            engine.release_all().await;
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
/// Choose the cipher provider once for the process.
///
/// A secure socket needs one picked before the first handshake, and rustls
/// panics rather than erroring when it cannot tell which. Doing it here means
/// a `wss://` view works without every caller remembering.
pub fn use_tls() {
    static TLS: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    TLS.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Every route the node serves, each with what it asks of its caller.
pub(crate) fn routes() -> access::Routes<Arc<Service>> {
    use access::{Required::*, Scope::*};
    access::Routes::default()
        .route(Method::GET, "/healthz", Open, healthz)
        .route(Method::GET, "/readyz", Open, readyz)
        .route(Method::GET, "/internal/cache/{key}", System, read)
        .route(Method::PUT, "/internal/cache/{key}", System, write)
        .route(Method::GET, "/internal/cache/stats", System, stats)
        .route(Method::GET, "/internal/snapshot/{name}", System, snapshot)
        .route(Method::POST, "/internal/cache/flush", System, flush)
        .route(Method::GET, "/internal/ownership", System, ownership)
        .route(Method::GET, "/internal/alarms", System, alarms)
        .route(Method::POST, "/v1/streams", Data(Manage), define)
        .route(
            Method::POST,
            "/v1/streams/{name}/events",
            Data(Write),
            ingest,
        )
        // Records of any shape become rows in tables it decides, so it is a
        // write like any other ingest.
        .route(Method::POST, "/v1/ingest/{source}", Data(Write), ingest_any)
        .route(Method::POST, "/v1/query", Data(Read), query)
        // Seeing is reading: a question or a table, drawn. A table's own
        // dashboard is saved as it is made, but that is a cache of reads.
        .route(Method::POST, "/v1/see", Data(Read), see)
        .route(Method::GET, "/v1/see/tables/{table}", Data(Read), see_table)
        .route(Method::GET, "/v1/dashboards", Data(Read), dashboards)
        .route(Method::GET, "/v1/dashboards/{name}", Data(Read), dashboard)
        .route(
            Method::PUT,
            "/v1/dashboards/{name}",
            Data(Manage),
            save_dashboard,
        )
        .route(
            Method::DELETE,
            "/v1/dashboards/{name}",
            Data(Manage),
            delete_dashboard,
        )
        .route(
            Method::POST,
            "/v1/dashboards/{name}/chat",
            Data(Manage),
            chat_dashboard,
        )
        .map(|router| router.layer(DefaultBodyLimit::max(8 * 1024 * 1024)))
        .merge(lancedb::routes())
}
pub fn router(service: Arc<Service>) -> Router {
    let (router, table) = routes().into_parts();
    let gate = access::Gate {
        access: service.access.clone(),
        table: Arc::new(table),
    };
    router
        .layer(axum::middleware::from_fn_with_state(
            service.clone(),
            cluster::route_to_owner,
        ))
        // Outermost, so nothing - not even the forward to a stream's owner,
        // which carries this node's own token - happens for a caller the
        // route does not admit.
        .layer(axum::middleware::from_fn_with_state(gate, access::gate))
        .with_state(service)
}
/// Liveness plus first-boot readiness. A node that has never been write-ready
/// reports unavailable so a load balancer does not route to it before its
/// quorum converges; once it has served, it stays healthy even if the quorum
/// is later lost, because reads remain correct without one. `/readyz` is the
/// strict current-write-readiness probe.
async fn healthz(State(s): State<Arc<Service>>) -> Response {
    if s.served.load(Ordering::Acquire) {
        (StatusCode::OK, "ok").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            "starting: waiting for the replica quorum",
        )
            .into_response()
    }
}
/// Write readiness, and with `?require=all` the stricter question a caller
/// with one address has to ask: is the whole cluster serving, rather than a
/// quorum of it. A ready answer names the members that are serving, so the
/// caller can tell those apart without reaching each node.
async fn readyz(
    State(s): State<Arc<Service>>,
    axum::extract::Query(query): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let mut detail = s.readiness().clone();
    let mut ready = s.write_ready.load(Ordering::Acquire);
    if query.get("require").map(String::as_str) == Some("all") {
        let absent = detail
            .get("unreachable")
            .and_then(|members| members.as_array())
            .map(|members| members.len())
            .unwrap_or(0);
        if ready && absent > 0 {
            ready = false;
            let serving = detail
                .get("healthy")
                .and_then(|members| members.as_array())
                .map(|members| members.len())
                .unwrap_or(0);
            detail["ready"] = serde_json::json!(false);
            detail["reason"] = serde_json::json!(format!(
                "{serving} of {} members are serving; strict readiness requires all",
                serving + absent
            ));
        }
    }
    if ready {
        (StatusCode::OK, Json(detail)).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1")],
            Json(detail),
        )
            .into_response()
    }
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
) -> Result<Response, StatusCode> {
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
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
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
/// Hand a peer every row of a stream this node owns, as an Arrow IPC file, so
/// it can run SQL that spans owners. The snapshot is taken here, so it
/// includes rows this node has not flushed.
async fn snapshot(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let engine = api(&s)?;
    let (schema, batches) = engine.snapshot_batches(&name).await.map_err(failure)?;
    let mut out = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::FileWriter::try_new(&mut out, &schema).map_err(failure)?;
        for batch in &batches {
            writer.write(batch).map_err(failure)?;
        }
        writer.finish().map_err(failure)?;
    }
    Ok(([("content-type", "application/vnd.apache.arrow.file")], out).into_response())
}
async fn stats(State(s): State<Arc<Service>>) -> Json<serde_json::Value> {
    let budget = s.engine.as_ref().map(|e| {
        let r = e.resources();
        serde_json::json!({
            "memory_budget": r.memory_budget(),
            "memory_reserved": r.memory_reserved(),
            "memory_available": r.memory_available(),
            "disk_budget": r.disk_budget(),
            "disk_used_elsewhere": r.disk_used_elsewhere(),
        })
    });
    Json(
        serde_json::json!({"node":s.config.node_id,"members":s.ring.snapshot().members(),"membership_epoch":s.ring.snapshot().epoch(),"hits":s.hits.load(Ordering::Relaxed),"misses":s.misses.load(Ordering::Relaxed),"stores":s.stores.load(Ordering::Relaxed),"entries":s.cache.num_entries().await,"memory_usage":s.cache.memory_usage(),"memory_capacity":s.cache.memory_capacity(),"disk_capacity":s.cache.persistent_capacity(),"budget":budget}),
    )
}
/// What this process believes about table ownership: its session, whether it
/// is authoritative, the tables it holds and at which epoch, and the leases
/// it has seen.
/// Every alarm pending on the keys this process owns.
async fn alarms(State(s): State<Arc<Service>>) -> Result<Json<serde_json::Value>, StatusCode> {
    let engine = s.engine.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(engine.pending_alarms()))
}
async fn ownership(State(s): State<Arc<Service>>) -> Result<Json<serde_json::Value>, StatusCode> {
    let engine = s.engine.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(engine.ownership().status()))
}
async fn flush(State(s): State<Arc<Service>>) -> StatusCode {
    s.cache.flush().await;
    StatusCode::NO_CONTENT
}
type ApiError = (StatusCode, Json<serde_json::Value>);
fn failure(e: impl std::fmt::Display) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"error":e.to_string()})),
    )
}
/// A failure whose outcome is genuinely unknown is not a bad request. A
/// writer fenced by its own WAL persistence failure may or may not have
/// stored the rows: the durable log is ahead of what this process can
/// account for, and only a read settles it. Saying "failed" would send a
/// client to retry a write that already happened.
fn write_failure(error: &Error) -> ApiError {
    match walleye_lance::writer_fence_reason(&**error) {
        Some(walleye_lance::FenceReason::PersistenceFailure) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": format!(
                    "the outcome of this write is unknown: {error}. Read the rows back before \
                     retrying; a retry of the same rows is safe when they carry a primary key."
                ),
                "outcome": "unknown",
            })),
        ),
        // A writer another process fenced did not store these rows: the
        // append was refused, not half-done. Whoever holds the writer now
        // will take them, so this is worth retrying and is not the caller's
        // mistake. It used to be a 400, which nothing retries.
        Some(walleye_lance::FenceReason::PeerClaimedEpoch) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": format!(
                    "another writer took this table while the write was in flight: {error}. \
                     The rows were not stored; retry and the request reaches the writer that \
                     holds it now."
                ),
                "outcome": "not written",
            })),
        ),
        _ => failure(error),
    }
}
type Error = Box<dyn std::error::Error + Send + Sync>;
/// The engine, for a request that must make a durable write. A node whose
/// quorum is unreachable declines with 503 and a `Retry-After` instead of
/// opening a writer that would fail recovery; the caller (or the forwarding
/// peer) retries.
pub(crate) fn writable(s: &Service) -> Result<&engine::Engine, ApiError> {
    let engine = api(s)?;
    if !s.write_ready.load(Ordering::Acquire) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"the replica quorum is unreachable; retry"})),
        ));
    }
    Ok(engine)
}
/// The engine, for a request the access gate has already admitted.
pub(crate) fn api(s: &Service) -> Result<&engine::Engine, ApiError> {
    s.engine.as_ref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"error":"use the configured stream ingress"})),
    ))
}
async fn define(
    State(s): State<Arc<Service>>,
    Json(def): Json<StreamRequest>,
) -> Result<Response, ApiError> {
    let def: StreamDefinition = def.into();
    let name = def.name.clone();
    let engine = writable(&s)?;
    let mut revision = s.revision.lock().await;
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    if let Err(error) = engine.define(def).await {
        // A table owned elsewhere, or owned here no longer, is a routing
        // answer rather than a bad request.
        return match cluster::route_error(&*error) {
            Some(response) => Ok(response),
            None => Err(failure(error)),
        };
    }
    Ok(Json(serde_json::json!({"stream":name})).into_response())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ingest {
    rows: Vec<serde_json::Value>,
}
/// Take records of any shape and turn them into a table, deciding what needs
/// deciding once and by rule after. See [`ingest`](crate::ingest).
///
/// The body is a JSON array of records, a single record, or an object whose
/// `records` field is the array.
async fn ingest_any(
    State(s): State<Arc<Service>>,
    Path(source): Path<String>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    use serde_json::value::RawValue;
    // Admitted already: the access gate checked this route's scope.
    let engine = writable(&s)?;
    let _lease = engine
        .resources()
        .reserve_memory("ingest body", body.len().saturating_mul(8))
        .map_err(|error| {
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({"error": error.to_string()})),
            )
        })?;
    let whole: Box<RawValue> = serde_json::from_slice(&body).map_err(failure)?;
    let text = whole.get().trim_start();
    let records: Vec<Box<RawValue>> = if text.starts_with('[') {
        serde_json::from_str(whole.get()).map_err(failure)?
    } else {
        #[derive(serde::Deserialize)]
        struct Wrapped {
            records: Vec<Box<RawValue>>,
        }
        match serde_json::from_str::<Wrapped>(whole.get()) {
            Ok(wrapped) => wrapped.records,
            Err(_) => vec![whole],
        }
    };
    let report = crate::ingest::ingest(engine, &source, records)
        .await
        .map_err(|error| write_failure(&error))?;
    s.changed.notify_one();
    Ok(Json(serde_json::to_value(report).map_err(failure)?))
}
async fn ingest(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    h: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let engine = writable(&s)?;
    // Parsing JSON into values costs several times the bytes it came from,
    // and the rows are copied again on the way into Arrow. Lease that before
    // paying it, as the Arrow path does, so a large insert is refused rather
    // than allowed to exceed the budget.
    let _lease = engine
        .resources()
        .reserve_memory("ingest body", body.len().saturating_mul(8))
        .map_err(|error| {
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({"error": error.to_string()})),
            )
        })?;
    let input: Ingest = serde_json::from_slice(&body).map_err(failure)?;
    let mut revision = s.revision.lock().await;
    if let Some(expected) = h.get("if-match") {
        if expected.to_str().ok() != Some(revision.as_str()) {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                Json(serde_json::json!({"error":"stream snapshot changed"})),
            ));
        }
        // A conditional commit has to fit one atomic Lance WAL batch.
        if input.rows.len() > 1024 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error":"conditional ingestion is limited to 1024 rows"})),
            ));
        }
    }
    // Invalidate snapshots before attempting ingestion, including a partial failure.
    *revision = format!("\"{}\"", uuid::Uuid::new_v4());
    let count = match engine.ingest(&name, input.rows).await {
        Ok(count) => count,
        Err(error) => {
            return match cluster::route_error(&*error) {
                Some(response) => Ok(response),
                None => Err(write_failure(&error)),
            };
        }
    };
    s.changed.notify_one();
    Ok((
        [("etag", revision.as_str())],
        Json(serde_json::json!({"stream":name,"ingested":count})),
    )
        .into_response())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// A query is a statement or a question. Both answer with rows.
struct Query {
    #[serde(default)]
    sql: Option<String>,
    /// A question in plain language. A model writes the statement, the planner
    /// checks it, and it runs like any other. The statement it wrote comes
    /// back with the rows.
    #[serde(default)]
    text: Option<String>,
}
async fn query(
    State(s): State<Arc<Service>>,
    Json(input): Json<Query>,
) -> Result<Response, ApiError> {
    let engine = api(&s)?;
    let revision = s.revision.lock().await;
    let headers = [
        ("content-type", "application/json"),
        ("etag", revision.as_str()),
    ];
    match (input.sql, input.text) {
        (Some(sql), None) => {
            let bytes = engine.query(&sql).await.map_err(failure)?;
            Ok((headers, bytes).into_response())
        }
        (None, Some(text)) => {
            let answered = engine.answer(&text).await.map_err(failure)?;
            let body = serde_json::to_vec(&answered).map_err(failure)?;
            Ok((headers, body).into_response())
        }
        (Some(_), Some(_)) => Err(failure("give a statement or a question, not both")),
        (None, None) => Err(failure("give a statement in `sql` or a question in `text`")),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
/// Something to see: a question in words, or a statement.
struct Look {
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    title: Option<String>,
}
fn not_found(what: String) -> ApiError {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": what })),
    )
}
fn dashboard_name(name: &str) -> Result<(), ApiError> {
    if see::valid(name) {
        Ok(())
    } else {
        Err(failure("a dashboard name is letters, digits, `_` and `-`"))
    }
}
/// One question or statement, drawn as Jev thinks it should be, not saved.
/// What comes back can be saved as it is with `PUT /v1/dashboards/{name}`.
async fn see(
    State(s): State<Arc<Service>>,
    Json(look): Json<Look>,
) -> Result<Json<see::Drawn>, ApiError> {
    let engine = api(&s)?;
    let made = see::look(
        engine,
        look.question.as_deref(),
        look.sql.as_deref(),
        look.title,
    )
    .await
    .map_err(failure)?;
    Ok(Json(see::draw(engine, made).await))
}
#[derive(Deserialize)]
struct Fresh {
    #[serde(default)]
    fresh: bool,
}
/// A table's own dashboard, made the first time it is asked for and again
/// whenever the table's columns change, or on `?fresh=true`.
async fn see_table(
    State(s): State<Arc<Service>>,
    Path(table): Path<String>,
    axum::extract::Query(fresh): axum::extract::Query<Fresh>,
) -> Result<Json<see::Drawn>, ApiError> {
    let engine = api(&s)?;
    if !engine::valid_name(&table) {
        return Err(failure("invalid table name"));
    }
    let made = see::for_table(engine, &table, fresh.fresh)
        .await
        .map_err(|error| {
            if error.to_string().starts_with("no table named") {
                not_found(error.to_string())
            } else {
                failure(error)
            }
        })?;
    Ok(Json(see::draw(engine, made).await))
}
async fn dashboards(State(s): State<Arc<Service>>) -> Result<Json<serde_json::Value>, ApiError> {
    let engine = api(&s)?;
    let listed = see::list(engine).await.map_err(failure)?;
    Ok(Json(serde_json::json!({ "dashboards": listed })))
}
async fn dashboard(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
) -> Result<Json<see::Drawn>, ApiError> {
    let engine = api(&s)?;
    dashboard_name(&name)?;
    let saved = see::load(engine, &name)
        .await
        .map_err(failure)?
        .ok_or_else(|| not_found(format!("no dashboard named {name}")))?;
    Ok(Json(see::draw(engine, saved).await))
}
/// Make a dashboard from questions and statements and save it, or save one
/// already composed as it is.
async fn save_dashboard(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    Json(request): Json<see::Request>,
) -> Result<Json<see::Drawn>, ApiError> {
    let engine = api(&s)?;
    dashboard_name(&name)?;
    let made = see::make(engine, &name, request).await.map_err(failure)?;
    see::save(engine, &made).await.map_err(failure)?;
    Ok(Json(see::draw(engine, made).await))
}
async fn delete_dashboard(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let engine = api(&s)?;
    dashboard_name(&name)?;
    match see::remove(engine, &name).await.map_err(failure)? {
        true => Ok(StatusCode::NO_CONTENT),
        false => Err(not_found(format!("no dashboard named {name}"))),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Said {
    message: String,
}
/// Change a saved dashboard by saying what to change.
async fn chat_dashboard(
    State(s): State<Arc<Service>>,
    Path(name): Path<String>,
    Json(said): Json<Said>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let engine = api(&s)?;
    dashboard_name(&name)?;
    let saved = see::load(engine, &name)
        .await
        .map_err(failure)?
        .ok_or_else(|| not_found(format!("no dashboard named {name}")))?;
    let (changed, did) = see::chat(engine, saved, &said.message)
        .await
        .map_err(failure)?;
    see::save(engine, &changed).await.map_err(failure)?;
    let drawn = see::draw(engine, changed).await;
    Ok(Json(serde_json::json!({ "did": did, "dashboard": drawn })))
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
        assert_eq!(c.listen, "[::]:8080");
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
        assert_eq!(c.listen, "[::]:9000");
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
    fn a_node_advertises_where_it_answers_and_takes_its_lease_timings() {
        let c = Config::from_env_with(env(&[
            ("WALLEYE_BUCKET", "walleye"),
            ("WALLEYE_ADVERTISE_URL", "http://[fdaa::3]:8080"),
            ("WALLEYE_LEASE_TTL_MS", "6000"),
            ("WALLEYE_LEASE_SKEW_MS", "1000"),
            ("WALLEYE_OWNERSHIP_SAMPLE_MS", "500"),
        ]))
        .unwrap();
        assert_eq!(c.members[0].endpoint, "http://[fdaa::3]:8080");
        assert_eq!(
            c.lease,
            crate::LeaseConfig {
                ttl_ms: 6000,
                skew_ms: 1000,
                sample_ms: 500
            }
        );
        // A skew that is not below a third of the ttl could let a late
        // renewal count out of order, so it is refused.
        assert!(
            Config::from_env_with(env(&[
                ("WALLEYE_BUCKET", "walleye"),
                ("WALLEYE_LEASE_TTL_MS", "3000"),
                ("WALLEYE_LEASE_SKEW_MS", "1000"),
            ]))
            .is_err()
        );
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
