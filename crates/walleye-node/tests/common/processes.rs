//! Processes on their own runtimes over one bucket, and the requests the
//! ownership tests make of them.
//!
//! Each process runs on a runtime of its own, so killing one is ending its
//! runtime - its lease stops renewing and its port closes, with nothing
//! released - and pausing one is blocking every one of its worker threads at
//! once, which stalls its renewals, its requests and its timers together, as
//! a stopped VM or a long GC pause would.
#![allow(dead_code)]
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::net::TcpListener;
use walleye_bitr_server::{
    DiskReplica, INTERNAL_AUTH_HEADER, OpaqueArchive, ReplicaNode,
    daemon::{CombinedReplica, serve_combined},
    gateway_router, node_router,
};
use walleye_node::{ApiConfig, Config, LeaseConfig, Service, cluster, router};
use walleye_ring::Node;

pub const TOKEN: &str = "deployment-secret-token";
pub const INTERNAL: &str = "node-internal-test-token";
pub const ROOT_KEY: [u8; 32] = [23; 32];

/// Short enough to run in seconds, with the production proportions: renew
/// every third of the ttl, a skew allowance under that, sampling well inside
/// the ttl. The skew is what a renewal may take to land, so it is sized for a
/// busy machine rather than an idle one: a renewal slower than it does not
/// count, and a process that misses enough of them stands down, which is right
/// but is not what these tests are about.
pub fn fast() -> LeaseConfig {
    LeaseConfig {
        ttl_ms: 3_000,
        skew_ms: 900,
        sample_ms: 250,
    }
}

pub enum Command {
    Pause(Duration),
    Kill,
    Stop,
}

/// Worker threads per process.
const WORKERS: usize = 2;

pub struct Proc {
    pub base: String,
    commands: tokio::sync::mpsc::UnboundedSender<Command>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Proc {
    /// Start a process. `node_id` is the configured id; two processes may
    /// share one, as an original and its replacement do.
    pub async fn start(
        node_id: &str,
        root: &str,
        cache: &std::path::Path,
        bitr: Option<&str>,
        lease: LeaseConfig,
    ) -> Self {
        Self::start_inner(node_id, root, cache, bitr, lease, None).await
    }

    /// Start a launch node: the engine and, in the same process, its own
    /// replica and coordinator, as one Machine runs them. Killing it kills
    /// both.
    pub async fn start_launch(
        node_id: &str,
        root: &str,
        cache: &std::path::Path,
        lease: LeaseConfig,
        replica: CombinedReplica,
    ) -> Self {
        let gateway = format!("http://{}", replica.gateway_address);
        Self::start_inner(node_id, root, cache, Some(&gateway), lease, Some(replica)).await
    }

    async fn start_inner(
        node_id: &str,
        root: &str,
        cache: &std::path::Path,
        bitr: Option<&str>,
        lease: LeaseConfig,
        replica: Option<CombinedReplica>,
    ) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let config = Config {
            node_id: node_id.into(),
            listen: base.clone(),
            directory: cache.join(uuid::Uuid::new_v4().simple().to_string()),
            memory_bytes: 1024 * 1024 * 1024,
            disk_bytes: 64 * 1024 * 1024,
            token: TOKEN.into(),
            bitr: bitr.is_some(),
            members: vec![Node::new(node_id, base.clone(), 1.0).unwrap()],
            kubernetes: None,
            lease,
            api: Some(ApiConfig {
                root_uri: root.to_owned(),
                bitr_url: bitr.map(str::to_owned),
            }),
        };
        let (commands, mut inbox) = tokio::sync::mpsc::unbounded_channel();
        let (ready, started) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(WORKERS)
                .enable_all()
                .build()
                .unwrap();
            let replica = async move {
                match replica {
                    Some(replica) => {
                        if let Err(error) = serve_combined(replica, std::future::pending()).await {
                            eprintln!("test replica stopped: {error}");
                        }
                        std::future::pending::<()>().await
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            let engine = async move {
                let service = Service::open(config).await.unwrap();
                let app = router(service.clone());
                use axum::serve::ListenerExt;
                let listener = TcpListener::from_std(listener).unwrap().tap_io(|tcp| {
                    let _ = tcp.set_nodelay(true);
                });
                let (quit, quitting) = tokio::sync::oneshot::channel::<()>();
                let server = tokio::spawn(async move {
                    let _ = axum::serve(listener, app)
                        .with_graceful_shutdown(async {
                            let _ = quitting.await;
                        })
                        .await;
                });
                let _ = ready.send(());
                while let Some(command) = inbox.recv().await {
                    match command {
                        // Every worker stops: nothing in this process runs.
                        // Each blocker waits for the others before it sleeps,
                        // so they hold all the workers at once rather than
                        // one after another.
                        Command::Pause(pause) => {
                            let all = Arc::new(std::sync::Barrier::new(WORKERS));
                            for _ in 0..WORKERS {
                                let all = all.clone();
                                tokio::spawn(async move {
                                    all.wait();
                                    std::thread::sleep(pause);
                                });
                            }
                        }
                        // Gone without a word, as a crash leaves it.
                        Command::Kill => return,
                        // As the binary stops on SIGTERM: hand the tables
                        // over while still answering, then stop serving.
                        Command::Stop => {
                            service.release().await;
                            let _ = quit.send(());
                            let _ = server.await;
                            service.close().await;
                            return;
                        }
                    }
                }
            };
            // The replica runs beside the engine for as long as the engine
            // does, and stops with it.
            runtime.block_on(async move {
                tokio::select! {
                    () = engine => {}
                    () = replica => {}
                }
            });
            // Wait for the workers to stop, so a killed process's port is
            // closed when `kill` returns rather than some time after.
            runtime.shutdown_timeout(Duration::from_secs(5));
        });
        started.await.expect("the process started");
        Self {
            base,
            commands,
            thread: Some(thread),
        }
    }
    pub fn pause(&self, pause: Duration) {
        self.commands.send(Command::Pause(pause)).unwrap();
    }
    async fn end(mut self, command: Command) {
        self.commands.send(command).unwrap();
        let thread = self.thread.take().unwrap();
        tokio::task::spawn_blocking(move || thread.join().unwrap())
            .await
            .unwrap();
    }
    pub async fn kill(self) {
        self.end(Command::Kill).await
    }
    pub async fn stop(self) {
        self.end(Command::Stop).await
    }
}
impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Kill);
    }
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

pub struct Answer {
    pub status: u16,
    pub owner: String,
    pub route_error: Option<String>,
    pub retry_after: Option<String>,
    pub body: Value,
}

pub async fn post(base: &str, path: &str, body: Value, forwarded: bool) -> Answer {
    let mut request = client()
        .post(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .json(&body);
    if forwarded {
        request = request.header(cluster::FORWARDED_HEADER, "1");
    }
    let response = request.send().await.unwrap();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    let status = response.status().as_u16();
    let owner = header(cluster::OWNER_HEADER).unwrap_or_default();
    let route_error = header(cluster::ROUTE_ERROR_HEADER);
    let retry_after = header("retry-after");
    let text = response.text().await.unwrap_or_default();
    Answer {
        status,
        owner,
        route_error,
        retry_after,
        body: serde_json::from_str(&text).unwrap_or(Value::String(text)),
    }
}

pub async fn status(base: &str) -> Value {
    client()
        .get(format!("{base}/internal/ownership"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

pub async fn held(base: &str) -> BTreeMap<String, u64> {
    status(base).await["held"]
        .as_object()
        .map(|held| {
            held.iter()
                .map(|(table, epoch)| (table.clone(), epoch.as_u64().unwrap()))
                .collect()
        })
        .unwrap_or_default()
}

pub async fn session(base: &str) -> String {
    status(base).await["node"].as_str().unwrap().to_owned()
}

/// Define a table through `base`, which claims it. A Bitr-backed process
/// refuses until it has seen its quorum, which any client waits out.
pub async fn define(base: &str, table: &str) {
    for _ in 0..120 {
        let answer = post(
            base,
            "/v1/streams",
            json!({"name": table, "primary_key": ["id"],
                   "columns": [{"name":"id","type":"int64"},{"name":"at","type":"int64"}]}),
            false,
        )
        .await;
        if answer.status == 200 {
            return;
        }
        assert_eq!(answer.status, 503, "define {table}: {}", answer.body);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("{table} was never defined");
}

pub async fn write(base: &str, table: &str, id: i64) -> Answer {
    post(
        base,
        &format!("/v1/streams/{table}/events"),
        json!({"rows": [{"id": id, "at": id}]}),
        false,
    )
    .await
}

/// Every id in `table`, and how many rows hold them, asked of `base`.
pub async fn ids(base: &str, table: &str) -> (Vec<i64>, u64) {
    let answer = post(
        base,
        "/v1/query",
        json!({"sql": format!("SELECT id FROM {table} ORDER BY id")}),
        false,
    )
    .await;
    assert_eq!(answer.status, 200, "read {table}: {}", answer.body);
    let rows = answer.body.as_array().unwrap();
    let ids: Vec<i64> = rows.iter().map(|row| row["id"].as_i64().unwrap()).collect();
    (ids, rows.len() as u64)
}

/// Every acknowledged id is present, and exactly once.
pub async fn exactly_once(base: &str, table: &str, acknowledged: &[i64]) {
    let (found, rows) = ids(base, table).await;
    let mut distinct = found.clone();
    distinct.dedup();
    assert_eq!(
        rows as usize,
        distinct.len(),
        "{table} holds a row twice: {found:?}"
    );
    let mut expected = acknowledged.to_vec();
    expected.sort_unstable();
    let missing: Vec<_> = expected.iter().filter(|id| !found.contains(id)).collect();
    assert!(
        missing.is_empty(),
        "{table} lost acknowledged rows {missing:?}"
    );
}

/// One Bitr replica set - three replicas that fsync, behind a gateway - that
/// every process in a test appends to, as the three members of a launch
/// instance do.
pub async fn bitr(dir: &std::path::Path) -> (String, Vec<tokio::task::JoinHandle<()>>) {
    keys();
    let encoded = STANDARD.encode(ROOT_KEY);
    let mut tasks = Vec::new();
    let mut members = Vec::new();
    for index in 0..3 {
        let node = Arc::new(
            DiskReplica::open_with_config(
                dir.join(format!("replica-{index}.log")),
                "test-node",
                "hot",
                dir,
            )
            .expect("replica storage"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = node_router(Arc::clone(&node), &encoded, Some(INTERNAL)).expect("node router");
        tasks.push(tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        }));
        members.push(ReplicaNode::new(
            format!("replica-{index}"),
            format!("http://{address}"),
        ));
    }
    let archive = Arc::new(
        OpaqueArchive::new(
            Arc::new(object_store_bitr::memory::InMemory::new()),
            "replica",
            64,
        )
        .expect("archive"),
    );
    let gateway = Arc::new(
        walleye_bitr_server::ReplicaGateway::new(members, 2, &encoded, INTERNAL, archive)
            .expect("gateway"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tasks.push(tokio::spawn(async move {
        let _ = axum::serve(listener, gateway_router(gateway)).await;
    }));
    (format!("http://{address}"), tasks)
}

pub async fn get_json(base: &str, path: &str) -> Value {
    client()
        .get(format!("{base}{path}"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// How many of `tables` each of `bases` holds, once every table has an owner
/// and nothing has moved for several sweeps.
///
/// Stillness counts only once every process sweeps: until a process has
/// watched the bucket for a lease verdict it hands nothing back, so the
/// spread holds still then without being settled.
pub async fn settled_spread(bases: &[&str], tables: &[String], limit: Duration) -> Vec<usize> {
    let started = std::time::Instant::now();
    let mut last: Option<Vec<usize>> = None;
    let mut steady = 0;
    loop {
        let mut counts = Vec::new();
        let mut sweeping = true;
        for base in bases {
            let status = status(base).await;
            sweeping &= status["settled"].as_bool() == Some(true);
            let held = status["held"].as_object().cloned().unwrap_or_default();
            counts.push(tables.iter().filter(|t| held.contains_key(*t)).count());
        }
        let owned: usize = counts.iter().sum();
        if sweeping && owned == tables.len() && last.as_ref() == Some(&counts) {
            steady += 1;
            if steady >= 6 {
                return counts;
            }
        } else {
            steady = 0;
        }
        last = Some(counts.clone());
        assert!(started.elapsed() < limit, "never settled: {counts:?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Which of `bases` holds `key`, once one does.
pub async fn holder_of(bases: &[&str], key: &str) -> usize {
    for _ in 0..200 {
        for (index, base) in bases.iter().enumerate() {
            if held(base).await.contains_key(key) {
                return index;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nobody holds {key}");
}

pub async fn view(base: &str, name: &str, definition: Value) {
    let answer = post(base, &format!("/v1/view/{name}/create/"), definition, false).await;
    assert_eq!(answer.status, 200, "create view {name}: {}", answer.body);
}

/// Rows of a query, or none while the table does not exist yet.
pub async fn rows(base: &str, sql: &str) -> Vec<Value> {
    let answer = post(base, "/v1/query", json!({ "sql": sql }), false).await;
    match answer.body {
        Value::Array(rows) if answer.status == 200 => rows,
        _ => Vec::new(),
    }
}

pub async fn wait_for<F, Fut>(what: &str, limit: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while !check().await {
        assert!(started.elapsed() < limit, "{what} within {limit:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Which of `bases` holds `key`.
pub async fn holder(bases: &[&str], key: &str) -> Option<usize> {
    for (index, base) in bases.iter().enumerate() {
        if held(base).await.contains_key(key) {
            return Some(index);
        }
    }
    None
}

/// A view on a one-second clock whose worker records which occurrence each
/// run stands for and how many it covered, into `ticks`.
pub fn ticker() -> Value {
    ticker_into("ticks")
}

/// The same, into `target`.
pub fn ticker_into(target: &str) -> Value {
    json!({
        "every_seconds": 1,
        "target": target,
        // Small, so several can run at once inside a test's memory budget.
        "worker_heap_mb": 16,
        "worker": "export default (rows, ctx) => [{ at: ctx.scheduled.scheduledTime, \
                   missed: ctx.scheduled.missed, nonce: Math.random() }]",
    })
}

/// Every run of the ticker, as (scheduled time, missed), ordered.
pub async fn ticks(base: &str) -> Vec<(u64, u64)> {
    ticks_in(base, "ticks").await
}

/// Every run recorded in `table`, as (scheduled time, missed), ordered.
pub async fn ticks_in(base: &str, table: &str) -> Vec<(u64, u64)> {
    let mut runs: Vec<(u64, u64)> = rows(base, &format!("SELECT at, missed FROM {table}"))
        .await
        .iter()
        .map(|row| (row["at"].as_u64().unwrap(), row["missed"].as_u64().unwrap()))
        .collect();
    runs.sort_unstable();
    runs
}

/// Each occurrence of a one-second schedule is accounted for exactly once:
/// run by one run, or covered by the next run's missed count. No time is run
/// twice and none is skipped silently.
pub fn each_occurrence_once(runs: &[(u64, u64)]) {
    let mut times: Vec<u64> = runs.iter().map(|(at, _)| *at).collect();
    times.dedup();
    assert_eq!(times.len(), runs.len(), "an occurrence ran twice: {runs:?}");
    for pair in runs.windows(2) {
        let ((previous, _), (at, missed)) = (pair[0], pair[1]);
        assert_eq!(
            at - previous,
            (missed + 1) * 1000,
            "occurrences between {previous} and {at} are unaccounted for: {runs:?}"
        );
    }
}

/// The deployment keys every engine and replica in this binary reads.
fn keys() {
    static KEYS: std::sync::Once = std::sync::Once::new();
    KEYS.call_once(|| {
        // SAFETY: set once, before any process in this binary reads them, and
        // to the same values every test would set.
        unsafe {
            std::env::set_var("LAKEDAY_DATAPLANE_ROOT_KEY", STANDARD.encode(ROOT_KEY));
            std::env::set_var("WALLEYE_DATA_KEY", STANDARD.encode([9_u8; 32]));
        }
    });
}

/// Three launch nodes' fixed identities: each one's replica name, data
/// directory and addresses, which a restart reuses, and the archive they
/// share.
pub struct Launch {
    pub root: String,
    pub archive: Arc<OpaqueArchive>,
    pub members: Vec<ReplicaNode>,
    gateways: Vec<String>,
    data: std::path::PathBuf,
}

fn free_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

impl Launch {
    pub fn new(dir: &std::path::Path) -> Self {
        keys();
        let store: Arc<dyn object_store_bitr::ObjectStore> =
            Arc::new(object_store_bitr::memory::InMemory::new());
        let archive = Arc::new(OpaqueArchive::new(store, "bitr", 16).expect("archive"));
        let members = (0..3)
            .map(|i| ReplicaNode::new(format!("node-{i}"), format!("http://{}", free_address())))
            .collect();
        Self {
            root: format!("file://{}/store", dir.display()),
            archive,
            members,
            gateways: (0..3).map(|_| free_address()).collect(),
            data: dir.join("replicas"),
        }
    }

    /// The replica half of node `index`, on the same volume every time.
    pub fn replica(&self, index: usize) -> CombinedReplica {
        let name = format!("node-{index}");
        let data_dir = self.data.join(&name);
        std::fs::create_dir_all(&data_dir).unwrap();
        CombinedReplica {
            root_key: STANDARD.encode(ROOT_KEY),
            log_path: data_dir.join("replica.log"),
            control_path: data_dir.join("control.json"),
            data_dir,
            node_name: name,
            tier: "nvme".into(),
            members: self.members.clone(),
            internal_token: INTERNAL.into(),
            admin_token: String::new(),
            quorum: 2,
            archive: Arc::clone(&self.archive),
            storage_address: self.members[index]
                .url
                .trim_start_matches("http://")
                .to_owned(),
            gateway_address: self.gateways[index].clone(),
        }
    }

    /// Start node `index`, engine and replica together.
    pub async fn start(&self, index: usize, cache: &std::path::Path, lease: LeaseConfig) -> Proc {
        Proc::start_launch(
            &format!("node-{index}"),
            &self.root,
            cache,
            lease,
            self.replica(index),
        )
        .await
    }

    /// How far node `index`'s replica holds each stream: the highest LSN in
    /// its log or in the archived prefix it has trimmed. `None` while the
    /// node is not answering.
    pub async fn positions(&self, index: usize) -> Option<BTreeMap<String, u64>> {
        let snapshot: Value = client()
            .get(format!("{}/internal/v1/records", self.members[index].url))
            .header(INTERNAL_AUTH_HEADER, INTERNAL)
            .query(&[("stream", "*")])
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        let mut positions = BTreeMap::new();
        for record in snapshot["records"].as_array()? {
            let (Some(stream), Some(lsn)) = (record["stream"].as_str(), record["lsn"].as_u64())
            else {
                continue;
            };
            let entry = positions.entry(stream.to_owned()).or_insert(0);
            *entry = (*entry).max(lsn);
        }
        for prefix in snapshot["trimmed"].as_array()? {
            let (Some(stream), Some(lsn)) =
                (prefix["stream"].as_str(), prefix["archived_lsn"].as_u64())
            else {
                continue;
            };
            let entry = positions.entry(stream.to_owned()).or_insert(0);
            *entry = (*entry).max(lsn);
        }
        Some(positions)
    }
}
