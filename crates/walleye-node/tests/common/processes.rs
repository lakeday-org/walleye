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
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaNode, gateway_router, node_router};
use walleye_node::{ApiConfig, Config, LeaseConfig, Service, cluster, router};
use walleye_ring::Node;

pub const TOKEN: &str = "deployment-secret-token";
pub const INTERNAL: &str = "node-internal-test-token";
pub const ROOT_KEY: [u8; 32] = [23; 32];

/// Short enough to run in seconds, with the production proportions: renew
/// every third of the ttl, a skew allowance under that, sampling well inside
/// the ttl.
pub fn fast() -> LeaseConfig {
    LeaseConfig {
        ttl_ms: 1_500,
        skew_ms: 300,
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
            processor: None,
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
            runtime.block_on(async move {
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
            });
            runtime.shutdown_background();
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
    static KEYS: std::sync::Once = std::sync::Once::new();
    KEYS.call_once(|| {
        // SAFETY: set once, before any process in this binary reads them, and
        // to the same values every test would set.
        unsafe {
            std::env::set_var("LAKEDAY_DATAPLANE_ROOT_KEY", STANDARD.encode(ROOT_KEY));
            std::env::set_var("WALLEYE_DATA_KEY", STANDARD.encode([9_u8; 32]));
        }
    });
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
