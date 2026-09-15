//! Append-latency profile for the direct (Fly-style) gateway write path.
//!
//! This is a measurement harness rather than a pass/fail test. It starts
//! three storage nodes and two stateless direct coordinators over them, then
//! appends single records under the placements that matter on Fly:
//!
//! * one coordinator (writer pinned) versus alternating coordinators
//!   (Flycast round-robin, where every request lands on a cache that is one
//!   append behind and must rebuild);
//! * an untrimmed hot log versus one compacted to the archive;
//! * with and without the one-second archive pass running in the background;
//! * a small control document versus one that has published as many stream
//!   routes as staging (every route CAS leaves a full serialized manifest in
//!   `manifest_operations`, so the document grows quadratically).
//!
//! Run it alone with output visible:
//!
//! ```text
//! LAKEDAY_PROFILE_APPENDS=200 LAKEDAY_PROFILE_ROUTES=160 \
//!   cargo test --release --test append_profile -- --nocapture --test-threads=1
//! ```
//!
//! `LAKEDAY_PROFILE_APPENDS` overrides the per-scenario append count (50 by
//! default so the plain `cargo test` run stays short; the investigation used
//! 200) and `LAKEDAY_PROFILE_ROUTES` the number of routes published for the
//! bloated control document (40 by default; 160 reproduces the staging-sized
//! document). An instrumented build of the crate additionally prints
//! `PROFILE <phase> <nanos>` lines on stderr when `LAKEDAY_PROFILE` is set;
//! the unmodified crate ignores the variable.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::EncryptedRecord;
use walleye_bitr_server::{
    DiskReplica, INTERNAL_AUTH_HEADER, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router,
};

const INTERNAL_TOKEN: &str = "append-profile-internal-token";
const ROOT_KEY: &str = "bW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW0=";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn env_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn record(stream: &str, lsn: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": 1,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 64],
        "authentication": vec![marker; 32],
    }))
    .expect("valid encrypted record fixture")
}

/// An in-memory object store with a fixed per-request latency, so the
/// archive head/segment reads on the coordinator rebuild path cost roughly
/// what one Tigris/S3 round trip costs instead of nothing.
#[derive(Debug)]
struct SlowStore {
    inner: object_store::memory::InMemory,
    latency: Duration,
    requests: AtomicUsize,
}

impl SlowStore {
    fn new(latency: Duration) -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            latency,
            requests: AtomicUsize::new(0),
        }
    }

    async fn delay(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
    }
}

impl std::fmt::Display for SlowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowStore({:?})", self.latency)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.delay().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.delay().await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.delay().await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.delay().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.delay().await;
        self.inner.copy_opts(from, to, options).await
    }
}

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    data_dir: std::path::PathBuf,
    _task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

struct Fixture {
    directory: tempfile::TempDir,
    nodes: Vec<RunningNode>,
    gateway_a: Arc<ReplicaGateway>,
    gateway_b: Arc<ReplicaGateway>,
    store: Arc<SlowStore>,
}

async fn wait_for_storage_ready(node: &RunningNode) -> TestResult {
    let client = reqwest::Client::new();
    let url = format!("{}/internal/v1/status", node.member.url);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let ready = client
                .get(&url)
                .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            if ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| format!("storage node {} did not become ready", node.member.id))?;
    Ok(())
}

async fn fixture(archive_latency: Duration) -> TestResult<Fixture> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(SlowStore::new(archive_latency));
    let archive = Arc::new(OpaqueArchive::new(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        "append-profile",
        64,
    )?);
    let names = ["node-a", "node-b", "node-c"];
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(name, format!("http://{}", listener.local_addr()?));
        bound.push((name, member, listener));
    }
    let seed = bound
        .iter()
        .map(|(_, member, _)| member.clone())
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &seed,
        )?);
        let app = node_router(Arc::clone(&disk), ROOT_KEY, Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode {
            member,
            disk,
            data_dir: data,
            _task: task,
        });
    }
    for node in &nodes {
        wait_for_storage_ready(node).await?;
    }
    let members = nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let gateway_a = Arc::new(ReplicaGateway::new_direct(
        members.clone(),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join("gateway-a-control.json"),
        Arc::clone(&archive),
    )?);
    let gateway_b = Arc::new(ReplicaGateway::new_direct(
        members,
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join("gateway-b-control.json"),
        archive,
    )?);
    Ok(Fixture {
        directory,
        nodes,
        gateway_a,
        gateway_b,
        store,
    })
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn describe(samples: &mut [Duration]) -> String {
    samples.sort();
    let total: Duration = samples.iter().sum();
    let mean = total
        .checked_div(samples.len().max(1) as u32)
        .unwrap_or_default();
    format!(
        "mean={:>7.2}ms p50={:>7.2}ms p95={:>7.2}ms max={:>7.2}ms",
        mean.as_secs_f64() * 1e3,
        percentile(samples, 0.50).as_secs_f64() * 1e3,
        percentile(samples, 0.95).as_secs_f64() * 1e3,
        samples.last().copied().unwrap_or_default().as_secs_f64() * 1e3,
    )
}

/// Appends `count` records to `stream`, rotating through `gateways`, and
/// prints the append and certificate latency distributions.
async fn run_appends(
    label: &str,
    gateways: &[&ReplicaGateway],
    stream: &str,
    next_lsn: &mut u64,
    count: usize,
) -> TestResult<Duration> {
    eprintln!("PROFILE-SCENARIO {label}");
    let mut appends = Vec::with_capacity(count);
    let mut certificates = Vec::with_capacity(count);
    let wall = Instant::now();
    for index in 0..count {
        let gateway = gateways[index % gateways.len()];
        let record = record(stream, *next_lsn, (index % 251) as u8);
        let started = Instant::now();
        gateway
            .append(record.clone())
            .await
            .map_err(|error| format!("{label}: append lsn {}: {error:?}", *next_lsn))?;
        appends.push(started.elapsed());
        let started = Instant::now();
        gateway
            .issue_commit_certificate(&record)
            .await
            .map_err(|error| format!("{label}: certificate lsn {}: {error:?}", *next_lsn))?;
        certificates.push(started.elapsed());
        *next_lsn += 1;
    }
    let wall = wall.elapsed();
    println!(
        "{label:<44} n={count:<4} append      {}",
        describe(&mut appends)
    );
    println!(
        "{:<44} {:<6} certificate {}",
        "",
        "",
        describe(&mut certificates)
    );
    Ok(wall)
}

fn file_len(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn print_control_sizes(fixture: &Fixture, label: &str) {
    let node_sizes = fixture
        .nodes
        .iter()
        .map(|node| file_len(&node.data_dir.join("control.json")))
        .collect::<Vec<_>>();
    let log_sizes = fixture
        .nodes
        .iter()
        .map(|node| file_len(&node.data_dir.join("replica.log")))
        .collect::<Vec<_>>();
    println!(
        "{label}: control.json bytes nodes={node_sizes:?} gateway-a={} gateway-b={} | replica.log bytes nodes={log_sizes:?}",
        file_len(&fixture.directory.path().join("gateway-a-control.json")),
        file_len(&fixture.directory.path().join("gateway-b-control.json")),
    );
}

async fn trim_all(fixture: &Fixture) -> TestResult<usize> {
    let mut archived = 0;
    for node in &fixture.nodes {
        archived += fixture.gateway_a.archive_local_commits(&node.disk).await?;
    }
    Ok(archived)
}

fn spawn_archive_loops(fixture: &Fixture) -> (Vec<tokio::task::JoinHandle<()>>, Arc<AtomicUsize>) {
    let failures = Arc::new(AtomicUsize::new(0));
    let handles = fixture
        .nodes
        .iter()
        .map(|node| {
            let gateway = Arc::clone(&fixture.gateway_a);
            let disk = Arc::clone(&node.disk);
            let failures = Arc::clone(&failures);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    if gateway.archive_local_commits(&disk).await.is_err() {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    (handles, failures)
}

async fn publish_routes(fixture: &Fixture, routes: usize) -> TestResult<Duration> {
    let started = Instant::now();
    for index in 0..routes {
        let stream = format!("profile/route-{index}");
        fixture.gateway_a.append(record(&stream, 1, 1)).await?;
    }
    Ok(started.elapsed())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_latency_profile() -> TestResult {
    let count = env_count("LAKEDAY_PROFILE_APPENDS", 50);
    let routes = env_count("LAKEDAY_PROFILE_ROUTES", 40);
    let fixture = fixture(Duration::ZERO).await?;
    let a = fixture.gateway_a.as_ref();
    let b = fixture.gateway_b.as_ref();
    let single = "profile/single";
    let alternating = "profile/alternating";
    let mut single_lsn = 1;
    let mut alternating_lsn = 1;

    println!("== append profile: 3 nodes, quorum 2, {count} appends per scenario ==");
    run_appends(
        "untrimmed / single gateway",
        &[a],
        single,
        &mut single_lsn,
        count,
    )
    .await?;
    run_appends(
        "untrimmed / alternating gateways",
        &[a, b],
        alternating,
        &mut alternating_lsn,
        count,
    )
    .await?;
    print_control_sizes(&fixture, "before trim");

    let archived = trim_all(&fixture).await?;
    println!(
        "trimmed hot logs: archived {archived} records, {} object-store requests so far",
        fixture.store.requests.load(Ordering::Relaxed)
    );
    print_control_sizes(&fixture, "after trim");
    run_appends(
        "trimmed / single gateway",
        &[a],
        single,
        &mut single_lsn,
        count,
    )
    .await?;
    run_appends(
        "trimmed / alternating gateways",
        &[a, b],
        alternating,
        &mut alternating_lsn,
        count,
    )
    .await?;

    let (loops, failures) = spawn_archive_loops(&fixture);
    run_appends(
        "trimmed + 1s archive loop / single gateway",
        &[a],
        single,
        &mut single_lsn,
        count,
    )
    .await?;
    run_appends(
        "trimmed + 1s archive loop / alternating",
        &[a, b],
        alternating,
        &mut alternating_lsn,
        count,
    )
    .await?;
    for handle in loops {
        handle.abort();
    }
    println!(
        "archive loop failures: {}",
        failures.load(Ordering::Relaxed)
    );

    let elapsed = publish_routes(&fixture, routes).await?;
    println!(
        "published {routes} extra stream routes in {:.2}s",
        elapsed.as_secs_f64()
    );
    print_control_sizes(&fixture, "after routes");
    run_appends(
        "bloated control / single gateway",
        &[a],
        single,
        &mut single_lsn,
        count,
    )
    .await?;
    run_appends(
        "bloated control / alternating gateways",
        &[a, b],
        alternating,
        &mut alternating_lsn,
        count,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_latency_profile_slow_archive() -> TestResult {
    let count = env_count("LAKEDAY_PROFILE_APPENDS", 50) / 2;
    let latency = Duration::from_millis(env_count("LAKEDAY_PROFILE_ARCHIVE_MS", 30) as u64);
    let fixture = fixture(latency).await?;
    let a = fixture.gateway_a.as_ref();
    let b = fixture.gateway_b.as_ref();
    let single = "profile/single";
    let alternating = "profile/alternating";
    let mut single_lsn = 1;
    let mut alternating_lsn = 1;

    println!("== append profile with {latency:?} archive latency, {count} appends per scenario ==");
    run_appends(
        "slow archive, untrimmed / single",
        &[a],
        single,
        &mut single_lsn,
        count,
    )
    .await?;
    run_appends(
        "slow archive, untrimmed / alternating",
        &[a, b],
        alternating,
        &mut alternating_lsn,
        count,
    )
    .await?;
    let archived = trim_all(&fixture).await?;
    println!("trimmed hot logs: archived {archived} records");
    run_appends(
        "slow archive, trimmed / single",
        &[a],
        single,
        &mut single_lsn,
        count,
    )
    .await?;
    run_appends(
        "slow archive, trimmed / alternating",
        &[a, b],
        alternating,
        &mut alternating_lsn,
        count,
    )
    .await?;
    println!(
        "object-store requests total: {}",
        fixture.store.requests.load(Ordering::Relaxed)
    );
    Ok(())
}
