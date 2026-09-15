//! Append-path cost bounds for the direct (Fly-style) gateway.
//!
//! Three storage nodes run behind a per-node request hook that counts every
//! HTTP request and can hold a member silent, with stateless direct
//! coordinators over them, as on a Fly cell. Each test pins one structural
//! property of the write path: the control document stays bounded while
//! routes accumulate, a member's append cost is independent of that
//! document, the gateway issues a bounded number of node requests per
//! append, and a member that stops answering does not stall the cell for
//! the client timeout.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::ReplicaError;
use walleye_bitr::{EncryptedRecord, Replica};
use walleye_bitr_server::{
    DiskReplica, DurableControl, INTERNAL_AUTH_HEADER, OpaqueArchive, ReplicaGateway, ReplicaNode,
    node_router,
};

const INTERNAL_TOKEN: &str = "append-path-internal-token";
const ROOT_KEY: &str = "bW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW0=";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn record(stream: &str, lsn: u64, marker: u8) -> EncryptedRecord {
    record_at_epoch(stream, lsn, 1, marker)
}

fn record_at_epoch(stream: &str, lsn: u64, writer_epoch: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": writer_epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 64],
        "authentication": vec![marker; 32],
    }))
    .expect("valid encrypted record fixture")
}

/// Per-node request hook: counts every request that reaches the node and,
/// while `silent` is set, parks the request forever so the member looks like
/// a suspended Machine whose keep-alive connection accepted the bytes.
#[derive(Clone, Default)]
struct NodeHook {
    requests: Arc<AtomicUsize>,
    storage_status_requests: Arc<AtomicUsize>,
    metadata_adopt_requests: Arc<AtomicUsize>,
    append_many_requests: Arc<AtomicUsize>,
    silent: Arc<AtomicBool>,
    pause_next_append: Arc<AtomicBool>,
    append_paused: Arc<tokio::sync::Notify>,
    release_append: Arc<tokio::sync::Notify>,
}

async fn hook_layer(State(hook): State<NodeHook>, request: Request, next: Next) -> Response {
    hook.requests.fetch_add(1, Ordering::SeqCst);
    if matches!(
        request.uri().path(),
        "/internal/v1/metrics" | "/internal/v1/status"
    ) {
        hook.storage_status_requests.fetch_add(1, Ordering::SeqCst);
    }
    if request.uri().path() == "/internal/v1/control/metadata/adopt" {
        hook.metadata_adopt_requests.fetch_add(1, Ordering::SeqCst);
    }
    if request.uri().path() == "/internal/v1/append-many" {
        hook.append_many_requests.fetch_add(1, Ordering::SeqCst);
    }
    if request.uri().path() == "/internal/v1/append-many"
        && hook.pause_next_append.swap(false, Ordering::SeqCst)
    {
        hook.append_paused.notify_one();
        hook.release_append.notified().await;
    }
    if hook.silent.load(Ordering::SeqCst) {
        std::future::pending::<()>().await;
    }
    next.run(request).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_inflight_stream_does_not_hold_other_stream_writers() -> TestResult {
    let fixture = fixture(1).await?;
    let gateway = Arc::clone(&fixture.gateways[0]);
    gateway.append(record("tenant-a/paused", 1, 1)).await?;
    gateway.append(record("tenant-a/independent", 1, 2)).await?;
    for node in &fixture.nodes {
        node.hook.pause_next_append.store(true, Ordering::SeqCst);
    }
    let slow_gateway = Arc::clone(&gateway);
    let slow =
        tokio::spawn(async move { slow_gateway.append(record("tenant-a/paused", 2, 3)).await });
    for node in &fixture.nodes {
        tokio::time::timeout(Duration::from_secs(2), node.hook.append_paused.notified()).await?;
    }
    let independent = tokio::time::timeout(
        Duration::from_secs(1),
        gateway.append(record("tenant-a/independent", 2, 4)),
    )
    .await;
    let slow_still_pending = !slow.is_finished();
    for node in &fixture.nodes {
        node.hook.release_append.notify_one();
    }
    tokio::time::timeout(Duration::from_secs(2), slow).await???;
    assert!(
        slow_still_pending,
        "independent append waited for the paused stream"
    );
    independent.map_err(|_| "an unrelated stream was blocked by the writer map lock")??;
    assert_eq!(gateway.recover("tenant-a/independent", 0).await?.len(), 2);
    Ok(())
}

/// Direct coordinators do not need a control-plane status sample before every
/// append.  The append request itself remains fenced by each node's durable
/// maintenance and placement state, while unrelated stream writers can enter
/// the data path concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_appends_skip_status_fanout_and_run_distinct_streams_in_parallel() -> TestResult {
    let fixture = fixture(1).await?;
    let gateway = Arc::clone(&fixture.gateways[0]);
    let streams = [
        "parallel/stream-0",
        "parallel/stream-1",
        "parallel/stream-2",
        "parallel/stream-3",
    ];
    for (index, stream) in streams.iter().enumerate() {
        gateway.append(record(stream, 1, index as u8)).await?;
    }
    let status_before = fixture
        .nodes
        .iter()
        .map(|node| node.hook.storage_status_requests.load(Ordering::SeqCst))
        .sum::<usize>();

    let started = std::time::Instant::now();
    let (first, second, third, fourth) = tokio::join!(
        gateway.append(record(streams[0], 2, 10)),
        gateway.append(record(streams[1], 2, 11)),
        gateway.append(record(streams[2], 2, 12)),
        gateway.append(record(streams[3], 2, 13)),
    );
    let elapsed = started.elapsed();
    first?;
    second?;
    third?;
    fourth?;
    let status_after = fixture
        .nodes
        .iter()
        .map(|node| node.hook.storage_status_requests.load(Ordering::SeqCst))
        .sum::<usize>();
    eprintln!("four distinct direct appends completed in {elapsed:?}");
    assert_eq!(
        status_after, status_before,
        "ordinary direct appends must not fan out storage status"
    );
    Ok(())
}

/// A coordinator that starts after a durable maintenance fence is installed
/// has no process-local fence state.  It must still fail at the storage
/// append boundary and can only acknowledge after the same fence is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_direct_gateway_cannot_ack_through_durable_fence() -> TestResult {
    let fixture = fixture(1).await?;
    let first = Arc::clone(&fixture.gateways[0]);
    let stream = "fence/restarted";
    first.append(record(stream, 1, 1)).await?;
    let token = first.begin_maintenance().await?;

    let members = fixture
        .nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let restarted = ReplicaGateway::new_direct(
        members,
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        fixture._directory.path().join("restarted-control.json"),
        Arc::new(OpaqueArchive::new(
            Arc::new(object_store::memory::InMemory::new()),
            "restarted-fence",
            64,
        )?),
    )?;
    let fenced = restarted.append(record(stream, 2, 2)).await;
    assert_eq!(fenced, Err(ReplicaError::WriterFenced));
    for node in &fixture.nodes {
        assert_eq!(node.disk.records(stream).await.len(), 1);
    }

    first.end_maintenance(&token).await?;
    let stale = record_at_epoch(stream, 2, 0, 3);
    assert_eq!(
        restarted.append(stale).await,
        Err(ReplicaError::WriterFenced)
    );
    for node in &fixture.nodes {
        assert_eq!(node.disk.records(stream).await.len(), 1);
    }
    assert_eq!(restarted.append(record(stream, 2, 2)).await?, 2);
    Ok(())
}

struct RunningNode {
    member: ReplicaNode,
    data_dir: std::path::PathBuf,
    disk: Arc<DiskReplica>,
    hook: NodeHook,
    _task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

struct Fixture {
    _directory: tempfile::TempDir,
    nodes: Vec<RunningNode>,
    gateways: Vec<Arc<ReplicaGateway>>,
}

impl Fixture {
    fn control_sizes(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .map(|node| {
                std::fs::metadata(node.data_dir.join("control.json"))
                    .map(|meta| meta.len())
                    .unwrap_or(0)
            })
            .collect()
    }

    /// Total requests every node has received so far.
    fn requests(&self) -> usize {
        self.nodes
            .iter()
            .map(|node| node.hook.requests.load(Ordering::SeqCst))
            .sum()
    }
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

async fn fixture(gateway_count: usize) -> TestResult<Fixture> {
    let directory = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(store, "append-path", 64)?);
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
        let hook = NodeHook::default();
        let app = node_router(Arc::clone(&disk), ROOT_KEY, Some(INTERNAL_TOKEN))?
            .layer(middleware::from_fn_with_state(hook.clone(), hook_layer));
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode {
            member,
            data_dir: data,
            disk,
            hook,
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
    let mut gateways = Vec::new();
    for index in 0..gateway_count {
        gateways.push(Arc::new(ReplicaGateway::new_direct(
            members.clone(),
            2,
            ROOT_KEY,
            INTERNAL_TOKEN,
            directory
                .path()
                .join(format!("gateway-{index}-control.json")),
            Arc::clone(&archive),
        )?));
    }
    Ok(Fixture {
        _directory: directory,
        nodes,
        gateways,
    })
}

/// Once the object-store head exists, a post-migration append must repair each
/// member's local metadata cache from that head before the data append runs.
/// A second append on the immutable route should reuse that proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrated_head_repairs_route_before_append() -> TestResult {
    let fixture = fixture(1).await?;
    let gateway = &fixture.gateways[0];
    let stream = "migrated-head/stream";
    gateway.append(record(stream, 1, 1)).await?;
    gateway
        .migrate_control_authority("migrated-head-test")
        .await?;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let adoptions = fixture
                .nodes
                .iter()
                .map(|node| node.hook.metadata_adopt_requests.load(Ordering::SeqCst))
                .sum::<usize>();
            if adoptions >= fixture.nodes.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| "migration metadata adoption did not settle")?;

    let before_repair = fixture
        .nodes
        .iter()
        .map(|node| node.hook.metadata_adopt_requests.load(Ordering::SeqCst))
        .sum::<usize>();
    gateway.append(record(stream, 2, 2)).await?;
    let after_repair = fixture
        .nodes
        .iter()
        .map(|node| node.hook.metadata_adopt_requests.load(Ordering::SeqCst))
        .sum::<usize>();
    assert!(
        after_repair > before_repair,
        "first post-migration append must repair the route on members"
    );

    let before_cached_append = fixture
        .nodes
        .iter()
        .map(|node| node.hook.append_many_requests.load(Ordering::SeqCst))
        .sum::<usize>();
    gateway.append(record(stream, 3, 3)).await?;
    let after_cached_append = fixture
        .nodes
        .iter()
        .map(|node| node.hook.metadata_adopt_requests.load(Ordering::SeqCst))
        .sum::<usize>();
    assert_eq!(
        after_cached_append, after_repair,
        "a route proof hit must skip metadata repair while append placement still runs"
    );
    let after_cached_data = fixture
        .nodes
        .iter()
        .map(|node| node.hook.append_many_requests.load(Ordering::SeqCst))
        .sum::<usize>();
    assert!(
        after_cached_data >= before_cached_append + 2,
        "cache hit must still send the placement-checked append to a quorum"
    );

    Ok(())
}

/// Publishes one new stream route per call by appending the first record of
/// a fresh stream, which is what every new tenant stream costs the cell.
async fn publish_routes(fixture: &Fixture, start: usize, count: usize) -> TestResult {
    for index in start..start + count {
        let stream = format!("routes/stream-{index}");
        fixture.gateways[0].append(record(&stream, 1, 1)).await?;
    }
    Ok(())
}

/// Every route CAS records its idempotency result in the control document.
/// Without a retention bound that record embeds a full manifest, so the
/// document grows quadratically with the number of streams and every control
/// read on the append path pays for it. Staging reached this regime at
/// manifest revision 167 with a 45 KB manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_document_stays_bounded_across_route_operations() -> TestResult {
    let fixture = fixture(1).await?;
    publish_routes(&fixture, 0, 80).await?;
    let at_80 = fixture.control_sizes();
    publish_routes(&fixture, 80, 80).await?;
    let at_160 = fixture.control_sizes();
    eprintln!("control.json bytes after 80 routes {at_80:?}, after 160 routes {at_160:?}");
    for (half, full) in at_80.iter().zip(&at_160) {
        assert!(*half > 0 && *full > 0, "control documents must exist");
        // Quadratic growth doubles the document four times over here; a
        // bounded record window keeps it proportional to the manifest.
        assert!(
            *full < half * 3,
            "control document grew super-linearly: {half} -> {full} bytes"
        );
        assert!(
            *full < 1_000_000,
            "control document with 160 routes must stay under 1 MB, got {full}"
        );
    }
    Ok(())
}

/// The retention window keeps the most recent records and the current
/// manifest operation, and an evicted operation id no longer replays: it is a
/// fresh CAS that fails closed on the expected revision.
#[test]
fn evicted_manifest_operation_fails_closed_instead_of_replaying() -> TestResult {
    let directory = tempfile::tempdir()?;
    let seed = ["node-a", "node-b", "node-c"]
        .iter()
        .enumerate()
        .map(|(index, name)| {
            ReplicaNode::new(*name, format!("http://127.0.0.1:{}", 40_000 + index))
        })
        .collect::<Vec<_>>();
    let control = DurableControl::open(directory.path().join("control.json"), &seed)?;
    let mut expected_revision = 0;
    let mut expected_digest = String::new();
    for index in 0..12_u64 {
        let published = control.cas_manifest(
            expected_revision,
            &expected_digest,
            BTreeMap::new(),
            &format!("op-{index}"),
            false,
        )?;
        expected_revision = published.revision;
        expected_digest = published.digest;
    }
    let state = control.state()?;
    assert_eq!(
        state.manifest_operations.len(),
        8,
        "retention keeps the eight most recent manifest operations"
    );
    assert!(
        state.manifest_operations.contains_key("op-11"),
        "the current manifest operation is retained"
    );
    assert!(
        !state.manifest_operations.contains_key("op-0"),
        "the oldest operation record is evicted"
    );
    let replay = control.cas_manifest(0, "", BTreeMap::new(), "op-0", false);
    assert!(
        matches!(replay, Err(ReplicaError::LsnConflict)),
        "an evicted operation id is a new CAS that fails on the stale revision, got {replay:?}"
    );
    Ok(())
}

fn seed_members() -> Vec<ReplicaNode> {
    ["node-a", "node-b", "node-c"]
        .iter()
        .enumerate()
        .map(|(index, name)| {
            ReplicaNode::new(*name, format!("http://127.0.0.1:{}", 40_000 + index))
        })
        .collect()
}

/// Opens one storage member whose control document carries `filler_bytes`
/// of retained idempotency records, the shape a long-lived cell has.
fn member_with_control(
    directory: &std::path::Path,
    filler_bytes: usize,
) -> TestResult<DiskReplica> {
    let control_path = directory.join("control.json");
    if filler_bytes > 0 {
        let control = DurableControl::open(&control_path, &seed_members())?;
        let mut state = control.state()?;
        drop(control);
        let per_record = filler_bytes / 8;
        for index in 0..8 {
            state
                .manifest_operations
                .insert(format!("filler-{index}"), "x".repeat(per_record));
        }
        std::fs::write(&control_path, serde_json::to_vec(&state)?)?;
    }
    Ok(DiskReplica::open_with_control(
        directory.join("replica.log"),
        "node-a",
        "hot",
        directory,
        control_path,
        &seed_members(),
    )?)
}

async fn time_batches(member: &DiskReplica, batches: usize, size: u64) -> TestResult<Duration> {
    let started = std::time::Instant::now();
    for batch in 0..batches as u64 {
        let records = (1..=size)
            .map(|offset| record("cost/stream", batch * size + offset, (offset % 251) as u8))
            .collect::<Vec<_>>();
        member.append_and_commit_many(&records).await?;
    }
    Ok(started.elapsed())
}

/// A member reads its control document under the lock and consults the
/// manifest boundary only for a record with no durable, compacted, or
/// in-batch predecessor. Before, every record cloned the whole document to
/// evaluate that boundary, so a large document made each append cost a full
/// copy of it. The bloated member carries 64 MB of retained records and
/// must append no slower than a member with a fresh document.
#[tokio::test]
async fn member_append_cost_is_independent_of_control_document_size() -> TestResult {
    let directory = tempfile::tempdir()?;
    let small_dir = directory.path().join("small");
    let bloated_dir = directory.path().join("bloated");
    std::fs::create_dir_all(&small_dir)?;
    std::fs::create_dir_all(&bloated_dir)?;
    let small = member_with_control(&small_dir, 0)?;
    let bloated = member_with_control(&bloated_dir, 64 * 1024 * 1024)?;
    // Warm both logs so the measured batches always have a durable predecessor.
    time_batches(&small, 1, 8).await?;
    time_batches(&bloated, 1, 8).await?;
    let small_records = (9..=1008)
        .map(|lsn| record("cost/stream", lsn, (lsn % 251) as u8))
        .collect::<Vec<_>>();
    let started = std::time::Instant::now();
    small.append_and_commit_many(&small_records).await?;
    let small_elapsed = started.elapsed();
    let started = std::time::Instant::now();
    bloated.append_and_commit_many(&small_records).await?;
    let bloated_elapsed = started.elapsed();
    eprintln!(
        "1000-record batch: fresh control {small_elapsed:?}, 64 MB control {bloated_elapsed:?}"
    );
    assert!(
        bloated_elapsed <= small_elapsed * 2 + Duration::from_millis(150),
        "append cost scaled with the control document: {small_elapsed:?} -> {bloated_elapsed:?}"
    );
    Ok(())
}

/// Appends `count` records through one pinned coordinator, issuing a commit
/// certificate for each, and returns the node requests that cost.
async fn append_certified(
    fixture: &Fixture,
    stream: &str,
    next_lsn: &mut u64,
    count: u64,
) -> TestResult<usize> {
    // Detached third-member requests land shortly after the caller resumes;
    // let them settle so every request is attributed to its append.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = fixture.requests();
    for _ in 0..count {
        let record = record(stream, *next_lsn, (*next_lsn % 251) as u8);
        fixture.gateways[0].append(record.clone()).await?;
        fixture.gateways[0]
            .issue_commit_certificate(&record)
            .await?;
        *next_lsn += 1;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(fixture.requests() - before)
}

/// A warm, pinned direct coordinator reads the manifest quorum, fans out the
/// data append, and reads the manifest once for the certificate: at most
/// twelve node requests plus a route repair to any member that did not answer
/// the quorum read. Ordinary direct appends do not add a status fan-out;
/// explicit maintenance transitions still reconcile durable node state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_and_certificate_issue_a_bounded_number_of_node_requests() -> TestResult {
    let fixture = fixture(1).await?;
    let stream = "requests/stream";
    let mut next_lsn = 1;
    // The first appends publish the route and rebuild the writer state.
    append_certified(&fixture, stream, &mut next_lsn, 5).await?;
    let count = 40;
    let requests = append_certified(&fixture, stream, &mut next_lsn, count).await?;
    let per_append = requests as f64 / count as f64;
    eprintln!("{requests} node requests for {count} appends + certificates: {per_append:.2} each");
    assert!(
        per_append <= 14.0,
        "a warm append + certificate must cost at most 14 node requests, got {per_append:.2}"
    );
    Ok(())
}

/// Appends one record and certifies it through `gateway`, bounding each call
/// by `limit` so a fan-out stalled on the client timeout fails fast.
async fn append_within(
    gateway: &ReplicaGateway,
    stream: &str,
    lsn: u64,
    limit: Duration,
) -> TestResult<(Duration, Duration)> {
    let record = record(stream, lsn, (lsn % 251) as u8);
    let started = std::time::Instant::now();
    tokio::time::timeout(limit, gateway.append(record.clone()))
        .await
        .map_err(|_| format!("append of lsn {lsn} did not finish within {limit:?}"))??;
    let append = started.elapsed();
    let started = std::time::Instant::now();
    tokio::time::timeout(limit, gateway.issue_commit_certificate(&record))
        .await
        .map_err(|_| format!("certificate for lsn {lsn} did not finish within {limit:?}"))??;
    Ok((append, started.elapsed()))
}

/// One member stops answering while its connections stay open, the shape of
/// a suspended Machine behind a keep-alive pool. Every fan-out phase used to
/// wait for that member until the five-second client timeout, so one such
/// member turned a warm append into tens of seconds. Each phase now ends
/// once a quorum answered: warm appends, certificates, and a cold
/// coordinator's rebuild all complete well inside the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_member_does_not_stall_appends_for_the_client_timeout() -> TestResult {
    let fixture = fixture(2).await?;
    let warm = fixture.gateways[0].as_ref();
    let cold = fixture.gateways[1].as_ref();
    let stream = "silent/stream";
    let mut lsn = 1;
    for _ in 0..3 {
        append_within(warm, stream, lsn, Duration::from_secs(5)).await?;
        lsn += 1;
    }
    fixture.nodes[2].hook.silent.store(true, Ordering::SeqCst);
    let limit = Duration::from_secs(2);
    let mut slowest = Duration::ZERO;
    for _ in 0..5 {
        let (append, certificate) = append_within(warm, stream, lsn, limit).await?;
        slowest = slowest.max(append).max(certificate);
        lsn += 1;
    }
    eprintln!("warm coordinator with one silent member: slowest call {slowest:?}");
    // A coordinator that never served this stream rebuilds its writer state
    // from member snapshots before appending; that fan-out must not stall
    // on the silent member either.
    let (append, certificate) = append_within(cold, stream, lsn, limit).await?;
    eprintln!(
        "cold coordinator with one silent member: append {append:?}, certificate {certificate:?}"
    );
    assert!(
        slowest < Duration::from_secs(1) && append < limit,
        "appends must not wait on the silent member: warm {slowest:?}, cold {append:?}"
    );
    Ok(())
}
