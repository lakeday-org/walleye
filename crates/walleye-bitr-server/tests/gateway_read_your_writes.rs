//! Read-your-writes coverage for the replica gateway.
//!
//! The Lance writer appends a fence sentinel, receives a quorum
//! acknowledgement, and immediately calls `recover(stream, after_lsn)` with
//! `after_lsn` below the tail. BtrLog treats the acknowledgement as the
//! durability boundary, so every recovery that follows it must include the
//! acknowledged record. Each test below exercises one way staging can
//! arrange the read: the same coordinator, a different stateless
//! coordinator, a recover that crosses the archive/hot split, a member that
//! never received the record, and a third copy still in flight.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::response::Response;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use walleye_bitr::{EncryptedRecord, Replica, ReplicaError};
use walleye_bitr_server::{
    DiskReplica, INTERNAL_AUTH_HEADER, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router,
};

const INTERNAL_TOKEN: &str = "read-your-writes-internal-token";
const ROOT_KEY: &str = "bW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW0=";
const ARCHIVE_PREFIX: &str = "read-your-writes";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    bind: SocketAddr,
    task: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
}

fn record(stream: &str, lsn: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": 1,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 12],
        "authentication": vec![marker; 32],
    }))
    .expect("valid encrypted record fixture")
}

fn records(stream: &str, range: std::ops::RangeInclusive<u64>) -> Vec<EncryptedRecord> {
    range.map(|lsn| record(stream, lsn, lsn as u8)).collect()
}

fn memory_archive() -> TestResult<(Arc<dyn ObjectStore>, Arc<OpaqueArchive>)> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(Arc::clone(&store), ARCHIVE_PREFIX, 64)?);
    Ok((store, archive))
}

/// One node specification: its member id and, when the node sits behind a
/// fault-injecting proxy, the URL the cohort advertises for it.
struct NodeSpec {
    name: &'static str,
    bind: Option<SocketAddr>,
    advertised: Option<String>,
}

fn plain(names: &[&'static str]) -> Vec<NodeSpec> {
    names
        .iter()
        .map(|name| NodeSpec {
            name,
            bind: None,
            advertised: None,
        })
        .collect()
}

async fn start_nodes(
    directory: &tempfile::TempDir,
    specs: Vec<NodeSpec>,
    initial_members: Option<&[ReplicaNode]>,
) -> TestResult<Vec<RunningNode>> {
    let mut bound = Vec::with_capacity(specs.len());
    for spec in specs {
        let listener = match spec.bind {
            Some(address) => TcpListener::bind(address).await?,
            None => TcpListener::bind("127.0.0.1:0").await?,
        };
        let bind = listener.local_addr()?;
        let url = spec.advertised.unwrap_or_else(|| format!("http://{bind}"));
        let member = ReplicaNode::new(spec.name, url);
        bound.push((spec.name, member, bind, listener));
    }
    let seed = initial_members
        .map(<[ReplicaNode]>::to_vec)
        .unwrap_or_else(|| {
            bound
                .iter()
                .map(|(_, member, _, _)| member.clone())
                .collect()
        });
    let mut nodes = Vec::with_capacity(bound.len());
    for (name, member, bind, listener) in bound {
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
            bind,
            task: Some(task),
        });
    }
    for node in &nodes {
        wait_for_storage_ready(node).await?;
    }
    Ok(nodes)
}

async fn start_group(
    directory: &tempfile::TempDir,
    names: &[&'static str],
) -> TestResult<Vec<RunningNode>> {
    start_nodes(directory, plain(names), None).await
}

async fn stop_node(node: &mut RunningNode) {
    if let Some(task) = node.task.take() {
        task.abort();
        let _ = task.await;
    }
    tokio::task::yield_now().await;
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

async fn resume_node(node: &mut RunningNode) -> TestResult {
    let listener = loop {
        match TcpListener::bind(node.bind).await {
            Ok(listener) => break listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let app = node_router(Arc::clone(&node.disk), ROOT_KEY, Some(INTERNAL_TOKEN))?;
    node.task = Some(tokio::spawn(
        async move { axum::serve(listener, app).await },
    ));
    wait_for_storage_ready(node).await?;
    Ok(())
}

async fn stop_all(nodes: &mut [RunningNode]) {
    for node in nodes {
        stop_node(node).await;
    }
}

fn members(nodes: &[RunningNode]) -> Vec<ReplicaNode> {
    nodes.iter().map(|node| node.member.clone()).collect()
}

/// A stateless coordinator over the cohort with its own empty local cache,
/// exactly like a Flycast peer that has never served the stream.
fn coordinator(
    nodes: &[RunningNode],
    directory: &tempfile::TempDir,
    name: &str,
    archive: Arc<OpaqueArchive>,
) -> TestResult<Arc<ReplicaGateway>> {
    Ok(Arc::new(ReplicaGateway::new_direct(
        members(nodes),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join(format!("{name}-control.json")),
        archive,
    )?))
}

/// Appends the prefix and waits until every copy has landed, so a later
/// assertion is about the record under test rather than the prefix.
async fn settle_prefix(
    gateway: &ReplicaGateway,
    stream: &str,
    prefix: &[EncryptedRecord],
) -> TestResult {
    for value in prefix {
        assert!(gateway.append(value.clone()).await? >= 2);
    }
    wait_for_records(gateway, stream, prefix).await
}

async fn wait_for_records(
    gateway: &ReplicaGateway,
    stream: &str,
    expected: &[EncryptedRecord],
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match gateway.recover(stream, 0).await {
                Ok(recovered) if recovered == expected => break,
                Ok(_) | Err(ReplicaError::QuorumUnavailable | ReplicaError::NodeUnavailable) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Ok::<(), ReplicaError>(())
    })
    .await??;
    Ok(())
}

async fn archive_all(gateway: &ReplicaGateway, nodes: &[RunningNode]) -> TestResult {
    for node in nodes {
        gateway
            .archive_local_commits(&node.disk)
            .await
            .map_err(|error| format!("archive {}: {error:?}", node.member.id))?;
    }
    Ok(())
}

/// Asserts that a recover started strictly after an acknowledgement returns
/// the acknowledged tail from every requested watermark below it.
async fn assert_read_your_writes(
    gateway: &ReplicaGateway,
    label: &str,
    stream: &str,
    expected: &[EncryptedRecord],
    after: &[u64],
) -> TestResult {
    for after_lsn in after {
        let want = expected
            .iter()
            .filter(|record| record.lsn() > *after_lsn)
            .cloned()
            .collect::<Vec<_>>();
        let got = gateway
            .recover(stream, *after_lsn)
            .await
            .map_err(|error| format!("{label}: recover after {after_lsn}: {error:?}"))?;
        assert_eq!(
            got.iter().map(EncryptedRecord::lsn).collect::<Vec<_>>(),
            want.iter().map(EncryptedRecord::lsn).collect::<Vec<_>>(),
            "{label}: recover after {after_lsn} omitted an acknowledged record"
        );
        assert_eq!(
            got, want,
            "{label}: recover after {after_lsn} returned other bytes"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Fault injection: a one-shot gate on the archive head read.
// ---------------------------------------------------------------------------

/// Pauses the next archive head read until the test releases it. This is the
/// window between a recover's archive pass and its hot pass, during which a
/// member's archive loop can publish and trim the acknowledged tail.
#[derive(Debug, Default)]
struct HeadGate {
    armed: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl HeadGate {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn wait_entered(&self) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .map_err(|_| "the recover never reached the archive head read")?;
        Ok(())
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Debug)]
struct GatedStore {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<HeadGate>,
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Serve the read first, then hold the (now stale) result until the
        // test releases it: the caller proceeds with the head it observed
        // before the archive advanced.
        let result = self.inner.get_opts(location, options).await;
        if location.as_ref().ends_with("head.json") && self.gate.armed.swap(false, Ordering::SeqCst)
        {
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
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

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Two views of one archive namespace: the writer's coordinator reads the
/// head through the gate, while the members' archive loops (a separate
/// process on staging) use the raw store.
fn gated_archive() -> TestResult<(Arc<HeadGate>, Arc<OpaqueArchive>, Arc<OpaqueArchive>)> {
    let (store, raw) = memory_archive()?;
    let gate = Arc::new(HeadGate::default());
    let gated: Arc<dyn ObjectStore> = Arc::new(GatedStore {
        inner: store,
        gate: Arc::clone(&gate),
    });
    let gated = Arc::new(OpaqueArchive::new(gated, ARCHIVE_PREFIX, 64)?);
    Ok((gate, gated, raw))
}

// ---------------------------------------------------------------------------
// Fault injection: a proxy that stalls one member's append.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct AppendStall {
    armed: AtomicBool,
    stalled: Notify,
    release: Notify,
}

impl AppendStall {
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn wait_stalled(&self) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), self.stalled.notified())
            .await
            .map_err(|_| "the append never reached the stalled member")?;
        Ok(())
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Clone)]
struct ProxyState {
    target: String,
    client: reqwest::Client,
    stall: Arc<AppendStall>,
}

async fn proxy_handler(State(state): State<ProxyState>, request: Request) -> Response {
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| "/".to_owned(), |value| value.as_str().to_owned());
    let headers = request.headers().clone();
    let body = match to_bytes(request.into_body(), usize::MAX).await {
        Ok(body) => body,
        Err(_) => return bad_gateway(),
    };
    let mut upstream = state
        .client
        .request(method, format!("{}{}", state.target, path_and_query));
    for (name, value) in &headers {
        if name == "host" || name == "content-length" {
            continue;
        }
        upstream = upstream.header(name.as_str(), value.as_bytes());
    }
    let Ok(response) = upstream.body(body).send().await else {
        return bad_gateway();
    };
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let Ok(bytes) = response.bytes().await else {
        return bad_gateway();
    };
    // Let the real production node finish its placement-validated atomic
    // append, then hold only the proxy response. The gateway therefore sees
    // the third request as in flight while the fixture never fabricates an
    // unbound data-only append on a fenced direct node.
    if path_and_query == "/internal/v1/append-many"
        && state.stall.armed.swap(false, Ordering::SeqCst)
    {
        state.stall.stalled.notify_one();
        state.stall.release.notified().await;
    }
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    builder
        .body(Body::from(bytes))
        .unwrap_or_else(|_| bad_gateway())
}

fn bad_gateway() -> Response {
    Response::builder()
        .status(502)
        .body(Body::empty())
        .expect("static response")
}

/// Starts a proxy that forwards to `target` and returns the URL the cohort
/// should advertise for the member behind it.
async fn start_proxy(
    target: String,
    stall: Arc<AppendStall>,
) -> TestResult<(String, tokio::task::JoinHandle<Result<(), std::io::Error>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let app = axum::Router::new()
        .fallback(proxy_handler)
        .with_state(ProxyState {
            target,
            client: reqwest::Client::new(),
            stall,
        });
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((url, task))
}

// ---------------------------------------------------------------------------
// (a) The same coordinator.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_same_coordinator_recovers_the_record_it_just_acknowledged() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, archive) = memory_archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"]).await?;
    let writer = coordinator(&nodes, &directory, "writer", Arc::clone(&archive))?;
    let stream = "tenant-a/same-coordinator";
    let prefix = records(stream, 1..=3);
    settle_prefix(&writer, stream, &prefix).await?;

    let fourth = record(stream, 4, 4);
    assert!(writer.append(fourth.clone()).await? >= 2);
    let mut expected = prefix.clone();
    expected.push(fourth);
    assert_read_your_writes(&writer, "single append", stream, &expected, &[0, 2, 3]).await?;

    // The Lance writer sends its fence through the batch route.
    let batch = records(stream, 5..=6);
    assert!(writer.append_many(batch.clone()).await? >= 2);
    expected.extend(batch);
    assert_read_your_writes(&writer, "batch append", stream, &expected, &[0, 3, 4, 5]).await?;

    stop_all(&mut nodes).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// (b) A different stateless coordinator.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn another_coordinator_recovers_a_record_acknowledged_elsewhere() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, archive) = memory_archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"]).await?;
    let stream = "tenant-a/other-coordinator";

    // `earlier` served the stream first, so its cache is warm but stale.
    let earlier = coordinator(&nodes, &directory, "earlier", Arc::clone(&archive))?;
    let prefix = records(stream, 1..=2);
    settle_prefix(&earlier, stream, &prefix).await?;

    // `writer` takes over, as Flycast round-robin does, and appends the tail.
    let writer = coordinator(&nodes, &directory, "writer", Arc::clone(&archive))?;
    let tail = records(stream, 3..=4);
    for value in &tail {
        assert!(writer.append(value.clone()).await? >= 2);
    }
    let mut expected = prefix.clone();
    expected.extend(tail.clone());

    // A coordinator that has never seen the stream.
    let fresh = coordinator(&nodes, &directory, "fresh", Arc::clone(&archive))?;
    assert_read_your_writes(&fresh, "fresh coordinator", stream, &expected, &[0, 2, 3]).await?;
    // The coordinator whose cache still ends at LSN 2.
    assert_read_your_writes(&earlier, "stale coordinator", stream, &expected, &[0, 2, 3]).await?;

    // The batch route through a third coordinator, read by the first two.
    let batch = records(stream, 5..=6);
    let third = coordinator(&nodes, &directory, "third", Arc::clone(&archive))?;
    assert!(third.append_many(batch.clone()).await? >= 2);
    expected.extend(batch);
    assert_read_your_writes(&fresh, "fresh after batch", stream, &expected, &[0, 4, 5]).await?;
    assert_read_your_writes(&earlier, "stale after batch", stream, &expected, &[0, 4, 5]).await?;

    stop_all(&mut nodes).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// (c) Across the archive/hot split.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn recover_across_the_archive_split_includes_the_hot_tail() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, archive) = memory_archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"]).await?;
    let writer = coordinator(&nodes, &directory, "writer", Arc::clone(&archive))?;
    let stream = "tenant-a/archive-split";
    let prefix = records(stream, 1..=3);
    settle_prefix(&writer, stream, &prefix).await?;
    archive_all(&writer, &nodes).await?;
    for node in &nodes {
        assert!(
            node.disk.records(stream).await.is_empty(),
            "{} keeps hot copies of an archived prefix",
            node.member.id
        );
    }

    // The tail lives only in the hot tier; the prefix only in the archive.
    let fourth = record(stream, 4, 4);
    assert!(writer.append(fourth.clone()).await? >= 2);
    let mut expected = prefix.clone();
    expected.push(fourth);
    assert_read_your_writes(&writer, "writer", stream, &expected, &[0, 1, 2, 3]).await?;
    let fresh = coordinator(&nodes, &directory, "fresh", Arc::clone(&archive))?;
    assert_read_your_writes(&fresh, "fresh coordinator", stream, &expected, &[0, 2, 3]).await?;

    // Members trim at different moments on staging: one member has archived
    // and trimmed the tail while the other two still hold it hot.
    writer.archive_local_commits(&nodes[0].disk).await?;
    assert!(nodes[0].disk.records(stream).await.is_empty());
    assert_eq!(nodes[1].disk.records(stream).await.len(), 1);
    assert_read_your_writes(
        &writer,
        "writer, one trimmed",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;
    assert_read_your_writes(&fresh, "fresh, one trimmed", stream, &expected, &[0, 2, 3]).await?;

    let fifth = record(stream, 5, 5);
    assert!(writer.append(fifth.clone()).await? >= 2);
    expected.push(fifth);
    assert_read_your_writes(&writer, "writer, next tail", stream, &expected, &[0, 3, 4]).await?;
    assert_read_your_writes(&fresh, "fresh, next tail", stream, &expected, &[0, 3, 4]).await?;

    stop_all(&mut nodes).await;
    Ok(())
}

/// The race staging runs every second: a recover reads the archive head
/// before a member's archive loop publishes and trims the acknowledged tail,
/// then reads the hot tier after that trim. The trimmed member no longer
/// holds the record, the member that was down during the append never
/// received it, and the remaining holder's marker is the only hot evidence.
/// The archive is authoritative for the trimmed prefix, so the recover must
/// still return the tail.
#[tokio::test]
async fn a_recover_that_read_the_archive_before_a_member_trimmed_the_tail_keeps_it() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (gate, gated, raw) = gated_archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"]).await?;
    let writer = coordinator(&nodes, &directory, "writer", Arc::clone(&gated))?;
    let archiver = coordinator(&nodes, &directory, "archiver", Arc::clone(&raw))?;
    let stream = "tenant-a/archive-race";
    let prefix = records(stream, 1..=3);
    settle_prefix(&writer, stream, &prefix).await?;
    archive_all(&archiver, &nodes).await?;

    // old-2 misses the fourth record, then returns and answers without it.
    stop_node(&mut nodes[2]).await;
    let fourth = record(stream, 4, 4);
    assert_eq!(writer.append(fourth.clone()).await?, 2);
    resume_node(&mut nodes[2]).await?;
    assert!(nodes[2].disk.records(stream).await.is_empty());

    // The recover reads the archive head (still at LSN 3) and pauses.
    gate.arm();
    let recovering = {
        let writer = Arc::clone(&writer);
        tokio::spawn(async move { writer.recover(stream, 2).await })
    };
    gate.wait_entered().await?;
    // old-0's archive loop publishes LSN 4 and trims its hot copy.
    archiver.archive_local_commits(&nodes[0].disk).await?;
    assert_eq!(raw.archived_lsn(stream).await?, 4);
    assert!(nodes[0].disk.records(stream).await.is_empty());
    gate.release();

    let recovered = recovering
        .await?
        .map_err(|error| format!("recover during the trim: {error:?}"))?;
    assert_eq!(
        recovered
            .iter()
            .map(EncryptedRecord::lsn)
            .collect::<Vec<_>>(),
        vec![3, 4],
        "the acknowledged tail was omitted while one member trimmed it"
    );
    assert_eq!(recovered, vec![prefix[2].clone(), fourth.clone()]);

    // Without the race the same state recovers cleanly.
    let mut expected = prefix.clone();
    expected.push(fourth);
    assert_read_your_writes(&writer, "after the trim", stream, &expected, &[0, 2, 3]).await?;

    stop_all(&mut nodes).await;
    Ok(())
}

/// The same window, but every member trims before the hot pass runs: no
/// member holds the tail hot at all, and only the archive can supply it.
#[tokio::test]
async fn a_recover_that_read_the_archive_before_every_member_trimmed_the_tail_keeps_it()
-> TestResult {
    let directory = tempfile::tempdir()?;
    let (gate, gated, raw) = gated_archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"]).await?;
    let writer = coordinator(&nodes, &directory, "writer", Arc::clone(&gated))?;
    let archiver = coordinator(&nodes, &directory, "archiver", Arc::clone(&raw))?;
    let stream = "tenant-a/archive-race-all";
    let prefix = records(stream, 1..=3);
    settle_prefix(&writer, stream, &prefix).await?;
    archive_all(&archiver, &nodes).await?;

    let fourth = record(stream, 4, 4);
    assert!(writer.append(fourth.clone()).await? >= 2);
    let mut expected = prefix.clone();
    expected.push(fourth.clone());
    // Let the third copy land so every member can trim it.
    wait_for_records(&writer, stream, &expected).await?;

    gate.arm();
    let recovering = {
        let writer = Arc::clone(&writer);
        tokio::spawn(async move { writer.recover(stream, 2).await })
    };
    gate.wait_entered().await?;
    archive_all(&archiver, &nodes).await?;
    for node in &nodes {
        assert!(node.disk.records(stream).await.is_empty());
    }
    gate.release();

    let recovered = recovering
        .await?
        .map_err(|error| format!("recover during the trims: {error:?}"))?;
    assert_eq!(recovered, vec![prefix[2].clone(), fourth]);
    assert_read_your_writes(&writer, "after the trims", stream, &expected, &[0, 2, 3]).await?;

    stop_all(&mut nodes).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// (d) A member that never held the record.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_member_that_missed_the_append_never_hides_it_from_recovery() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, archive) = memory_archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"]).await?;
    let writer = coordinator(&nodes, &directory, "writer", Arc::clone(&archive))?;
    let stream = "tenant-a/missed-member";
    let prefix = records(stream, 1..=3);
    settle_prefix(&writer, stream, &prefix).await?;

    stop_node(&mut nodes[2]).await;
    let fourth = record(stream, 4, 4);
    assert_eq!(writer.append(fourth.clone()).await?, 2);
    let mut expected = prefix.clone();
    expected.push(fourth);

    // While the member is still down.
    assert_read_your_writes(
        &writer,
        "writer, member down",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;
    let fresh_down = coordinator(&nodes, &directory, "fresh-down", Arc::clone(&archive))?;
    assert_read_your_writes(
        &fresh_down,
        "fresh, member down",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;

    // The member returns without the record and answers every read.
    resume_node(&mut nodes[2]).await?;
    assert_eq!(nodes[2].disk.records(stream).await, prefix);
    assert_read_your_writes(
        &writer,
        "writer, member back",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;
    assert_read_your_writes(
        &fresh_down,
        "warm, member back",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;
    let fresh_back = coordinator(&nodes, &directory, "fresh-back", Arc::clone(&archive))?;
    assert_read_your_writes(
        &fresh_back,
        "fresh, member back",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;

    // A holder goes silent while the returned member is the one answering.
    stop_node(&mut nodes[1]).await;
    assert_read_your_writes(
        &writer,
        "writer, holder silent",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;
    let fresh_silent = coordinator(&nodes, &directory, "fresh-silent", Arc::clone(&archive))?;
    assert_read_your_writes(
        &fresh_silent,
        "fresh, holder silent",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;
    resume_node(&mut nodes[1]).await?;

    stop_all(&mut nodes).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// (e) The third copy is still in flight.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn recover_racing_the_third_copy_returns_the_acknowledged_record() -> TestResult {
    let directory = tempfile::tempdir()?;
    let (_, archive) = memory_archive()?;
    // old-2 sits behind a proxy so its append can be held after the quorum
    // has already acknowledged.
    let stall = Arc::new(AppendStall::default());
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_address = backend.local_addr()?;
    drop(backend);
    let (proxied_url, proxy) =
        start_proxy(format!("http://{backend_address}"), Arc::clone(&stall)).await?;
    let mut nodes = start_nodes(
        &directory,
        vec![
            NodeSpec {
                name: "old-0",
                bind: None,
                advertised: None,
            },
            NodeSpec {
                name: "old-1",
                bind: None,
                advertised: None,
            },
            NodeSpec {
                name: "old-2",
                bind: Some(backend_address),
                advertised: Some(proxied_url),
            },
        ],
        None,
    )
    .await?;

    let writer = coordinator(&nodes, &directory, "writer", Arc::clone(&archive))?;
    let stream = "tenant-a/third-copy-in-flight";
    let prefix = records(stream, 1..=3);
    settle_prefix(&writer, stream, &prefix).await?;

    // The quorum acknowledges while old-2's copy is held at the proxy.
    stall.arm();
    let fourth = record(stream, 4, 4);
    let appending = {
        let writer = Arc::clone(&writer);
        let fourth = fourth.clone();
        tokio::spawn(async move { writer.append(fourth).await })
    };
    stall.wait_stalled().await?;
    assert!(appending.await?? >= 2);
    let mut expected = prefix.clone();
    expected.push(fourth.clone());
    assert_eq!(nodes[2].disk.records(stream).await, expected);

    assert_read_your_writes(
        &writer,
        "writer, copy in flight",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;
    let fresh = coordinator(&nodes, &directory, "fresh", Arc::clone(&archive))?;
    assert_read_your_writes(
        &fresh,
        "fresh, copy in flight",
        stream,
        &expected,
        &[0, 2, 3],
    )
    .await?;

    stall.release();
    let fifth = record(stream, 5, 5);
    assert!(writer.append(fifth.clone()).await? >= 2);
    expected.push(fifth);
    assert_read_your_writes(&writer, "writer, released", stream, &expected, &[0, 3, 4]).await?;
    assert_read_your_writes(&fresh, "fresh, released", stream, &expected, &[0, 3, 4]).await?;

    stop_all(&mut nodes).await;
    proxy.abort();
    Ok(())
}
