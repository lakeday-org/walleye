//! Online Bitr scaling acceptance tests.
//!
//! These tests are deliberately stricter than the maintenance-fence tests in
//! `cohort_lifecycle.rs`. A scale operation is held at a real storage control
//! or data boundary after the handoff has started while a writer sends each
//! record exactly once. There is no client retry loop or polling helper: if the
//! operation fences or queues writes, the writer fails while handoff I/O is
//! still pending.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request};
use axum::middleware::{self, Next};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use walleye_bitr::{ENCRYPTED_RECORD_CONTENT_TYPE, EncryptedRecord};
use walleye_bitr_server::{
    ControlHeadStore, DiskReplica, INTERNAL_AUTH_HEADER, OpaqueArchive, PLACEMENT_DIGEST_HEADER,
    PLACEMENT_EPOCH_HEADER, PendingHandoffStage, ReplicaGateway, ReplicaNode, gateway_router,
    node_router,
};

const INTERNAL_TOKEN: &str = "live-scaling-internal-token";
const ROOT_KEY: &str = "y8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8s=";
const DERIVATION_VERSION: &str = "lakeday-cloud/deployment-identity/v1";
const TENANT: &str = "tenant-a";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn archive(prefix: &str) -> TestResult<Arc<OpaqueArchive>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        prefix,
        64,
    )?))
}

fn record(stream: &str, lsn: u64, writer_epoch: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": writer_epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 32],
        "authentication": vec![marker; 32],
    }))
    .expect("valid opaque test record")
}

/// The same rendezvous domain as the direct gateway.  This chooses a stream
/// that starts on cohort zero and moves to cohort one after activation.
fn cohort_score(stream: &str, cohort_id: u64) -> u128 {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/cohort-ring/v1/rendezvous\0");
    digest.update(stream.as_bytes());
    digest.update([0]);
    digest.update(cohort_id.to_le_bytes());
    let digest = digest.finalize();
    let mut prefix = [0_u8; 16];
    prefix.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(prefix)
}

fn stream_that_moves_to(cohort_id: u64) -> String {
    (0..100_000_u64)
        .map(|index| format!("tenant-a/live-scale-{index}"))
        .find(|stream| cohort_score(stream, cohort_id) > cohort_score(stream, 0))
        .expect("a deterministic stream must be available")
}

fn stream_that_stays_with_control(cohort_id: u64) -> String {
    (100_000..200_000_u64)
        .map(|index| format!("tenant-a/live-scale-{index}"))
        .find(|stream| cohort_score(stream, 0) > cohort_score(stream, cohort_id))
        .expect("a deterministic stream must remain on cohort zero")
}

fn tenant_token(tenant: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(&[203_u8; 32]).expect("HMAC key");
    mac.update(
        format!("{DERIVATION_VERSION}\0{tenant}\0replica-gateway-authentication").as_bytes(),
    );
    hex::encode(mac.finalize().into_bytes())
}

struct RunningNode {
    member: ReplicaNode,
    task: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl RunningNode {
    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        tokio::task::yield_now().await;
    }
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn start_group(
    directory: &tempfile::TempDir,
    names: &[&str],
    seed: Option<&[ReplicaNode]>,
) -> TestResult<Vec<RunningNode>> {
    start_group_with_node_gates(directory, names, seed, &BTreeMap::new()).await
}

async fn start_group_with_node_gates(
    directory: &tempfile::TempDir,
    names: &[&str],
    seed: Option<&[ReplicaNode]>,
    gates: &BTreeMap<String, Arc<HoldGate>>,
) -> TestResult<Vec<RunningNode>> {
    let mut bound = Vec::with_capacity(names.len());
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?));
        bound.push((*name, member, listener));
    }
    let bootstrap = seed
        .map(<[ReplicaNode]>::to_vec)
        .unwrap_or_else(|| bound.iter().map(|(_, member, _)| member.clone()).collect());
    let mut nodes = Vec::with_capacity(bound.len());
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &bootstrap,
        )?);
        let app = node_router(Arc::clone(&disk), ROOT_KEY, Some(INTERNAL_TOKEN))?;
        let app = if let Some(gate) = gates.get(name) {
            hold_request(app, Arc::clone(gate))
        } else {
            app
        };
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode {
            member,
            task: Some(task),
        });
    }
    wait_for_ready(&nodes).await?;
    Ok(nodes)
}

async fn start_group_with_node_gate_layers(
    directory: &tempfile::TempDir,
    names: &[&str],
    seed: Option<&[ReplicaNode]>,
    gates: &BTreeMap<String, Vec<Arc<HoldGate>>>,
) -> TestResult<Vec<RunningNode>> {
    let mut bound = Vec::with_capacity(names.len());
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?));
        bound.push((*name, member, listener));
    }
    let bootstrap = seed
        .map(<[ReplicaNode]>::to_vec)
        .unwrap_or_else(|| bound.iter().map(|(_, member, _)| member.clone()).collect());
    let mut nodes = Vec::with_capacity(bound.len());
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &bootstrap,
        )?);
        let app = node_router(Arc::clone(&disk), ROOT_KEY, Some(INTERNAL_TOKEN))?;
        let app = if let Some(node_gates) = gates.get(name) {
            hold_requests(app, node_gates.clone())
        } else {
            app
        };
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode {
            member,
            task: Some(task),
        });
    }
    wait_for_ready(&nodes).await?;
    Ok(nodes)
}

async fn wait_for_ready(nodes: &[RunningNode]) -> TestResult {
    let client = reqwest::Client::new();
    for node in nodes {
        let url = format!("{}/internal/v1/status", node.member.url);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if client
                    .get(&url)
                    .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| format!("storage node {} did not become ready", node.member.id))?;
    }
    Ok(())
}

fn members(nodes: &[RunningNode]) -> Vec<ReplicaNode> {
    nodes.iter().map(|node| node.member.clone()).collect()
}

async fn direct_gateway(
    nodes: &[RunningNode],
    directory: &tempfile::TempDir,
    archive: Arc<OpaqueArchive>,
    name: &str,
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

/// Holds one online operation after the request reaches the gateway.  The
/// writer therefore runs against a genuinely pending handoff, not a race
/// where the operation happened to finish before the first append.
struct HoldGate {
    path: &'static str,
    armed: AtomicBool,
    next_generation: AtomicU64,
    active_generation: AtomicU64,
    released_generation: AtomicU64,
    entries: AtomicUsize,
    exits: AtomicUsize,
    entered_notify: Notify,
    exited_notify: Notify,
    release: Notify,
}

impl HoldGate {
    fn new(path: &'static str) -> Self {
        Self {
            path,
            armed: AtomicBool::new(false),
            next_generation: AtomicU64::new(0),
            active_generation: AtomicU64::new(0),
            released_generation: AtomicU64::new(0),
            entries: AtomicUsize::new(0),
            exits: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            exited_notify: Notify::new(),
            release: Notify::new(),
        }
    }

    fn arm(&self) {
        self.next_generation.fetch_add(1, Ordering::AcqRel);
        self.armed.store(true, Ordering::Release);
    }

    async fn wait_until_entered(&self) -> TestResult {
        self.wait_until_entries(1).await
    }

    async fn wait_until_entries(&self, count: usize) -> TestResult {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let notified = self.entered_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.entries.load(Ordering::Acquire) >= count {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| format!("online operation {} never reached gateway", self.path))?;
        Ok(())
    }

    fn release(&self) {
        let active_generation = self.active_generation.load(Ordering::Acquire);
        self.released_generation
            .store(active_generation, Ordering::Release);
        self.release.notify_waiters();
    }
}

fn hold_request(app: axum::Router, gate: Arc<HoldGate>) -> axum::Router {
    hold_requests(app, vec![gate])
}

fn hold_requests(app: axum::Router, gates: Vec<Arc<HoldGate>>) -> axum::Router {
    app.layer(middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let gates = gates.clone();
            async move {
                for gate in gates {
                    if matches!(request.method(), &Method::GET | &Method::POST)
                        && request.uri().path() == gate.path
                        && gate.armed.swap(false, Ordering::AcqRel)
                    {
                        let generation = gate.next_generation.load(Ordering::Acquire);
                        gate.active_generation.store(generation, Ordering::Release);
                        gate.entries.fetch_add(1, Ordering::AcqRel);
                        gate.entered_notify.notify_waiters();
                        loop {
                            let notified = gate.release.notified();
                            tokio::pin!(notified);
                            notified.as_mut().enable();
                            if gate.released_generation.load(Ordering::Acquire) >= generation {
                                break;
                            }
                            notified.await;
                        }
                        gate.exits.fetch_add(1, Ordering::AcqRel);
                        gate.exited_notify.notify_waiters();
                    }
                }
                next.run(request).await
            }
        },
    ))
}

struct RunningGateway {
    base_url: String,
    task: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl RunningGateway {
    async fn start(gateway: Arc<ReplicaGateway>) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = gateway_router(Arc::clone(&gateway));
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        Ok(Self {
            base_url: format!("http://{address}"),
            task: Some(task),
        })
    }

    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for RunningGateway {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn post_control(base_url: &str, path: &str, body: Value) -> TestResult<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!("{base_url}{path}"))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .json(&body)
        .send()
        .await?)
}

async fn register_online(
    server: &RunningGateway,
    node: &RunningNode,
    cohort_id: u64,
    ordinal: u64,
) -> TestResult<()> {
    let response = post_control(
        &server.base_url,
        "/internal/v1/membership/join-online",
        json!({
            "operation_id": format!("join-online-{}", node.member.id),
            "node": {"id": node.member.id, "url": node.member.url},
            "cohort_id": cohort_id,
            "name": node.member.id,
            "machine_id": format!("machine-{}", node.member.id),
            "volume_id": format!("volume-{}", node.member.id),
            "ordinal": ordinal,
            "tier": "medium",
        }),
    )
    .await?;
    if !response.status().is_success() {
        let client = reqwest::Client::new();
        let health = client
            .get(format!("{}/internal/v1/healthz", node.member.url))
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .send()
            .await
            .map(|response| response.status().to_string())
            .unwrap_or_else(|error| format!("request error: {error}"));
        let control = client
            .get(format!("{}/internal/v1/control", node.member.url))
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .send()
            .await
            .map(|response| response.status().to_string())
            .unwrap_or_else(|error| format!("request error: {error}"));
        return Err(format!(
            "join-online {} failed with {}: {}; node health={health}, control={control}",
            node.member.id,
            response.status(),
            response.text().await.unwrap_or_default()
        )
        .into());
    }
    Ok(())
}

async fn append_http(base_url: &str, record: &EncryptedRecord) -> TestResult<()> {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/append"))
        .bearer_auth(tenant_token(TENANT))
        .json(record)
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::NO_CONTENT {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!(
            "HTTP append lsn {} failed with {status}: {body}",
            record.lsn()
        )
        .into());
    }
    Ok(())
}

async fn append_node_with_placement(
    node: &ReplicaNode,
    record: &EncryptedRecord,
    placement_epoch: u64,
    placement_digest: &str,
) -> TestResult<reqwest::StatusCode> {
    let body = EncryptedRecord::encode_binary_batch(std::slice::from_ref(record))?;
    let response = reqwest::Client::new()
        .post(format!("{}/internal/v1/append-many", node.url))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
        .header(PLACEMENT_EPOCH_HEADER, placement_epoch.to_string())
        .header(PLACEMENT_DIGEST_HEADER, placement_digest)
        .body(body)
        .send()
        .await?;
    Ok(response.status())
}

/// Sends one HTTP request per record.  There is deliberately no retry or
/// polling path here: a handoff that rejects or parks a write is a test
/// failure, even when a later client retry would make the data appear.
async fn append_exact_http(
    base_url: &str,
    records: Vec<EncryptedRecord>,
) -> TestResult<Vec<EncryptedRecord>> {
    let mut acknowledged = Vec::with_capacity(records.len());
    for record in records {
        let lsn = record.lsn();
        let append = tokio::time::timeout(Duration::from_secs(2), append_http(base_url, &record))
            .await
            .map_err(|_| format!("append lsn {lsn} was queued or stalled during online handoff"))?;
        append
            .map_err(|error| format!("append lsn {lsn} failed during online handoff: {error}"))?;
        acknowledged.push(record);
    }
    Ok(acknowledged)
}

async fn stop_all(nodes: &mut [RunningNode]) {
    for node in nodes {
        node.stop().await;
    }
}

async fn membership_ids(gateway: &ReplicaGateway) -> TestResult<BTreeSet<String>> {
    Ok(gateway
        .membership()
        .await?
        .members
        .into_iter()
        .filter(|member| member.status == walleye_bitr_server::MemberStatus::Active)
        .map(|member| member.id)
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_activation_keeps_http_appends_live_until_handoff_commits() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive("online-activation")?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let candidate_gate = Arc::new(HoldGate::new("/internal/v1/control/metadata/adopt"));
    let candidate_gates = BTreeMap::from([("new-0".to_owned(), Arc::clone(&candidate_gate))]);
    let mut new = start_group_with_node_gates(
        &directory,
        &["new-0", "new-1", "new-2"],
        Some(&old_members),
        &candidate_gates,
    )
    .await?;
    let gateway = direct_gateway(&old, &directory, archive, "activation").await?;
    let mut server = RunningGateway::start(Arc::clone(&gateway)).await?;
    for (index, node) in new.iter().enumerate() {
        register_online(&server, node, 1, index as u64 + 10).await?;
    }

    let stream = stream_that_moves_to(1);
    let first = record(&stream, 1, 1, 1);
    append_http(&server.base_url, &first).await?;
    candidate_gate.arm();
    let activation_server = server.base_url.clone();
    let activation = tokio::spawn(async move {
        activate_online_from_url(&activation_server, "activate-live", 1).await
    });
    candidate_gate.wait_until_entered().await?;

    // The activation has already reached a candidate's online control CAS,
    // after the coordinator's prepare stage. These calls are each made once;
    // they must finish while that real handoff I/O is held open.
    let expected_tail = (2..=12)
        .map(|lsn| record(&stream, lsn, 1, lsn as u8))
        .collect::<Vec<_>>();
    let append_server = server.base_url.clone();
    let acknowledged = tokio::time::timeout(
        Duration::from_secs(5),
        append_exact_http(&append_server, expected_tail.clone()),
    )
    .await
    .map_err(|_| "writer did not complete while activation was pending")??;
    assert_eq!(acknowledged, expected_tail);

    candidate_gate.release();
    assert!(activation.await??, "online activation failed");

    // The next append is the route cutover point for this stream.  Use the
    // epoch published by the handoff rather than assuming whether the
    // implementation advanced it before or after sealing the old range.
    let after = gateway.membership().await?;
    let writer_epoch = after
        .stream_segments
        .get(&stream)
        .and_then(|segments| segments.last())
        .map_or(1, |segment| segment.writer_epoch.max(1));
    let final_record = record(&stream, 13, writer_epoch, 13);
    append_http(&server.base_url, &final_record).await?;
    let final_route = gateway.replicas_for_record(&final_record).await?;
    assert!(
        final_route.iter().all(|node| node.id.starts_with("new-")),
        "post-handoff append remained on the old cohort: {final_route:?}"
    );
    assert_eq!(
        gateway.recover(&stream, 0).await?,
        [vec![first], acknowledged, vec![final_record]].concat()
    );

    // Adding a cohort is a rendezvous-ring expansion, not a global switch to
    // the newest cohort. A fresh stream whose winner remains zero must still
    // use the original cohort while the stream above cuts over to cohort one.
    let control_stream = stream_that_stays_with_control(1);
    let control_record = record(&control_stream, 1, writer_epoch, 14);
    append_http(&server.base_url, &control_record).await?;
    let control_route = gateway.replicas_for_record(&control_record).await?;
    assert!(
        control_route.iter().all(|node| node.id.starts_with("old-")),
        "horizontal expansion globally redirected a stream whose ring winner is cohort zero: {control_route:?}"
    );

    server.stop().await;
    stop_all(&mut old).await;
    stop_all(&mut new).await;
    Ok(())
}

async fn activate_online_from_url(
    base_url: &str,
    operation_id: &str,
    cohort_id: u64,
) -> TestResult<bool> {
    let response = post_control(
        base_url,
        "/internal/v1/membership/activate-cohort-online",
        json!({"operation_id": operation_id, "cohort_id": cohort_id}),
    )
    .await?;
    Ok(response.status().is_success())
}

async fn activate_online_from_url_strict(
    base_url: &str,
    operation_id: &str,
    cohort_id: u64,
) -> TestResult<bool> {
    let response = post_control(
        base_url,
        "/internal/v1/membership/activate-cohort-online",
        json!({"operation_id": operation_id, "cohort_id": cohort_id}),
    )
    .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(
            format!("activate-cohort-online {operation_id} failed with {status}: {body}").into(),
        );
    }
    Ok(true)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_old_append_after_target_cas_is_not_acknowledged_as_new_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive("delayed-old-append")?;
    // This gate is on source snapshot I/O after the stream claim and
    // placement fence have been committed. It marks the real SourceFenced
    // handoff boundary; holding a membership request would only park a
    // control-plane call before the data-plane transition.
    let source_snapshot_gate = Arc::new(HoldGate::new("/internal/v1/records"));
    let old_zero_gate = Arc::new(HoldGate::new("/internal/v1/append-many"));
    let old_one_gate = Arc::new(HoldGate::new("/internal/v1/append-many"));
    let old_two_gate = Arc::new(HoldGate::new("/internal/v1/append-many"));
    let old_gates = BTreeMap::from([
        ("old-0".to_owned(), vec![Arc::clone(&old_zero_gate)]),
        (
            "old-1".to_owned(),
            vec![Arc::clone(&source_snapshot_gate), Arc::clone(&old_one_gate)],
        ),
        ("old-2".to_owned(), vec![Arc::clone(&old_two_gate)]),
    ]);
    let mut old = start_group_with_node_gate_layers(
        &directory,
        &["old-0", "old-1", "old-2"],
        None,
        &old_gates,
    )
    .await?;
    let old_members = members(&old);
    let candidate_gate = Arc::new(HoldGate::new("/internal/v1/control/metadata/adopt"));
    let candidate_gates = BTreeMap::from([("new-0".to_owned(), Arc::clone(&candidate_gate))]);
    let mut new = start_group_with_node_gates(
        &directory,
        &["new-0", "new-1", "new-2"],
        Some(&old_members),
        &candidate_gates,
    )
    .await?;
    let gateway = direct_gateway(&old, &directory, archive, "delayed-old-append").await?;
    let mut server = RunningGateway::start(Arc::clone(&gateway)).await?;
    for (index, node) in new.iter().enumerate() {
        register_online(&server, node, 1, index as u64 + 60).await?;
    }

    let stream = stream_that_moves_to(1);
    let first = record(&stream, 1, 1, 71);
    append_http(&server.base_url, &first).await?;

    // Enter authority propagation before starting the delayed append. The
    // candidate is eligible but the per-stream claim has not yet been made,
    // so the HTTP request below enters the old route instead of waiting for a
    // handoff completion call at the gateway boundary.
    candidate_gate.arm();
    source_snapshot_gate.arm();
    let activation_server = server.base_url.clone();
    let activation = tokio::spawn(async move {
        activate_online_from_url_strict(&activation_server, "delayed-old-target", 1).await
    });
    candidate_gate.wait_until_entered().await?;

    // Pause two of the three old data writes.  The HTTP append has entered
    // the real node fan-out and cannot obtain its quorum until these requests
    // resume; this is after the gateway has selected the old immutable range,
    // rather than a synthetic pause before the handoff handler runs.
    old_zero_gate.arm();
    old_one_gate.arm();
    old_two_gate.arm();
    let delayed = record(&stream, 2, 1, 72);
    let append_server = server.base_url.clone();
    let delayed_for_http = delayed.clone();
    let delayed_append =
        tokio::spawn(async move { append_http(&append_server, &delayed_for_http).await });
    old_zero_gate.wait_until_entered().await?;
    old_one_gate.wait_until_entered().await?;
    old_two_gate.wait_until_entered().await?;

    // Let the eligibility CAS finish, then wait for the source snapshot gate.
    // Reaching it proves that the stream-local claim and placement fence are
    // durable while the old append is already executing.
    candidate_gate.release();
    source_snapshot_gate.wait_until_entered().await?;

    // Let the source quorum certify the prefix and commit the successor route
    // while the old append remains held at two storage nodes. Once activation
    // returns, the durable target route CAS has completed.
    source_snapshot_gate.release();
    assert!(activation.await??, "online target activation failed");
    let delayed_route = gateway.replicas_for_record(&delayed).await?;
    assert!(
        delayed_route.iter().all(|node| node.id.starts_with("new-")),
        "target route CAS did not publish the prepared successor: {delayed_route:?}"
    );

    // The request entered the old fan-out before the target route CAS.  Once
    // the source nodes resume, the gateway must forward this exact request
    // to the committed successor and acknowledge it.  It must not require a
    // client retry, synthesize a replacement record, or change the
    // authenticated writer epoch.
    old_zero_gate.release();
    old_one_gate.release();
    old_two_gate.release();
    let delayed_result = tokio::time::timeout(Duration::from_secs(5), delayed_append)
        .await
        .map_err(|_| "delayed old append did not resolve after target CAS")??;
    delayed_result?;

    assert_eq!(delayed.writer_epoch(), first.writer_epoch());

    // A direct stale-node submission with the old host placement is still
    // rejected.  Forwarding belongs to the gateway's durable handoff path;
    // a retired source must never accept the same-epoch record on its own.
    assert_eq!(
        append_node_with_placement(&old[0].member, &delayed, 1, "stale-placement").await?,
        reqwest::StatusCode::CONFLICT,
        "retired source accepted a stale placement after forwarding"
    );

    let final_record = record(&stream, 3, 1, 73);
    append_http(&server.base_url, &final_record).await?;
    let final_route = gateway.replicas_for_record(&final_record).await?;
    assert!(final_route.iter().all(|node| node.id.starts_with("new-")));
    assert_eq!(
        gateway.recover(&stream, 0).await?,
        vec![first, delayed, final_record]
    );

    server.stop().await;
    stop_all(&mut old).await;
    stop_all(&mut new).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_candidate_never_interrupts_the_old_cohort() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive("offline-candidate")?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let mut new = start_group(
        &directory,
        &["offline-0", "offline-1", "offline-2"],
        Some(&old_members),
    )
    .await?;
    let gateway = direct_gateway(&old, &directory, archive, "offline").await?;
    let mut server = RunningGateway::start(Arc::clone(&gateway)).await?;
    for (index, node) in new.iter().enumerate() {
        register_online(&server, node, 1, index as u64 + 20).await?;
    }
    let stream = stream_that_moves_to(1);
    let first = record(&stream, 1, 1, 31);
    append_http(&server.base_url, &first).await?;
    let before = membership_ids(&gateway).await?;
    stop_all(&mut new).await;

    let activation_server = server.base_url.clone();
    let activation = tokio::spawn(async move {
        activate_online_from_url(&activation_server, "activate-offline", 1).await
    });
    let expected = (2..=9)
        .map(|lsn| record(&stream, lsn, 1, lsn as u8))
        .collect::<Vec<_>>();
    let writer = append_exact_http(&server.base_url, expected.clone());
    let (activation_result, writes) = tokio::join!(activation, writer);
    assert!(
        !activation_result??,
        "offline candidate activation unexpectedly succeeded"
    );
    assert_eq!(writes?, expected);
    assert_eq!(membership_ids(&gateway).await?, before);
    let route = gateway
        .replicas_for_record(expected.last().expect("tail"))
        .await?;
    assert!(route.iter().all(|node| node.id.starts_with("old-")));

    server.stop().await;
    stop_all(&mut old).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cutover_failure_keeps_old_writer_serving_and_does_not_publish_target_route() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let archive = archive("cutover-failure")?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let candidate_gate = Arc::new(HoldGate::new("/internal/v1/control/metadata/adopt"));
    let candidate_gates = BTreeMap::from([("failed-0".to_owned(), Arc::clone(&candidate_gate))]);
    let mut new = start_group_with_node_gates(
        &directory,
        &["failed-0", "failed-1", "failed-2"],
        Some(&old_members),
        &candidate_gates,
    )
    .await?;
    let gateway = direct_gateway(&old, &directory, archive, "cutover-failure").await?;
    let mut server = RunningGateway::start(Arc::clone(&gateway)).await?;
    for (index, node) in new.iter().enumerate() {
        register_online(&server, node, 1, index as u64 + 30).await?;
    }
    let stream = stream_that_moves_to(1);
    let first = record(&stream, 1, 1, 51);
    append_http(&server.base_url, &first).await?;

    candidate_gate.arm();
    let activation_server = server.base_url.clone();
    let activation = tokio::spawn(async move {
        activate_online_from_url(&activation_server, "activate-failure", 1).await
    });
    candidate_gate.wait_until_entered().await?;
    // The candidate is still outside the write path.  Removing it while the
    // operation is inside the gateway must make the operation fail closed,
    // while the old cohort continues to acknowledge exact-once appends.
    stop_all(&mut new).await;
    let expected = (2..=8)
        .map(|lsn| record(&stream, lsn, 1, lsn as u8))
        .collect::<Vec<_>>();
    let writes = append_exact_http(&server.base_url, expected.clone());
    candidate_gate.release();
    let (activation_result, writes) = tokio::join!(activation, writes);
    assert!(
        !activation_result??,
        "cutover with an unavailable target succeeded"
    );
    assert_eq!(writes?, expected);

    let snapshot = gateway.membership().await?;
    let route = snapshot
        .stream_segments
        .get(&stream)
        .and_then(|segments| segments.last())
        .ok_or("old route disappeared after failed cutover")?;
    assert_eq!(
        route.cohort_id, 0,
        "failed cutover published a target route"
    );
    assert_eq!(gateway.recover(&stream, 0).await?.len(), 8);

    server.stop().await;
    stop_all(&mut old).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_replacement_retires_control_cohort_and_survives_restart() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive("control-replacement")?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_member_ids = old
        .iter()
        .map(|node| node.member.id.clone())
        .collect::<Vec<_>>();
    let old_members = members(&old);
    let mut unrelated = start_group(
        &directory,
        &["data-0", "data-1", "data-2"],
        Some(&old_members),
    )
    .await?;
    // Candidate readiness now propagates through the authoritative metadata
    // head. Hold that real adoption boundary after the replacement operation
    // has begun; parking an obsolete membership endpoint would only test a
    // request queue and could let the operation complete before the writer
    // reaches the handoff.
    let replacement_gate = Arc::new(HoldGate::new("/internal/v1/control/metadata/adopt"));
    let replacement_gates =
        BTreeMap::from([("replacement-0".to_owned(), Arc::clone(&replacement_gate))]);
    let mut replacements = start_group_with_node_gates(
        &directory,
        &["replacement-0", "replacement-1", "replacement-2"],
        Some(&old_members),
        &replacement_gates,
    )
    .await?;
    let gateway = direct_gateway(
        &old,
        &directory,
        Arc::clone(&archive),
        "control-replacement",
    )
    .await?;
    let mut server = RunningGateway::start(Arc::clone(&gateway)).await?;
    for (index, node) in unrelated.iter().enumerate() {
        register_online(&server, node, 1, index as u64 + 40).await?;
    }

    // Establish an open range on the original control cohort before the
    // unrelated data cohort is activated. That range is the source history
    // whose authority the whole-cohort replacement must move.
    let stream = "tenant-a/control-cohort-replacement";
    let prefix = (1..=4)
        .map(|lsn| record(stream, lsn, 1, lsn as u8))
        .collect::<Vec<_>>();
    append_exact_http(&server.base_url, prefix.clone()).await?;

    let activate_data = activate_online_from_url(&server.base_url, "activate-unrelated", 1).await?;
    assert!(activate_data, "unrelated data cohort activation failed");
    let unrelated_stream = "tenant-a/unrelated-data-cohort";
    let unrelated_record = record(unrelated_stream, 1, 1, 40);
    append_http(&server.base_url, &unrelated_record).await?;
    let unrelated_route_before = gateway.replicas_for_record(&unrelated_record).await?;
    assert!(
        unrelated_route_before
            .iter()
            .all(|node| node.id.starts_with("data-"))
    );

    replacement_gate.arm();
    let replacement_server = server.base_url.clone();
    let replacement_ids = old_member_ids.clone();
    let replacement_nodes = replacements
        .iter()
        .enumerate()
        .map(|(ordinal, node)| {
            json!({
                "id": node.member.id,
                "url": node.member.url,
                "status": "joining",
                "cohort_id": 2,
                "name": node.member.id,
                "machine_id": format!("machine-{}", node.member.id),
                "volume_id": format!("volume-{}", node.member.id),
                "ordinal": ordinal as u64 + 60,
                "tier": "medium",
            })
        })
        .collect::<Vec<_>>();
    let mut replacement = tokio::spawn(async move {
        replace_cohort_online_from_url(
            &replacement_server,
            "replace-control-cohort",
            0,
            2,
            &replacement_ids,
            &replacement_nodes,
        )
        .await
    });
    tokio::select! {
        result = replacement_gate.wait_until_entered() => result?,
        result = &mut replacement => {
            let operation_result = result??;
            return Err(format!(
                "online replacement completed before candidate propagation gate: {operation_result}"
            ).into());
        }
    }
    let during = (5..=12)
        .map(|lsn| record(stream, lsn, 1, lsn as u8))
        .collect::<Vec<_>>();
    let acknowledged = append_exact_http(&server.base_url, during.clone()).await?;
    replacement_gate.release();
    assert!(
        replacement.await??,
        "online control-cohort replacement failed"
    );

    let current = gateway.membership().await?;
    assert!(
        current
            .members
            .iter()
            .filter(|member| member.status == walleye_bitr_server::MemberStatus::Active)
            .all(|member| {
                member.id.starts_with("replacement-") || member.id.starts_with("data-")
            })
    );
    assert!(
        current
            .members
            .iter()
            .filter(|member| member.id.starts_with("old-"))
            .all(|member| member.status == walleye_bitr_server::MemberStatus::Removed),
        "retired source members were not tombstoned"
    );
    let writer_epoch = current
        .stream_segments
        .get(stream)
        .and_then(|segments| segments.last())
        .map_or(1, |segment| segment.writer_epoch.max(1));
    let final_record = record(stream, 13, writer_epoch, 13);
    append_http(&server.base_url, &final_record).await?;
    let unrelated_route_after = gateway.replicas_for_record(&unrelated_record).await?;
    assert_eq!(unrelated_route_after, unrelated_route_before);

    // Placement authority advances independently of the client-supplied
    // writer epoch. An old control member must reject a same-epoch append
    // after it has been retired, even though that epoch remains valid on the
    // replacement cohort.
    let stale = record(stream, 14, writer_epoch, 14);
    assert_eq!(
        append_node_with_placement(&old[0].member, &stale, 1, "stale-placement").await?,
        reqwest::StatusCode::CONFLICT,
        "retired old member accepted a stale host placement"
    );

    // The old control cohort is now gone.  A new coordinator must recover the
    // archive/hot history from the successor nodes, including records written
    // while replacement was in flight.
    stop_all(&mut old).await;
    let restarted = direct_gateway(
        &replacements,
        &directory,
        Arc::clone(&archive),
        "control-replacement-restarted",
    )
    .await?;
    let mut restarted_server = RunningGateway::start(Arc::clone(&restarted)).await?;
    // A restarted coordinator must serve an exact resend from the immutable
    // archive after the source cohort has been retired. It must still reject
    // a different ciphertext at that acknowledged LSN; accepting it would
    // let a stale writer rewrite the historical prefix.
    let duplicate = reqwest::Client::new()
        .post(format!("{}/v1/append", restarted_server.base_url))
        .bearer_auth(tenant_token(TENANT))
        .json(&prefix[0])
        .send()
        .await?;
    assert_eq!(duplicate.status(), reqwest::StatusCode::NO_CONTENT);
    let conflicting = record(stream, 1, 1, 250);
    let conflict_response = reqwest::Client::new()
        .post(format!("{}/v1/append", restarted_server.base_url))
        .bearer_auth(tenant_token(TENANT))
        .json(&conflicting)
        .send()
        .await?;
    assert_eq!(conflict_response.status(), reqwest::StatusCode::CONFLICT);
    let expected = [prefix, acknowledged, vec![final_record]].concat();
    assert_eq!(restarted.recover(stream, 0).await?, expected);

    restarted_server.stop().await;
    server.stop().await;
    stop_all(&mut replacements).await;
    stop_all(&mut unrelated).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_gateway_resumes_source_fenced_claim_after_source_loss() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive("source-fenced-restart")?;
    let source_snapshot_gate = Arc::new(HoldGate::new("/internal/v1/records"));
    let source_gates = BTreeMap::from([("old-1".to_owned(), Arc::clone(&source_snapshot_gate))]);
    let mut old = start_group_with_node_gates(
        &directory,
        &["old-0", "old-1", "old-2"],
        None,
        &source_gates,
    )
    .await?;
    let old_member_ids = old
        .iter()
        .map(|node| node.member.id.clone())
        .collect::<Vec<_>>();
    let old_members = members(&old);
    let mut replacement = start_group(
        &directory,
        &["resume-0", "resume-1", "resume-2"],
        Some(&old_members),
    )
    .await?;
    let gateway = direct_gateway(
        &old,
        &directory,
        Arc::clone(&archive),
        "source-fenced-restart",
    )
    .await?;
    let mut server = RunningGateway::start(Arc::clone(&gateway)).await?;

    let stream = "tenant-a/source-fenced-restart";
    let first = record(stream, 1, 1, 81);
    append_http(&server.base_url, &first).await?;

    for (index, node) in replacement.iter().enumerate() {
        register_online(&server, node, 1, index as u64 + 80).await?;
    }
    for node in &replacement {
        // A replacement operation consumes the exact prepared member
        // incarnations from the durable membership document.  Confirm the
        // candidate is reachable before starting the crash window.
        let response = reqwest::Client::new()
            .get(format!("{}/internal/v1/healthz", node.member.url))
            .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
            .send()
            .await?;
        assert!(response.status().is_success());
    }

    let replacement_nodes = replacement
        .iter()
        .enumerate()
        .map(|(ordinal, node)| {
            json!({
                "id": node.member.id,
                "url": node.member.url,
                "status": "joining",
                "cohort_id": 1,
                "name": node.member.id,
                "machine_id": format!("machine-{}", node.member.id),
                "volume_id": format!("volume-{}", node.member.id),
                "ordinal": ordinal as u64 + 80,
                "tier": "medium",
            })
        })
        .collect::<Vec<_>>();

    source_snapshot_gate.arm();
    let replacement_server = server.base_url.clone();
    let replacement_ids = old_member_ids.clone();
    let replacement_task = tokio::spawn(async move {
        replace_cohort_online_from_url(
            &replacement_server,
            "resume-source-fenced",
            0,
            1,
            &replacement_ids,
            &replacement_nodes,
        )
        .await
    });
    source_snapshot_gate.wait_until_entered().await?;

    let control_head = ControlHeadStore::new(
        archive.object_store(),
        format!("{}/control-head", archive.prefix()),
    )?;
    let pending = control_head
        .load()
        .await?
        .ok_or("authoritative control head missing during crash window")?;
    let claim = pending
        .head
        .metadata
        .pending_handoffs
        .get(stream)
        .ok_or("source-fenced stream claim was not persisted")?;
    assert_eq!(claim.stage, PendingHandoffStage::SourceFenced);
    assert_eq!(claim.target_cohort_id, 1);
    assert_eq!(claim.target_writer_epoch, first.writer_epoch());

    // Simulate a coordinator crash while the source archive read is in
    // flight. The request gate is released only after the operation task is
    // gone, so no later cutover can race the restart below.
    replacement_task.abort();
    let _ = replacement_task.await;
    source_snapshot_gate.release();
    assert_eq!(
        control_head
            .load()
            .await?
            .ok_or("control head disappeared after crash")?
            .head
            .metadata
            .pending_handoffs
            .get(stream)
            .map(|descriptor| descriptor.stage),
        Some(PendingHandoffStage::SourceFenced)
    );

    // One source volume is gone, but the other two remain available to
    // certify the acknowledged prefix and advance the claim to
    // ArchiveVerified. A fresh gateway must adopt that exact claim, finish
    // the archive boundary, and send this single HTTP append to the target
    // cohort with the original client epoch.
    old[1].stop().await;
    let restarted = direct_gateway(
        &replacement,
        &directory,
        Arc::clone(&archive),
        "source-fenced-restart",
    )
    .await?;
    let mut restarted_server = RunningGateway::start(Arc::clone(&restarted)).await?;
    let resumed = record(stream, 2, 1, 82);
    append_http(&restarted_server.base_url, &resumed).await?;
    let route = restarted.replicas_for_record(&resumed).await?;
    assert!(
        route.iter().all(|node| node.id.starts_with("resume-")),
        "resumed append did not use the prepared successor: {route:?}"
    );
    assert_eq!(resumed.writer_epoch(), first.writer_epoch());
    assert_eq!(restarted.recover(stream, 0).await?, vec![first, resumed]);

    let final_head = control_head
        .load()
        .await?
        .ok_or("control head missing after resumed append")?;
    assert!(
        !final_head
            .head
            .metadata
            .pending_handoffs
            .contains_key(stream),
        "resumed append left the stream handoff claim open"
    );

    restarted_server.stop().await;
    server.stop().await;
    stop_all(&mut old).await;
    stop_all(&mut replacement).await;
    Ok(())
}

async fn replace_cohort_online_from_url(
    base_url: &str,
    operation_id: &str,
    source_cohort_id: u64,
    cohort_id: u64,
    old_member_ids: &[String],
    nodes: &[Value],
) -> TestResult<bool> {
    let response = post_control(
        base_url,
        "/internal/v1/membership/replace-cohort-online",
        json!({
            "operation_id": operation_id,
            "source_cohort_id": source_cohort_id,
            "cohort_id": cohort_id,
            "old_member_ids": old_member_ids,
            "nodes": nodes,
        }),
    )
    .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(
            format!("replace-cohort-online {operation_id} failed with {status}: {body}").into(),
        );
    }
    Ok(true)
}
