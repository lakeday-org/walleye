//! Adversarial live-handoff checks.
//!
//! This test holds an old-cohort append after the gateway has selected its
//! route, then lets a second stateless coordinator publish the next range and
//! write the same LSN on the new cohort.  The two distinct encrypted payloads
//! must not both be acknowledged.  `writer_epoch` is intentionally the same
//! in both records: it is authenticated client data and cannot act as the
//! host-side placement fence.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::middleware::{self, Next};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use walleye_bitr::{EncryptedRecord, ReplicaError};
use walleye_bitr_server::{
    DiskReplica, DurableMember, DurableMemberIdentity, MemberStatus, OpaqueArchive, ReplicaGateway,
    ReplicaNode, node_router,
};

const INTERNAL_TOKEN: &str = "live-handoff-model-internal";
const ROOT_KEY: &str = "y8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8s=";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn record(stream: &str, lsn: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": 1,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 32],
        "authentication": vec![marker; 32],
    }))
    .expect("valid opaque test record")
}

/// Holds exactly the first append-many request on each old member.  The
/// release bit makes the gate safe even when a waiter has not yet registered
/// its notification at the instant the test releases the cohort.
struct AppendBarrier {
    remaining: AtomicUsize,
    entered: AtomicUsize,
    released: AtomicBool,
    entered_notify: Notify,
    release_notify: Notify,
}

impl AppendBarrier {
    fn new() -> Self {
        Self {
            remaining: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            entered_notify: Notify::new(),
            release_notify: Notify::new(),
        }
    }

    fn arm(&self, members: usize) {
        self.remaining.store(members, Ordering::Release);
        self.entered.store(0, Ordering::Release);
        self.released.store(false, Ordering::Release);
    }

    async fn hold_if_armed(&self) {
        let mut current = self.remaining.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return;
            }
            match self.remaining.compare_exchange(
                current,
                current.saturating_sub(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
        self.entered.fetch_add(1, Ordering::AcqRel);
        self.entered_notify.notify_waiters();
        loop {
            let notified = self.release_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    async fn wait_until_all_entered(&self, members: usize) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.entered_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.entered.load(Ordering::Acquire) == members {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| "old-cohort append did not reach every node")?;
        Ok(())
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }
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

fn gated_node_router(
    disk: Arc<DiskReplica>,
    gate: Option<Arc<AppendBarrier>>,
) -> TestResult<Router> {
    let mut app = node_router(Arc::clone(&disk), ROOT_KEY, Some(INTERNAL_TOKEN))?;
    if let Some(gate) = gate {
        app = app.layer(middleware::from_fn(
            move |request: Request<Body>, next: Next| {
                let gate = Arc::clone(&gate);
                async move {
                    if request.uri().path() == "/internal/v1/append-many" {
                        gate.hold_if_armed().await;
                    }
                    next.run(request).await
                }
            },
        ));
    }
    Ok(app)
}

/// Pauses the source snapshot read after every source member has durably
/// installed the transition placement. The test can then stop one source
/// member while the remaining two are inside the real HTTP snapshot path.
struct SourceFenceGate {
    armed: AtomicBool,
    fenced: AtomicUsize,
    snapshots: AtomicUsize,
    released: AtomicBool,
    fenced_notify: Notify,
    snapshots_notify: Notify,
    release_notify: Notify,
}

impl SourceFenceGate {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            fenced: AtomicUsize::new(0),
            snapshots: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            fenced_notify: Notify::new(),
            snapshots_notify: Notify::new(),
            release_notify: Notify::new(),
        }
    }

    fn arm(&self) {
        self.fenced.store(0, Ordering::Release);
        self.snapshots.store(0, Ordering::Release);
        self.released.store(false, Ordering::Release);
        self.armed.store(true, Ordering::Release);
    }

    fn note_fence(&self) {
        self.fenced.fetch_add(1, Ordering::AcqRel);
        self.fenced_notify.notify_waiters();
    }

    async fn wait_until_fenced(&self, members: usize) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.fenced_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.fenced.load(Ordering::Acquire) >= members {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| "source cohort did not persist every transition fence")?;
        Ok(())
    }

    async fn hold_source_snapshot(&self) {
        self.snapshots.fetch_add(1, Ordering::AcqRel);
        self.snapshots_notify.notify_waiters();
        loop {
            let notified = self.release_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    async fn wait_until_snapshots(&self, members: usize) -> TestResult {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.snapshots_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.snapshots.load(Ordering::Acquire) >= members {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| "source quorum did not reach the snapshot barrier")?;
        Ok(())
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }
}

fn source_quorum_node_router(
    disk: Arc<DiskReplica>,
    gate: Arc<SourceFenceGate>,
) -> TestResult<Router> {
    let app = node_router(Arc::clone(&disk), ROOT_KEY, Some(INTERNAL_TOKEN))?;
    Ok(app.layer(middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let gate = Arc::clone(&gate);
            async move {
                let path = request.uri().path();
                if path == "/internal/v1/control/placement/fence"
                    && gate.armed.load(Ordering::Acquire)
                {
                    let response = next.run(request).await;
                    if response.status().is_success() {
                        gate.note_fence();
                    }
                    return response;
                }
                if path == "/internal/v1/records"
                    && gate.armed.load(Ordering::Acquire)
                    && gate.fenced.load(Ordering::Acquire) >= 3
                {
                    gate.hold_source_snapshot().await;
                }
                next.run(request).await
            }
        },
    )))
}

async fn start_source_quorum_group(
    directory: &tempfile::TempDir,
    names: &[&str],
    seed: Option<&[ReplicaNode]>,
    gate: Arc<SourceFenceGate>,
) -> TestResult<Vec<RunningNode>> {
    let mut bound = Vec::with_capacity(names.len());
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        bound.push((
            *name,
            ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?)),
            listener,
        ));
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
        let app = source_quorum_node_router(Arc::clone(&disk), Arc::clone(&gate))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode {
            member,
            task: Some(task),
        });
    }
    wait_for_ready(&nodes).await?;
    Ok(nodes)
}

async fn start_group(
    directory: &tempfile::TempDir,
    names: &[&str],
    seed: Option<&[ReplicaNode]>,
    gate: Option<Arc<AppendBarrier>>,
) -> TestResult<Vec<RunningNode>> {
    let mut bound = Vec::with_capacity(names.len());
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        bound.push((
            *name,
            ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?)),
            listener,
        ));
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
        let node_gate = gate.as_ref().map(Arc::clone);
        let app = gated_node_router(Arc::clone(&disk), node_gate)?;
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
                    .header(walleye_bitr_server::INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delayed_old_append_cannot_cross_a_live_cutover() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "live-handoff-model",
        64,
    )?);
    let old_gate = Arc::new(AppendBarrier::new());
    let old = start_group(
        &directory,
        &["handoff-old-0", "handoff-old-1", "handoff-old-2"],
        None,
        Some(Arc::clone(&old_gate)),
    )
    .await?;
    let old_members = members(&old);
    let new = start_group(
        &directory,
        &["handoff-new-0", "handoff-new-1", "handoff-new-2"],
        Some(&old_members),
        None,
    )
    .await?;

    let control_path = directory.path().join("gateway-control.json");
    let gateway_a = Arc::new(ReplicaGateway::new_direct(
        old_members.clone(),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?);
    // This stream is chosen by the same rendezvous domain as the gateway so
    // cohort 0 owns its first range and cohort 1 wins after activation.  A
    // test that happens to hash to cohort 0 would never exercise a cutover.
    let stream = "tenant/live-handoff-0";
    let first = record(stream, 1, 1);
    gateway_a.append(first).await?;

    for (ordinal, node) in new.iter().enumerate() {
        gateway_a
            .join_member_in_cohort_online_with_identity(
                &format!("join-live-handoff-{}", node.member.id),
                node.member.clone(),
                Some(1),
                DurableMemberIdentity {
                    ordinal: ordinal as u64,
                    ..DurableMemberIdentity::default()
                },
            )
            .await?;
    }

    // A second coordinator is constructed only after candidate registration,
    // so it starts with the same durable membership but an independent writer
    // cache and admission lock.
    let gateway_b = Arc::new(ReplicaGateway::new_direct(
        old_members,
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?);
    // A stateless successor must reconstruct the durable tail before it
    // computes the successor range that the placement fence will name.
    gateway_b.recover(stream, 0).await?;

    // The old request has selected its route before the cutover starts. Hold
    // all three node writes so it cannot finish while the successor fence is
    // installed. The middleware gate is before node placement admission,
    // which models an in-flight request whose old token is delayed in transit.
    old_gate.arm(3);
    let old_record = record(stream, 2, 2);
    let old_append = {
        let gateway = Arc::clone(&gateway_a);
        let old_record = old_record.clone();
        tokio::spawn(async move { gateway.append(old_record).await })
    };
    old_gate.wait_until_all_entered(3).await?;

    gateway_b
        .activate_cohort_online("activate-live-handoff", 1)
        .await?;

    // The second coordinator now routes a different payload to the successor
    // cohort while the old request is still held behind its old route token.
    let new_record = record(stream, 2, 200);
    let new_append = {
        let gateway = Arc::clone(&gateway_b);
        let new_record = new_record.clone();
        tokio::spawn(async move { gateway.append(new_record).await })
    };
    let mut new_append = new_append;
    old_gate.release();

    let old_result = tokio::time::timeout(Duration::from_secs(5), old_append)
        .await
        .map_err(|_| "delayed old append did not finish after release")??;
    let new_result = tokio::time::timeout(Duration::from_secs(5), &mut new_append)
        .await
        .map_err(|_| "new append did not finish after cutover")??;

    let old_ack = old_result.is_ok();
    let new_ack = new_result.is_ok();
    assert_ne!(
        (old_ack, new_ack),
        (true, true),
        "two different payloads were acknowledged for one stream/LSN: old={old_result:?}, new={new_result:?}"
    );
    assert_eq!(old_ack as u8 + new_ack as u8, 1, "handoff lost both writes");
    assert!(
        matches!(
            (old_result, new_result),
            (
                Ok(_),
                Err(ReplicaError::WriterFenced | ReplicaError::LsnConflict)
            ) | (
                Err(ReplicaError::WriterFenced | ReplicaError::LsnConflict),
                Ok(_)
            )
        ),
        "the losing placement must fail with a placement conflict"
    );

    drop(new);
    drop(old);
    Ok(())
}

/// A source member may disappear after the transition fence is durable. The
/// remaining two source members still form the recovery quorum needed to
/// archive the source tail and publish the successor. Both records keep the
/// same authenticated client writer epoch; only the host placement changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replacement_finishes_from_a_source_quorum_after_one_member_stops() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "live-handoff-source-quorum",
        64,
    )?);
    let source_gate = Arc::new(SourceFenceGate::new());
    let mut old = start_source_quorum_group(
        &directory,
        &["quorum-old-0", "quorum-old-1", "quorum-old-2"],
        None,
        Arc::clone(&source_gate),
    )
    .await?;
    let old_members = members(&old);
    let mut new = start_group(
        &directory,
        &["quorum-new-0", "quorum-new-1", "quorum-new-2"],
        Some(&old_members),
        None,
    )
    .await?;
    let gateway = Arc::new(ReplicaGateway::new_direct(
        old_members.clone(),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join("source-quorum-control.json"),
        Arc::clone(&archive),
    )?);

    let stream = "tenant/source-quorum-handoff";
    let first = record(stream, 1, 11);
    gateway.append(first.clone()).await?;

    for (ordinal, node) in new.iter().enumerate() {
        gateway
            .join_member_in_cohort_online_with_identity(
                &format!("join-source-quorum-{}", node.member.id),
                node.member.clone(),
                Some(1),
                DurableMemberIdentity {
                    ordinal: ordinal as u64 + 100,
                    ..DurableMemberIdentity::default()
                },
            )
            .await?;
    }
    let candidate_specs = gateway
        .membership()
        .await?
        .members
        .into_iter()
        .filter(|member| member.cohort_id == 1)
        .map(|mut member| {
            member.status = MemberStatus::Joining;
            member
        })
        .collect::<Vec<DurableMember>>();
    assert_eq!(candidate_specs.len(), 3);

    source_gate.arm();
    let operation_gateway = Arc::clone(&gateway);
    let source_ids = old_members
        .iter()
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    let replacement = tokio::spawn(async move {
        operation_gateway
            .replace_cohort_online(
                "source-quorum-replacement",
                0,
                1,
                source_ids,
                candidate_specs,
            )
            .await
    });

    source_gate.wait_until_fenced(3).await?;
    // The fence has drained the old admission path. Stop one source volume
    // while the other two are held in the real HTTP snapshot requests.
    old[0].stop().await;
    source_gate.wait_until_snapshots(2).await?;
    source_gate.release();

    let snapshot = tokio::time::timeout(Duration::from_secs(15), replacement)
        .await
        .map_err(|_| "replacement did not finish from the source quorum")???;
    let route = snapshot
        .stream_segments
        .get(stream)
        .and_then(|segments| segments.last())
        .ok_or("successor route was not published")?;
    assert_eq!(route.cohort_id, 1);
    assert!(
        snapshot
            .members
            .iter()
            .filter(|member| member.cohort_id == 0)
            .all(|member| member.status == walleye_bitr_server::MemberStatus::Removed)
    );

    let second = record(stream, 2, 22);
    gateway.append(second.clone()).await?;
    assert_eq!(first.writer_epoch(), second.writer_epoch());
    assert_eq!(archive.archived_lsn(stream).await?, 1);
    assert_eq!(archive.recover(stream, 0).await?, vec![first.clone()]);
    assert_eq!(gateway.recover(stream, 0).await?, vec![first, second]);

    for node in &mut old {
        node.stop().await;
    }
    for node in &mut new {
        node.stop().await;
    }
    Ok(())
}
