//! Deterministic concurrency and crash-boundary acceptance tests.
//!
//! These tests deliberately exercise the public gateway/node HTTP seam rather
//! than calling private helpers.  Each scenario has a fixed schedule and
//! asserts the two invariants that matter most for the durability plane:
//! acknowledged records are contiguous, and a record that was not
//! acknowledged is not recovered as committed data.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::{EncryptedRecord, Replica, ReplicaError};
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router};

const INTERNAL_TOKEN: &str = "concurrency-chaos-internal-token";

fn root() -> String {
    STANDARD.encode([191_u8; 32])
}

fn archive() -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "replica",
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

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

async fn running_node(
    directory: &tempfile::TempDir,
    name: &str,
    root_key: &str,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let data_dir = directory.path().join(name);
    let disk = Arc::new(DiskReplica::open_with_config(
        data_dir.join("replica.log"),
        name,
        "hot",
        &data_dir,
    )?);
    running_node_with_app(directory, name, root_key, disk, node_router, None).await
}

type RouterBuilder =
    fn(Arc<DiskReplica>, &str, Option<&str>) -> Result<Router, walleye_bitr::ReplicaError>;

async fn running_node_with_app(
    _directory: &tempfile::TempDir,
    name: &str,
    root_key: &str,
    disk: Arc<DiskReplica>,
    builder: RouterBuilder,
    app_layer: Option<fn(Router) -> Router>,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let member = ReplicaNode::new(name, format!("http://{}", listener.local_addr()?));
    let mut app = builder(Arc::clone(&disk), root_key, Some(INTERNAL_TOKEN))?;
    if let Some(layer) = app_layer {
        app = layer(app);
    }
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode { member, disk, task })
}

/// Atomically consumes one planned fault.  A counter, rather than a random
/// failure, makes every crash boundary reproducible and keeps retries honest.
fn consume_failure(counter: &AtomicUsize) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    while current > 0 {
        match counter.compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(next) => current = next,
        }
    }
    false
}

fn fail_atomic_append_responses_after_persist(app: Router, failures: Arc<AtomicUsize>) -> Router {
    app.layer(axum::middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let failures = Arc::clone(&failures);
            async move {
                let should_mask = request.uri().path() == "/internal/v1/append-many";
                let response = next.run(request).await;
                if should_mask && consume_failure(&failures) {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                }
                response
            }
        },
    ))
}

fn fail_manifest_responses_after_persist(app: Router, failures: Arc<AtomicUsize>) -> Router {
    app.layer(axum::middleware::from_fn(
        move |request: Request<Body>, next: Next| {
            let failures = Arc::clone(&failures);
            async move {
                let should_mask = request.method() == Method::POST
                    && request.uri().path() == "/internal/v1/control/manifest";
                let response = next.run(request).await;
                if should_mask && consume_failure(&failures) {
                    StatusCode::SERVICE_UNAVAILABLE.into_response()
                } else {
                    response
                }
            }
        },
    ))
}

async fn running_node_with_atomic_response_loss(
    directory: &tempfile::TempDir,
    name: &str,
    root_key: &str,
    failures: Arc<AtomicUsize>,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let data_dir = directory.path().join(name);
    let disk = Arc::new(DiskReplica::open_with_config(
        data_dir.join("replica.log"),
        name,
        "hot",
        &data_dir,
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let member = ReplicaNode::new(name, format!("http://{}", listener.local_addr()?));
    let app = fail_atomic_append_responses_after_persist(
        node_router(Arc::clone(&disk), root_key, Some(INTERNAL_TOKEN))?,
        failures,
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode { member, disk, task })
}

async fn restarted_node(
    name: &str,
    root_key: &str,
    disk: Arc<DiskReplica>,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let member = ReplicaNode::new(name, format!("http://{}", listener.local_addr()?));
    let app = node_router(Arc::clone(&disk), root_key, Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode { member, disk, task })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_gateways_retry_identical_records_without_duplicates_or_gaps()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root_key = root();
    let first = running_node(&directory, "two-gateway-0", &root_key).await?;
    let second = running_node(&directory, "two-gateway-1", &root_key).await?;
    let third = running_node(&directory, "two-gateway-2", &root_key).await?;
    let third_disk = Arc::clone(&third.disk);
    let third_member = third.member.clone();
    third.task.abort();

    // The third owner is unavailable for the whole write run. Both gateways
    // must continue with the live two-member quorum; no test timeout or
    // scheduler race is involved.
    let members = vec![first.member.clone(), second.member.clone(), third_member];
    let left = ReplicaGateway::new(members.clone(), 2, &root_key, INTERNAL_TOKEN, archive()?)?;
    let right = ReplicaGateway::new(members, 2, &root_key, INTERNAL_TOKEN, archive()?)?;

    let mut expected = Vec::new();
    for lsn in 1..=16 {
        let event = record("tenant-a/concurrent", lsn, 7, lsn as u8);
        let (left_result, right_result) =
            tokio::join!(left.append(event.clone()), right.append(event.clone()),);
        assert_eq!(left_result, Ok(2), "left gateway lsn={lsn}");
        assert_eq!(right_result, Ok(2), "right gateway lsn={lsn}");
        expected.push(event);
    }

    // Each idempotent retry may receive an ACK, but it must never allocate a
    // second physical frame.  The node snapshots and quorum recovery are the
    // authoritative proof of that property.
    for node in [&first, &second] {
        assert_eq!(node.disk.records("tenant-a/concurrent").await, expected);
        assert_eq!(node.disk.snapshot().committed, expected);
    }
    // The live quorum is sufficient for exact recovery while the third owner
    // is unreachable. Reopening it must not change the recovered prefix.
    assert_eq!(left.recover("tenant-a/concurrent", 0).await?, expected);
    let reopened_third = restarted_node("two-gateway-2", &root_key, third_disk).await?;
    let recovery_gateway = ReplicaGateway::new(
        vec![
            first.member.clone(),
            second.member.clone(),
            reopened_third.member.clone(),
        ],
        2,
        &root_key,
        INTERNAL_TOKEN,
        archive()?,
    )?;
    assert_eq!(
        recovery_gateway.recover("tenant-a/concurrent", 0).await?,
        expected
    );

    first.task.abort();
    second.task.abort();
    reopened_third.task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_append_response_loss_is_retried_without_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root_key = root();
    let mut failed_nodes = Vec::new();
    for index in 0..3 {
        let gate = Arc::new(AtomicUsize::new(1));
        failed_nodes.push(
            running_node_with_atomic_response_loss(
                &directory,
                &format!("append-crash-{index}"),
                &root_key,
                gate,
            )
            .await?,
        );
    }
    let members = failed_nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let gateway = ReplicaGateway::new(members, 2, &root_key, INTERNAL_TOKEN, archive()?)?;
    let event = record("tenant-a/crash", 1, 11, 91);

    // Each node atomically persists the append and commit marker, then loses
    // its first response. The gateway retries the same record and receives a
    // successful idempotent acknowledgement without duplicating the frame.
    assert_eq!(gateway.append(event.clone()).await?, 2);
    for node in &failed_nodes {
        assert_eq!(
            node.disk.records("tenant-a/crash").await,
            vec![event.clone()]
        );
        assert_eq!(node.disk.snapshot().committed, vec![event.clone()]);
    }
    assert_eq!(
        gateway.recover("tenant-a/crash", 0).await?,
        vec![event.clone()]
    );
    let metrics = gateway.metrics().await?;
    assert_eq!(metrics.append_acks, 1);
    assert_eq!(metrics.append_failures, 0);

    // Kill the failed HTTP processes and reopen the same durable disks behind
    // fresh endpoints.  This models a real gateway/node restart, not merely a
    // retry inside one in-memory client.
    for node in &failed_nodes {
        node.task.abort();
    }
    let mut restarted = Vec::new();
    for (index, node) in failed_nodes.into_iter().enumerate() {
        restarted
            .push(restarted_node(&format!("append-crash-{index}"), &root_key, node.disk).await?);
    }
    let retry_gateway = ReplicaGateway::new(
        restarted.iter().map(|node| node.member.clone()).collect(),
        2,
        &root_key,
        INTERNAL_TOKEN,
        archive()?,
    )?;
    assert_eq!(
        retry_gateway.recover("tenant-a/crash", 0).await?,
        vec![event.clone()]
    );
    for node in &restarted {
        assert_eq!(
            node.disk.records("tenant-a/crash").await,
            vec![event.clone()]
        );
        assert_eq!(node.disk.snapshot().committed, vec![event.clone()]);
        node.task.abort();
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manifest_cas_response_loss_converges_for_a_second_gateway()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root_key = root();

    // Bind all addresses before opening the disks so each direct control
    // document starts with the exact same immutable membership snapshot.
    let mut bound = Vec::new();
    for index in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(
            format!("cas-loss-{index}"),
            format!("http://{}", listener.local_addr()?),
        );
        bound.push((listener, member));
    }
    let initial_members = bound
        .iter()
        .map(|(_, member)| member.clone())
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for (index, (listener, member)) in bound.into_iter().enumerate() {
        let data_dir = directory.path().join(format!("cas-loss-{index}"));
        let disk = Arc::new(DiskReplica::open_with_control(
            data_dir.join("replica.log"),
            &member.id,
            "hot",
            &data_dir,
            data_dir.join("control.json"),
            &initial_members,
        )?);
        let failures = Arc::new(AtomicUsize::new(1));
        let app = fail_manifest_responses_after_persist(
            node_router(Arc::clone(&disk), &root_key, Some(INTERNAL_TOKEN))?,
            failures,
        );
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }

    let first_gateway = ReplicaGateway::new_direct(
        initial_members.clone(),
        2,
        &root_key,
        INTERNAL_TOKEN,
        directory.path().join("gateway-first/control.json"),
        archive()?,
    )?;
    let event = record("tenant-a/cas", 1, 13, 131);

    // Every control node persists the CAS and then loses its response.  The
    // first coordinator therefore cannot invent a local success certificate.
    assert_eq!(
        first_gateway.append(event.clone()).await,
        Err(ReplicaError::QuorumUnavailable)
    );
    // The gateway stops waiting once a quorum has answered, so the last
    // member's write may still be landing: every member persists it, but not
    // necessarily before `append` returns.
    for node in &nodes {
        let control = node.disk.control().ok_or("direct node control missing")?;
        let started = std::time::Instant::now();
        while control.state()?.manifest_revision != 1 {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "{} never persisted the manifest",
                node.member.id
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(node.disk.records("tenant-a/cas").await.is_empty());
    }

    // A separate coordinator reconstructs the exact persisted manifest from
    // the control quorum, then appends the record.  Retrying the same request
    // remains idempotent and recovery returns one contiguous record.
    let second_gateway = ReplicaGateway::new_direct(
        initial_members,
        2,
        &root_key,
        INTERNAL_TOKEN,
        directory.path().join("gateway-second/control.json"),
        archive()?,
    )?;
    assert_eq!(second_gateway.append(event.clone()).await?, 2);
    assert_eq!(second_gateway.append(event.clone()).await?, 2);
    assert_eq!(
        second_gateway.recover("tenant-a/cas", 0).await?,
        vec![event.clone()]
    );
    for node in &nodes {
        assert_eq!(node.disk.records("tenant-a/cas").await, vec![event.clone()]);
        assert_eq!(node.disk.snapshot().committed, vec![event.clone()]);
        node.task.abort();
    }
    Ok(())
}
