//! Direct-cell tests for stateless coordination and three-way replication.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use walleye_bitr::{EncryptedRecord, Replica, ReplicaError};
use walleye_bitr_server::{
    CohortStatus, DiskReplica, DurableCohort, DurableControl, OpaqueArchive, ReplicaGateway,
    ReplicaNode, gateway_router, node_router,
};

const INTERNAL_TOKEN: &str = "direct-internal-test-token";
const DERIVATION_VERSION: &str = "lakeday-cloud/deployment-identity/v1";

fn archive() -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "replica",
        64,
    )?))
}

fn root() -> String {
    STANDARD.encode([101_u8; 32])
}

fn tenant_token(root_key: &[u8; 32], tenant: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(root_key).expect("HMAC key");
    mac.update(
        format!("{DERIVATION_VERSION}\0{tenant}\0replica-gateway-authentication").as_bytes(),
    );
    hex::encode(mac.finalize().into_bytes())
}

fn record(lsn: u64, marker: u8) -> EncryptedRecord {
    record_for_stream("tenant-a/catalog", lsn, marker)
}

fn record_for_stream(stream: &str, lsn: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": 1,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 12],
        "authentication": vec![marker; 32],
    }))
    .expect("record")
}

fn record_at_epoch(stream: &str, lsn: u64, writer_epoch: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": writer_epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 12],
        "authentication": vec![marker; 32],
    }))
    .expect("record")
}

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl RunningNode {
    /// Stops the HTTP server while retaining the replica's durable disk state.
    async fn stop(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }

    /// Restarts the HTTP server at its stable member URL over the same disk state.
    async fn restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let address = self
            .member
            .url
            .strip_prefix("http://")
            .ok_or("node URL scheme")?
            .parse::<std::net::SocketAddr>()?;
        let listener = TcpListener::bind(address).await?;
        let app = node_router(Arc::clone(&self.disk), &root(), Some(INTERNAL_TOKEN))?;
        self.task = tokio::spawn(async move { axum::serve(listener, app).await });
        Ok(())
    }
}

async fn running_node(
    directory: &tempfile::TempDir,
    name: &str,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    // This fixture intentionally exercises the legacy static gateway. Direct
    // gateway tests must use `initial_direct_group`, which provisions the
    // control and placement endpoints on every member.
    let data = directory.path().join(name);
    let disk = Arc::new(DiskReplica::open_with_config(
        data.join("replica.log"),
        name,
        "hot",
        &data,
    )?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode {
        member: ReplicaNode::new(name, format!("http://{address}")),
        disk,
        task,
    })
}

fn members(nodes: &[&RunningNode]) -> Vec<ReplicaNode> {
    nodes.iter().map(|node| node.member.clone()).collect()
}

async fn gateway(
    nodes: Vec<ReplicaNode>,
    directory: &tempfile::TempDir,
) -> Result<ReplicaGateway, Box<dyn std::error::Error>> {
    Ok(ReplicaGateway::new_direct(
        nodes,
        2,
        &root(),
        INTERNAL_TOKEN,
        directory.path().join("control.json"),
        archive()?,
    )?)
}

#[tokio::test]
async fn every_normal_write_is_sent_to_all_three_members() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let nodes = initial_direct_group(&directory, &["node-0", "node-1", "node-2"]).await?;
    let configured = members(&[&nodes[0], &nodes[1], &nodes[2]]);
    let gateway = gateway(configured, &directory).await?;

    let event = record(1, 1);
    assert_eq!(gateway.append(event.clone()).await?, 2);
    for node in &nodes {
        assert_eq!(
            node.disk.records("tenant-a/catalog").await,
            vec![event.clone()]
        );
    }

    for node in nodes {
        node.task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn direct_gateway_restarts_after_all_nodes_compact_and_accepts_successor()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let nodes = initial_direct_group(&directory, &["node-0", "node-1", "node-2"]).await?;
    let configured = members(&[&nodes[0], &nodes[1], &nodes[2]]);
    let archive: Arc<OpaqueArchive> = Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "replica",
        2,
    )?);
    let control_path = directory.path().join("gateway-control.json");
    let first = ReplicaGateway::new_direct(
        configured.clone(),
        2,
        &root(),
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?;
    let streams = ["tenant-a/catalog", "tenant-a/events"];
    let mut expected = BTreeMap::<String, Vec<EncryptedRecord>>::new();
    for stream in streams {
        let records = (1..=3)
            .map(|lsn| record_at_epoch(stream, lsn, 8, (lsn as u8) + stream.len() as u8))
            .collect::<Vec<_>>();
        assert_eq!(first.append_many(records.clone()).await?, 2);
        expected
            .entry(stream.to_owned())
            .or_default()
            .extend(records);
    }

    futures::future::try_join_all(
        nodes
            .iter()
            .map(|node| first.archive_local_commits(&node.disk)),
    )
    .await?;
    for node in &nodes {
        let snapshot = node.disk.snapshot();
        assert!(snapshot.records.is_empty(), "node={}", node.member.id);
        assert_eq!(snapshot.trimmed.len(), streams.len());
        assert!(
            snapshot
                .trimmed
                .iter()
                .all(|prefix| prefix.archived_lsn == 3 && prefix.writer_epoch == 8)
        );
    }

    for node in &nodes {
        node.task.abort();
    }
    for node in nodes {
        let _ = node.task.await;
    }
    let mut restarted_nodes = Vec::new();
    for member in &configured {
        let address = member
            .url
            .strip_prefix("http://")
            .ok_or("node URL scheme")?
            .parse::<std::net::SocketAddr>()?;
        let listener = loop {
            match TcpListener::bind(address).await {
                Ok(listener) => break listener,
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error.into()),
            }
        };
        let data = directory.path().join(&member.id);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            &member.id,
            "hot",
            &data,
            data.join("control.json"),
            &configured,
        )?);
        let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        restarted_nodes.push(RunningNode {
            member: member.clone(),
            disk,
            task,
        });
    }

    let stale = ReplicaGateway::new_direct(
        configured.clone(),
        2,
        &root(),
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?;
    for stream in streams {
        assert_eq!(stale.recover(stream, 0).await?, expected[stream]);
    }
    let advancer = ReplicaGateway::new_direct(
        configured,
        2,
        &root(),
        INTERNAL_TOKEN,
        control_path,
        Arc::clone(&archive),
    )?;
    for stream in streams {
        let successor = record_at_epoch(stream, 4, 8, 44 + stream.len() as u8);
        assert_eq!(
            advancer.append(successor.clone()).await,
            Ok(2),
            "advancing {stream}"
        );
        expected.get_mut(stream).expect("stream").push(successor);
        let next = record_at_epoch(stream, 5, 8, 55 + stream.len() as u8);
        assert_eq!(
            stale.append(next.clone()).await,
            Ok(2),
            "refreshing stale coordinator for {stream}"
        );
        expected.get_mut(stream).expect("stream").push(next);
        assert_eq!(stale.recover(stream, 0).await?, expected[stream]);
    }

    for node in restarted_nodes {
        node.task.abort();
    }
    Ok(())
}

async fn direct_group_with_append_gate(
    directory: &tempfile::TempDir,
    names: &[&str],
    gated_name: &str,
) -> Result<(Vec<RunningNode>, Arc<AtomicBool>, Arc<Notify>), Box<dyn std::error::Error>> {
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?));
        bound.push((*name, member, listener));
    }
    let initial_members = bound
        .iter()
        .map(|(_, member, _)| member.clone())
        .collect::<Vec<_>>();
    let append_seen = Arc::new(AtomicBool::new(false));
    let append_release = Arc::new(Notify::new());
    let mut nodes = Vec::new();
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &initial_members,
        )?);
        let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
        let app = if name == gated_name {
            let append_seen = Arc::clone(&append_seen);
            let append_release = Arc::clone(&append_release);
            app.layer(axum::middleware::from_fn(
                move |request: Request<axum::body::Body>, next: Next| {
                    let append_seen = Arc::clone(&append_seen);
                    let append_release = Arc::clone(&append_release);
                    async move {
                        if request.uri().path() == "/internal/v1/append-many" {
                            append_seen.store(true, Ordering::Release);
                            append_release.notified().await;
                        }
                        next.run(request).await
                    }
                },
            ))
        } else {
            app
        };
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }
    Ok((nodes, append_seen, append_release))
}

#[tokio::test]
async fn returns_after_two_fsyncs_while_the_third_write_continues()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (nodes, append_seen, append_release) =
        direct_group_with_append_gate(&directory, &["node-0", "node-1", "node-2"], "node-2")
            .await?;
    let gateway = gateway(members(&[&nodes[0], &nodes[1], &nodes[2]]), &directory).await?;

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), gateway.append(record(1, 2))).await??,
        2
    );
    assert!(append_seen.load(Ordering::Acquire));
    assert!(nodes[2].disk.records("tenant-a/catalog").await.is_empty());
    append_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while nodes[2].disk.records("tenant-a/catalog").await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    for node in nodes {
        node.task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn stream_placement_is_exactly_three_even_with_more_members()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let nodes = initial_direct_group(
        &directory,
        &["node-0", "node-1", "node-2", "node-3", "node-4"],
    )
    .await?;
    let configured = nodes.iter().collect::<Vec<_>>();
    let gateway = gateway(members(&configured), &directory).await?;
    let replicas = gateway.replicas_for_stream("tenant-a/catalog").await?;
    assert_eq!(replicas.len(), 3);
    assert_eq!(
        replicas
            .iter()
            .map(|node| &node.id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );

    let event = record(1, 3);
    assert_eq!(gateway.append(event).await?, 2);
    let mut copied = 0;
    for node in &nodes {
        if !node.disk.records("tenant-a/catalog").await.is_empty() {
            copied += 1;
        }
    }
    assert_eq!(copied, 3);
    for node in nodes {
        node.task.abort();
    }
    Ok(())
}

async fn direct_group(
    directory: &tempfile::TempDir,
    names: &[&str],
    initial_members: &[ReplicaNode],
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?));
        bound.push((*name, member, listener));
    }
    let mut nodes = Vec::new();
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            initial_members,
        )?);
        let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }
    Ok(nodes)
}

async fn initial_direct_group(
    directory: &tempfile::TempDir,
    names: &[&str],
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    initial_direct_group_with_commit_delay(directory, names, None).await
}

async fn initial_direct_group_with_commit_delay(
    directory: &tempfile::TempDir,
    names: &[&str],
    commit_delay: Option<Duration>,
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?));
        bound.push((*name, member, listener));
    }
    let initial_members = bound
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
            &initial_members,
        )?);
        let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
        let app = if let Some(delay) = commit_delay {
            app.layer(axum::middleware::from_fn(
                move |request: Request<axum::body::Body>, next: Next| async move {
                    if request.uri().path() == "/internal/v1/commit" {
                        tokio::time::sleep(delay).await;
                    }
                    next.run(request).await
                },
            ))
        } else {
            app
        };
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }
    Ok(nodes)
}

#[tokio::test]
async fn direct_gateway_acknowledges_with_one_of_three_members_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut nodes = initial_direct_group(&directory, &["node-0", "node-1", "node-2"]).await?;
    let configured = nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let gateway = gateway(configured, &directory).await?;

    assert_eq!(gateway.append(record(1, 41)).await?, 2);
    nodes[2].stop().await;
    let second = record(2, 42);
    assert_eq!(gateway.append(second.clone()).await?, 2);
    assert!(!gateway.issue_commit_certificate(&second).await?.is_empty());
    assert_eq!(
        nodes[0].disk.records("tenant-a/catalog").await,
        vec![record(1, 41), record(2, 42)]
    );
    assert_eq!(
        nodes[1].disk.records("tenant-a/catalog").await,
        vec![record(1, 41), record(2, 42)]
    );
    assert_eq!(
        nodes[2].disk.records("tenant-a/catalog").await,
        vec![record(1, 41)]
    );

    nodes[2].restart().await?;
    let report = gateway.repair("tenant-a/catalog", 2).await?;
    assert_eq!(report.repaired, 1);
    assert_eq!(
        nodes[2].disk.records("tenant-a/catalog").await,
        vec![record(1, 41), record(2, 42)]
    );

    nodes[0].task.abort();
    nodes[1].task.abort();
    nodes[2].task.abort();
    Ok(())
}

async fn three_active_cohort_cell(
    directory: &tempfile::TempDir,
) -> Result<(Vec<RunningNode>, ReplicaGateway), Box<dyn std::error::Error>> {
    let old_nodes = initial_direct_group(directory, &["old-0", "old-1", "old-2"]).await?;
    let old_members = old_nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let cohort_one = direct_group(
        directory,
        &["cohort-1-0", "cohort-1-1", "cohort-1-2"],
        &old_members,
    )
    .await?;
    let cohort_two = direct_group(
        directory,
        &["cohort-2-0", "cohort-2-1", "cohort-2-2"],
        &old_members,
    )
    .await?;
    let coordinator = gateway(old_members, directory).await?;

    for (index, node) in cohort_one.iter().enumerate() {
        coordinator
            .join_member_in_cohort(
                &format!("join-cohort-1-{index}"),
                node.member.clone(),
                Some(1),
            )
            .await?;
    }
    coordinator.activate_cohort("activate-cohort-1", 1).await?;
    for (index, node) in cohort_two.iter().enumerate() {
        coordinator
            .join_member_in_cohort(
                &format!("join-cohort-2-{index}"),
                node.member.clone(),
                Some(2),
            )
            .await?;
    }
    coordinator.activate_cohort("activate-cohort-2", 2).await?;

    let mut nodes = old_nodes;
    nodes.extend(cohort_one);
    nodes.extend(cohort_two);
    Ok((nodes, coordinator))
}

#[tokio::test]
async fn cohort_cutover_routes_new_lsn_and_recovers_across_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let old_names = ["old-0", "old-1", "old-2"];
    let old_listeners = {
        let mut listeners = Vec::new();
        for name in old_names {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            listeners.push((
                name,
                ReplicaNode::new(name, format!("http://{}", listener.local_addr()?)),
                listener,
            ));
        }
        listeners
    };
    let old_members = old_listeners
        .iter()
        .map(|(_, member, _)| member.clone())
        .collect::<Vec<_>>();
    let mut old_nodes = Vec::new();
    for (name, member, listener) in old_listeners {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &old_members,
        )?);
        let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        old_nodes.push(RunningNode { member, disk, task });
    }
    let coordinator = gateway(
        old_nodes.iter().map(|node| node.member.clone()).collect(),
        &directory,
    )
    .await
    .map_err(|error| format!("gateway: {error:?}"))?;

    let first = record(1, 31);
    assert_eq!(
        coordinator
            .append(first.clone())
            .await
            .map_err(|error| format!("first append: {error:?}"))?,
        2
    );

    let new_names = ["new-0", "new-1", "new-2"];
    let new_nodes = direct_group(&directory, &new_names, &old_members).await?;
    for (index, node) in new_nodes.iter().enumerate() {
        let operation_id = format!("join-{index}");
        let snapshot = coordinator
            .join_member_in_cohort(&operation_id, node.member.clone(), Some(1))
            .await
            .map_err(|error| format!("join {index}: {error:?}"))?;
        assert_eq!(
            snapshot.cohorts[0].status,
            walleye_bitr_server::CohortStatus::Active
        );
    }
    let joined = coordinator.membership().await?;
    assert_eq!(joined.cohorts.len(), 2);
    assert_eq!(
        joined.cohorts[1].status,
        walleye_bitr_server::CohortStatus::Joining
    );
    let activated = coordinator
        .activate_cohort("activate-1", 1)
        .await
        .map_err(|error| format!("activate: {error:?}"))?;
    assert_eq!(
        activated.cohorts[0].status,
        walleye_bitr_server::CohortStatus::Active
    );
    assert_eq!(
        activated.cohorts[1].status,
        walleye_bitr_server::CohortStatus::Active
    );

    let second = record(2, 32);
    coordinator
        .replicas_for_record(&second)
        .await
        .map_err(|error| format!("route before second append: {error:?}"))?;
    assert_eq!(
        coordinator
            .append(second.clone())
            .await
            .map_err(|error| format!("second append: {error:?}"))?,
        2
    );
    let second_replicas = coordinator.replicas_for_record(&second).await?;
    let second_ids = second_replicas
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    // Rendezvous placement can legitimately keep this stream on cohort zero;
    // when the winner changes, only the new cohort receives the cutover LSN.
    // The test follows the manifest route instead of assuming that every
    // stream must move on activation.
    for node in &old_nodes {
        let expected = if second_ids.contains(node.member.id.as_str()) {
            vec![first.clone(), second.clone()]
        } else {
            vec![first.clone()]
        };
        assert_eq!(node.disk.records("tenant-a/catalog").await, expected);
    }
    for node in &new_nodes {
        let expected = if second_ids.contains(node.member.id.as_str()) {
            vec![second.clone()]
        } else {
            Vec::new()
        };
        assert_eq!(node.disk.records("tenant-a/catalog").await, expected);
    }
    assert_eq!(
        coordinator
            .replicas_for_record(&first)
            .await?
            .iter()
            .map(|node| node.id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        old_nodes
            .iter()
            .map(|node| node.member.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
    );
    assert_eq!(
        second_ids,
        coordinator
            .replicas_for_record(&second)
            .await?
            .iter()
            .map(|node| node.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
    );
    assert_eq!(
        coordinator.membership().await?.cohorts[0].status,
        walleye_bitr_server::CohortStatus::Active
    );
    let segments = coordinator
        .membership()
        .await?
        .stream_segments
        .remove("tenant-a/catalog")
        .ok_or("missing stream routing")?;
    if second_ids
        == old_nodes
            .iter()
            .map(|node| node.member.id.as_str())
            .collect::<std::collections::BTreeSet<_>>()
    {
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].end_lsn, None);
        assert_eq!(segments[0].cohort_id, 0);
    } else {
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].end_lsn, Some(1));
        assert_eq!(segments[0].cohort_id, 0);
        assert_eq!(segments[1].start_lsn, 2);
        assert_eq!(segments[1].cohort_id, 1);
    }

    let restarted = gateway(
        old_nodes
            .iter()
            .chain(&new_nodes)
            .map(|node| node.member.clone())
            .collect(),
        &directory,
    )
    .await?;
    assert_eq!(
        restarted.recover("tenant-a/catalog", 0).await?,
        vec![first.clone(), second.clone()]
    );
    // A stateless coordinator may restart with a fresh local cache. It must
    // recover the same immutable route from the fixed control quorum before
    // filtering evidence by cohort.
    let stateless = ReplicaGateway::new_direct(
        old_nodes
            .iter()
            .chain(&new_nodes)
            .map(|node| node.member.clone())
            .collect(),
        2,
        &root(),
        INTERNAL_TOKEN,
        directory.path().join("stateless-control.json"),
        archive()?,
    )?;
    assert_eq!(
        stateless.recover("tenant-a/catalog", 0).await?,
        vec![first.clone(), second.clone()]
    );
    for node in old_nodes.into_iter().chain(new_nodes) {
        node.task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn storage_rejects_conflicting_payload_but_accepts_identical_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let node = DiskReplica::open_with_config(
        directory.path().join("replica.log"),
        "node",
        "hot",
        directory.path(),
    )?;
    let first = record(1, 11);
    let conflicting = record(1, 12);
    node.append(first.clone()).await?;
    assert_eq!(node.append(first).await, Ok(()));
    assert_eq!(
        node.append(conflicting).await,
        Err(ReplicaError::LsnConflict)
    );
    Ok(())
}

#[tokio::test]
async fn oversized_append_is_rejected_before_fanout() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let nodes = initial_direct_group(&directory, &["node-0", "node-1", "node-2"]).await?;
    let configured = members(&[&nodes[0], &nodes[1], &nodes[2]]);
    let control_path = directory.path().join("capacity-control.json");
    let control = DurableControl::open(&control_path, &configured)?;
    let snapshot = control.membership()?;
    let mut limited_members = snapshot.members;
    for member in &mut limited_members {
        member.tier = "tiny".to_owned();
        member.max_append_bytes = 8;
    }
    control.cas_membership_document(
        snapshot.membership_epoch,
        limited_members.clone(),
        Some(vec![DurableCohort {
            id: 0,
            members: limited_members
                .iter()
                .map(|member| member.id.clone())
                .collect(),
            status: CohortStatus::Active,
            tier: "tiny".to_owned(),
            max_append_bytes: 8,
        }]),
        Some(snapshot.stream_segments),
        "capacity-policy",
    )?;
    let gateway = Arc::new(ReplicaGateway::new_direct(
        configured,
        2,
        &root(),
        INTERNAL_TOKEN,
        control_path,
        archive()?,
    )?);

    let event = record(1, 77);
    let response = tower::ServiceExt::oneshot(
        gateway_router(Arc::clone(&gateway)),
        Request::builder()
            .method("POST")
            .uri("/v1/append")
            .header(
                "authorization",
                format!("Bearer {}", tenant_token(&[101; 32], "tenant-a")),
            )
            .header("content-type", "application/json")
            .body(axum::body::Body::from(serde_json::to_vec(&event)?))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body)?,
        json!({
            "code": "replica_capacity_exceeded",
            "retryable": false,
            "requested_bytes": 12,
            "max_append_bytes": 8,
            "cohort_id": 0,
        })
    );
    for node in &nodes {
        assert!(
            node.disk.records("tenant-a/catalog").await.is_empty(),
            "capacity rejection must happen before node fanout"
        );
    }

    for node in nodes {
        node.task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn stateless_direct_gateways_ack_adjacent_lsns_after_commit_markers()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let nodes = initial_direct_group_with_commit_delay(
        &directory,
        &["node-0", "node-1", "node-2"],
        Some(Duration::from_millis(200)),
    )
    .await?;
    let configured = members(&[&nodes[0], &nodes[1], &nodes[2]]);
    let mut servers = Vec::new();
    for index in 0..2 {
        let gateway = Arc::new(ReplicaGateway::new_direct(
            configured.clone(),
            2,
            &root(),
            INTERNAL_TOKEN,
            directory.path().join(format!("gateway-{index}.json")),
            archive()?,
        )?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task =
            tokio::spawn(async move { axum::serve(listener, gateway_router(gateway)).await });
        servers.push((address, task));
    }

    let client = reqwest::Client::new();
    let first = record(1, 80);
    let first_response = client
        .post(format!("http://{}/v1/append", servers[0].0))
        .bearer_auth(tenant_token(&[101; 32], "tenant-a"))
        .json(&first)
        .send()
        .await?;
    assert_eq!(first_response.status(), StatusCode::NO_CONTENT);
    assert!(
        first_response
            .headers()
            .get(walleye_bitr_server::COMMIT_CERTIFICATE_HEADER)
            .is_some_and(|value| !value.is_empty())
    );
    let committed_after_first = nodes
        .iter()
        .filter(|node| {
            node.disk
                .snapshot()
                .committed
                .iter()
                .any(|candidate| candidate == &first)
        })
        .count();
    assert!(
        committed_after_first >= 2,
        "HTTP 204 must imply quorum commit markers, found {committed_after_first}"
    );

    let second = record(2, 81);
    let second_response = client
        .post(format!("http://{}/v1/append", servers[1].0))
        .bearer_auth(tenant_token(&[101; 32], "tenant-a"))
        .json(&second)
        .send()
        .await?;
    assert_eq!(second_response.status(), StatusCode::NO_CONTENT);
    assert!(
        second_response
            .headers()
            .get(walleye_bitr_server::COMMIT_CERTIFICATE_HEADER)
            .is_some_and(|value| !value.is_empty())
    );

    // The third operation is detached once the gateway has its quorum. It
    // still must finish both phases, even though its response channel was
    // closed when the first request returned.
    let caught_up = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let complete = nodes.iter().all(|node| {
                let snapshot = node.disk.snapshot();
                snapshot.records.contains(&first)
                    && snapshot.records.contains(&second)
                    && snapshot.committed.contains(&first)
                    && snapshot.committed.contains(&second)
            });
            if complete {
                break true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok_and(|complete| complete);
    assert!(
        caught_up,
        "all three nodes must finish data and commit phases"
    );

    for node in nodes {
        assert_eq!(
            node.disk.records("tenant-a/catalog").await,
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            node.disk.snapshot().committed,
            vec![first.clone(), second.clone()]
        );
        node.task.abort();
    }
    for (_, task) in servers {
        task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn two_stateless_coordinators_cannot_both_ack_conflicting_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let node0 = running_node(&directory, "node-0").await?;
    let node1 = running_node(&directory, "node-1").await?;
    let node2 = running_node(&directory, "node-2").await?;
    let configured = members(&[&node0, &node1, &node2]);
    let first = ReplicaGateway::new(configured.clone(), 2, &root(), INTERNAL_TOKEN, archive()?)?;
    let second = ReplicaGateway::new(configured, 2, &root(), INTERNAL_TOKEN, archive()?)?;

    let (left, right) = tokio::join!(first.append(record(1, 21)), second.append(record(1, 22)));
    assert!(usize::from(left.is_ok()) + usize::from(right.is_ok()) <= 1);
    for node in [&node0, &node1, &node2] {
        assert!(node.disk.records("tenant-a/catalog").await.len() <= 1);
    }

    node0.task.abort();
    node1.task.abort();
    node2.task.abort();
    Ok(())
}

#[tokio::test]
async fn three_active_cohorts_are_ring_eligible_and_distribute_many_streams()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (nodes, coordinator) = three_active_cohort_cell(&directory).await?;
    let snapshot = coordinator.membership().await?;
    assert_eq!(snapshot.cohorts.len(), 3);
    assert!(
        snapshot
            .cohorts
            .iter()
            .all(|cohort| { cohort.status == CohortStatus::Active && cohort.members.len() == 3 })
    );

    let member_cohorts = snapshot
        .cohorts
        .iter()
        .flat_map(|cohort| {
            cohort
                .members
                .iter()
                .map(move |id| (id.as_str(), cohort.id))
        })
        .collect::<BTreeMap<_, _>>();
    let mut selected = BTreeMap::<u64, usize>::new();
    for stream_id in 0..96_u64 {
        let replicas = coordinator
            .replicas_for_stream(&format!("tenant-a/ring-{stream_id}"))
            .await?;
        assert_eq!(replicas.len(), 3);
        let cohort_ids = replicas
            .iter()
            .map(|node| member_cohorts[node.id.as_str()])
            .collect::<BTreeSet<_>>();
        assert_eq!(cohort_ids.len(), 1, "stream={stream_id}");
        *selected
            .entry(*cohort_ids.first().expect("one cohort"))
            .or_default() += 1;
    }
    assert_eq!(selected.len(), 3, "ring must select every active cohort");
    assert!(
        selected.values().all(|count| *count > 8),
        "selected={selected:?}"
    );

    for node in nodes {
        node.task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn each_append_targets_exactly_one_active_cohort_and_all_three_members()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let (nodes, coordinator) = three_active_cohort_cell(&directory).await?;
    let snapshot = coordinator.membership().await?;
    let member_cohorts = snapshot
        .cohorts
        .iter()
        .flat_map(|cohort| {
            cohort
                .members
                .iter()
                .map(move |id| (id.as_str(), cohort.id))
        })
        .collect::<BTreeMap<_, _>>();
    let mut selected = BTreeSet::new();

    for stream_id in 0..24_u64 {
        let stream = format!("tenant-a/append-{stream_id}");
        let event = record_for_stream(&stream, 1, stream_id as u8);
        let append_result = coordinator.append(event.clone()).await;
        assert_eq!(
            append_result.map_err(|error| format!("append {stream}: {error:?}"))?,
            2
        );
        let replicas = coordinator.replicas_for_record(&event).await?;
        let replica_ids = replicas
            .iter()
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(replica_ids.len(), 3, "stream={stream}");
        let cohorts = replica_ids
            .iter()
            .map(|id| member_cohorts[id])
            .collect::<BTreeSet<_>>();
        assert_eq!(cohorts.len(), 1, "stream={stream}");
        selected.extend(cohorts);

        // The third write is detached after the quorum ACK. Give it a short
        // bounded window before checking the all-three-members invariant.
        for _ in 0..100 {
            let copied = nodes
                .iter()
                .filter(|node| {
                    futures::executor::block_on(node.disk.records(&stream)).contains(&event)
                })
                .count();
            if copied == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for node in &nodes {
            let records = node.disk.records(&stream).await;
            if replica_ids.contains(node.member.id.as_str()) {
                assert_eq!(records, vec![event.clone()], "member={}", node.member.id);
            } else {
                assert!(records.is_empty(), "non-owner={}", node.member.id);
            }
        }
    }
    assert_eq!(
        selected.len(),
        3,
        "append ring must use every active cohort"
    );

    for node in nodes {
        node.task.abort();
    }
    Ok(())
}

#[tokio::test]
async fn stale_local_manifest_cache_needs_two_control_peers()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let nodes = initial_direct_group(&directory, &["control-0", "control-1", "control-2"]).await?;
    let configured = nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let writer = gateway(configured.clone(), &directory).await?;
    writer.append(record(1, 91)).await?;

    // This coordinator has a fresh, stale local cache. Only one of the fixed
    // control-cohort peers remains available, so the cache cannot substitute
    // for a two-member manifest quorum.
    nodes[1].task.abort();
    nodes[2].task.abort();
    let stale = ReplicaGateway::new_direct(
        configured,
        2,
        &root(),
        INTERNAL_TOKEN,
        directory.path().join("stale-cache/control.json"),
        archive()?,
    )?;
    assert_eq!(
        stale.replicas_for_stream("tenant-a/catalog").await,
        Err(ReplicaError::QuorumUnavailable)
    );

    nodes[0].task.abort();
    Ok(())
}

#[tokio::test]
async fn adjacent_lsn_stateless_coordinators_cannot_ack_out_of_order()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let nodes = initial_direct_group(&directory, &["ordered-0", "ordered-1", "ordered-2"]).await?;
    let configured = nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let first = ReplicaGateway::new_direct(
        configured.clone(),
        2,
        &root(),
        INTERNAL_TOKEN,
        directory.path().join("ordered-first/control.json"),
        archive()?,
    )?;
    let second = ReplicaGateway::new_direct(
        configured,
        2,
        &root(),
        INTERNAL_TOKEN,
        directory.path().join("ordered-second/control.json"),
        archive()?,
    )?;
    let first_record = record(1, 101);
    let second_record = record_for_stream("tenant-a/catalog", 2, 102);

    // Poll the higher LSN first. It may fail while the first coordinator is
    // still reconstructing the prefix, but it must never be the first ACK.
    let started = Instant::now();
    let (second_result, first_result) = tokio::join!(
        async {
            let result = second.append(second_record.clone()).await;
            (result, started.elapsed())
        },
        async {
            let result = first.append(first_record.clone()).await;
            (result, started.elapsed())
        },
    );
    assert!(first_result.0.is_ok(), "first={:?}", first_result.0);
    if second_result.0.is_ok() {
        assert!(
            first_result.1 <= second_result.1,
            "higher LSN acknowledged first: first={:?}, second={:?}",
            first_result.1,
            second_result.1
        );
    }
    for node in &nodes {
        let records = node.disk.records("tenant-a/catalog").await;
        assert!(!records.contains(&second_record) || records.contains(&first_record));
    }

    for node in nodes {
        node.task.abort();
    }
    Ok(())
}
