//! Control-cohort failure and durable-manifest quorum acceptance tests.
//!
//! These tests intentionally use real `DiskReplica` volumes and HTTP node
//! listeners.  The gateway remains stateless between constructions; the only
//! control authority is the original three-member cohort persisted on each
//! volume.  A replacement therefore restarts the same logical member with
//! its durable volume instead of silently introducing a new identity.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use walleye_bitr::{EncryptedRecord, ReplicaError};
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router};

const INTERNAL_TOKEN: &str = "control-quorum-replacement-test-token";

fn root() -> String {
    STANDARD.encode([181_u8; 32])
}

fn archive() -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "control-quorum",
        64,
    )?))
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
    .expect("opaque record fixture")
}

struct RunningNode {
    name: String,
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl RunningNode {
    async fn stop(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

async fn start_cluster(
    directory: &tempfile::TempDir,
    names: &[&str],
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let mut bound = Vec::with_capacity(names.len());
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        bound.push((
            (*name).to_owned(),
            ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?)),
            listener,
        ));
    }
    let initial_members = bound
        .iter()
        .map(|(_, member, _)| member.clone())
        .collect::<Vec<_>>();
    let mut nodes = Vec::with_capacity(bound.len());
    for (name, member, listener) in bound {
        let node = start_node(directory, name, member, &initial_members, listener)?;
        wait_for_ready(&node.member).await?;
        nodes.push(node);
    }
    Ok(nodes)
}

fn start_node(
    directory: &tempfile::TempDir,
    name: String,
    member: ReplicaNode,
    initial_members: &[ReplicaNode],
    listener: TcpListener,
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let data = directory.path().join(&name);
    let disk = Arc::new(DiskReplica::open_with_control(
        data.join("replica.log"),
        &name,
        "hot",
        &data,
        data.join("control.json"),
        initial_members,
    )?);
    let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode {
        name,
        member,
        disk,
        task,
    })
}

async fn wait_for_ready(member: &ReplicaNode) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    for _ in 0..200 {
        if let Ok(response) = client.get(format!("{}/readyz", member.url)).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    Err(format!("node {} did not become ready", member.id).into())
}

async fn restart_node(
    directory: &tempfile::TempDir,
    name: String,
    member: ReplicaNode,
    initial_members: &[ReplicaNode],
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let address = member
        .url
        .strip_prefix("http://")
        .ok_or("test member URL must use http")?
        .parse::<std::net::SocketAddr>()?;
    let listener = loop {
        match TcpListener::bind(address).await {
            Ok(listener) => break listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let node = start_node(directory, name, member, initial_members, listener)?;
    wait_for_ready(&node.member).await?;
    Ok(node)
}

async fn gateway(
    directory: &tempfile::TempDir,
    members: &[ReplicaNode],
    cache_name: &str,
) -> Result<ReplicaGateway, Box<dyn std::error::Error>> {
    Ok(ReplicaGateway::new_direct(
        members.to_vec(),
        2,
        &root(),
        INTERNAL_TOKEN,
        directory.path().join(cache_name),
        archive()?,
    )?)
}

async fn wait_for_manifest(
    nodes: &[RunningNode],
    revision: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..200 {
        if nodes.iter().all(|node| {
            node.disk
                .control()
                .and_then(|control| control.manifest().ok())
                .is_some_and(|manifest| manifest.revision >= revision)
        }) {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    Err(format!("manifest revision {revision} did not reach all control peers").into())
}

fn members(nodes: &[RunningNode]) -> Vec<ReplicaNode> {
    nodes.iter().map(|node| node.member.clone()).collect()
}

async fn stop_all(nodes: &mut [RunningNode]) {
    for node in nodes {
        node.stop().await;
    }
}

#[tokio::test]
async fn one_original_control_member_failure_and_same_volume_restart_preserve_data()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut nodes = start_cluster(&directory, &["control-0", "control-1", "control-2"]).await?;
    let configured = members(&nodes);
    let stream = "tenant-a/control-replacement";
    let first = record(stream, 1, 11);
    let second = record(stream, 2, 12);
    let initial = gateway(&directory, &configured, "coordinator-control.json").await?;
    assert_eq!(initial.append(first.clone()).await?, 2);
    wait_for_manifest(&nodes, 1).await?;

    // One original control peer can disappear while the other two continue
    // to certify the immutable route manifest and recover the committed tail.
    nodes[0].stop().await;
    let degraded = gateway(&directory, &configured, "degraded-cache/control.json").await?;
    // Two surviving owners provide both the control and data quorum. Recovery
    // and the successor write remain available while the third copy is down.
    assert_eq!(degraded.recover(stream, 0).await?, vec![first.clone()]);
    assert_eq!(
        degraded.recover_with_watermark(stream, 0, Some(1)).await?,
        vec![first.clone()]
    );
    assert_eq!(degraded.append(second.clone()).await?, 2);

    // Restarting the same logical member from its existing volume is an
    // ordinary replacement. It must retain the control manifest and history;
    // no new member identity is allowed to rewrite the original cohort.
    let replacement = restart_node(
        &directory,
        nodes[0].name.clone(),
        nodes[0].member.clone(),
        &configured,
    )
    .await?;
    assert_eq!(
        replacement
            .disk
            .control()
            .expect("control")
            .manifest()?
            .revision,
        1
    );
    let report = degraded.repair(stream, 2).await?;
    assert_eq!(report.repaired, 1);
    assert_eq!(degraded.recover(stream, 0).await?, vec![first, second]);

    replacement.task.abort();
    let _ = replacement.task.await;
    nodes[1].stop().await;
    nodes[2].stop().await;
    Ok(())
}

#[tokio::test]
async fn insufficient_original_control_peers_fails_closed_then_recovers_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut nodes = start_cluster(&directory, &["control-a", "control-b", "control-c"]).await?;
    let configured = members(&nodes);
    let stream = "tenant-a/control-quorum";
    let first = record(stream, 1, 21);
    let second = record(stream, 2, 22);
    let initial = gateway(&directory, &configured, "initial/control.json").await?;
    assert_eq!(initial.append(first.clone()).await?, 2);
    wait_for_manifest(&nodes, 1).await?;

    // A stale local cache never substitutes for the fixed two-of-three
    // control-manifest quorum. With two original peers down, reads and new
    // writes are unavailable rather than risking a divergent route.
    nodes[1].stop().await;
    nodes[2].stop().await;
    let degraded = gateway(&directory, &configured, "stale/control.json").await?;
    assert_eq!(
        degraded.replicas_for_stream(stream).await,
        Err(ReplicaError::QuorumUnavailable)
    );
    assert_eq!(
        degraded.recover(stream, 0).await,
        Err(ReplicaError::QuorumUnavailable)
    );
    assert_eq!(
        degraded.append(second.clone()).await,
        Err(ReplicaError::QuorumUnavailable)
    );

    // Bringing back either original volume restores two agreeing control
    // peers. The committed record is recovered before accepting its successor.
    let replacement = restart_node(
        &directory,
        nodes[1].name.clone(),
        nodes[1].member.clone(),
        &configured,
    )
    .await?;
    assert_eq!(degraded.recover(stream, 0).await?, vec![first.clone()]);
    assert_eq!(
        degraded.recover_with_watermark(stream, 0, Some(1)).await?,
        vec![first.clone()]
    );
    assert_eq!(degraded.append(second.clone()).await?, 2);

    let second_replacement = restart_node(
        &directory,
        nodes[2].name.clone(),
        nodes[2].member.clone(),
        &configured,
    )
    .await?;
    let report = degraded.repair(stream, 2).await?;
    assert_eq!(report.repaired, 1);
    assert_eq!(degraded.recover(stream, 0).await?, vec![first, second]);

    replacement.task.abort();
    let _ = replacement.task.await;
    second_replacement.task.abort();
    let _ = second_replacement.task.await;
    nodes[0].stop().await;
    Ok(())
}

#[tokio::test]
async fn manifest_quorum_chooses_agreeing_peers_and_rejects_same_revision_split_brain()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut nodes = start_cluster(&directory, &["manifest-0", "manifest-1", "manifest-2"]).await?;
    let configured = members(&nodes);
    let stream = "tenant-a/manifest-quorum";
    let first = record(stream, 1, 31);
    let initial_gateway = gateway(&directory, &configured, "manifest-gateway/control.json").await?;
    assert_eq!(initial_gateway.append(first.clone()).await?, 2);
    wait_for_manifest(&nodes, 1).await?;
    let canonical = nodes[0].disk.control().expect("control").manifest()?;

    // A corrupt or stale third copy is ignored when two control peers agree
    // on exactly the same revision/content. Recovery still returns the data.
    let mut divergent = canonical.clone();
    divergent.operation_id = "divergent-control-copy".to_owned();
    nodes[2]
        .disk
        .control()
        .expect("control")
        .adopt_manifest(&divergent, true)?;
    let stateless = gateway(&directory, &configured, "fresh-coordinator/control.json").await?;
    assert_eq!(stateless.recover(stream, 0).await?, vec![first.clone()]);

    // Two different same-revision values have no quorum. The previously
    // cached canonical copy is deliberately not used as an authority.
    let mut divergent_again = canonical.clone();
    divergent_again.operation_id = "second-divergent-control-copy".to_owned();
    nodes[1]
        .disk
        .control()
        .expect("control")
        .adopt_manifest(&divergent_again, true)?;
    assert_eq!(
        stateless.replicas_for_stream(stream).await,
        Err(ReplicaError::QuorumUnavailable)
    );
    assert_eq!(
        stateless.recover(stream, 0).await,
        Err(ReplicaError::QuorumUnavailable)
    );

    // Repairing both durable copies to the canonical value restores the
    // quorum; no record is discarded or re-routed.
    for node in &nodes[1..] {
        node.disk
            .control()
            .expect("control")
            .adopt_manifest(&canonical, true)?;
    }
    assert_eq!(stateless.recover(stream, 0).await?, vec![first]);

    stop_all(&mut nodes).await;
    Ok(())
}
