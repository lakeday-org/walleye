//! Cross-feature durability tests for archive segments, compaction, and
//! direct-mode cohort changes.
//!
//! These tests deliberately keep a sizeable immutable prefix in the archive
//! while the hot writer changes cohorts.  That is the boundary where a
//! seemingly healthy implementation can accidentally lose either the cold
//! prefix or the first record on the new cohort.

use std::sync::Arc;

use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use walleye_bitr::{EncryptedRecord, Replica};
use walleye_bitr_server::{
    CohortStatus, DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router,
};

const INTERNAL_TOKEN: &str = "archive-scaling-internal-token";
const ROOT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

fn record(stream: &str, lsn: u64, epoch: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 64],
        "authentication": vec![marker; 32],
    }))
    .expect("archive scaling record")
}

async fn initial_group(
    directory: &tempfile::TempDir,
    names: &[&str],
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        bound.push((
            *name,
            ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?)),
            listener,
        ));
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
        let app = node_router(Arc::clone(&disk), ROOT, Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }
    Ok(nodes)
}

async fn joining_group(
    directory: &tempfile::TempDir,
    names: &[&str],
    initial_members: &[ReplicaNode],
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        bound.push((
            *name,
            ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?)),
            listener,
        ));
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
        let app = node_router(Arc::clone(&disk), ROOT, Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }
    Ok(nodes)
}

fn members(nodes: &[&RunningNode]) -> Vec<ReplicaNode> {
    nodes.iter().map(|node| node.member.clone()).collect()
}

fn archive(
    store: Arc<dyn ObjectStore>,
    max_records_per_segment: usize,
) -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        store,
        "replica",
        max_records_per_segment,
    )?))
}

/// This is the rendezvous domain used by the direct gateway. Keeping the
/// helper local lets the test choose a stream that is guaranteed to move when
/// cohort one is activated, instead of making a probabilistic assertion.
fn cohort_score(stream: &str, cohort_id: u64) -> u128 {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/cohort-ring/v1/rendezvous\0");
    digest.update(stream.as_bytes());
    digest.update([0]);
    digest.update(cohort_id.to_le_bytes());
    let digest = digest.finalize();
    let mut score = [0_u8; 16];
    score.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(score)
}

fn stream_for_cutover() -> String {
    (0..10_000_u64)
        .map(|index| format!("tenant-a/archive-cutover-{index}"))
        .find(|stream| cohort_score(stream, 1) > cohort_score(stream, 0))
        .expect("a stream must select the joining cohort")
}

async fn stop_nodes(nodes: Vec<RunningNode>) {
    for node in nodes {
        node.task.abort();
    }
}

/// A large multi-segment archive remains the source of truth while the hot
/// writer cuts from cohort zero to cohort one.  Both cohorts compact their
/// immutable portions, then a fresh gateway recovers the complete history.
#[tokio::test]
async fn large_archived_history_survives_compaction_cohort_cutover_and_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let old_nodes = initial_group(&directory, &["old-0", "old-1", "old-2"]).await?;
    let old_members = members(&[&old_nodes[0], &old_nodes[1], &old_nodes[2]]);
    let new_nodes = joining_group(&directory, &["new-0", "new-1", "new-2"], &old_members).await?;
    let all_members = old_nodes
        .iter()
        .chain(new_nodes.iter())
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = archive(Arc::clone(&store), 3)?;
    let control_path = directory.path().join("gateway-control.json");
    let gateway = ReplicaGateway::new_direct(
        old_members.clone(),
        2,
        ROOT,
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?;
    let stream = stream_for_cutover();
    let prefix = (1..=30_u64)
        .map(|lsn| record(&stream, lsn, 11, lsn as u8))
        .collect::<Vec<_>>();
    assert_eq!(gateway.append_many(prefix.clone()).await?, 2);

    // One archive pass creates ten immutable segments. Every old member then
    // proves the same cold prefix before replacing its hot log with a trim
    // fence; no member is allowed to compact from a local guess.
    assert_eq!(gateway.archive_local_commits(&old_nodes[0].disk).await?, 30);
    gateway.archive_local_commits(&old_nodes[1].disk).await?;
    gateway.archive_local_commits(&old_nodes[2].disk).await?;
    assert_eq!(archive.archived_lsn(&stream).await?, 30);
    let segments = store
        .list(Some(&object_store::path::Path::from("replica")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        segments
            .iter()
            .filter(|meta| meta.location.as_ref().contains("/segments/"))
            .count(),
        10,
        "history must cross the bounded segment size"
    );
    for node in &old_nodes {
        let snapshot = node.disk.snapshot();
        assert!(snapshot.records.is_empty(), "member={}", node.member.id);
        assert_eq!(
            snapshot
                .trimmed
                .iter()
                .find(|prefix| prefix.stream == stream)
                .map(|prefix| prefix.archived_lsn),
            Some(30)
        );
    }

    for (index, node) in new_nodes.iter().enumerate() {
        gateway
            .join_member_in_cohort(&format!("join-new-{index}"), node.member.clone(), Some(1))
            .await?;
    }
    // Activation only makes the prepared cohort eligible for new streams.
    // Existing ranges stay immutable until the explicit online handoff has
    // fenced the old tail, archived its certified prefix, and published the
    // successor route.
    let joined = gateway.activate_cohort_online("activate-new", 1).await?;
    assert_eq!(
        joined
            .cohorts
            .iter()
            .find(|cohort| cohort.id == 1)
            .map(|cohort| cohort.status),
        Some(CohortStatus::Active)
    );

    // The chosen stream has a new-cohort winner. Its first post-cutover record
    // is accepted by the new members through the manifest predecessor proof;
    // the old cohort remains owner of the finite [1, 30] range.
    let first_tail = record(&stream, 31, 11, 131);
    assert_eq!(gateway.append(first_tail.clone()).await?, 2);
    let route = gateway
        .membership()
        .await?
        .stream_segments
        .get(&stream)
        .cloned()
        .ok_or("cutover route")?;
    assert_eq!(route.len(), 2);
    assert_eq!(route[0].end_lsn, Some(30));
    assert_eq!(route[0].cohort_id, 0);
    assert_eq!(route[1].start_lsn, 31);
    assert_eq!(route[1].cohort_id, 1);
    for node in &old_nodes {
        assert!(node.disk.records(&stream).await.is_empty());
    }
    for node in &new_nodes {
        assert_eq!(node.disk.records(&stream).await, vec![first_tail.clone()]);
    }

    let tail = (32..=37_u64)
        .map(|lsn| record(&stream, lsn, 11, lsn as u8))
        .collect::<Vec<_>>();
    assert_eq!(gateway.append_many(tail.clone()).await?, 2);
    let mut expected = prefix;
    expected.push(first_tail);
    expected.extend(tail);
    assert_eq!(archive.archived_lsn(&stream).await?, 30);

    // Archive and compact the new cohort as well.  The old cohort's finite
    // range and the new cohort's open range now have one shared cold history.
    assert_eq!(gateway.archive_local_commits(&new_nodes[0].disk).await?, 7);
    gateway.archive_local_commits(&new_nodes[1].disk).await?;
    gateway.archive_local_commits(&new_nodes[2].disk).await?;
    assert_eq!(archive.archived_lsn(&stream).await?, 37);
    for node in &new_nodes {
        let snapshot = node.disk.snapshot();
        assert!(snapshot.records.is_empty(), "member={}", node.member.id);
        assert_eq!(
            snapshot
                .trimmed
                .iter()
                .find(|prefix| prefix.stream == stream)
                .map(|prefix| prefix.archived_lsn),
            Some(37)
        );
    }

    // The coordinator is disposable. Reopening it from the durable control
    // document must recover the immutable archive plus the compacted tail
    // without using any process-local writer cache.
    let restarted = ReplicaGateway::new_direct(
        all_members,
        2,
        ROOT,
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?;
    assert_eq!(restarted.recover(&stream, 0).await?, expected);

    stop_nodes(old_nodes).await;
    stop_nodes(new_nodes).await;
    Ok(())
}

/// Missing or altered immutable segments must stop both recovery and hot-log
/// compaction.  The untouched hot member remains readable, so a transient
/// object-store failure cannot turn into data loss.
#[tokio::test]
async fn missing_or_corrupt_archive_segments_fail_closed_and_retain_hot_data()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let nodes = initial_group(&directory, &["node-0", "node-1", "node-2"]).await?;
    let configured = members(&[&nodes[0], &nodes[1], &nodes[2]]);
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = archive(Arc::clone(&store), 2)?;
    let gateway = ReplicaGateway::new(configured, 2, ROOT, INTERNAL_TOKEN, Arc::clone(&archive))?;
    let stream = "tenant-a/archive-failure";
    let records = (1..=12_u64)
        .map(|lsn| record(stream, lsn, 5, lsn as u8))
        .collect::<Vec<_>>();
    assert_eq!(gateway.append_many(records.clone()).await?, 2);
    assert_eq!(gateway.archive_local_commits(&nodes[0].disk).await?, 12);
    assert_eq!(archive.archived_lsn(stream).await?, 12);

    let segment = store
        .list(Some(&object_store::path::Path::from("replica")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|meta| meta.location.as_ref().contains("/segments/"))
        .ok_or("archive segment")?;
    let segment_bytes = store.get(&segment.location).await?.bytes().await?;

    store.delete(&segment.location).await?;
    let missing = archive
        .recover(stream, 0)
        .await
        .expect_err("missing segment must not be skipped");
    assert!(
        missing
            .to_string()
            .contains("archive object-store operation failed"),
        "{missing}"
    );
    assert!(gateway.archive_local_commits(&nodes[1].disk).await.is_err());
    assert_eq!(nodes[1].disk.records(stream).await, records);

    // Restore the exact object, then corrupt its bytes without changing the
    // head's content-addressed name. Checksum verification must catch this
    // before compaction is permitted.
    store
        .put(&segment.location, segment_bytes.clone().into())
        .await?;
    store
        .put(
            &segment.location,
            bytes::Bytes::from_static(b"corrupt").into(),
        )
        .await?;
    let corrupt = archive
        .recover(stream, 0)
        .await
        .expect_err("corrupt segment must not be accepted");
    assert!(corrupt.to_string().contains("checksum mismatch"));
    assert!(gateway.archive_local_commits(&nodes[2].disk).await.is_err());
    assert_eq!(nodes[2].disk.records(stream).await, records);

    stop_nodes(nodes).await;
    Ok(())
}
