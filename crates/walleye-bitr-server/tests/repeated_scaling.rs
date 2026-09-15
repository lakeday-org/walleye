//! Repeated direct-replica scale tests.
//!
//! These tests exercise immutable cohort placement through several scale-out
//! and scale-in operations. Every acknowledged record is checked again after
//! control-coordinator and storage-process restart.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use object_store::memory::InMemory;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use walleye_bitr::{EncryptedRecord, ReplicaError};
use walleye_bitr_server::{
    CohortStatus, DiskReplica, MemberStatus, MembershipSnapshot, OpaqueArchive, ReplicaGateway,
    ReplicaNode, StreamSegment, node_router,
};

const INTERNAL_TOKEN: &str = "repeated-scaling-internal-token";

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

fn root() -> String {
    base64::engine::general_purpose::STANDARD.encode([109_u8; 32])
}

fn record(stream: &str, lsn: u64) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": 1,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![(lsn as u8).wrapping_add(17); 24],
        "ciphertext": vec![(lsn as u8).wrapping_add(31); 12],
        "authentication": vec![(lsn as u8).wrapping_add(47); 32],
    }))
    .expect("record")
}

fn ring_score(stream: &str, cohort_id: u64) -> u128 {
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

fn winner(stream: &str, last_cohort: u64) -> u64 {
    (0..=last_cohort)
        .max_by(|left, right| {
            ring_score(stream, *left)
                .cmp(&ring_score(stream, *right))
                .then_with(|| left.cmp(right))
        })
        .expect("at least one cohort")
}

fn repeated_streams(count: usize) -> Vec<String> {
    let mut streams = Vec::with_capacity(count);
    for index in 0..100_000_u64 {
        let stream = format!("tenant-a/repeated-scale-{index}");
        if (0..=3).all(|cohort| winner(&stream, cohort) == cohort) {
            streams.push(stream);
            if streams.len() == count {
                return streams;
            }
        }
    }
    panic!("deterministic rendezvous search did not find {count} streams");
}

fn archive() -> Result<Arc<OpaqueArchive>, Box<dyn std::error::Error>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(InMemory::new()),
        "repeated-scaling",
        64,
    )?))
}

async fn start_group(
    directory: &tempfile::TempDir,
    names: &[&str],
    initial_members: &[ReplicaNode],
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let mut bound = Vec::with_capacity(names.len());
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?));
        bound.push((*name, member, listener));
    }
    let bootstrap_members = if initial_members.is_empty() {
        bound
            .iter()
            .map(|(_, member, _)| member.clone())
            .collect::<Vec<_>>()
    } else {
        initial_members.to_vec()
    };
    let mut nodes = Vec::with_capacity(bound.len());
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(walleye_bitr_server::DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &bootstrap_members,
        )?);
        let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, disk, task });
    }
    Ok(nodes)
}

async fn stop_node(node: RunningNode) {
    node.task.abort();
    let _ = node.task.await;
}

async fn stop_nodes(nodes: Vec<RunningNode>) {
    for node in nodes {
        stop_node(node).await;
    }
}

/// A bound listener is not necessarily accepting requests when `spawn`
/// returns.  Membership joins are authenticated control-plane operations, so
/// wait for the same status endpoint the gateway uses before attempting the
/// first CAS.  This keeps the test from turning a short startup window into a
/// false data-loss/availability failure.
async fn wait_for_storage_ready(nodes: &[RunningNode]) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    for node in nodes {
        let url = format!("{}/internal/v1/status", node.member.url);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let ready = client
                    .get(&url)
                    .header("x-lakeday-replica-internal-token", INTERNAL_TOKEN)
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success());
                if ready {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| format!("storage node {} did not become ready", node.member.id))?;
    }
    Ok(())
}

async fn restart_node(
    directory: &tempfile::TempDir,
    node: RunningNode,
    initial_members: &[ReplicaNode],
) -> Result<RunningNode, Box<dyn std::error::Error>> {
    let member = node.member;
    node.task.abort();
    let _ = node.task.await;
    let address = member
        .url
        .strip_prefix("http://")
        .ok_or("node URL scheme")?
        .parse::<SocketAddr>()?;
    let listener = loop {
        match TcpListener::bind(address).await {
            Ok(listener) => break listener,
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let data = directory.path().join(&member.id);
    let disk = Arc::new(walleye_bitr_server::DiskReplica::open_with_control(
        data.join("replica.log"),
        &member.id,
        "hot",
        &data,
        data.join("control.json"),
        initial_members,
    )?);
    let app = node_router(Arc::clone(&disk), &root(), Some(INTERNAL_TOKEN))?;
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RunningNode { member, disk, task })
}

async fn add_cohort(
    coordinator: &ReplicaGateway,
    directory: &tempfile::TempDir,
    _initial_members: &[ReplicaNode],
    cohort_id: u64,
) -> Result<Vec<RunningNode>, Box<dyn std::error::Error>> {
    let names = match cohort_id {
        1 => ["cohort-1-0", "cohort-1-1", "cohort-1-2"],
        2 => ["cohort-2-0", "cohort-2-1", "cohort-2-2"],
        3 => ["cohort-3-0", "cohort-3-1", "cohort-3-2"],
        _ => return Err("test supports cohorts one through three".into()),
    };
    // Seed replacement volumes from the current authoritative membership,
    // not the bootstrap cohort. Preserve cohort order: `DurableControl::open`
    // derives the initial cohort id from this order, and BTreeMap member order
    // would otherwise silently assign the old cohorts to different ids.
    let current = coordinator.membership().await?;
    let members_by_id = current
        .members
        .iter()
        .filter(|member| member.status != MemberStatus::Removed)
        .map(|member| (member.id.as_str(), member))
        .collect::<BTreeMap<_, _>>();
    let mut seeded_ids = BTreeSet::new();
    let mut bootstrap_members = current
        .cohorts
        .iter()
        .flat_map(|cohort| cohort.members.iter())
        .filter_map(|id| members_by_id.get(id.as_str()))
        .map(|member| {
            seeded_ids.insert(member.id.clone());
            ReplicaNode::new(member.id.clone(), member.url.clone())
        })
        .collect::<Vec<_>>();
    bootstrap_members.extend(
        current
            .members
            .iter()
            .filter(|member| {
                member.status != MemberStatus::Removed && !seeded_ids.contains(&member.id)
            })
            .map(|member| ReplicaNode::new(member.id.clone(), member.url.clone())),
    );
    let nodes = start_group(directory, &names, &bootstrap_members)
        .await
        .map_err(|error| format!("start cohort {cohort_id}: {error}"))?;
    wait_for_storage_ready(&nodes).await?;
    for (index, node) in nodes.iter().enumerate() {
        let operation_id = format!("join-cohort-{cohort_id}-{index}");
        let mut joined = false;
        for attempt in 0..100 {
            match coordinator
                .join_member_in_cohort(&operation_id, node.member.clone(), Some(cohort_id))
                .await
            {
                Ok(_) => {
                    joined = true;
                    break;
                }
                Err(ReplicaError::NodeUnavailable) if attempt < 99 => {
                    // A membership CAS is durable and idempotent before its
                    // fan-out completes. Retry the same operation id while
                    // lagging members catch up; never issue a new operation,
                    // which could create a second epoch or split the cohort.
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => {
                    return Err(format!(
                        "join cohort {cohort_id} member {index} after {attempt} retries: {error:?}"
                    )
                    .into());
                }
            }
        }
        if !joined {
            return Err(format!(
                "join cohort {cohort_id} member {index} did not converge after 100 retries"
            )
            .into());
        }
    }
    coordinator
        .activate_cohort_online(&format!("activate-cohort-{cohort_id}"), cohort_id)
        .await
        .map_err(|error| format!("activate cohort {cohort_id}: {error:?}"))?;
    Ok(nodes)
}

fn cohort_members(snapshot: &MembershipSnapshot, cohort_id: u64) -> BTreeSet<String> {
    snapshot
        .cohorts
        .iter()
        .find(|cohort| cohort.id == cohort_id)
        .expect("cohort")
        .members
        .iter()
        .cloned()
        .collect()
}

fn segment_members(segment: &StreamSegment) -> BTreeSet<String> {
    segment.member_ids.iter().cloned().collect()
}

async fn retire_cohort(
    coordinator: &ReplicaGateway,
    cohort_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = coordinator.membership().await?;
    let member_ids = snapshot
        .cohorts
        .iter()
        .find(|cohort| cohort.id == cohort_id)
        .ok_or("cohort missing")?
        .members
        .clone();
    let proof = coordinator.cohort_archive_proof(cohort_id).await?;
    for member_id in member_ids {
        coordinator
            .drain_member_with_archive_proof(
                &format!("drain-cohort-{cohort_id}-{member_id}"),
                &member_id,
                Some(&proof),
            )
            .await?;
        coordinator
            .remove_member_with_archive_proof(
                &format!("remove-cohort-{cohort_id}-{member_id}"),
                &member_id,
                Some(&proof),
            )
            .await?;
    }
    let final_snapshot = coordinator.membership().await?;
    assert_eq!(
        final_snapshot
            .cohorts
            .iter()
            .find(|cohort| cohort.id == cohort_id)
            .map(|cohort| cohort.status),
        Some(CohortStatus::Retired)
    );
    Ok(())
}

#[tokio::test]
async fn repeated_group_scaling_preserves_segments_and_acknowledged_records_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let streams = repeated_streams(12);
    let old_nodes = start_group(&directory, &["old-0", "old-1", "old-2"], &[]).await?;
    let initial_members = old_nodes
        .iter()
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let control_path = directory.path().join("control.json");
    let archive = archive()?;
    let coordinator = ReplicaGateway::new_direct(
        initial_members.clone(),
        2,
        &root(),
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?;
    let mut expected = BTreeMap::<String, Vec<EncryptedRecord>>::new();

    // Bootstrap one immutable range in cohort zero for every stream.
    for stream in &streams {
        let event = record(stream, 1);
        assert_eq!(coordinator.append(event.clone()).await?, 2);
        expected.entry(stream.clone()).or_default().push(event);
    }

    let cohort_one = add_cohort(&coordinator, &directory, &initial_members, 1)
        .await
        .map_err(|error| format!("add cohort one: {error}"))?;
    for stream in &streams {
        let event = record(stream, 2);
        assert_eq!(coordinator.append(event.clone()).await?, 2);
        expected.get_mut(stream).expect("stream").push(event);
    }

    let cohort_two = add_cohort(&coordinator, &directory, &initial_members, 2)
        .await
        .map_err(|error| format!("add cohort two: {error}"))?;
    for stream in &streams {
        let event = record(stream, 3);
        assert_eq!(coordinator.append(event.clone()).await?, 2);
        expected.get_mut(stream).expect("stream").push(event);
    }

    let cohort_three = add_cohort(&coordinator, &directory, &initial_members, 3)
        .await
        .map_err(|error| format!("add cohort three: {error}"))?;
    for stream in &streams {
        let event = record(stream, 4);
        assert_eq!(coordinator.append(event.clone()).await?, 2);
        expected.get_mut(stream).expect("stream").push(event);
    }

    // The deterministic ring must have produced exactly one immutable range
    // per cohort. In particular, no later scale-out may rewrite ownership of
    // an already acknowledged LSN.
    let snapshot = coordinator.membership().await?;
    for stream in &streams {
        let segments = snapshot
            .stream_segments
            .get(stream)
            .ok_or("stream route missing")?;
        assert_eq!(segments.len(), 4, "stream={stream}");
        for (index, segment) in segments.iter().enumerate() {
            assert_eq!(segment.cohort_id, index as u64, "stream={stream}");
            assert_eq!(
                segment.end_lsn,
                (index < 3).then_some((index + 1) as u64),
                "stream={stream} cohort={index}"
            );
            assert_eq!(
                segment_members(segment),
                cohort_members(&snapshot, index as u64),
                "stream={stream} cohort={index}"
            );
            assert!(!segment.member_hash.is_empty());
            let replicas = coordinator
                .replicas_for_record(&expected[stream][index])
                .await?;
            assert_eq!(
                replicas
                    .into_iter()
                    .map(|node| node.id)
                    .collect::<BTreeSet<_>>(),
                segment_members(segment),
                "stream={stream} lsn={} ownership",
                index + 1
            );
        }
    }

    // Archive the old immutable ranges before group removal. The archive is
    // the durable source for those records after their physical cohorts are
    // gone; the manifest proof below prevents draining an open range.
    for node in cohort_one.iter().chain(&cohort_two) {
        coordinator.archive_local_commits(&node.disk).await?;
    }
    assert!(coordinator.cohort_archive_proof(1).await.is_ok());
    assert!(coordinator.cohort_archive_proof(2).await.is_ok());

    // Cohort zero is retained as the fixed control-manifest quorum. Cohorts
    // one and two are removed as complete groups, one member at a time under
    // the same archive proof, while cohort three remains the live data tail.
    retire_cohort(&coordinator, 1)
        .await
        .map_err(|error| format!("retire cohort one: {error}"))?;
    retire_cohort(&coordinator, 2)
        .await
        .map_err(|error| format!("retire cohort two: {error}"))?;
    let after_scale_in = coordinator.membership().await?;
    assert_eq!(
        after_scale_in
            .cohorts
            .iter()
            .filter(|cohort| cohort.status == CohortStatus::Active)
            .map(|cohort| cohort.id)
            .collect::<Vec<_>>(),
        vec![0, 3]
    );

    // Writes continue on the surviving cohort after repeated scale-in.
    for stream in &streams {
        let event = record(stream, 5);
        let acknowledgements = coordinator
            .append(event.clone())
            .await
            .map_err(|error| format!("append lsn 5 stream={stream}: {error:?}"))?;
        assert_eq!(acknowledgements, 2);
        expected.get_mut(stream).expect("stream").push(event);
    }

    let active_ids = coordinator
        .membership()
        .await?
        .members
        .into_iter()
        .filter(|member| member.status == walleye_bitr_server::MemberStatus::Active)
        .map(|member| member.id)
        .collect::<BTreeSet<_>>();
    let all_nodes = old_nodes
        .into_iter()
        .chain(cohort_one)
        .chain(cohort_two)
        .chain(cohort_three)
        .collect::<Vec<_>>();
    let active_members = all_nodes
        .iter()
        .filter(|node| active_ids.contains(&node.member.id))
        .map(|node| node.member.clone())
        .collect::<Vec<_>>();
    let mut restarted_nodes = Vec::new();
    for node in all_nodes {
        if active_ids.contains(&node.member.id) {
            restarted_nodes.push(restart_node(&directory, node, &active_members).await?);
        } else {
            stop_node(node).await;
        }
    }

    // Reopen a stateless coordinator and all surviving storage processes from
    // their durable control/log files. Every acknowledged LSN must be present
    // exactly once, including the archived prefixes from removed cohorts.
    let restarted = ReplicaGateway::new_direct(
        active_members,
        2,
        &root(),
        INTERNAL_TOKEN,
        &control_path,
        Arc::clone(&archive),
    )?;
    for (stream, records) in &expected {
        assert_eq!(
            restarted.recover(stream, 0).await?,
            *records,
            "stream={stream}"
        );
    }

    stop_nodes(restarted_nodes).await;
    Ok(())
}
