//! Fault-injection coverage for direct-cohort lifecycle transitions.
//!
//! These tests deliberately exercise the durable operation boundary rather
//! than calling private implementation helpers.  A lifecycle CAS may be
//! committed locally before its broadcast reaches every member; every test
//! below kills a member at that point and proves that retrying the same
//! operation id resumes the exact committed state without losing an
//! acknowledged log record.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use walleye_bitr::{ENCRYPTED_RECORD_CONTENT_TYPE, EncryptedRecord, Replica, ReplicaError};
use walleye_bitr_server::{
    CohortStatus, DiskReplica, DurableMemberIdentity, INTERNAL_AUTH_HEADER,
    MAINTENANCE_AUTH_HEADER, MemberStatus, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router,
};

const INTERNAL_TOKEN: &str = "cohort-lifecycle-internal-token";
const ROOT_KEY: &str = "bW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW1tbW0=";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct RunningNode {
    member: ReplicaNode,
    disk: Arc<DiskReplica>,
    task: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
}

fn archive() -> TestResult<Arc<OpaqueArchive>> {
    Ok(Arc::new(OpaqueArchive::new(
        Arc::new(object_store::memory::InMemory::new()),
        "cohort-lifecycle",
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
    .expect("valid encrypted record fixture")
}

/// Return the same rendezvous winner used by the production ring. Keeping
/// this calculation in the fixture lets it choose a stream whose immutable
/// ranges exercise a particular cohort transition without guessing hashes.
fn cohort_winner(stream: &str, cohort_ids: &[u64]) -> u64 {
    fn score(stream: &str, cohort_id: u64) -> u128 {
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

    *cohort_ids
        .iter()
        .max_by_key(|cohort_id| score(stream, **cohort_id))
        .expect("rendezvous requires one active cohort")
}

async fn start_group(
    directory: &tempfile::TempDir,
    names: &[&str],
    initial_members: Option<&[ReplicaNode]>,
) -> TestResult<Vec<RunningNode>> {
    let mut bound = Vec::with_capacity(names.len());
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let member = ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?));
        bound.push((*name, member, listener));
    }
    let seed = initial_members
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
            &seed,
        )?);
        let app = node_router(Arc::clone(&disk), ROOT_KEY, Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode {
            member,
            disk,
            task: Some(task),
        });
    }
    for node in &nodes {
        wait_for_storage_ready(node).await?;
    }
    Ok(nodes)
}

async fn stop_node(node: &mut RunningNode) {
    if let Some(task) = node.task.take() {
        task.abort();
        let _ = task.await;
    }
    // Let the OS release the listener before a same-address restart.  The
    // bind helper below still retries, since this is a scheduling boundary.
    tokio::task::yield_now().await;
}

/// A bound listener is not necessarily accepting requests when its task is
/// spawned.  Lifecycle operations are authenticated control-plane requests,
/// so wait for the same authenticated status endpoint the gateway uses before
/// attempting a join, retry, or route cutover.
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
    let address = node
        .member
        .url
        .strip_prefix("http://")
        .ok_or("node URL scheme")?
        .parse::<std::net::SocketAddr>()?;
    let listener = loop {
        match TcpListener::bind(address).await {
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

async fn direct_gateway(
    nodes: &[RunningNode],
    directory: &tempfile::TempDir,
    archive: Arc<OpaqueArchive>,
) -> TestResult<ReplicaGateway> {
    Ok(ReplicaGateway::new_direct(
        members(nodes),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join("gateway-control.json"),
        archive,
    )?)
}

async fn provision_joining_cohort(
    directory: &tempfile::TempDir,
    archive: Arc<OpaqueArchive>,
) -> TestResult<(Vec<RunningNode>, ReplicaGateway)> {
    let mut old = start_group(directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let mut new = start_group(directory, &["new-0", "new-1", "new-2"], Some(&old_members)).await?;
    let coordinator = direct_gateway(&old, directory, archive).await?;
    for (index, node) in new.iter().enumerate() {
        coordinator
            .join_member_in_cohort(
                &format!("join-cohort-1-{index}"),
                node.member.clone(),
                Some(1),
            )
            .await?;
    }
    old.append(&mut new);
    Ok((old, coordinator))
}

async fn wait_for_records(
    gateway: &ReplicaGateway,
    stream: &str,
    expected: &[EncryptedRecord],
) -> TestResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match gateway.recover(stream, 0).await {
                Ok(records) if records == expected => break,
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

#[tokio::test]
async fn fenced_vertical_replacement_copies_records_and_rewrites_routes_atomically() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let mut replacement = start_group(&directory, &["replacement-0"], Some(&old_members)).await?;
    let gateway = direct_gateway(&old, &directory, archive).await?;
    let stream = "tenant-a/vertical-replacement";
    let records = (1..=4)
        .map(|lsn| record(stream, lsn, lsn as u8))
        .collect::<Vec<_>>();
    for value in &records {
        assert!(gateway.append(value.clone()).await? >= 2);
    }
    let token = gateway.begin_maintenance_for("replace-0").await?;
    // Simulate an interrupted append that reached only one member. Use the
    // authenticated maintenance append surface so this fixture carries the
    // same durable fence as the replacement operation while deliberately
    // omitting its commit marker. Replacement must ignore the one-copy tail
    // rather than deadlock the cohort forever.
    let minority_tail = record(stream, 5, 99);
    let response = reqwest::Client::new()
        .post(format!(
            "{}/internal/v1/maintenance/append",
            old[0].member.url
        ))
        .header(INTERNAL_AUTH_HEADER, INTERNAL_TOKEN)
        .header(MAINTENANCE_AUTH_HEADER, &token)
        .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
        .body(minority_tail.encode_binary()?)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);

    let snapshot = gateway
        .replace_member_with_maintenance(
            "replace-0",
            "old-1",
            replacement[0].member.clone(),
            0,
            DurableMemberIdentity {
                name: "replacement-0".to_owned(),
                machine_id: "machine-replacement-0".to_owned(),
                volume_id: "volume-replacement-0".to_owned(),
                ordinal: 1,
                tier: "medium".to_owned(),
                max_append_bytes: 0,
            },
            &token,
        )
        .await?;
    assert!(snapshot.members.iter().all(|member| member.id != "old-1"));
    assert!(
        snapshot.members.iter().any(|member| {
            member.id == "replacement-0" && member.status == MemberStatus::Active
        })
    );
    assert_eq!(replacement[0].disk.records(stream).await, records);
    assert!(snapshot.stream_segments[stream].iter().all(|segment| {
        segment.member_ids.contains(&"replacement-0".to_owned())
            && !segment.member_ids.contains(&"old-1".to_owned())
    }));

    let retried = gateway
        .replace_member_with_maintenance(
            "replace-0",
            "old-1",
            replacement[0].member.clone(),
            0,
            DurableMemberIdentity {
                name: "replacement-0".to_owned(),
                machine_id: "machine-replacement-0".to_owned(),
                volume_id: "volume-replacement-0".to_owned(),
                ordinal: 1,
                tier: "medium".to_owned(),
                max_append_bytes: 0,
            },
            &token,
        )
        .await?;
    assert_eq!(retried, snapshot);
    gateway.end_maintenance(&token).await?;
    assert_eq!(gateway.recover(stream, 0).await?, records);

    stop_all(&mut old).await;
    stop_all(&mut replacement).await;
    Ok(())
}

#[tokio::test]
async fn join_and_activation_retry_after_broadcast_fault_are_lossless() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let mut new = start_group(&directory, &["new-0", "new-1", "new-2"], Some(&old_members)).await?;
    let gateway = Arc::new(direct_gateway(&old, &directory, Arc::clone(&archive)).await?);
    let stream = "tenant-a/continuous-lifecycle";
    let started = Arc::new(Notify::new());
    let writer_gateway = Arc::clone(&gateway);
    let writer_started = Arc::clone(&started);
    let writer = tokio::spawn(async move {
        let mut acknowledged = Vec::new();
        for lsn in 1..=24_u64 {
            let value = record(stream, lsn, lsn as u8);
            loop {
                match writer_gateway.append(value.clone()).await {
                    Ok(acknowledgements) => {
                        assert!(acknowledgements >= 2);
                        acknowledged.push(value.clone());
                        writer_started.notify_one();
                        break;
                    }
                    Err(
                        ReplicaError::QuorumUnavailable
                        | ReplicaError::NodeUnavailable
                        | ReplicaError::WriterFenced,
                    ) => {
                        tokio::time::sleep(Duration::from_millis(3)).await;
                    }
                    // A route CAS can race a lifecycle operation while the
                    // same gateway is reconstructing its stateless cache.
                    // Retry the exact payload; storage-level LSN fencing
                    // still rejects any conflicting value.
                    Err(ReplicaError::LsnConflict) => {
                        tokio::time::sleep(Duration::from_millis(3)).await;
                    }
                    Err(error) => {
                        return Err(format!("continuous append lsn={lsn} failed: {error:?}"));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(4)).await;
        }
        Ok::<_, String>(acknowledged)
    });
    started.notified().await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    // The membership document CAS is durable even though the broadcast is
    // interrupted. Restarting the missing member and retrying the same
    // operation id must resume, not allocate a second membership epoch.
    stop_node(&mut new[0]).await;
    assert!(
        gateway
            .join_member_in_cohort("join-fault-0", new[0].member.clone(), Some(1))
            .await
            .is_err()
    );
    resume_node(&mut new[0]).await?;
    let joined = gateway
        .join_member_in_cohort("join-fault-0", new[0].member.clone(), Some(1))
        .await?;
    assert_eq!(joined.membership_epoch, 2);
    assert_eq!(joined.cohorts[1].status, CohortStatus::Joining);

    gateway
        .join_member_in_cohort("join-1-1", new[1].member.clone(), Some(1))
        .await?;
    gateway
        .join_member_in_cohort("join-1-2", new[2].member.clone(), Some(1))
        .await?;

    // Activation follows the same local-CAS/remote-propagation boundary, but
    // this time the operation retains an explicit maintenance token so the
    // retry can complete the interrupted operation without reopening writes.
    let activation_token = gateway
        .begin_maintenance_for("activate-fault")
        .await
        .map_err(|error| format!("acquire activation fence: {error:?}"))?;
    stop_node(&mut new[1]).await;
    assert!(
        gateway
            .activate_cohort_with_maintenance("activate-fault", 1, &activation_token)
            .await
            .is_err()
    );
    resume_node(&mut new[1]).await?;
    let activated = gateway
        .activate_cohort_with_maintenance("activate-fault", 1, &activation_token)
        .await?;
    assert!(
        activated
            .cohorts
            .iter()
            .any(|cohort| cohort.id == 1 && cohort.status == CohortStatus::Active)
    );
    assert!(gateway.maintenance_token().await.is_none());

    let acknowledged = tokio::time::timeout(Duration::from_secs(10), writer)
        .await??
        .map_err(|error| error.to_owned())?;
    assert_eq!(acknowledged.len(), 24);
    wait_for_records(&gateway, stream, &acknowledged).await?;

    let restarted = direct_gateway(&old, &directory, archive).await?;
    assert_eq!(restarted.recover(stream, 0).await?, acknowledged);
    stop_all(&mut old).await;
    stop_all(&mut new).await;
    Ok(())
}

#[tokio::test]
async fn drain_and_remove_retry_after_fault_preserve_archived_history() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let (mut nodes, gateway) = provision_joining_cohort(&directory, Arc::clone(&archive))
        .await
        .map_err(|error| format!("provision cohort: {error}"))?;
    let old_ids = ["old-0", "old-1", "old-2"];
    let new_ids = BTreeSet::from(["new-0".to_owned(), "new-1".to_owned(), "new-2".to_owned()]);
    let mut newest = start_group(
        &directory,
        &["newest-0", "newest-1", "newest-2"],
        Some(&members(&nodes)),
    )
    .await?;
    for (index, node) in newest.iter().enumerate() {
        gateway
            .join_member_in_cohort(
                &format!("join-cohort-2-{index}"),
                node.member.clone(),
                Some(2),
            )
            .await
            .map_err(|error| format!("join cohort 2 member {}: {error:?}", node.member.id))?;
    }
    nodes.append(&mut newest);
    let newest_ids = BTreeSet::from([
        "newest-0".to_owned(),
        "newest-1".to_owned(),
        "newest-2".to_owned(),
    ]);
    let stream = (0..256)
        .map(|index| format!("tenant-a/drain-{index}"))
        .find(|candidate| {
            cohort_winner(candidate, &[0, 1]) == 1 && cohort_winner(candidate, &[0, 1, 2]) == 2
        })
        .ok_or("failed to choose a stream that moves through cohorts one and two")?;
    let first = record(&stream, 1, 201);
    assert_eq!(
        gateway
            .append(first.clone())
            .await
            .map_err(|error| format!("append first: {error:?}"))?,
        2
    );
    let before = gateway
        .replicas_for_record(&first)
        .await
        .map_err(|error| format!("route first: {error:?}"))?;
    assert!(
        before
            .iter()
            .all(|node| old_ids.contains(&node.id.as_str()))
    );

    gateway
        .activate_cohort_online("activate-cohort-1", 1)
        .await
        .map_err(|error| format!("activate cohort: {error:?}"))?;

    // The online activation explicitly hands this existing stream to cohort
    // one after fencing and sealing cohort zero. The next record therefore
    // proves the successor route at a real durable LSN.
    let second = record(&stream, 2, 202);
    assert_eq!(
        gateway
            .append(second.clone())
            .await
            .map_err(|error| format!("append second: {error:?}"))?,
        2
    );
    let after = gateway
        .replicas_for_record(&second)
        .await
        .map_err(|error| format!("route second: {error:?}"))?;
    assert!(after.iter().all(|node| new_ids.contains(&node.id)));
    gateway
        .activate_cohort_online("activate-cohort-2", 2)
        .await
        .map_err(|error| format!("activate cohort 2: {error:?}"))?;
    let third = record(&stream, 3, 203);
    assert_eq!(
        gateway
            .append(third.clone())
            .await
            .map_err(|error| format!("append third: {error:?}"))?,
        2
    );
    let after = gateway
        .replicas_for_record(&third)
        .await
        .map_err(|error| format!("route third: {error:?}"))?;
    assert!(after.iter().all(|node| newest_ids.contains(&node.id)));
    let expected = vec![first.clone(), second.clone(), third.clone()];
    wait_for_records(&gateway, &stream, &expected)
        .await
        .map_err(|error| format!("recover prefix: {error}"))?;

    // Publish the immutable cohort-one range and compact every local copy
    // before asking the lifecycle API for its archive proof. Cohort zero is
    // the fixed control quorum and remains active throughout this test.
    for node in nodes.iter().skip(3).take(3) {
        gateway
            .archive_local_commits(&node.disk)
            .await
            .map_err(|error| format!("archive {}: {error:?}", node.member.id))?;
    }
    let proof = gateway
        .cohort_archive_proof(1)
        .await
        .map_err(|error| format!("archive proof: {error:?}"))?;

    // Drain: stop a member after the durable fence is acquired. The local CAS
    // may commit, but the operation must not be reported complete until the
    // same operation id is retried after the member returns.
    let drain_token = gateway
        .begin_maintenance_for("drain-fault")
        .await
        .map_err(|error| format!("begin drain fence: {error:?}"))?;
    stop_node(&mut nodes[3]).await;
    assert!(
        gateway
            .drain_member_with_maintenance_and_archive_proof(
                "drain-fault",
                "new-0",
                &drain_token,
                Some(&proof),
            )
            .await
            .is_err()
    );
    resume_node(&mut nodes[3])
        .await
        .map_err(|error| format!("resume drain node: {error}"))?;
    let drained = gateway
        .drain_member_with_maintenance_and_archive_proof(
            "drain-fault",
            "new-0",
            &drain_token,
            Some(&proof),
        )
        .await
        .map_err(|error| format!("retry drain: {error:?}"))?;
    assert_eq!(
        drained
            .members
            .iter()
            .find(|member| member.id == "new-0")
            .map(|member| member.status),
        Some(walleye_bitr_server::MemberStatus::Draining)
    );
    gateway
        .end_maintenance(&drain_token)
        .await
        .map_err(|error| format!("release drain fence: {error:?}"))?;

    // Removal has the same retry contract and leaves a tombstone. The removed
    // disk is retained for historical inspection but is no longer an active
    // write target.
    let remove_token = gateway
        .begin_maintenance_for("remove-fault")
        .await
        .map_err(|error| format!("begin remove fence: {error:?}"))?;
    // The removed member is intentionally not part of the post-CAS
    // propagation set. Fault the retained active member instead: the
    // operation must remain incomplete until that peer returns and the same
    // idempotency key is retried.
    stop_node(&mut nodes[4]).await;
    let removal_attempt = gateway
        .remove_member_with_maintenance_and_archive_proof(
            "remove-fault",
            "new-0",
            &remove_token,
            Some(&proof),
        )
        .await;
    assert!(
        removal_attempt.is_err(),
        "removal unexpectedly succeeded: {removal_attempt:?}"
    );
    resume_node(&mut nodes[4])
        .await
        .map_err(|error| format!("resume remove peer: {error}"))?;
    let removed = gateway
        .remove_member_with_maintenance_and_archive_proof(
            "remove-fault",
            "new-0",
            &remove_token,
            Some(&proof),
        )
        .await
        .map_err(|error| format!("retry remove: {error:?}"))?;
    assert_eq!(
        removed
            .members
            .iter()
            .find(|member| member.id == "new-0")
            .map(|member| member.status),
        Some(walleye_bitr_server::MemberStatus::Removed)
    );
    gateway
        .end_maintenance(&remove_token)
        .await
        .map_err(|error| format!("release remove fence: {error:?}"))?;

    // Finish retirement of the remaining two cohort-one members while
    // retaining the archive proof. This verifies that the fixed control
    // quorum stays available and that the data cohort reaches Retired only
    // after all tombstones are durable.
    for id in ["new-1", "new-2"] {
        let drain_operation = format!("drain-{id}");
        gateway
            .drain_member_with_archive_proof(&drain_operation, id, Some(&proof))
            .await
            .map_err(|error| format!("drain {id}: {error:?}"))?;
        let remove_operation = format!("remove-{id}");
        gateway
            .remove_member_with_archive_proof(&remove_operation, id, Some(&proof))
            .await
            .map_err(|error| format!("remove {id}: {error:?}"))?;
    }
    let retired = gateway.membership().await?;
    assert!(
        retired
            .cohorts
            .iter()
            .any(|cohort| cohort.id == 1 && cohort.status == CohortStatus::Retired)
    );

    // Writes continue on the surviving cohort, and a fresh coordinator can
    // recover both the archived cohort-one range and the hot cohort-two tail.
    let live_stream = "tenant-a/post-retirement";
    let live = record(live_stream, 1, 204);
    assert_eq!(
        gateway
            .append(live.clone())
            .await
            .map_err(|error| format!("append post-retirement: {error:?}"))?,
        2
    );
    let expected_with_tail = expected;
    let restarted = direct_gateway(&nodes, &directory, archive)
        .await
        .map_err(|error| format!("restart gateway: {error:?}"))?;
    assert_eq!(
        restarted
            .recover(&stream, 0)
            .await
            .map_err(|error| format!("recover after retire: {error:?}"))?,
        expected_with_tail
    );
    assert_eq!(restarted.recover(live_stream, 0).await?, vec![live]);
    stop_all(&mut nodes).await;
    Ok(())
}

#[tokio::test]
async fn stale_membership_cas_and_idempotent_retry_never_regress_routes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut nodes = start_group(&directory, &["cas-0", "cas-1", "cas-2"], None)
        .await
        .map_err(|error| format!("start CAS group: {error}"))?;
    let gateway = direct_gateway(&nodes, &directory, archive)
        .await
        .map_err(|error| format!("create CAS gateway: {error}"))?;
    let first = record("tenant-a/cas", 1, 211);
    assert_eq!(
        gateway
            .append(first.clone())
            .await
            .map_err(|error| format!("append CAS first: {error:?}"))?,
        2
    );

    let before = gateway
        .membership()
        .await
        .map_err(|error| format!("read CAS membership: {error:?}"))?;
    let applied = gateway
        .membership_cas(
            before.membership_epoch,
            before.members.clone(),
            "cas-boundary",
        )
        .await
        .map_err(|error| format!("first membership CAS: {error:?}"))?;
    assert_eq!(
        applied.membership_epoch,
        before.membership_epoch.saturating_add(1)
    );
    assert_eq!(
        gateway
            .membership_cas(
                before.membership_epoch,
                before.members.clone(),
                "cas-boundary",
            )
            .await
            .map_err(|error| format!("idempotent membership CAS: {error:?}"))?,
        applied
    );
    assert_eq!(
        gateway
            .membership_cas(before.membership_epoch, before.members, "cas-stale-epoch",)
            .await,
        Err(ReplicaError::LsnConflict)
    );
    assert_eq!(
        gateway
            .membership()
            .await
            .map_err(|error| format!("read applied CAS membership: {error:?}"))?
            .membership_epoch,
        applied.membership_epoch
    );

    let second = record("tenant-a/cas", 2, 212);
    assert_eq!(
        gateway
            .append(second.clone())
            .await
            .map_err(|error| format!("append CAS second: {error:?}"))?,
        2
    );
    assert_eq!(
        gateway
            .recover("tenant-a/cas", 0)
            .await
            .map_err(|error| format!("recover CAS stream: {error:?}"))?,
        vec![first, second]
    );
    stop_all(&mut nodes).await;
    Ok(())
}

/// A quiet stream keeps its open tail on the cohort it was placed on, which
/// would pin that cohort in the cell forever. Under the maintenance fence the
/// gateway seals the tail at the certified durable LSN and opens the
/// successor on another active cohort; the cohort then has an archive proof
/// and can drain, and the stream's history stays contiguous across the seam.
#[tokio::test]
async fn fenced_cutover_seals_quiet_tails_so_the_cohort_can_drain() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let (mut nodes, gateway) = provision_joining_cohort(&directory, Arc::clone(&archive))
        .await
        .map_err(|error| format!("provision cohort: {error}"))?;
    let old_ids = ["old-0", "old-1", "old-2"];
    let new_ids = BTreeSet::from(["new-0".to_owned(), "new-1".to_owned(), "new-2".to_owned()]);
    gateway
        .activate_cohort("activate-cohort-1", 1)
        .await
        .map_err(|error| format!("activate cohort: {error:?}"))?;
    let stream = (0..256)
        .map(|index| format!("tenant-a/quiet-{index}"))
        .find(|candidate| cohort_winner(candidate, &[0, 1]) == 1)
        .ok_or("failed to choose a stream the ring places on cohort one")?;
    let first = record(&stream, 1, 211);
    let second = record(&stream, 2, 212);
    for entry in [&first, &second] {
        assert_eq!(
            gateway
                .append(entry.clone())
                .await
                .map_err(|error| format!("append {}: {error:?}", entry.lsn()))?,
            2
        );
    }
    let placed = gateway.replicas_for_record(&second).await?;
    assert!(placed.iter().all(|node| new_ids.contains(&node.id)));
    assert!(
        gateway.cohort_archive_proof(1).await.is_err(),
        "an open tail must block the archive proof"
    );

    let token = gateway.begin_maintenance_for("cutover-1").await?;
    let snapshot = gateway
        .cutover_cohort_with_maintenance("cutover-1", 1, &token)
        .await
        .map_err(|error| format!("cutover: {error:?}"))?;
    let segments = snapshot
        .stream_segments
        .get(&stream)
        .ok_or("cutover dropped the stream route")?
        .clone();
    assert_eq!(segments.len(), 2, "{segments:?}");
    assert_eq!(segments[0].cohort_id, 1);
    assert_eq!(segments[0].end_lsn, Some(2));
    assert_eq!(segments[1].cohort_id, 0);
    assert_eq!(segments[1].start_lsn, 3);
    assert_eq!(segments[1].end_lsn, None);
    let again = gateway
        .cutover_cohort_with_maintenance("cutover-1", 1, &token)
        .await
        .map_err(|error| format!("cutover retry: {error:?}"))?;
    assert_eq!(
        again.stream_segments.get(&stream),
        Some(&segments),
        "a retried cutover is idempotent"
    );
    gateway.end_maintenance(&token).await?;

    let third = record(&stream, 3, 213);
    assert_eq!(
        gateway
            .append(third.clone())
            .await
            .map_err(|error| format!("append after cutover: {error:?}"))?,
        2
    );
    let routed = gateway.replicas_for_record(&third).await?;
    assert!(
        routed
            .iter()
            .all(|node| old_ids.contains(&node.id.as_str())),
        "the successor range must live on the remaining cohort: {routed:?}"
    );
    wait_for_records(&gateway, &stream, &[first, second, third])
        .await
        .map_err(|error| format!("recover across the seam: {error}"))?;

    let proof = gateway
        .cohort_archive_proof(1)
        .await
        .map_err(|error| format!("archive proof after cutover: {error:?}"))?;
    let drain_token = gateway.begin_maintenance_for("drain-1").await?;
    // The proof only names the sealed ranges. Draining trims copies, so the
    // archive must cover every one of them first; nothing has been archived
    // yet in this test.
    let refused = gateway
        .drain_member_with_maintenance_and_archive_proof(
            "drain-1",
            "new-0",
            &drain_token,
            Some(&proof),
        )
        .await;
    assert!(
        matches!(refused, Err(ReplicaError::Protocol(ref reason)) if reason.contains("archived only through")),
        "a drain must wait for the archive: {refused:?}"
    );
    let archived = gateway
        .archive_cohort_with_maintenance("archive-1", 1, &drain_token)
        .await
        .map_err(|error| format!("archive cohort: {error:?}"))?;
    assert!(archived.complete, "{archived:?}");
    assert_eq!(archived.archived_records, 2);
    let again = gateway
        .archive_cohort_with_maintenance("archive-1", 1, &drain_token)
        .await
        .map_err(|error| format!("archive cohort retry: {error:?}"))?;
    assert!(again.complete && again.archived_records == 0, "{again:?}");
    let drained = gateway
        .drain_member_with_maintenance_and_archive_proof(
            "drain-1",
            "new-0",
            &drain_token,
            Some(&proof),
        )
        .await
        .map_err(|error| format!("drain: {error:?}"))?;
    assert_eq!(
        drained
            .members
            .iter()
            .find(|member| member.id == "new-0")
            .map(|member| member.status),
        Some(MemberStatus::Draining)
    );
    gateway.end_maintenance(&drain_token).await?;
    stop_all(&mut nodes).await;
    Ok(())
}

/// Quorum certification counts the members that hold a record, so a source
/// member that merely fails to answer would turn every committed record into
/// a minority tail and the replacement would be activated empty. The copy
/// must refuse until every retained source member answers; the same
/// operation id then completes losslessly.
#[tokio::test]
async fn replacement_refuses_to_copy_while_a_source_member_is_unreachable() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let mut replacement = start_group(&directory, &["replacement-0"], Some(&old_members)).await?;
    let gateway = direct_gateway(&old, &directory, archive).await?;
    let stream = "tenant-a/unreachable-source";
    let records = (1..=3)
        .map(|lsn| record(stream, lsn, lsn as u8))
        .collect::<Vec<_>>();
    for value in &records {
        assert!(gateway.append(value.clone()).await? >= 2);
    }
    let identity = || DurableMemberIdentity {
        name: "replacement-0".to_owned(),
        machine_id: "machine-replacement-0".to_owned(),
        volume_id: "volume-replacement-0".to_owned(),
        ordinal: 1,
        tier: "medium".to_owned(),
        max_append_bytes: 0,
    };

    let token = gateway.begin_maintenance_for("replace-1").await?;
    stop_node(&mut old[2]).await;
    let refused = gateway
        .replace_member_with_maintenance(
            "replace-1",
            "old-1",
            replacement[0].member.clone(),
            0,
            identity(),
            &token,
        )
        .await;
    assert!(
        matches!(refused, Err(ReplicaError::NodeUnavailable)),
        "a silent source must not be judged as holding nothing: {refused:?}"
    );
    assert!(
        replacement[0].disk.records(stream).await.is_empty(),
        "nothing is copied while a source is unreachable"
    );
    let membership = gateway.membership().await?;
    assert!(membership.members.iter().any(|member| member.id == "old-1"));

    resume_node(&mut old[2])
        .await
        .map_err(|error| format!("resume source: {error}"))?;
    let snapshot = gateway
        .replace_member_with_maintenance(
            "replace-1",
            "old-1",
            replacement[0].member.clone(),
            0,
            identity(),
            &token,
        )
        .await
        .map_err(|error| format!("replace after resume: {error:?}"))?;
    assert!(snapshot.members.iter().all(|member| member.id != "old-1"));
    assert_eq!(replacement[0].disk.records(stream).await, records);
    gateway.end_maintenance(&token).await?;
    assert_eq!(gateway.recover(stream, 0).await?, records);

    stop_all(&mut old).await;
    stop_all(&mut replacement).await;
    Ok(())
}

/// Once a prefix is archived and trimmed from the hot nodes, a member
/// replaced under the fence must inherit the trim checkpoint with the hot
/// records. Without it the replacement has no proof of the archived prefix
/// and rejects the exact successor LSN as out of order, which is how a
/// blue-green release turned a healthy stream into a 409 on staging.
#[tokio::test]
async fn replacement_inherits_archived_trim_checkpoints() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let mut replacement = start_group(&directory, &["replacement-0"], Some(&old_members)).await?;
    let gateway = direct_gateway(&old, &directory, Arc::clone(&archive)).await?;
    let stream = "tenant-a/archived-prefix";
    let records = (1..=3)
        .map(|lsn| record(stream, lsn, lsn as u8))
        .collect::<Vec<_>>();
    for value in &records {
        assert!(gateway.append(value.clone()).await? >= 2);
    }
    for node in &old {
        gateway
            .archive_local_commits(&node.disk)
            .await
            .map_err(|error| format!("archive {}: {error:?}", node.member.id))?;
    }
    for node in &old {
        assert!(
            node.disk.records(stream).await.is_empty(),
            "{} keeps hot copies of an archived prefix",
            node.member.id
        );
    }

    let token = gateway.begin_maintenance_for("replace-archived").await?;
    let snapshot = gateway
        .replace_member_with_maintenance(
            "replace-archived",
            "old-1",
            replacement[0].member.clone(),
            0,
            DurableMemberIdentity {
                name: "replacement-0".to_owned(),
                machine_id: "machine-replacement-0".to_owned(),
                volume_id: "volume-replacement-0".to_owned(),
                ordinal: 1,
                tier: "medium".to_owned(),
                max_append_bytes: 0,
            },
            &token,
        )
        .await
        .map_err(|error| format!("replace: {error:?}"))?;
    assert!(snapshot.members.iter().all(|member| member.id != "old-1"));
    gateway.end_maintenance(&token).await?;
    stop_node(&mut old[1]).await;

    // The successor of the archived prefix must land on the replacement,
    // and the replacement must fence a stale writer for the trimmed range.
    let fourth = record(stream, 4, 4);
    assert!(
        gateway
            .append(fourth.clone())
            .await
            .map_err(|error| format!("append successor: {error:?}"))?
            >= 2
    );
    assert_eq!(
        replacement[0].disk.records(stream).await,
        vec![fourth.clone()]
    );
    let mut expected = records.clone();
    expected.push(fourth);
    assert_eq!(gateway.recover(stream, 0).await?, expected);

    stop_all(&mut old).await;
    stop_all(&mut replacement).await;
    Ok(())
}

/// Coordinators are stateless: a route published through one of them lives
/// on the control cohort, not in another coordinator's local cache. A member
/// replacement must therefore compute the successor manifest from the
/// quorum manifest, never from the local copy, or the swap silently erases
/// every route published since that coordinator last synced.
#[tokio::test]
async fn replacement_through_a_stale_coordinator_keeps_published_routes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut old = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let old_members = members(&old);
    let mut replacement = start_group(&directory, &["replacement-0"], Some(&old_members)).await?;
    let writer = direct_gateway(&old, &directory, Arc::clone(&archive)).await?;
    let stale = ReplicaGateway::new_direct(
        members(&old),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join("stale-gateway-control.json"),
        Arc::clone(&archive),
    )?;
    // Warm the stale coordinator's cache before the writer publishes a route.
    assert!(stale.membership().await?.members.len() >= 3);
    let stream = "tenant-a/route-survives-swap";
    let records = (1..=2)
        .map(|lsn| record(stream, lsn, lsn as u8))
        .collect::<Vec<_>>();
    for value in &records {
        assert!(writer.append(value.clone()).await? >= 2);
    }

    let token = stale.begin_maintenance_for("replace-stale").await?;
    stale
        .replace_member_with_maintenance(
            "replace-stale",
            "old-1",
            replacement[0].member.clone(),
            0,
            DurableMemberIdentity {
                name: "replacement-0".to_owned(),
                machine_id: "machine-replacement-0".to_owned(),
                volume_id: "volume-replacement-0".to_owned(),
                ordinal: 1,
                tier: "medium".to_owned(),
                max_append_bytes: 0,
            },
            &token,
        )
        .await
        .map_err(|error| format!("replace: {error:?}"))?;
    stale.end_maintenance(&token).await?;
    stop_node(&mut old[1]).await;

    let snapshot = stale.membership().await?;
    assert!(
        snapshot.stream_segments.contains_key(stream),
        "the swap erased the route published by another coordinator: {:?}",
        snapshot.stream_segments.keys().collect::<Vec<_>>()
    );
    assert_eq!(replacement[0].disk.records(stream).await, records);
    // The stream continues on the swapped member set from the coordinator
    // that performed the swap, and the writer's view recovers the same log.
    let third = record(stream, 3, 3);
    assert!(
        stale
            .append(third.clone())
            .await
            .map_err(|error| format!("append after swap: {error:?}"))?
            >= 2
    );
    let mut expected = records.clone();
    expected.push(third);
    assert_eq!(stale.recover(stream, 0).await?, expected);

    stop_all(&mut old).await;
    stop_all(&mut replacement).await;
    Ok(())
}

/// A record acknowledged by a quorum stays committed while one of its
/// holders is silent: the remaining holder's durable marker is evidence, and
/// the member that answered without the record cannot confirm it absent on
/// its own. Recovery restores the missing copy so the writer's successor
/// reaches quorum instead of being refused as an LSN conflict. Staging lost
/// two minutes of writes to exactly this while a member was being swapped.
#[tokio::test]
async fn a_silent_holder_never_hides_an_acknowledged_tail() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let gateway = direct_gateway(&nodes, &directory, Arc::clone(&archive)).await?;
    let stream = "tenant-a/silent-holder";
    let first = record(stream, 1, 1);
    let second = record(stream, 2, 2);
    assert!(gateway.append(first.clone()).await? >= 2);
    assert!(gateway.append(second.clone()).await? >= 2);
    wait_for_records(&gateway, stream, &[first.clone(), second.clone()])
        .await
        .map_err(|error| format!("settle prefix: {error}"))?;
    // The third record reaches a quorum while old-2 is down.
    stop_node(&mut nodes[2]).await;
    let third = record(stream, 3, 3);
    assert_eq!(gateway.append(third.clone()).await?, 2);
    // Now one holder falls silent and the member that missed the record
    // returns. A fresh coordinator sees: old-0 holds it with a durable
    // marker, old-2 lacks it, old-1 says nothing.
    stop_node(&mut nodes[1]).await;
    resume_node(&mut nodes[2])
        .await
        .map_err(|error| format!("resume old-2: {error}"))?;
    assert_eq!(nodes[2].disk.records(stream).await.len(), 2);
    let restarted = ReplicaGateway::new_direct(
        members(&nodes),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join("restarted-gateway-control.json"),
        Arc::clone(&archive),
    )?;
    let fourth = record(stream, 4, 4);
    assert_eq!(
        restarted
            .append(fourth.clone())
            .await
            .map_err(|error| format!("append after the silent holder: {error:?}"))?,
        2
    );
    assert_eq!(
        nodes[2].disk.records(stream).await,
        vec![first.clone(), second.clone(), third.clone(), fourth.clone()],
        "recovery restored the missing copy before extending the log"
    );
    resume_node(&mut nodes[1])
        .await
        .map_err(|error| format!("resume old-1: {error}"))?;
    assert_eq!(
        restarted.recover(stream, 0).await?,
        vec![first, second, third, fourth]
    );
    stop_all(&mut nodes).await;
    Ok(())
}

/// Staging's writer stalled one position behind after every wake: the
/// restarted coordinator's watermark followed the trim checkpoint and
/// ignored the acknowledged hot record above it. A record appended after a
/// trim must be certified by a fresh coordinator like any other.
#[tokio::test]
async fn a_fresh_coordinator_certifies_the_hot_record_above_a_trim_checkpoint() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let gateway = direct_gateway(&nodes, &directory, Arc::clone(&archive)).await?;
    let stream = "tenant-a/hot-above-trim";
    let prefix = (1..=3)
        .map(|lsn| record(stream, lsn, lsn as u8))
        .collect::<Vec<_>>();
    for value in &prefix {
        assert!(gateway.append(value.clone()).await? >= 2);
    }
    wait_for_records(&gateway, stream, &prefix)
        .await
        .map_err(|error| format!("settle prefix: {error}"))?;
    for node in &nodes {
        gateway
            .archive_local_commits(&node.disk)
            .await
            .map_err(|error| format!("archive {}: {error:?}", node.member.id))?;
    }
    let fourth = record(stream, 4, 4);
    assert!(
        gateway
            .append(fourth.clone())
            .await
            .map_err(|error| format!("append above the trim: {error:?}"))?
            >= 2
    );
    wait_for_records(
        &gateway,
        stream,
        &[
            prefix[0].clone(),
            prefix[1].clone(),
            prefix[2].clone(),
            fourth.clone(),
        ],
    )
    .await
    .map_err(|error| format!("settle hot tail: {error}"))?;

    let restarted = ReplicaGateway::new_direct(
        members(&nodes),
        2,
        ROOT_KEY,
        INTERNAL_TOKEN,
        directory.path().join("restarted-gateway-control.json"),
        Arc::clone(&archive),
    )?;
    let fifth = record(stream, 5, 5);
    assert!(
        restarted
            .append(fifth.clone())
            .await
            .map_err(|error| format!("append above the hot tail: {error:?}"))?
            >= 2
    );
    // A resend of the archived record and of the hot record are both duplicates.
    assert!(
        restarted
            .append(prefix[2].clone())
            .await
            .map_err(|error| format!("resend archived: {error:?}"))?
            >= 2
    );
    assert!(
        restarted
            .append(fourth.clone())
            .await
            .map_err(|error| format!("resend hot: {error:?}"))?
            >= 2
    );
    let mut expected = prefix.clone();
    expected.push(fourth);
    expected.push(fifth);
    assert_eq!(restarted.recover(stream, 0).await?, expected);
    stop_all(&mut nodes).await;
    Ok(())
}

/// A writer that published a route and never appended leaves an empty
/// reservation on the cohort. It cannot be sealed, so the fenced cutover
/// drops it once every member has confirmed it is empty; the stream's next
/// append then publishes a fresh route on the remaining cohort.
#[tokio::test]
async fn fenced_cutover_drops_an_empty_reservation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let (mut nodes, gateway) = provision_joining_cohort(&directory, Arc::clone(&archive))
        .await
        .map_err(|error| format!("provision cohort: {error}"))?;
    gateway
        .activate_cohort("activate-cohort-1", 1)
        .await
        .map_err(|error| format!("activate: {error:?}"))?;
    let stream = (0..256)
        .map(|index| format!("tenant-a/reserved-{index}"))
        .find(|candidate| cohort_winner(candidate, &[0, 1]) == 1)
        .ok_or("failed to choose a stream the ring places on cohort one")?;
    let first = record(&stream, 1, 221);
    // The append path publishes the route before it sends the record. With
    // every cohort-one member down the route lands but the record does not,
    // which is exactly the reservation a crashed writer leaves behind.
    for node in nodes.iter_mut().skip(3).take(3) {
        stop_node(node).await;
    }
    assert!(
        gateway.append(first.clone()).await.is_err(),
        "the record cannot land while the cohort is down"
    );
    for node in nodes.iter_mut().skip(3).take(3) {
        resume_node(node)
            .await
            .map_err(|error| format!("resume cohort one: {error}"))?;
    }
    let before = gateway
        .membership()
        .await
        .map_err(|error| format!("membership: {error:?}"))?;
    assert!(
        before.stream_segments.contains_key(&stream),
        "publishing the route reserves the range: {:?}",
        before.stream_segments.keys().collect::<Vec<_>>()
    );
    assert!(
        gateway.cohort_archive_proof(1).await.is_err(),
        "an empty reservation still blocks the archive proof"
    );

    let token = gateway
        .begin_maintenance_for("cutover-empty")
        .await
        .map_err(|error| format!("fence: {error:?}"))?;
    let snapshot = gateway
        .cutover_cohort_with_maintenance("cutover-empty", 1, &token)
        .await
        .map_err(|error| format!("cutover: {error:?}"))?;
    assert!(
        !snapshot.stream_segments.contains_key(&stream),
        "the reservation is dropped: {:?}",
        snapshot.stream_segments.get(&stream)
    );
    gateway
        .end_maintenance(&token)
        .await
        .map_err(|error| format!("release: {error:?}"))?;
    let proof = gateway.cohort_archive_proof(1).await;
    assert!(
        proof.is_ok(),
        "the cohort is sealed once the reservation is gone: {proof:?}"
    );

    assert_eq!(
        gateway
            .append(first.clone())
            .await
            .map_err(|error| format!("append after drop: {error:?}"))?,
        2
    );
    // Cohort one is still active in this test (the operator drains it under
    // the same fence in production), so the ring may place the fresh route
    // there again; what matters is that the route is new and the record is
    // durable.
    let after = gateway
        .membership()
        .await
        .map_err(|error| format!("membership after append: {error:?}"))?;
    let segments = after
        .stream_segments
        .get(&stream)
        .ok_or("the append published a fresh route")?;
    assert_eq!(segments.len(), 1, "{segments:?}");
    assert_eq!(segments[0].start_lsn, 1);
    wait_for_records(&gateway, &stream, &[first])
        .await
        .map_err(|error| format!("recover: {error}"))?;
    stop_all(&mut nodes).await;
    Ok(())
}

/// Quorum certification counts the members that hold a record, so a silent
/// member turns committed records into minority tails. A rebalance that could
/// not hear every retained member must say so, because the operator destroys
/// Machines after a pass it believes complete.
#[tokio::test]
async fn rebalance_reports_unproven_attendance_while_a_member_is_silent() -> TestResult {
    let directory = tempfile::tempdir()?;
    let archive = archive()?;
    let mut nodes = start_group(&directory, &["old-0", "old-1", "old-2"], None).await?;
    let gateway = direct_gateway(&nodes, &directory, Arc::clone(&archive)).await?;
    let stream = "tenant-a/attendance";
    for lsn in 1..=3 {
        assert!(
            gateway
                .append(record(stream, lsn, lsn as u8))
                .await
                .map_err(|error| format!("append {lsn}: {error:?}"))?
                >= 2
        );
    }
    let heard = gateway
        .rebalance(None)
        .await
        .map_err(|error| format!("first rebalance: {error:?}"))?;
    assert!(heard.quorum_proven, "{heard:?}");
    assert_eq!(heard.committed_records, 3);

    // Under the fence every retained member must be reachable: the pass
    // refuses outright rather than certifying against two of three copies.
    stop_node(&mut nodes[2]).await;
    let partial = gateway.rebalance(None).await;
    assert!(
        partial.is_err(),
        "a silent member must not be treated as empty: {partial:?}"
    );

    resume_node(&mut nodes[2])
        .await
        .map_err(|error| format!("resume: {error}"))?;
    let restored = gateway
        .rebalance(None)
        .await
        .map_err(|error| format!("rebalance after resume: {error:?}"))?;
    assert!(restored.quorum_proven, "{restored:?}");
    stop_all(&mut nodes).await;
    Ok(())
}
