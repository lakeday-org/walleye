//! A member that missed writes catches up from its peers, including when it
//! holds a record its own writer left behind that the committed log replaced,
//! and a member that is behind never stands between the cohort and a quorum.
use std::sync::Arc;

use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::EncryptedRecord;
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router};

const INTERNAL_TOKEN: &str = "catch-up-internal-token";
const ROOT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const STREAM: &str = "walleye/catch-up";

/// A record whose bytes depend on its writer's epoch, so two writers'
/// records at one LSN differ.
fn record(lsn: u64, epoch: u64) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": STREAM,
        "writer_epoch": epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![lsn as u8; 24],
        "ciphertext": vec![(lsn as u8).wrapping_add(epoch as u8); 64],
        "authentication": vec![epoch as u8; 32],
    }))
    .expect("record")
}

struct Member {
    direct: bool,
    node: ReplicaNode,
    address: std::net::SocketAddr,
    dir: std::path::PathBuf,
    disk: Option<Arc<DiskReplica>>,
    task: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
}

impl Member {
    async fn start(&mut self, members: &[ReplicaNode]) {
        let disk = Arc::new(if self.direct {
            DiskReplica::open_with_control(
                self.dir.join("replica.log"),
                self.node.id.clone(),
                "hot",
                &self.dir,
                self.dir.join("control.json"),
                members,
            )
            .expect("open member")
        } else {
            DiskReplica::open_with_config(
                self.dir.join("replica.log"),
                self.node.id.clone(),
                "hot",
                &self.dir,
            )
            .expect("open member")
        });
        let app = node_router(Arc::clone(&disk), ROOT, Some(INTERNAL_TOKEN)).expect("router");
        // The port of a server just stopped can take a moment to come free.
        let mut listener = TcpListener::bind(self.address).await;
        for _ in 0..100 {
            if listener.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            listener = TcpListener::bind(self.address).await;
        }
        let listener = listener.expect("rebind");
        self.task = Some(tokio::spawn(
            async move { axum::serve(listener, app).await },
        ));
        self.disk = Some(disk);
    }
    fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        self.disk = None;
    }
    fn held(&self) -> Vec<EncryptedRecord> {
        let mut records: Vec<EncryptedRecord> = self
            .disk
            .as_ref()
            .expect("running")
            .snapshot()
            .records
            .into_iter()
            .filter(|record| record.stream() == STREAM)
            .collect();
        records.sort_by_key(EncryptedRecord::lsn);
        records
    }
}

async fn cohort(dir: &std::path::Path, direct: bool) -> (Vec<Member>, Vec<ReplicaNode>) {
    let mut members = Vec::new();
    for name in ["a", "b", "c"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("port");
        let address = listener.local_addr().expect("address");
        drop(listener);
        let member_dir = dir.join(name);
        std::fs::create_dir_all(&member_dir).expect("member dir");
        members.push(Member {
            direct,
            node: ReplicaNode::new(name, format!("http://{address}")),
            address,
            dir: member_dir,
            disk: None,
            task: None,
        });
    }
    let nodes: Vec<ReplicaNode> = members.iter().map(|m| m.node.clone()).collect();
    for member in &mut members {
        member.start(&nodes).await;
    }
    (members, nodes)
}

fn gateway(dir: &std::path::Path, nodes: &[ReplicaNode], direct: bool) -> ReplicaGateway {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(store, "bitr", 64).expect("archive"));
    if direct {
        ReplicaGateway::new_direct(
            nodes.to_vec(),
            2,
            ROOT,
            INTERNAL_TOKEN,
            dir.join("gateway-control.json"),
            archive,
        )
        .expect("gateway")
    } else {
        ReplicaGateway::new(nodes.to_vec(), 2, ROOT, INTERNAL_TOKEN, archive).expect("gateway")
    }
}

/// C misses records 6 to 10 while it is down. With `orphan`, it also holds
/// a record 6 its dying writer sent it and nobody else: a static cohort,
/// because there a node takes an append with no route placement, which is
/// how a test puts one record on one member. Returns the cohort with C
/// restarted and behind.
async fn with_c_behind(
    dir: &std::path::Path,
    orphan: bool,
) -> (Vec<Member>, Vec<ReplicaNode>, ReplicaGateway) {
    let direct = !orphan;
    let (mut members, nodes) = cohort(dir, direct).await;
    let gateway = gateway(dir, &nodes, direct);
    gateway
        .append_many((1..=5).map(|lsn| record(lsn, 1)).collect())
        .await
        .expect("first five");
    if orphan {
        members[2]
            .disk
            .as_ref()
            .expect("running")
            .append_and_commit_many(&[record(6, 1)])
            .await
            .expect("orphan on c");
    }
    members[2].stop();
    // The next writer recovers without it and carries on at epoch 2.
    gateway
        .append_many((6..=10).map(|lsn| record(lsn, 2)).collect())
        .await
        .expect("written while c was down");
    members[2].start(&nodes).await;
    assert_eq!(
        members[2].held().last().map(EncryptedRecord::lsn),
        Some(if orphan { 6 } else { 5 })
    );
    (members, nodes, gateway)
}

/// A member that missed writes while it was down catches up from its peers
/// and is then a full member of the quorum.
#[tokio::test]
async fn a_member_behind_catches_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut members, _nodes, gateway) = with_c_behind(dir.path(), false).await;
    let c = members[2].node.clone();
    assert_eq!(
        gateway
            .catch_up_member(STREAM, &c)
            .await
            .expect("caught up"),
        Some(6..=10)
    );
    assert_eq!(members[2].held(), members[0].held());
    members[0].stop();
    assert_eq!(
        gateway
            .append_many(vec![record(11, 2)])
            .await
            .expect("b and c"),
        2
    );
    for member in &mut members {
        member.stop();
    }
}

#[tokio::test]
async fn a_member_behind_catches_up_past_its_orphan() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut members, _nodes, gateway) = with_c_behind(dir.path(), true).await;
    let c = members[2].node.clone();
    let range = gateway
        .catch_up_member(STREAM, &c)
        .await
        .expect("caught up");
    assert_eq!(range, Some(6..=10));
    assert_eq!(
        members[2].held(),
        members[0].held(),
        "C holds exactly what A holds, record for record"
    );
    assert_eq!(
        gateway.catch_up_member(STREAM, &c).await.expect("again"),
        None,
        "nothing more to do"
    );
    // With A gone, B and C are the quorum.
    members[0].stop();
    assert_eq!(
        gateway
            .append_many(vec![record(11, 2)])
            .await
            .expect("b and c"),
        2
    );
    for member in &mut members {
        member.stop();
    }
}

/// A member behind is caught up by the write that needs it: with A gone and
/// C not yet caught up, the append brings C up to date and is acknowledged
/// by B and C rather than refused.
#[tokio::test]
async fn a_write_that_needs_a_member_behind_catches_it_up() {
    for orphan in [false, true] {
        a_write_catches_up(orphan).await;
    }
}

async fn a_write_catches_up(orphan: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut members, _nodes, gateway) = with_c_behind(dir.path(), orphan).await;
    members[0].stop();
    assert_eq!(
        gateway
            .append_many(vec![record(11, 2)])
            .await
            .expect("b and c"),
        2
    );
    assert_eq!(members[2].held().last().map(EncryptedRecord::lsn), Some(11));
    assert_eq!(members[2].held(), members[1].held());
    for member in &mut members {
        member.stop();
    }
}

/// A member never counts for a position it does not hold, and one member is
/// not a quorum however up to date it is made: with A and B gone the append
/// is refused, though C was brought up to date on the way.
#[tokio::test]
async fn a_member_behind_does_not_count_towards_a_quorum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut members, _nodes, gateway) = with_c_behind(dir.path(), true).await;
    members[0].stop();
    members[1].stop();
    assert!(
        gateway.append_many(vec![record(11, 2)]).await.is_err(),
        "one member is not a quorum"
    );
    let held = members[2].held();
    assert_eq!(
        held.iter()
            .find(|r| r.lsn() == 6)
            .map(EncryptedRecord::writer_epoch),
        Some(2),
        "the orphan was replaced by the committed record, from this coordinator's own \
         acknowledgements"
    );
    for member in &mut members {
        member.stop();
    }
}

/// Withdrawing an orphan survives a restart of the member, and a member never
/// withdraws a record from the writer that replaced it or a newer one.
#[tokio::test]
async fn a_withdrawn_orphan_stays_withdrawn_and_committed_records_are_kept() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut members, nodes, gateway) = with_c_behind(dir.path(), true).await;
    let disk = members[2].disk.clone().expect("running");
    assert!(
        disk.supersede(STREAM, 6, 1).is_err(),
        "a record from the superseding writer's own epoch is not an orphan"
    );
    let c = members[2].node.clone();
    gateway
        .catch_up_member(STREAM, &c)
        .await
        .expect("caught up");
    members[2].stop();
    members[2].start(&nodes).await;
    assert_eq!(
        members[2].held(),
        members[0].held(),
        "replayed as withdrawn"
    );
    for member in &mut members {
        member.stop();
    }
}

/// A member behind while writes carry on without it rejoins them. Catch-up
/// alone leaves it one short, since the newest record is not yet certainly
/// committed, so it refuses every live append after; the refusal of a batch
/// that already had its quorum is noted, and repairing it offers the member
/// that batch once it is level, after which it takes live appends itself.
#[tokio::test]
async fn a_member_behind_rejoins_live_appends() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut members, nodes, gateway) = with_c_behind(dir.path(), false).await;
    let c = members[2].node.clone();
    let last = |member: &Member| member.held().last().map(EncryptedRecord::lsn);

    // A and B carry 11; C refuses it for want of 6 to 10, after the quorum.
    assert_eq!(
        gateway
            .append_many(vec![record(11, 2)])
            .await
            .expect("a and b"),
        2
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), gateway.lagging_noted())
        .await
        .expect("C's refusal is noted");

    // Catch-up by any other coordinator stops at 10, which is what C's own
    // node does: only the writer's knows 11 had its quorum.
    let elsewhere = dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).expect("dir");
    let other = self::gateway(&elsewhere, &nodes, true);
    assert_eq!(
        other.catch_up_member(STREAM, &c).await.expect("caught up"),
        Some(6..=10)
    );
    assert_eq!(last(&members[2]), Some(10));
    assert_eq!(
        other.catch_up_member(STREAM, &c).await.expect("again"),
        None,
        "and no further however often it runs"
    );

    // Repairing offers C the batch it refused, which it now takes.
    assert_eq!(gateway.repair_lagging().await, 1);
    assert_eq!(last(&members[2]), Some(11));
    assert_eq!(members[2].held(), members[0].held());
    assert_eq!(gateway.repair_lagging().await, 0, "nothing left to repair");

    // And the next append reaches C directly.
    assert_eq!(
        gateway.append_many(vec![record(12, 2)]).await.expect("all"),
        2
    );
    let started = std::time::Instant::now();
    while last(&members[2]) != Some(12) {
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "C took 12 itself"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        gateway.repair_lagging().await,
        0,
        "and was never behind again"
    );
    for member in &mut members {
        member.stop();
    }
}
