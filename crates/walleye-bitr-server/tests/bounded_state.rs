//! What a replica keeps for a stream stays the same size however long the
//! stream is written: the archive head every member reads and rewrites on
//! every pass, and the acknowledged records a writer holds in memory. And an
//! archive pass costs about one stream's round trips, not all of them added
//! together. Nothing recovery or catch-up needs is given up for it.
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::EncryptedRecord;
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router};

const INTERNAL_TOKEN: &str = "bounded-state-internal-token";
const ROOT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const STREAM: &str = "walleye/bounded";
const PAYLOAD: usize = 64;

fn record(stream: &str, lsn: u64, epoch: u64) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![lsn as u8; 24],
        "ciphertext": vec![(lsn as u8).wrapping_add(epoch as u8); PAYLOAD],
        "authentication": vec![epoch as u8; 32],
    }))
    .expect("record")
}

/// A head holds at most 64 segment references of about 250 bytes each,
/// however long its stream: it moves the oldest into an index page when it
/// has more. Between two such folds it grows and shrinks again, so this is
/// the bound it must stay under, not a size it must keep.
const HEAD_BOUND: u64 = 20 * 1024;

/// The size in bytes of every stream's archive head under `store`.
async fn head_bytes(store: &Arc<dyn ObjectStore>) -> u64 {
    store
        .list(None)
        .filter_map(|entry| async move { entry.ok() })
        .filter(|meta| {
            std::future::ready(
                meta.location.filename() == Some("head.json")
                    && meta.location.as_ref().contains("/streams/"),
            )
        })
        .map(|meta| meta.size)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .sum()
}

struct Member {
    node: ReplicaNode,
    address: std::net::SocketAddr,
    dir: std::path::PathBuf,
    disk: Option<Arc<DiskReplica>>,
    task: Option<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
}

impl Member {
    async fn start(&mut self, members: &[ReplicaNode]) {
        let disk = Arc::new(
            DiskReplica::open_with_control(
                self.dir.join("replica.log"),
                self.node.id.clone(),
                "hot",
                &self.dir,
                self.dir.join("control.json"),
                members,
            )
            .expect("open member"),
        );
        let app = node_router(Arc::clone(&disk), ROOT, Some(INTERNAL_TOKEN)).expect("router");
        let mut listener = TcpListener::bind(self.address).await;
        for _ in 0..100 {
            if listener.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
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
    fn disk(&self) -> &DiskReplica {
        self.disk.as_ref().expect("running")
    }
    fn held(&self, stream: &str) -> Vec<EncryptedRecord> {
        let mut records: Vec<EncryptedRecord> = self
            .disk()
            .snapshot()
            .records
            .into_iter()
            .filter(|record| record.stream() == stream)
            .collect();
        records.sort_by_key(EncryptedRecord::lsn);
        records
    }
    /// How far it holds `stream`: its newest record, or its trimmed prefix.
    fn position(&self, stream: &str) -> u64 {
        let snapshot = self.disk().snapshot();
        let trimmed = snapshot
            .trimmed
            .iter()
            .filter(|prefix| prefix.stream == stream)
            .map(|prefix| prefix.archived_lsn)
            .max()
            .unwrap_or(0);
        self.held(stream)
            .last()
            .map_or(trimmed, |last| last.lsn().max(trimmed))
    }
}

struct Cluster {
    members: Vec<Member>,
    nodes: Vec<ReplicaNode>,
    store: Arc<dyn ObjectStore>,
    gateway: ReplicaGateway,
}

impl Cluster {
    /// Three direct members and one coordinator over an in-memory archive,
    /// each archive request delayed by `latency`.
    async fn start(dir: &std::path::Path, latency: Duration) -> Self {
        let mut members = Vec::new();
        for name in ["a", "b", "c"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("port");
            let address = listener.local_addr().expect("address");
            drop(listener);
            let member_dir = dir.join(name);
            std::fs::create_dir_all(&member_dir).expect("member dir");
            members.push(Member {
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
        let memory: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let store: Arc<dyn ObjectStore> = if latency.is_zero() {
            memory
        } else {
            Arc::new(object_store::throttle::ThrottledStore::new(
                memory,
                object_store::throttle::ThrottleConfig {
                    wait_get_per_call: latency,
                    wait_put_per_call: latency,
                    wait_list_per_call: latency,
                    ..Default::default()
                },
            ))
        };
        let archive =
            Arc::new(OpaqueArchive::new(Arc::clone(&store), "bitr", 64).expect("archive"));
        let gateway = ReplicaGateway::new_direct(
            nodes.clone(),
            2,
            ROOT,
            INTERNAL_TOKEN,
            dir.join("gateway-control.json"),
            archive,
        )
        .expect("gateway");
        Self {
            members,
            nodes,
            store,
            gateway,
        }
    }

    /// One archive pass on every running member, as each member's own
    /// daemon runs it, through this coordinator.
    async fn archive_pass(&self) {
        for member in &self.members {
            if member.disk.is_some() {
                self.gateway
                    .archive_local_commits(member.disk())
                    .await
                    .expect("archive pass");
            }
        }
    }

    fn stop(&mut self) {
        for member in &mut self.members {
            member.stop();
        }
    }
}

/// The head stays the same size over thousands of archive passes, and
/// recovery still reads every record from the first, from the middle and
/// from near the tail.
#[tokio::test]
async fn the_archive_head_stays_small_however_long_the_stream() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = OpaqueArchive::new(Arc::clone(&store), "bitr", 64).expect("archive");
    // One small segment per pass, as a busy archive loop writes them.
    let mut sizes = Vec::new();
    for lsn in 1..=2_000 {
        archive
            .archive_committed(&[record(STREAM, lsn, 1)])
            .await
            .expect("archived");
        if lsn % 500 == 0 {
            sizes.push(head_bytes(&store).await);
        }
    }
    assert!(
        sizes.iter().all(|size| *size < HEAD_BOUND),
        "the head grew with the stream: {sizes:?} bytes"
    );
    for after in [0, 1, 777, 1_500, 1_990, 1_999, 2_000] {
        let recovered = archive.recover(STREAM, after).await.expect("recovered");
        assert_eq!(
            recovered
                .iter()
                .map(EncryptedRecord::lsn)
                .collect::<Vec<_>>(),
            (after + 1..=2_000).collect::<Vec<_>>(),
            "recovered after {after}"
        );
        assert!(recovered.iter().all(|r| *r == record(STREAM, r.lsn(), 1)));
    }
    assert_eq!(archive.archived_lsn(STREAM).await.expect("head"), 2_000);
    assert_eq!(
        archive.stream_heads().await.expect("heads"),
        vec![(STREAM.to_owned(), 2_000, 1)]
    );
}

/// A member that was away while its peers archived and trimmed hundreds of
/// passes' worth - far enough back that the archive holds those references
/// in its index pages, not its head - is caught up in full.
#[tokio::test]
async fn a_member_behind_catches_up_from_a_folded_archive() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = Cluster::start(dir.path(), Duration::ZERO).await;
    for lsn in 1..=5 {
        cluster
            .gateway
            .append_many(vec![record(STREAM, lsn, 1)])
            .await
            .expect("written");
    }
    cluster.archive_pass().await;
    cluster.members[2].stop();
    for lsn in 6..=400 {
        cluster
            .gateway
            .append_many(vec![record(STREAM, lsn, 1)])
            .await
            .expect("written without c");
        cluster.archive_pass().await;
    }
    let nodes = cluster.nodes.clone();
    cluster.members[2].start(&nodes).await;
    assert!(cluster.members[2].position(STREAM) < 10);
    let c = cluster.members[2].node.clone();
    cluster
        .gateway
        .catch_up_member(STREAM, &c)
        .await
        .expect("caught up");
    assert!(
        cluster.members[2].position(STREAM) >= 399,
        "c holds {}",
        cluster.members[2].position(STREAM)
    );
    // And the writer, which forgot most of it, brings it the rest of the
    // way, so it carries the next append with one other member.
    cluster.members[0].stop();
    cluster
        .gateway
        .append_many(vec![record(STREAM, 401, 1)])
        .await
        .expect("b and c");
    assert_eq!(cluster.members[2].position(STREAM), 401);
    // Recovery from the start still reads the whole stream.
    assert_eq!(
        cluster
            .gateway
            .recover(STREAM, 0)
            .await
            .expect("recovered")
            .len(),
        401
    );
    cluster.stop();
}

/// A writer forgets what the archive holds, keeping only its newest records.
#[tokio::test]
async fn a_writer_forgets_what_the_archive_holds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = Cluster::start(dir.path(), Duration::ZERO).await;
    for lsn in 1..=1_000 {
        cluster
            .gateway
            .append_many(vec![record(STREAM, lsn, 1)])
            .await
            .expect("written");
        if lsn % 50 == 0 {
            cluster.archive_pass().await;
        }
    }
    cluster.archive_pass().await;
    let held = cluster.gateway.acknowledged_records().await;
    assert!(held <= 200, "the writer holds {held} records");
    cluster.stop();
}

/// One pass over many streams costs about one stream's round trips.
#[tokio::test]
async fn an_archive_pass_works_on_its_streams_together() {
    const STREAMS: u64 = 24;
    let dir = tempfile::tempdir().expect("tempdir");
    let latency = Duration::from_millis(40);
    let mut cluster = Cluster::start(dir.path(), latency).await;
    for stream in 0..STREAMS {
        cluster
            .gateway
            .append_many(vec![record(&format!("walleye/s{stream}"), 1, 1)])
            .await
            .expect("written");
    }
    let started = Instant::now();
    cluster
        .gateway
        .archive_local_commits(cluster.members[0].disk())
        .await
        .expect("archive pass");
    let elapsed = started.elapsed();
    // Each stream takes several archive round trips; one after another,
    // 24 of them would take well over three seconds.
    assert!(
        elapsed < latency * 40,
        "the pass took {elapsed:?} for {STREAMS} streams"
    );
    cluster.stop();
}

/// A stream written without pause over many archive passes: the head, the
/// writer's records and the pass itself stay the same size from the first
/// passes to the last.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_written_stream_stays_the_same_size() {
    const ROUNDS: u64 = 400;
    const PER_ROUND: u64 = 5;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = Cluster::start(dir.path(), Duration::ZERO).await;
    let mut samples = Vec::new();
    let mut passes = Vec::new();
    let mut lsn = 0;
    for round in 0..ROUNDS {
        for _ in 0..PER_ROUND {
            lsn += 1;
            cluster
                .gateway
                .append_many(vec![record(STREAM, lsn, 1)])
                .await
                .expect("written");
        }
        let started = Instant::now();
        cluster.archive_pass().await;
        passes.push(started.elapsed());
        if round % 50 == 49 {
            // The median pass of the last fifty, so one slow pass on a busy
            // machine is not taken for a trend.
            let mut recent = passes[passes.len() - 50..].to_vec();
            recent.sort();
            samples.push((
                round + 1,
                head_bytes(&cluster.store).await,
                cluster.gateway.acknowledged_records().await,
                recent[25],
            ));
        }
    }
    for (round, head, held, pass) in &samples {
        println!(
            "  after {round} passes: head {head} bytes, writer holds {held} records \
             (~{} KiB), median pass {} ms",
            held * (PAYLOAD + 24 + 32 + 64) / 1024,
            pass.as_millis()
        );
    }
    let (_, _, first_held, first_pass) = samples[0];
    let (_, _, last_held, last_pass) = *samples.last().expect("samples");
    assert!(
        samples.iter().all(|(_, head, _, _)| *head < HEAD_BOUND),
        "the head grew: {:?} bytes",
        samples
            .iter()
            .map(|(_, head, _, _)| head)
            .collect::<Vec<_>>()
    );
    assert!(
        last_held <= first_held.max(200),
        "the writer's records grew: {first_held} to {last_held}"
    );
    assert!(
        last_pass <= first_pass * 3 + Duration::from_millis(50),
        "the pass slowed: {first_pass:?} to {last_pass:?}"
    );
    let recovered = cluster.gateway.recover(STREAM, 0).await.expect("recovered");
    assert_eq!(recovered.len() as u64, lsn);
    cluster.stop();
}
