//! A table written for a long time costs the same to claim, and keeps the same
//! amount of log in its bucket, as a young one: what a claim reads and what
//! the archive holds track what the table has not flushed yet, not its age.
//!
//! The table runs over a real replica cluster - three members, a coordinator
//! and an archive - with its checkpoints taken as the engine takes them. Each
//! round writes, archives and checkpoints; every so often the table is claimed
//! afresh, as a new owner claims it, and what that claim read from the log and
//! how many objects the archive holds are measured.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use walleye_bitr::{
    EncryptedRecord, QuorumWriter, ReplicaError, ReplicaGateway as GatewayTrait, StreamExtent,
};
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router};
use walleye_lance::{BitrWalBackend, LanceDurability, LanceStorageOptions, Table, TableConfig};

const INTERNAL_TOKEN: &str = "long-lived-internal-token";
const ROOT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

/// The coordinator, counting the records every recovery returns.
struct Counting {
    inner: Arc<ReplicaGateway>,
    recovered: AtomicU64,
}

#[async_trait]
impl GatewayTrait for Counting {
    async fn append(&self, record: EncryptedRecord) -> Result<Option<String>, ReplicaError> {
        GatewayTrait::append(self.inner.as_ref(), record).await
    }
    async fn append_many(
        &self,
        records: Vec<EncryptedRecord>,
    ) -> Result<Option<String>, ReplicaError> {
        GatewayTrait::append_many(self.inner.as_ref(), records).await
    }
    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        let records = GatewayTrait::recover(self.inner.as_ref(), stream, after_lsn).await?;
        self.recovered
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        Ok(records)
    }
    async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        let records = GatewayTrait::recover_with_watermark(
            self.inner.as_ref(),
            stream,
            after_lsn,
            committed_lsn,
            certificate,
        )
        .await?;
        self.recovered
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        Ok(records)
    }
    async fn extent(&self, stream: &str) -> Result<StreamExtent, ReplicaError> {
        GatewayTrait::extent(self.inner.as_ref(), stream).await
    }
    async fn release(&self, stream: &str, through_lsn: u64) -> Result<(), ReplicaError> {
        GatewayTrait::release(self.inner.as_ref(), stream, through_lsn).await
    }
}

struct Cluster {
    disks: Vec<Arc<DiskReplica>>,
    tasks: Vec<tokio::task::JoinHandle<Result<(), std::io::Error>>>,
    store: Arc<dyn object_store_bitr::ObjectStore>,
    gateway: Arc<ReplicaGateway>,
}

impl Cluster {
    async fn start(dir: &std::path::Path) -> Self {
        let mut listeners = Vec::new();
        let mut nodes = Vec::new();
        for name in ["a", "b", "c"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("port");
            let address = listener.local_addr().expect("address");
            nodes.push(ReplicaNode::new(name, format!("http://{address}")));
            listeners.push(listener);
        }
        let mut disks = Vec::new();
        let mut tasks = Vec::new();
        for (node, listener) in nodes.iter().zip(listeners) {
            let member_dir = dir.join(&node.id);
            std::fs::create_dir_all(&member_dir).expect("member dir");
            let disk = Arc::new(
                DiskReplica::open_with_control(
                    member_dir.join("replica.log"),
                    node.id.clone(),
                    "hot",
                    &member_dir,
                    member_dir.join("control.json"),
                    &nodes,
                )
                .expect("member"),
            );
            let app = node_router(Arc::clone(&disk), ROOT, Some(INTERNAL_TOKEN)).expect("router");
            tasks.push(tokio::spawn(
                async move { axum::serve(listener, app).await },
            ));
            disks.push(disk);
        }
        let store: Arc<dyn object_store_bitr::ObjectStore> =
            Arc::new(object_store_bitr::memory::InMemory::new());
        let archive =
            Arc::new(OpaqueArchive::new(Arc::clone(&store), "bitr", 64).expect("archive"));
        let gateway = Arc::new(
            ReplicaGateway::new_direct(
                nodes,
                2,
                ROOT,
                INTERNAL_TOKEN,
                dir.join("gateway-control.json"),
                archive,
            )
            .expect("gateway"),
        );
        Self {
            disks,
            tasks,
            store,
            gateway,
        }
    }

    /// One archive pass on every member, as each member's daemon runs it.
    async fn archive_pass(&self) {
        for disk in &self.disks {
            self.gateway
                .archive_local_commits(disk)
                .await
                .expect("archive pass");
        }
    }

    /// How many objects the archive holds for streams: segments, pages and
    /// heads.
    async fn archive_objects(&self) -> usize {
        self.store
            .list(Some(&object_store_bitr::path::Path::from("bitr/streams")))
            .filter_map(|entry| async move { entry.ok() })
            .count()
            .await
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn row(schema: Arc<Schema>, id: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec!["long-lived"])),
        ],
    )
    .expect("row")
}

/// Claims the table as a new owner does: the takeover check, then an open
/// at the next writer epoch. Returns the table, how many log records the
/// claim read, and how long it took.
async fn claim(
    config: &TableConfig,
    writer: &Arc<QuorumWriter>,
    counting: &Counting,
) -> (Table, u64, Duration) {
    let started = Instant::now();
    let before = counting.recovered.load(Ordering::Relaxed);
    walleye_lance::prepare_bitr_takeover(
        &LanceStorageOptions::default(),
        &config.uri,
        config.shard_id,
        &config.stream,
        writer,
    )
    .await
    .expect("takeover check");
    let epoch = walleye_lance::next_writer_epoch(
        &LanceStorageOptions::default(),
        &config.uri,
        config.shard_id,
    )
    .await
    .expect("epoch");
    let backend = Arc::new(
        BitrWalBackend::new(Arc::clone(writer), &config.stream, config.shard_id, epoch)
            .expect("backend"),
    );
    let table = Table::open(
        config.clone(),
        LanceStorageOptions::default(),
        LanceDurability::Bitr(backend),
    )
    .await
    .expect("claimed");
    let read = counting.recovered.load(Ordering::Relaxed) - before;
    (table, read, started.elapsed())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_lived_table_claims_and_archives_like_a_young_one() {
    const ROUNDS: i64 = 120;
    const PER_ROUND: i64 = 5;
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = Cluster::start(dir.path()).await;
    let counting = Arc::new(Counting {
        inner: Arc::clone(&cluster.gateway),
        recovered: AtomicU64::new(0),
    });
    let writer = Arc::new(QuorumWriter::new(
        Arc::clone(&counting) as Arc<dyn GatewayTrait>,
        [7; 32],
    ));
    let schema = schema();
    let config = TableConfig::new(
        "events",
        format!("file://{}/table", dir.path().display()),
        schema.clone(),
        vec!["id".into()],
    )
    .expect("config")
    .with_log("bitr:long-lived");
    let (mut table, _, _) = claim(&config, &writer, &counting).await;
    let mut id = 0;
    let mut samples = Vec::new();
    for round in 1..=ROUNDS {
        for _ in 0..PER_ROUND {
            table
                .append(vec![row(schema.clone(), id)])
                .await
                .expect("append");
            id += 1;
        }
        cluster.archive_pass().await;
        table.checkpoint().await.expect("checkpoint");
        // The release the checkpoint set off runs beside the writer.
        tokio::time::sleep(Duration::from_millis(20)).await;
        cluster.archive_pass().await;
        if round % 20 == 0 {
            // One more append that no checkpoint covers, so the claim has a
            // tail to replay, as a claim after a failure does.
            table
                .append(vec![row(schema.clone(), id)])
                .await
                .expect("append");
            id += 1;
            drop(table);
            let (claimed, read, took) = claim(&config, &writer, &counting).await;
            table = claimed;
            let objects = cluster.archive_objects().await;
            samples.push((round, read, took, objects));
        }
    }
    for (round, read, took, objects) in &samples {
        println!(
            "  after {round} rounds: the claim read {read} log records in {} ms; the archive \
             holds {objects} objects",
            took.as_millis()
        );
    }
    let (_, first_read, _, first_objects) = samples[0];
    let (_, last_read, _, last_objects) = *samples.last().expect("samples");
    assert!(
        last_read <= first_read + 5,
        "a claim read more of the log as the table aged: {:?}",
        samples
            .iter()
            .map(|(_, read, _, _)| read)
            .collect::<Vec<_>>()
    );
    assert!(
        last_objects <= first_objects + first_objects / 2,
        "the archive grew with the table's age: {:?} objects",
        samples
            .iter()
            .map(|(_, _, _, objects)| objects)
            .collect::<Vec<_>>()
    );
    // Every row is still there.
    let rows: usize = table
        .scan(None, 100_000)
        .await
        .expect("scan")
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows as i64, id);
}
