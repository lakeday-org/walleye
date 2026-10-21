//! Lance checkpoints release only the covered WAL prefix, without rewinding writers.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::dataset::mem_wal::WalBackend;
use std::sync::Arc;
use walleye_bitr::{MemoryReplica, QuorumWriter};
use walleye_lance::{BitrWalBackend, LanceDurability, LanceStorageOptions, Table, TableConfig};
fn setup(uri: String) -> (TableConfig, Arc<BitrWalBackend>, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let config = TableConfig::new("events", uri, schema.clone(), vec!["id".into()]).unwrap();
    let backend = Arc::new(
        BitrWalBackend::new(
            Arc::new(QuorumWriter::new(
                Arc::new(MemoryReplica::healthy()),
                [7; 32],
            )),
            &config.stream,
            config.shard_id,
            1,
        )
        .unwrap(),
    );
    (config, backend, schema)
}
/// A reopen claims the next MemWAL epoch, so its Bitr identity must be
/// minted at that epoch over the same quorum writer, as the engine does.
async fn reopen_backend(config: &TableConfig, previous: &BitrWalBackend) -> Arc<BitrWalBackend> {
    let epoch = walleye_lance::next_writer_epoch(
        &LanceStorageOptions::default(),
        &config.uri,
        config.shard_id,
    )
    .await
    .unwrap();
    Arc::new(
        BitrWalBackend::new(previous.writer(), &config.stream, config.shard_id, epoch).unwrap(),
    )
}
fn row(schema: Arc<Schema>, id: i64, payload: &str) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![payload])),
        ],
    )
    .unwrap()
}
#[tokio::test]
async fn checkpoints_release_payloads_and_reopen_recovers_only_the_new_tail() {
    let d = tempfile::tempdir().unwrap();
    let (config, backend, schema) = setup(format!("file://{}/table", d.path().display()));
    let mut table = Table::open(
        config.clone(),
        LanceStorageOptions::default(),
        LanceDurability::Bitr(backend.clone()),
    )
    .await
    .unwrap();
    for id in 0..4 {
        table
            .append(vec![row(schema.clone(), id, "payload")])
            .await
            .unwrap();
        let before = backend.commit_knowledge().await.unwrap();
        assert!(backend.retained_wal_bytes().await > 0);
        table.checkpoint().await.unwrap();
        assert_eq!(backend.retained_wal_bytes().await, 0);
        assert_eq!(backend.commit_knowledge().await.unwrap(), before);
    }
    // The checkpointed prefix is released from the log itself, not only from
    // this writer's buffers: nothing reads below a durable checkpoint again,
    // and a read that asks is refused rather than served from nowhere.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while backend.read_entry(1).await.is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a checkpointed position is released from the log");
    assert_eq!(backend.retained_wal_bytes().await, 0);
    table
        .append(vec![row(schema.clone(), 4, "tail")])
        .await
        .unwrap();
    let tail = backend.retained_wal_bytes().await;
    assert!(tail > 0);
    // An older delayed notification cannot discard the newer tail.
    backend.checkpointed(config.shard_id, 1).await;
    assert_eq!(backend.retained_wal_bytes().await, tail);
    table.close().await.unwrap();
    let backend = reopen_backend(&config, &backend).await;
    let mut reopened = Table::open(
        config,
        LanceStorageOptions::default(),
        LanceDurability::Bitr(backend.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .scan(None, 100)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        5
    );
    reopened
        .append(vec![row(schema, 5, "after reopen")])
        .await
        .unwrap();
    reopened.checkpoint().await.unwrap();
    assert_eq!(backend.retained_wal_bytes().await, 0);
    assert_eq!(
        reopened
            .scan(None, 100)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        6
    );
    reopened.close().await.unwrap();
}
#[tokio::test]
async fn automatic_memtable_flush_also_releases_covered_payloads() {
    let d = tempfile::tempdir().unwrap();
    let (config, backend, schema) = setup(format!("file://{}/table", d.path().display()));
    let mut table = Table::open(
        config,
        LanceStorageOptions::default(),
        LanceDurability::Bitr(backend.clone()),
    )
    .await
    .unwrap();
    let payload = "x".repeat(1024 * 1024);
    for id in 0..24 {
        table
            .append(vec![row(schema.clone(), id, &payload)])
            .await
            .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while backend.retained_wal_bytes().await >= 16 * 1024 * 1024 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("automatic flush must reclaim WAL buffers without an explicit checkpoint");
    table.checkpoint().await.unwrap();
    assert_eq!(backend.retained_wal_bytes().await, 0);
    table.close().await.unwrap();
}
/// A stream written far below the size threshold releases its covered WAL
/// prefix anyway, once the memtable's age elapses — no `checkpoint()` call, and
/// no size trigger to reach. Without the age trigger this stream's WAL grows for
/// the life of the writer and a successor's open has to replay all of it.
#[tokio::test]
async fn a_slow_stream_releases_its_wal_prefix_once_the_memtable_ages() {
    let d = tempfile::tempdir().unwrap();
    let (config, backend, schema) = setup(format!("file://{}/table", d.path().display()));
    let config = config.with_memtable_max_age(std::time::Duration::from_millis(400));
    let mut table = Table::open(
        config.clone(),
        LanceStorageOptions::default(),
        LanceDurability::Bitr(backend.clone()),
    )
    .await
    .unwrap();
    // Four tiny rows, written slowly. Nowhere near Table::MEMTABLE_BYTES, so
    // nothing here can reach the size trigger.
    for id in 0..4 {
        table
            .append(vec![row(schema.clone(), id, "payload")])
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(backend.retained_wal_bytes().await > 0);
    assert!(table.lsm_stats().await.unwrap().sstables.is_empty());
    // No further appends and no checkpoint: only the memtable age can release
    // the prefix from here.
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while backend.retained_wal_bytes().await > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("an aged memtable must release its covered WAL prefix without an explicit checkpoint");
    assert_eq!(
        table
            .lsm_stats()
            .await
            .unwrap()
            .sstables
            .iter()
            .map(|s| s.rows)
            .sum::<u64>(),
        4,
        "the released rows must be durable in a generation, not merely dropped"
    );
    // Only what is written after the release is still WAL-resident.
    table
        .append(vec![row(schema.clone(), 4, "tail")])
        .await
        .unwrap();
    assert!(backend.retained_wal_bytes().await > 0);
    // Dropped, not closed: `close` would flush the tail and leave the reopen
    // below nothing to replay.
    drop(table);
    let backend = reopen_backend(&config, &backend).await;
    let mut reopened = Table::open(
        config,
        LanceStorageOptions::default(),
        LanceDurability::Bitr(backend.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .lsm_stats()
            .await
            .unwrap()
            .sstables
            .iter()
            .map(|s| s.rows)
            .sum::<u64>(),
        4,
        "replay saw only the tail, so it sealed no further generation"
    );
    assert_eq!(
        reopened
            .scan(None, 100)
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        5,
        "the generation and the replayed tail together hold every row"
    );
    reopened.close().await.unwrap();
}
