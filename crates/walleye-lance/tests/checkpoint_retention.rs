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
    // Explicit historical reads still work, but cannot repopulate checkpointed buffers.
    assert!(backend.read_entry(1).await.unwrap().is_some());
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
