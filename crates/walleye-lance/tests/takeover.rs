//! Ramp to launch: a stream written by a single node over the object-store
//! WAL is reopened by a Bitr-backed writer on the same root after a clean
//! checkpoint. Bitr must accept the stream from LSN 1 and keep every row.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use walleye_bitr::{MemoryReplica, QuorumWriter};
use walleye_lance::{
    BitrWalBackend, LanceDurability, LanceStorageOptions, Table, TableConfig, next_writer_epoch,
    prepare_bitr_takeover,
};

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
async fn rows(table: &mut Table) -> usize {
    table
        .scan(None, 100)
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

#[tokio::test]
async fn single_node_stream_moves_to_bitr_after_clean_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}/events", dir.path().display());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let config =
        TableConfig::new("events", uri.clone(), schema.clone(), vec!["id".into()]).unwrap();
    let storage = LanceStorageOptions::default();

    // Ramp: two commits over the object-store WAL, then a clean drain.
    let mut single = Table::open(
        config.clone(),
        storage.clone(),
        LanceDurability::ObjectStore,
    )
    .await
    .unwrap();
    single
        .append(vec![row(schema.clone(), 1, "a")])
        .await
        .unwrap();
    single
        .append(vec![row(schema.clone(), 2, "b")])
        .await
        .unwrap();
    single.checkpoint().await.unwrap();
    single.close().await.unwrap();

    // Launch: a Bitr cluster with no history for this stream takes over.
    let writer = Arc::new(QuorumWriter::new(
        Arc::new(MemoryReplica::healthy()),
        [7; 32],
    ));
    let reset = prepare_bitr_takeover(&storage, &uri, config.shard_id, &config.stream, &writer)
        .await
        .unwrap();
    assert!(
        reset,
        "a checkpointed single-node tail resets the WAL positions"
    );
    let epoch = next_writer_epoch(&storage, &uri, config.shard_id)
        .await
        .unwrap();
    let backend = Arc::new(
        BitrWalBackend::new(writer.clone(), &config.stream, config.shard_id, epoch).unwrap(),
    );
    let mut cluster = Table::open(
        config.clone(),
        storage.clone(),
        LanceDurability::Bitr(backend.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&mut cluster).await,
        2,
        "checkpointed rows survive the move"
    );
    cluster
        .append(vec![row(schema.clone(), 3, "c")])
        .await
        .unwrap();
    assert_eq!(rows(&mut cluster).await, 3);
    assert!(
        backend.retained_wal_bytes().await > 0,
        "the new row went through Bitr"
    );

    // A second open on the same cluster is not a takeover: Bitr has history now.
    cluster.checkpoint().await.unwrap();
    cluster.close().await.unwrap();
    let again = prepare_bitr_takeover(&storage, &uri, config.shard_id, &config.stream, &writer)
        .await
        .unwrap();
    assert!(!again);
    let epoch = next_writer_epoch(&storage, &uri, config.shard_id)
        .await
        .unwrap();
    let backend = Arc::new(
        BitrWalBackend::new(writer.clone(), &config.stream, config.shard_id, epoch).unwrap(),
    );
    let mut reopened = Table::open(
        config.clone(),
        storage.clone(),
        LanceDurability::Bitr(backend),
    )
    .await
    .unwrap();
    assert_eq!(rows(&mut reopened).await, 3);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn unclean_single_node_tail_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}/events", dir.path().display());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let config =
        TableConfig::new("events", uri.clone(), schema.clone(), vec!["id".into()]).unwrap();
    let storage = LanceStorageOptions::default();
    let mut single = Table::open(
        config.clone(),
        storage.clone(),
        LanceDurability::ObjectStore,
    )
    .await
    .unwrap();
    single
        .append(vec![row(schema.clone(), 1, "a")])
        .await
        .unwrap();
    // No checkpoint: the row lives only in the object-store WAL.
    drop(single);
    let writer = Arc::new(QuorumWriter::new(
        Arc::new(MemoryReplica::healthy()),
        [7; 32],
    ));
    let error = prepare_bitr_takeover(&storage, &uri, config.shard_id, &config.stream, &writer)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("checkpoint"), "{error}");
}
