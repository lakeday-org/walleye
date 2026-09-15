//! Single-node durability and unified hot/flushed reads use the same table implementation.
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use walleye_lance::{LanceDurability, LanceStorageOptions, Table, TableConfig};
#[tokio::test]
async fn object_store_wal_survives_checkpoint_and_reopen_without_duplicate_rows() {
    let d = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let config = TableConfig::new(
        "table",
        format!("file://{}/table", d.path().display()),
        schema.clone(),
        vec!["id".into()],
    )
    .unwrap();
    let mut table = Table::open(
        config.clone(),
        LanceStorageOptions::default(),
        LanceDurability::ObjectStore,
    )
    .await
    .unwrap();
    table
        .append(vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        ])
        .await
        .unwrap();
    assert_eq!(
        table
            .scan(None, 100)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        3
    );
    table.checkpoint().await.unwrap();
    assert_eq!(
        table
            .scan(None, 100)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        3
    );
    table.close().await.unwrap();
    let mut reopened = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        Table::open(
            config,
            LanceStorageOptions::default(),
            LanceDurability::ObjectStore,
        ),
    )
    .await
    .expect("MemWAL reopen must complete within the bounded test deadline")
    .unwrap();
    assert_eq!(
        reopened
            .scan(Some("id > 1"), 100)
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        2
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn append_rejects_relabelled_columns_and_foreign_owner_metadata() {
    let d = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let config = TableConfig::new(
        "table",
        format!("file://{}/table", d.path().display()),
        schema.clone(),
        vec!["id".into()],
    )
    .unwrap();
    let mut table = Table::open(
        config,
        LanceStorageOptions::default(),
        LanceDurability::ObjectStore,
    )
    .await
    .unwrap();
    let other = Arc::new(Schema::new(vec![Field::new(
        "different",
        DataType::Int64,
        false,
    )]));
    let wrong = RecordBatch::try_new(other, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    assert!(
        table.append(vec![wrong]).await.is_err(),
        "columns must not be silently relabelled"
    );
    let foreign = walleye_lance::with_owner(
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2]))]).unwrap(),
        "other/table",
    )
    .unwrap();
    assert!(
        table.append(vec![foreign]).await.is_err(),
        "owner metadata must survive validation"
    );
    assert!(
        table
            .scan(None, 100)
            .await
            .unwrap()
            .iter()
            .all(|b| b.num_rows() == 0)
    );
    table.close().await.unwrap();
}

#[tokio::test]
async fn opening_a_different_stream_on_an_existing_dataset_fails_closed() {
    let d = tempfile::tempdir().unwrap();
    let uri = format!("file://{}/table", d.path().display());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let first = TableConfig::new("a", &uri, schema.clone(), vec!["id".into()]).unwrap();
    Table::open(
        first,
        LanceStorageOptions::default(),
        LanceDurability::ObjectStore,
    )
    .await
    .unwrap()
    .close()
    .await
    .unwrap();
    let other = TableConfig::new("b", &uri, schema, vec!["id".into()]).unwrap();
    assert!(
        Table::open(
            other,
            LanceStorageOptions::default(),
            LanceDurability::ObjectStore
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn returned_query_batches_keep_their_memory_reservation_until_dropped() {
    use datafusion_execution::memory_pool::MemoryPool;
    let d = tempfile::tempdir().unwrap();
    let cache = walleye_lance::CachedStorage::open(
        d.path().join("cache"),
        "tenant",
        8 * 1024 * 1024,
        16 * 1024 * 1024,
        lance_io::object_store::ObjectStoreParams::default(),
        None,
    )
    .await
    .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let config = TableConfig::new(
        "table",
        format!("file://{}/table", d.path().display()),
        schema.clone(),
        vec!["id".into()],
    )
    .unwrap();
    let mut table = Table::open(config, cache.storage.clone(), LanceDurability::ObjectStore)
        .await
        .unwrap();
    table
        .append(vec![
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap(),
        ])
        .await
        .unwrap();
    let rows = table.scan(None, 100).await.unwrap();
    assert!(
        cache.resources.reserved() > 0,
        "caller-held query results remain accounted"
    );
    drop(rows);
    assert_eq!(cache.resources.reserved(), 0);
    table.close().await.unwrap();
    cache.backend.close().await.unwrap();
}
