//! Single-node durability and unified hot/flushed reads use the same table implementation.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::{path::Path, sync::Arc};
use walleye_lance::{LanceDurability, LanceStorageOptions, Table, TableConfig};

fn contains_filename(root: &Path, wanted: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        path.file_name().and_then(|name| name.to_str()) == Some(wanted)
            || (path.is_dir() && contains_filename(&path, wanted))
    })
}
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
async fn composite_checkpoint_flush_supports_multi_key_query_without_duplicates() {
    let d = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("scope", DataType::Utf8, false),
        Field::new("entry", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let config = TableConfig::new(
        "table",
        format!("file://{}/table", d.path().display()),
        schema.clone(),
        vec!["scope".into(), "entry".into()],
    )
    .unwrap();
    let mut table = Table::open(
        config,
        LanceStorageOptions::default(),
        LanceDurability::ObjectStore,
    )
    .await
    .unwrap();

    table
        .append(vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["scan", "scan", "other"])),
                    Arc::new(StringArray::from(vec!["one", "two", "one"])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
            )
            .unwrap(),
        ])
        .await
        .unwrap();
    // This is the production MemTableFlusher path: the persisted generation
    // and its PK sidecar are created before the multi-key read below.
    table.checkpoint().await.unwrap();
    assert!(
        contains_filename(&d.path().join("table"), "page_lookup.lance"),
        "checkpoint must persist the composite PK sidecar before it is queried"
    );

    let result = table
        .scan(Some("scope IN ('scan') AND entry IN ('one', 'two')"), 100)
        .await
        .unwrap();
    let rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 2, "each composite key should be returned once");

    // Rewrite one key in a fresh generation and ensure the same multi-key
    // query returns the newest row while retaining the untouched key.
    table
        .append(vec![
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["scan"])),
                    Arc::new(StringArray::from(vec!["one"])),
                    Arc::new(Int64Array::from(vec![11])),
                ],
            )
            .unwrap(),
        ])
        .await
        .unwrap();
    let result = table
        .scan(Some("scope IN ('scan') AND entry IN ('one', 'two')"), 100)
        .await
        .unwrap();
    let mut values = result
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, vec![2, 11]);
    table.close().await.unwrap();
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
