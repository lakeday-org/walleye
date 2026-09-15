//! Generic stream table using Lance's manifest CAS, MemWAL, and unified LSM scanner.
//! The caller serializes operations through `&mut Table`; cache loss never changes durability.
use crate::{LanceDurability, LanceStorageOptions, OWNER_DO_ID_KEY, with_owner};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::Schema;
use lance::deps::datafusion::execution::memory_pool::MemoryReservation;
use lance::{
    Dataset,
    dataset::mem_wal::{
        DatasetMemWalExt, ShardWriter, ShardWriterConfig,
        scanner::{LsmScanner, ShardSnapshot},
    },
};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct TableConfig {
    pub name: String,
    pub uri: String,
    pub schema: Arc<Schema>,
    pub primary_keys: Vec<String>,
    pub shard_id: Uuid,
    pub stream: String,
}
impl TableConfig {
    pub fn new(
        name: impl Into<String>,
        uri: impl Into<String>,
        schema: Arc<Schema>,
        primary_keys: Vec<String>,
    ) -> lance::Result<Self> {
        let name = name.into();
        for value in [&name] {
            if value.is_empty()
                || !value
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
            {
                return Err(lance::Error::invalid_input(
                    "stream identifiers must be ASCII letters, digits, underscore or dash",
                ));
            }
        }
        if primary_keys.is_empty()
            || primary_keys
                .iter()
                .any(|k| schema.field_with_name(k).is_err())
        {
            return Err(lance::Error::invalid_input(
                "table requires existing primary-key columns",
            ));
        }
        let fields = schema
            .fields()
            .iter()
            .map(|f| {
                let mut f = f.as_ref().clone();
                if primary_keys.contains(f.name()) {
                    let mut m = f.metadata().clone();
                    m.insert("lance-schema:unenforced-primary-key".into(), "true".into());
                    f = f.with_metadata(m);
                }
                f
            })
            .collect::<Vec<_>>();
        let stream = format!("walleye/{name}");
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNER_DO_ID_KEY.into(), stream.clone());
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let shard_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, stream.as_bytes());
        Ok(Self {
            name,
            uri: uri.into(),
            schema,
            primary_keys,
            shard_id,
            stream,
        })
    }
}
/// One owned writer and its read view. The same implementation handles both durability modes.
pub struct Table {
    config: TableConfig,
    dataset: Arc<Dataset>,
    writer: ShardWriter,
    storage: LanceStorageOptions,
    durability: LanceDurability,
}
impl Table {
    pub async fn open(
        config: TableConfig,
        storage: LanceStorageOptions,
        durability: LanceDurability,
    ) -> lance::Result<Self> {
        let mut dataset = match storage.open_dataset(&config.uri).await {
            Ok(d) => d,
            Err(lance::Error::DatasetNotFound { .. }) => {
                let reader = RecordBatchIterator::new(
                    vec![Ok(RecordBatch::new_empty(config.schema.clone()))],
                    config.schema.clone(),
                );
                Dataset::write(reader, config.uri.as_str(), Some(storage.write_params())).await?
            }
            Err(e) => return Err(e),
        };
        let actual_schema: Schema = dataset.schema().into();
        if actual_schema.metadata().get(OWNER_DO_ID_KEY) != Some(&config.stream)
            || !same_fields(&actual_schema, &config.schema)
        {
            return Err(lance::Error::invalid_input(
                "dataset identity or schema differs from the configured stream",
            ));
        }
        if dataset.mem_wal_index_details().await?.is_none() {
            dataset.initialize_mem_wal().unsharded().execute().await?;
        }
        let mut writer_config = ShardWriterConfig::new(config.shard_id)
            .with_durable_write(true)
            .with_max_wal_flush_interval(Duration::from_millis(10))
            .with_max_memtable_size(16 * 1024 * 1024)
            .with_max_unflushed_memtable_bytes(32 * 1024 * 1024);
        if let LanceDurability::Bitr(backend) = &durability {
            if backend.stream() != config.stream
                || backend.shard_id() != config.shard_id
                || backend.owner_do_id() != config.stream
            {
                return Err(lance::Error::invalid_input(
                    "Bitr backend does not belong to this stream",
                ));
            }
            writer_config = writer_config.with_wal_backend(backend.clone());
        }
        writer_config.store_params = storage.object_store_params();
        let writer = dataset
            .mem_wal_writer(config.shard_id, writer_config)
            .await?;
        Ok(Self {
            config,
            dataset: Arc::new(dataset),
            writer,
            storage,
            durability,
        })
    }
    /// Acknowledge only after the configured WAL authority accepts the Arrow IPC entry.
    pub async fn append(&mut self, batches: Vec<RecordBatch>) -> lance::Result<()> {
        let mut owned = Vec::new();
        for b in batches {
            if !same_fields(b.schema().as_ref(), &self.config.schema)
                || b.columns()
                    .iter()
                    .zip(self.config.schema.fields())
                    .any(|(a, f)| !f.is_nullable() && a.null_count() > 0)
            {
                return Err(lance::Error::invalid_input(
                    "append schema or nullability differs from table",
                ));
            }
            let b = with_owner(b, &self.config.stream)
                .map_err(|e| lance::Error::invalid_input(e.to_string()))?;
            let b = RecordBatch::try_new(self.config.schema.clone(), b.columns().to_vec())
                .map_err(|e| lance::Error::invalid_input(e.to_string()))?;
            owned.push(
                with_owner(b, &self.config.stream)
                    .map_err(|e| lance::Error::invalid_input(e.to_string()))?,
            );
        }
        self.writer.put(owned).await?;
        Ok(())
    }
    /// Read one stable snapshot spanning base data, flushed generations, and the active log tail.
    pub async fn scan(&mut self, filter: Option<&str>, limit: usize) -> lance::Result<ScanResult> {
        tokio::time::timeout(self.storage.query_timeout(), self.scan_inner(filter, limit))
            .await
            .map_err(|_| lance::Error::io("query deadline exceeded"))?
    }
    async fn scan_inner(
        &mut self,
        filter: Option<&str>,
        limit: usize,
    ) -> lance::Result<ScanResult> {
        let plan = self
            .snapshot_plan(filter, Some(limit.min(1_000_000)))
            .await?;
        crate::sql::execute(&self.storage, plan).await
    }
    /// Capture a read view while holding this stream's writer lock. The returned
    /// snapshot can be planned and executed after that lock is released.
    pub async fn snapshot(&self) -> lance::Result<crate::TableSnapshot> {
        Ok(crate::TableSnapshot::new(
            self.snapshot_plan(None, None).await?,
        ))
    }

    pub(crate) async fn snapshot_plan(
        &self,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> lance::Result<Arc<dyn lance::deps::datafusion::physical_plan::ExecutionPlan>> {
        let manifest = self
            .writer
            .manifest()
            .await?
            .ok_or_else(|| lance::Error::io("opened shard has no manifest"))?;
        let snapshot = manifest.sstables.iter().fold(
            ShardSnapshot::new(self.writer.shard_id())
                .with_spec_id(manifest.shard_spec_id)
                .with_current_generation(manifest.current_generation),
            |s, t| s.with_sstable(t.generation, t.path.clone()),
        );
        let memtables = self.writer.in_memory_memtable_refs().await?;
        let mut scanner = LsmScanner::new(
            self.dataset.clone(),
            vec![snapshot],
            self.config.primary_keys.clone(),
        )
        .with_in_memory_memtables(self.writer.shard_id(), memtables);
        if let Some(filter) = filter {
            scanner = scanner.filter(filter)?;
        }
        if let Some(limit) = limit {
            scanner = scanner.limit(Some(limit as i64), None)?;
        }
        scanner.create_plan().await
    }
    /// Flush to Lance SSTables and advance the manifest replay watermark.
    pub async fn checkpoint(&mut self) -> lance::Result<()> {
        self.writer.checkpoint().await
    }
    pub async fn close(self) -> lance::Result<()> {
        self.writer.close().await
    }
    pub fn durability(&self) -> &LanceDurability {
        &self.durability
    }
}

/// Owned query batches retaining their allocation until the caller drops the result.
pub struct ScanResult {
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) _memory: MemoryReservation,
}
impl std::ops::Deref for ScanResult {
    type Target = [RecordBatch];
    fn deref(&self) -> &Self::Target {
        &self.batches
    }
}
fn same_fields(left: &Schema, right: &Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}
