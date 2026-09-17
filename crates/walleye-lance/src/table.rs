//! Generic stream table using Lance's manifest CAS, MemWAL, and unified LSM scanner.
//! The caller serializes operations through `&mut Table`; cache loss never changes durability.
use crate::{LanceDurability, LanceStorageOptions, OWNER_DO_ID_KEY, with_owner};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::Schema;
use futures::TryStreamExt;
use lance::deps::datafusion::execution::memory_pool::MemoryReservation;
use lance::deps::datafusion::{
    common::{DataFusionError, Result as DfResult},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use lance::{
    Dataset,
    dataset::mem_wal::{
        CompactionResult, Compactor, DatasetMemWalExt, ShardWriter, ShardWriterConfig,
        index::MemIndexConfig,
        scanner::{
            DatasetCache, FreshTierWatermark, InMemoryMemTables, LsmScanner, ShardSnapshot,
            SsTableCache,
        },
    },
    index::DatasetIndexExt,
};
use lance_linalg::distance::DistanceType;

/// A vector index maintained by the table: an in-memory HNSW graph over the
/// memtable, flushed as IVF_HNSW_SQ on every generation and rebuilt by
/// compaction. No base-table training is involved, so it works from the first
/// row.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VectorIndexSpec {
    pub name: String,
    pub column: String,
    /// `l2`, `cosine`, or `dot`.
    pub metric: String,
}
/// Per-generation view of the LSM for diagnostics and tests.
#[derive(Clone, Debug, serde::Serialize)]
pub struct LsmStats {
    pub sstables: Vec<SsTableStats>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct SsTableStats {
    pub generation: u64,
    pub path: String,
    pub rows: u64,
    pub indices: Vec<String>,
}

/// A LanceDB-style search: filter, projection, paging, and an optional
/// nearest-neighbor query over one vector column.
#[derive(Clone, Debug, Default)]
pub struct SearchRequest {
    pub filter: Option<String>,
    pub columns: Option<Vec<String>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub vector: Option<VectorQuery>,
}
#[derive(Clone, Debug)]
pub struct VectorQuery {
    pub column: String,
    pub vector: Vec<f32>,
    pub k: usize,
    pub nprobes: usize,
    pub refine_factor: u32,
    pub metric: Option<String>,
    /// HNSW search beam width on indexed generations.
    pub ef: Option<usize>,
}
fn parse_metric(metric: Option<&str>) -> lance::Result<DistanceType> {
    match metric {
        None => Ok(DistanceType::L2),
        Some(m) => {
            DistanceType::try_from(m).map_err(|e| lance::Error::invalid_input(e.to_string()))
        }
    }
}
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct TableConfig {
    pub name: String,
    pub uri: String,
    pub schema: Arc<Schema>,
    pub primary_keys: Vec<String>,
    pub shard_id: Uuid,
    pub stream: String,
    pub vector_indexes: Vec<VectorIndexSpec>,
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
            vector_indexes: Vec::new(),
        })
    }
    /// Smallest possible encoded row: fixed-width columns at their width,
    /// variable-width ones at one byte. Used to bound rows per memtable.
    pub fn min_row_bytes(&self) -> usize {
        self.schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                arrow_schema::DataType::FixedSizeList(inner, dim) => {
                    *dim as usize * inner.data_type().primitive_width().unwrap_or(1)
                }
                other => other.primitive_width().unwrap_or(1),
            })
            .sum::<usize>()
            .max(1)
    }
    pub fn with_vector_indexes(mut self, specs: Vec<VectorIndexSpec>) -> lance::Result<Self> {
        for spec in &specs {
            let field = self
                .schema
                .field_with_name(&spec.column)
                .map_err(|e| lance::Error::invalid_input(e.to_string()))?;
            if !matches!(field.data_type(), arrow_schema::DataType::FixedSizeList(inner, _)
                if inner.data_type() == &arrow_schema::DataType::Float32)
            {
                return Err(lance::Error::invalid_input(format!(
                    "vector index {} needs a FixedSizeList<Float32> column, {} is {}",
                    spec.name,
                    spec.column,
                    field.data_type()
                )));
            }
            parse_metric(Some(&spec.metric))?;
        }
        self.vector_indexes = specs;
        Ok(self)
    }
}
/// One owned writer and its read view. The same implementation handles both durability modes.
pub struct Table {
    config: TableConfig,
    dataset: Arc<Dataset>,
    writer: ShardWriter,
    storage: LanceStorageOptions,
    durability: LanceDurability,
    /// Opened generation datasets, keyed by path. Generations are immutable,
    /// so reusing the open handle saves the manifest resolution and index
    /// load that every query would otherwise repeat against object storage.
    sstables: Arc<SsTableCache>,
}

/// Emit bounded, stage-level open diagnostics without exposing a dataset URI,
/// credentials, or row data. A start event is deliberately emitted before
/// every remote operation so a supervisor that kills a stalled process still
/// leaves enough evidence to identify the operation that was waiting.
fn open_stage_start(config: &TableConfig, stage: &str) -> Instant {
    let started = Instant::now();
    eprintln!(
        "walleye.storage table_open stream={} stage={} phase=start",
        config.stream, stage
    );
    started
}

fn open_stage_finish(
    config: &TableConfig,
    stage: &str,
    started: Instant,
    outcome: &str,
    error: Option<&lance::Error>,
) {
    let elapsed_ms = started.elapsed().as_millis();
    match error {
        Some(error) => eprintln!(
            "walleye.storage table_open stream={} stage={} phase=finish elapsed_ms={} outcome={} error_kind={}",
            config.stream,
            stage,
            elapsed_ms,
            outcome,
            open_error_kind(error),
        ),
        None => eprintln!(
            "walleye.storage table_open stream={} stage={} phase=finish elapsed_ms={} outcome={}",
            config.stream, stage, elapsed_ms, outcome
        ),
    }
}

fn open_error_kind(error: &lance::Error) -> &'static str {
    match error {
        lance::Error::DatasetNotFound { .. } => "dataset_not_found",
        lance::Error::Timeout { .. } => "timeout",
        lance::Error::IO { .. } => "io",
        lance::Error::External { .. } => "external",
        lance::Error::InvalidInput { .. } => "invalid_input",
        _ => "lance",
    }
}

impl Table {
    /// Upper bound on one memtable's bytes.
    pub const MEMTABLE_BYTES: usize = 16 * 1024 * 1024;
    /// Row capacity of a memtable's in-memory vector graph.
    pub const MEMTABLE_ROWS: usize = 100_000;
    /// Largest single put; see `open` for why it is half the row capacity.
    pub const PUT_ROWS: usize = 50_000;
    pub async fn open(
        config: TableConfig,
        storage: LanceStorageOptions,
        durability: LanceDurability,
    ) -> lance::Result<Self> {
        let load_started = open_stage_start(&config, "dataset_load");
        let mut dataset = match storage.open_dataset(&config.uri).await {
            Ok(d) => {
                open_stage_finish(&config, "dataset_load", load_started, "ok", None);
                d
            }
            Err(lance::Error::DatasetNotFound { .. }) => {
                open_stage_finish(&config, "dataset_load", load_started, "missing", None);
                let reader = RecordBatchIterator::new(
                    vec![Ok(RecordBatch::new_empty(config.schema.clone()))],
                    config.schema.clone(),
                );
                let create_started = open_stage_start(&config, "dataset_create");
                match Dataset::write(reader, config.uri.as_str(), Some(storage.write_params()))
                    .await
                {
                    Ok(dataset) => {
                        open_stage_finish(&config, "dataset_create", create_started, "ok", None);
                        dataset
                    }
                    Err(error) => {
                        open_stage_finish(
                            &config,
                            "dataset_create",
                            create_started,
                            "error",
                            Some(&error),
                        );
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                open_stage_finish(&config, "dataset_load", load_started, "error", Some(&error));
                return Err(error);
            }
        };
        let schema_started = open_stage_start(&config, "schema_validate");
        let actual_schema: Schema = dataset.schema().into();
        if actual_schema.metadata().get(OWNER_DO_ID_KEY) != Some(&config.stream)
            || !same_fields(&actual_schema, &config.schema)
        {
            let error = lance::Error::invalid_input(
                "dataset identity or schema differs from the configured stream",
            );
            open_stage_finish(
                &config,
                "schema_validate",
                schema_started,
                "error",
                Some(&error),
            );
            return Err(error);
        }
        open_stage_finish(&config, "schema_validate", schema_started, "ok", None);
        let index_started = open_stage_start(&config, "mem_wal_index");
        let index = match dataset.mem_wal_index_details().await {
            Ok(index) => {
                open_stage_finish(&config, "mem_wal_index", index_started, "ok", None);
                index
            }
            Err(error) => {
                open_stage_finish(
                    &config,
                    "mem_wal_index",
                    index_started,
                    "error",
                    Some(&error),
                );
                return Err(error);
            }
        };
        if index.is_none() {
            let initialize_started = open_stage_start(&config, "mem_wal_initialize");
            match dataset.initialize_mem_wal().unsharded().execute().await {
                Ok(()) => open_stage_finish(
                    &config,
                    "mem_wal_initialize",
                    initialize_started,
                    "ok",
                    None,
                ),
                Err(error) => {
                    open_stage_finish(
                        &config,
                        "mem_wal_initialize",
                        initialize_started,
                        "error",
                        Some(&error),
                    );
                    return Err(error);
                }
            }
        }
        // The memtable's in-memory vector graph is sized for MEMTABLE_ROWS
        // rows and a put is never split across memtables, so appends are
        // chunked to PUT_ROWS rows and the writer freezes a memtable once it
        // holds half its row capacity. A memtable never exceeds MEMTABLE_ROWS.
        let mut writer_config = ShardWriterConfig::new(config.shard_id)
            .with_durable_write(true)
            .with_max_wal_flush_interval(Duration::from_millis(10))
            .with_max_memtable_size(Self::MEMTABLE_BYTES)
            .with_max_memtable_rows(Self::MEMTABLE_ROWS)
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
        for spec in &config.vector_indexes {
            let field_id = dataset
                .schema()
                .field(&spec.column)
                .map(|f| f.id)
                .ok_or_else(|| {
                    lance::Error::invalid_input(format!("vector column {} missing", spec.column))
                })?;
            writer_config = writer_config.with_index_config(MemIndexConfig::hnsw(
                spec.name.clone(),
                field_id,
                spec.column.clone(),
                parse_metric(Some(&spec.metric))?,
            ));
        }
        let writer_started = open_stage_start(&config, "mem_wal_writer");
        let writer = match dataset.mem_wal_writer(config.shard_id, writer_config).await {
            Ok(writer) => {
                open_stage_finish(&config, "mem_wal_writer", writer_started, "ok", None);
                writer
            }
            Err(error) => {
                open_stage_finish(
                    &config,
                    "mem_wal_writer",
                    writer_started,
                    "error",
                    Some(&error),
                );
                return Err(error);
            }
        };
        if let LanceDurability::Bitr(backend) = &durability
            && backend.writer_epoch() != writer.epoch()
        {
            // The Bitr identity was minted from the manifest before the claim;
            // a concurrent claimant moved the epoch in between. Refuse rather
            // than write under an epoch Bitr would not fence correctly.
            let claimed = writer.epoch();
            let _ = writer.close().await;
            return Err(lance::Error::io(format!(
                "Bitr epoch {} does not match the claimed writer epoch {}; another node \
                 claimed the stream during open, retry",
                backend.writer_epoch(),
                claimed
            )));
        }
        Ok(Self {
            config,
            dataset: Arc::new(dataset),
            writer,
            storage,
            durability,
            sstables: Arc::new(SsTableCache::new(256)),
        })
    }
    /// Acknowledge only after the configured WAL authority accepts the Arrow IPC entry.
    /// Batches are put in slices of at most [`Self::PUT_ROWS`] rows, each its
    /// own WAL entry, so a large insert can roll across memtables.
    pub async fn append(&mut self, batches: Vec<RecordBatch>) -> lance::Result<()> {
        let mut owned = Vec::new();
        for b in batches.into_iter().flat_map(|b| {
            let rows = b.num_rows();
            (0..rows.max(1))
                .step_by(Self::PUT_ROWS)
                .map(move |start| b.slice(start, (rows - start).min(Self::PUT_ROWS)))
                .collect::<Vec<_>>()
        }) {
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
        for batch in owned {
            self.writer.put(vec![batch]).await?;
        }
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
        Ok(crate::TableSnapshot::from_source(Arc::new(
            self.capture_snapshot().await?,
        )))
    }

    pub(crate) async fn snapshot_plan(
        &self,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> lance::Result<Arc<dyn lance::deps::datafusion::physical_plan::ExecutionPlan>> {
        let snapshot = self.capture_snapshot().await?;
        snapshot.plan_sql(filter, limit).await
    }

    async fn capture_snapshot(&self) -> lance::Result<CapturedSnapshot> {
        // Capture the in-memory handles first, then read the manifest.  A
        // background flush can commit a generation between those operations;
        // if it does, the generation is present in both views and we retain
        // exactly one representation below (the captured in-memory handle).
        // A generation created after the refs capture cannot be in the refs,
        // and cannot be in the manifest until the manifest read completes, so
        // it is excluded from this snapshot.  This ordering avoids the hole
        // that refs-after-manifest would create when a zero-grace flush evicts
        // its frozen handle before the second read.
        let memtables = self.writer.in_memory_memtable_refs().await?;
        let visible_counts = std::iter::once(&memtables.active)
            .chain(memtables.frozen.iter())
            .map(|memtable| {
                (
                    (self.writer.shard_id(), memtable.generation),
                    memtable.index_store.visible_count(),
                )
            })
            .collect::<HashMap<_, _>>();
        let manifest = self
            .writer
            .manifest()
            .await?
            .ok_or_else(|| lance::Error::io("opened shard has no manifest"))?;
        let snapshot = manifest.sstables.iter().fold(
            ShardSnapshot::new(self.writer.shard_id())
                .with_spec_id(manifest.shard_spec_id)
                .with_current_generation(manifest.current_generation),
            |s, t| {
                // The in-memory Arc is the exact handle captured at the
                // snapshot boundary.  Prefer it over an SSTable that a
                // concurrent flush committed after that boundary; otherwise
                // the same generation would be scanned twice.
                let captured_in_memory = std::iter::once(&memtables.active)
                    .chain(memtables.frozen.iter())
                    .any(|memtable| memtable.generation == t.generation);
                let created_after_capture = t.generation >= memtables.active.generation;
                if captured_in_memory || created_after_capture {
                    // The active generation is the newest generation visible
                    // at the refs boundary.  A manifest generation at or
                    // above it either duplicates a captured handle (flush
                    // raced the capture) or was committed after the capture;
                    // neither belongs as a second source in this view.
                    s
                } else {
                    s.with_sstable(t.generation, t.path.clone())
                }
            },
        );
        let active_generation = memtables.active.generation;
        let active_batch_count = visible_counts
            .get(&(self.writer.shard_id(), active_generation))
            .copied()
            .unwrap_or(0);
        let fresh_tier_watermarks = [(
            self.writer.shard_id(),
            FreshTierWatermark {
                active_generation,
                active_batch_count: active_batch_count as u64,
            },
        )]
        .into_iter()
        .collect();
        Ok(CapturedSnapshot {
            dataset: self.dataset.clone(),
            shard_id: self.writer.shard_id(),
            shard: snapshot,
            memtables,
            visible_counts,
            fresh_tier_watermarks,
            primary_keys: self.config.primary_keys.clone(),
            schema: self.config.schema.clone(),
            sstables: self.sstables.clone(),
        })
    }
    pub fn config(&self) -> &TableConfig {
        &self.config
    }
    /// The MemWAL writer epoch this table claimed when it opened.
    pub fn writer_epoch(&self) -> u64 {
        self.writer.epoch()
    }
    /// A handle for merging flushed generations that outlives the caller's
    /// lock on this table.
    pub fn compactor(&self) -> Option<Compactor> {
        self.writer.compactor()
    }
    /// Merge flushed generations into one indexed generation when at least
    /// `min_sstables` exist. The replaced generation directories are left in
    /// place; the caller deletes them once no snapshot can reference them.
    pub async fn compact(&self, min_sstables: usize) -> lance::Result<Option<CompactionResult>> {
        let result = match self.writer.compactor() {
            Some(compactor) => compactor.compact(min_sstables).await?,
            None => None,
        };
        if result.is_some()
            && let Some(manifest) = self.writer.manifest().await?
        {
            let base = self.config.uri.trim_end_matches('/');
            let shard = self.writer.shard_id();
            let live = manifest
                .sstables
                .iter()
                .map(|s| format!("{base}/_mem_wal/{shard}/{}", s.path))
                .collect();
            self.sstables.retain_paths(&live);
        }
        Ok(result)
    }
    /// Open every flushed generation into the dataset cache and load its
    /// indexes into the session caches, so the first query pays nothing that
    /// a later one would not. Purely a cache optimization.
    pub async fn warm(&self) -> lance::Result<usize> {
        let Some(manifest) = self.writer.manifest().await? else {
            return Ok(0);
        };
        let snapshot = manifest.sstables.iter().fold(
            ShardSnapshot::new(self.writer.shard_id())
                .with_spec_id(manifest.shard_spec_id)
                .with_current_generation(manifest.current_generation),
            |s, t| s.with_sstable(t.generation, t.path.clone()),
        );
        let cache: Arc<dyn DatasetCache> = self.sstables.clone();
        self.dataset
            .prewarm_mem_wal(&[snapshot], Some(&cache))
            .await?;
        Ok(manifest.sstables.len())
    }
    /// Generations currently in the manifest, with row counts and index names.
    pub async fn lsm_stats(&self) -> lance::Result<LsmStats> {
        let Some(manifest) = self.writer.manifest().await? else {
            return Ok(LsmStats {
                sstables: Vec::new(),
            });
        };
        let mut sstables = Vec::with_capacity(manifest.sstables.len());
        for sstable in &manifest.sstables {
            let uri = format!(
                "{}/_mem_wal/{}/{}",
                self.config.uri.trim_end_matches('/'),
                self.writer.shard_id(),
                sstable.path
            );
            let dataset = self.storage.open_dataset(&uri).await?;
            let indices = dataset
                .load_indices()
                .await?
                .iter()
                .map(|i| i.name.clone())
                .collect();
            sstables.push(SsTableStats {
                generation: sstable.generation,
                path: sstable.path.clone(),
                rows: dataset.count_rows(None).await? as u64,
                indices,
            });
        }
        Ok(LsmStats { sstables })
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

/// Immutable inputs needed to build an LSM plan for one point-in-time view.
/// The manifest and memtable references are captured together while the table
/// is held by the caller, so repeated SQL scans cannot observe a later write or
/// checkpoint even when they carry different predicates.
struct CapturedSnapshot {
    dataset: Arc<Dataset>,
    shard_id: Uuid,
    shard: ShardSnapshot,
    memtables: InMemoryMemTables,
    visible_counts: HashMap<(Uuid, u64), usize>,
    fresh_tier_watermarks: HashMap<Uuid, FreshTierWatermark>,
    primary_keys: Vec<String>,
    schema: Arc<Schema>,
    sstables: Arc<SsTableCache>,
}

impl CapturedSnapshot {
    fn scanner(&self) -> LsmScanner {
        LsmScanner::new(
            self.dataset.clone(),
            vec![self.shard.clone()],
            self.primary_keys.clone(),
        )
        .with_sstable_cache(self.sstables.clone() as Arc<dyn DatasetCache>)
        .with_in_memory_memtables(self.shard_id, self.memtables.clone())
        .with_in_memory_visible_counts(
            self.shard_id,
            self.visible_counts
                .iter()
                .map(|((_, generation), count)| (*generation, *count)),
        )
        .with_fresh_tier_watermarks(self.fresh_tier_watermarks.clone())
    }

    async fn plan_sql(
        &self,
        filter: Option<&str>,
        limit: Option<usize>,
    ) -> lance::Result<Arc<dyn ExecutionPlan>> {
        let mut scanner = self.scanner();
        if let Some(filter) = filter {
            scanner = scanner.filter(filter)?;
        }
        if let Some(limit) = limit {
            scanner = scanner.limit(Some(limit as i64), None)?;
        }
        scanner.create_plan().await
    }
}

#[async_trait::async_trait]
impl crate::sql::SnapshotPlanSource for CapturedSnapshot {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn plan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let mut scanner = self.scanner();
        if let Some((first, rest)) = filters.split_first() {
            let filter = rest
                .iter()
                .cloned()
                .fold(first.clone(), |all, next| all.and(next));
            scanner = scanner.filter_expr(filter);
        }
        if let Some(limit) = limit {
            scanner = scanner
                .limit(Some(limit as i64), None)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        scanner
            .create_plan()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn search_plan(&self, request: &SearchRequest) -> lance::Result<Arc<dyn ExecutionPlan>> {
        let mut scanner = self.scanner();
        if let Some(filter) = request.filter.as_deref() {
            scanner = scanner.filter(filter)?;
        }
        if let Some(columns) = &request.columns {
            scanner = scanner.project(columns)?;
        }
        if request.limit.is_some() || request.offset.is_some() {
            scanner = scanner.limit(
                request.limit.map(|l| l as i64),
                request.offset.map(|o| o as i64),
            )?;
        }
        if let Some(query) = &request.vector {
            let key = arrow_array::Float32Array::from(query.vector.clone());
            scanner = scanner
                .nearest(&query.column, &key, query.k.max(1))?
                .nprobes(query.nprobes.max(1))
                .refine(query.refine_factor)
                .distance_metric(parse_metric(query.metric.as_deref())?);
            if let Some(ef) = query.ef {
                scanner = scanner.ef(ef);
            }
        }
        scanner.create_plan().await
    }

    async fn count(&self, filter: Option<&str>) -> lance::Result<u64> {
        let mut scanner = self.scanner();
        if let Some(filter) = filter {
            scanner = scanner.filter(filter)?;
        }
        scanner.count_rows().await
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

/// Prepare a stream that was last written by a single node for a Bitr
/// cluster taking ownership. Bitr LSNs must start at 1 for a stream it has
/// never seen, while the MemWAL manifest continues from the object-store WAL
/// tail. When Bitr holds no history for the stream and that tail was fully
/// checkpointed, the manifest's WAL positions are reset so the first Bitr
/// append lands at LSN 1. Returns whether a reset happened.
///
/// A tail with entries after the last checkpoint is refused: only the
/// object-store WAL holds them, and a Bitr-backed writer could not replay
/// them. Reopen in single-node mode, checkpoint, then move.
pub async fn prepare_bitr_takeover(
    storage: &LanceStorageOptions,
    uri: &str,
    shard_id: Uuid,
    stream: &str,
    writer: &walleye_bitr::QuorumWriter,
) -> lance::Result<bool> {
    use lance::dataset::mem_wal::ShardManifestStore;
    use lance_index::mem_wal::ShardManifest;
    let dataset = match storage.open_dataset(uri).await {
        Ok(dataset) => dataset,
        Err(lance::Error::DatasetNotFound { .. }) => return Ok(false),
        Err(error) => return Err(error),
    };
    let object_store = dataset.object_store(None).await?;
    let base_path = dataset.branch_location().path;
    let store = ShardManifestStore::new(object_store, &base_path, shard_id, 64);
    let Some(manifest) = store.read_latest().await? else {
        return Ok(false);
    };
    // The manifest only learns positions at flush time, so the object-store
    // WAL directory is the authority on what a single-node writer left behind.
    let wal_dir = lance::dataset::mem_wal::util::shard_wal_path(&base_path, &shard_id);
    let object_store = dataset.object_store(None).await?;
    let mut newest_wal_entry = 0u64;
    let mut entries = object_store.inner.list(Some(&wal_dir));
    while let Some(object) = entries.try_next().await? {
        if let Some(position) = object
            .location
            .filename()
            .and_then(lance::dataset::mem_wal::util::parse_bit_reversed_filename)
        {
            newest_wal_entry = newest_wal_entry.max(position);
        }
    }
    if newest_wal_entry == 0 && manifest.wal_entry_position_last_seen == 0 {
        return Ok(false);
    }
    let history = writer
        .recover(stream, 0)
        .await
        .map_err(|e| lance::Error::io(format!("Bitr recovery for {stream}: {e}")))?;
    if !history.is_empty() {
        return Ok(false);
    }
    if newest_wal_entry > manifest.replay_after_wal_entry_position {
        return Err(lance::Error::invalid_input(format!(
            "stream {stream} has WAL entries after its last checkpoint (positions {}..={}) that \
             only the object-store WAL holds; reopen it in single-node mode and checkpoint \
             before moving it to Bitr",
            manifest.replay_after_wal_entry_position + 1,
            newest_wal_entry
        )));
    }
    let (epoch, _) = store.claim_epoch(manifest.shard_spec_id).await?;
    store
        .commit_update(epoch, |current| ShardManifest {
            version: current.version + 1,
            replay_after_wal_entry_position: 0,
            wal_entry_position_last_seen: 0,
            ..current.clone()
        })
        .await?;
    Ok(true)
}

/// The epoch the next `Table::open` will claim for this shard: one past the
/// manifest's current writer epoch, or 1 for a shard that has never been
/// opened. A Bitr backend built with this epoch is fenced exactly like the
/// MemWAL writer it accompanies.
pub async fn next_writer_epoch(
    storage: &LanceStorageOptions,
    uri: &str,
    shard_id: Uuid,
) -> lance::Result<u64> {
    use lance::dataset::mem_wal::ShardManifestStore;
    let dataset = match storage.open_dataset(uri).await {
        Ok(dataset) => dataset,
        Err(lance::Error::DatasetNotFound { .. }) => return Ok(1),
        Err(error) => return Err(error),
    };
    let object_store = dataset.object_store(None).await?;
    let base_path = dataset.branch_location().path;
    let store = ShardManifestStore::new(object_store, &base_path, shard_id, 64);
    Ok(store
        .read_latest()
        .await?
        .map(|m| m.writer_epoch + 1)
        .unwrap_or(1))
}
