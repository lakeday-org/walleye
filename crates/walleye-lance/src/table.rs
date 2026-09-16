//! Generic stream table using Lance's manifest CAS, MemWAL, and unified LSM scanner.
//! The caller serializes operations through `&mut Table`; cache loss never changes durability.
use crate::{LanceDurability, LanceStorageOptions, OWNER_DO_ID_KEY, with_owner};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::Schema;
use lance::deps::datafusion::execution::memory_pool::MemoryReservation;
use lance::deps::datafusion::{
    common::{DataFusionError, Result as DfResult},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use lance::{
    Dataset,
    dataset::mem_wal::{
        DatasetMemWalExt, ShardWriter, ShardWriterConfig,
        scanner::{FreshTierWatermark, InMemoryMemTables, LsmScanner, ShardSnapshot},
    },
    index::{DatasetIndexExt, vector::VectorIndexParams},
};
use lance_index::IndexType;
use lance_linalg::distance::DistanceType;

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
}
/// Parameters for a vector index on the base table.
#[derive(Clone, Debug)]
pub struct VectorIndexRequest {
    pub column: String,
    pub name: Option<String>,
    pub metric: Option<String>,
    pub replace: bool,
    /// IVF_FLAT when false, IVF_PQ when true.
    pub product_quantization: bool,
    pub num_partitions: Option<usize>,
    pub num_sub_vectors: Option<usize>,
}
#[derive(Clone, Debug)]
pub struct IndexInfo {
    pub name: String,
    pub uuid: String,
    pub columns: Vec<String>,
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
        })
    }
    pub fn config(&self) -> &TableConfig {
        &self.config
    }
    /// Create (or replace) a vector index on the base table. Rows in the
    /// memtables and SSTables are searched exactly and merged with the index
    /// results, so the index never has to be rebuilt after ingest.
    pub async fn create_vector_index(
        &mut self,
        request: &VectorIndexRequest,
    ) -> lance::Result<String> {
        let field = self
            .config
            .schema
            .field_with_name(&request.column)
            .map_err(|e| lance::Error::invalid_input(e.to_string()))?;
        let dim = match field.data_type() {
            arrow_schema::DataType::FixedSizeList(inner, dim)
                if inner.data_type() == &arrow_schema::DataType::Float32 =>
            {
                *dim as usize
            }
            other => {
                return Err(lance::Error::invalid_input(format!(
                    "vector index requires a FixedSizeList<Float32> column, got {other}"
                )));
            }
        };
        if self.dataset.count_rows(None).await? == 0 {
            return Err(lance::Error::not_supported(
                "vector indexes need rows in the base table, and this build has no LSM \
                 compaction yet; searches run exactly over every tier without an index",
            ));
        }
        let metric = parse_metric(request.metric.as_deref())?;
        let partitions = request.num_partitions.unwrap_or(1).max(1);
        let params = if request.product_quantization {
            let sub_vectors = request
                .num_sub_vectors
                .unwrap_or_else(|| (dim / 8).max(1))
                .max(1);
            VectorIndexParams::ivf_pq(partitions, 8, sub_vectors, metric, 50)
        } else {
            VectorIndexParams::ivf_flat(partitions, metric)
        };
        let name = request
            .name
            .clone()
            .unwrap_or_else(|| format!("{}_idx", request.column));
        let mut dataset = (*self.dataset).clone();
        dataset
            .create_index(
                &[request.column.as_str()],
                IndexType::Vector,
                Some(name.clone()),
                &params,
                request.replace,
            )
            .await?;
        self.dataset = Arc::new(dataset);
        Ok(name)
    }
    pub async fn list_indices(&self) -> lance::Result<Vec<IndexInfo>> {
        let schema = self.dataset.schema();
        Ok(self
            .dataset
            .load_indices()
            .await?
            .iter()
            // The MemWAL manifest is registered as a system index; clients
            // only see indexes on their own columns.
            .filter(|index| !index.name.starts_with("__lance") && !index.fields.is_empty())
            .map(|index| IndexInfo {
                name: index.name.clone(),
                uuid: index.uuid.to_string(),
                columns: index
                    .fields
                    .iter()
                    .filter_map(|id| schema.field_by_id(*id).map(|f| f.name.clone()))
                    .collect(),
            })
            .collect())
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
}

impl CapturedSnapshot {
    fn scanner(&self) -> LsmScanner {
        LsmScanner::new(
            self.dataset.clone(),
            vec![self.shard.clone()],
            self.primary_keys.clone(),
        )
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
