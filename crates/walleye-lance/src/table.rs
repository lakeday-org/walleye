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
        index::{FtsIndexConfig, MemIndexConfig},
        scanner::{
            DatasetCache, FreshTierWatermark, InMemoryMemTables, LsmScanner, ShardSnapshot,
            SsTableCache,
        },
    },
    index::DatasetIndexExt,
};
use lance_index::scalar::FullTextSearchQuery;
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
/// A full-text index maintained by the table: an inverted index over the
/// memtable, flushed with every generation and rebuilt by compaction, exactly
/// as the vector index is. Term matching, not substring matching: searching
/// for "ship" finds the word "ship" and not the word "shipment".
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TextIndexSpec {
    pub name: String,
    pub column: String,
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
    pub text: Option<TextQuery>,
}
/// A full-text query: terms matched against an inverted index, not a
/// substring scan. `columns` empty means every indexed text column.
#[derive(Clone, Debug)]
pub struct TextQuery {
    pub query: String,
    pub columns: Vec<String>,
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
    /// How long the writer's active memtable may hold rows before it rotates,
    /// regardless of size. Defaults to [`Table::MEMTABLE_AGE`].
    pub memtable_max_age: Duration,
    pub text_indexes: Vec<TextIndexSpec>,
    /// Which write-ahead log this writer appends to, as a name every process
    /// writing this stream would spell the same way: a Bitr gateway's address,
    /// or the object store's own WAL. Two writers that name the same log can
    /// replay each other's entries; two that do not, cannot, whatever either
    /// one's log happens to contain.
    pub log: Option<String>,
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
            memtable_max_age: Table::MEMTABLE_AGE,
            text_indexes: Vec::new(),
            log: None,
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
    /// Name the write-ahead log this writer appends to. See [`Self::log`].
    #[must_use]
    pub fn with_log(mut self, log: impl Into<String>) -> Self {
        self.log = Some(log.into());
        self
    }
    /// Override how long the active memtable may hold rows before it rotates.
    pub fn with_memtable_max_age(mut self, age: Duration) -> Self {
        self.memtable_max_age = age;
        self
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
    pub fn with_text_indexes(mut self, specs: Vec<TextIndexSpec>) -> lance::Result<Self> {
        for spec in &specs {
            let field = self
                .schema
                .field_with_name(&spec.column)
                .map_err(|e| lance::Error::invalid_input(e.to_string()))?;
            if !matches!(
                field.data_type(),
                arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8
            ) {
                return Err(lance::Error::invalid_input(format!(
                    "text index {} needs a text column, {} is {}",
                    spec.name,
                    spec.column,
                    field.data_type()
                )));
            }
        }
        self.text_indexes = specs;
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
    /// Whether this writer has already said, in shared storage, that it is
    /// holding rows nobody else can read. Set on the first append after a
    /// drain and cleared by the next checkpoint, so the note costs one put a
    /// flush rather than one an append. See [`crate::open_tail`].
    tail_noted: bool,
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
    /// How long the active memtable may hold rows before it rotates regardless
    /// of size. A walleye stream writing well under [`Self::MEMTABLE_BYTES`]
    /// would otherwise never rotate, so its WAL would grow without bound and a
    /// successor's open would replay all of it.
    pub const MEMTABLE_AGE: Duration = Duration::from_secs(60);
    pub async fn open(
        config: TableConfig,
        storage: LanceStorageOptions,
        durability: LanceDurability,
    ) -> lance::Result<Self> {
        Self::open_prepared(config, storage, durability, None).await
    }
    /// As [`Self::open`], on a dataset already loaded and warmed by
    /// [`Prepared`]. The dataset is used as of its last
    /// [`Prepared::refresh`], so a caller refreshes it before reading the
    /// epoch it is about to claim. Nothing about the claim changes: the
    /// writer claims its epoch and fences its predecessor exactly as an open
    /// from nothing does, it just does not load what it already has.
    pub async fn open_prepared(
        config: TableConfig,
        storage: LanceStorageOptions,
        durability: LanceDurability,
        prepared: Option<Prepared>,
    ) -> lance::Result<Self> {
        let (loaded, sstables) = match prepared {
            Some(prepared) => (Ok(prepared.dataset), prepared.sstables),
            None => {
                let load_started = open_stage_start(&config, "dataset_load");
                let loaded = storage.open_dataset(&config.uri).await;
                match &loaded {
                    Ok(_) => open_stage_finish(&config, "dataset_load", load_started, "ok", None),
                    Err(lance::Error::DatasetNotFound { .. }) => {
                        open_stage_finish(&config, "dataset_load", load_started, "missing", None)
                    }
                    Err(error) => open_stage_finish(
                        &config,
                        "dataset_load",
                        load_started,
                        "error",
                        Some(error),
                    ),
                }
                (loaded, Arc::new(SsTableCache::new(256)))
            }
        };
        let mut dataset = match loaded {
            Ok(d) => d,
            Err(lance::Error::DatasetNotFound { .. }) => {
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
            Err(error) => return Err(error),
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
            .with_max_unflushed_memtable_bytes(32 * 1024 * 1024)
            .with_max_memtable_age(Some(config.memtable_max_age));
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
        for spec in &config.text_indexes {
            let field_id = dataset
                .schema()
                .field(&spec.column)
                .map(|f| f.id)
                .ok_or_else(|| {
                    lance::Error::invalid_input(format!("text column {} missing", spec.column))
                })?;
            writer_config = writer_config.with_index_config(MemIndexConfig::Fts(
                FtsIndexConfig::new(spec.name.clone(), field_id, spec.column.clone()),
            ));
        }
        // Before the claim, not after. Claiming is what fences the writer that
        // holds the stream, so a writer that is going to refuse has to refuse
        // before it takes anything: otherwise a doomed open still knocks the
        // incumbent off a stream it was serving correctly.
        reachable_tail(&dataset, &config).await?;
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
            sstables,
            tail_noted: false,
        })
    }
    /// Acknowledge only after the configured WAL authority accepts the Arrow IPC entry.
    /// Batches are put in slices of at most [`Self::PUT_ROWS`] rows, each its
    /// own WAL entry, so a large insert can roll across memtables.
    pub async fn append(&mut self, batches: Vec<RecordBatch>) -> lance::Result<()> {
        // Before the rows, not after: a note left for a write that then fails
        // costs a successor one refusal it did not need, and a note missing
        // for a write that succeeded costs it the rows.
        self.note_open_tail().await;
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
    /// Say, in shared storage, that this writer is holding acknowledged rows
    /// that are only in its own log. Cheap and idempotent after the first
    /// call, because the flag is what decides whether a put happens at all.
    async fn note_open_tail(&mut self) {
        if self.tail_noted {
            return;
        }
        let Ok(store) = self.dataset.object_store(None).await else {
            return;
        };
        let base = self.dataset.branch_location().path;
        let note = crate::open_tail::OpenTail {
            after: 0,
            stream: self.config.stream.clone(),
            log: self.config.log.clone(),
        };
        // A note that cannot be written is not worth failing a write over: it
        // costs a successor its safety check, and losing the write costs the
        // caller its row. Say so loudly instead.
        match crate::open_tail::write(&store, &base, self.config.shard_id, &note).await {
            Ok(()) => self.tail_noted = true,
            Err(error) => eprintln!(
                "walleye.storage open_tail stream={} outcome=unwritten error={error}",
                self.config.stream
            ),
        }
    }

    /// Take the note down: the tail is in shared storage now.
    async fn clear_open_tail(&mut self) {
        let Ok(store) = self.dataset.object_store(None).await else {
            return;
        };
        let base = self.dataset.branch_location().path;
        if let Err(error) = crate::open_tail::clear(&store, &base, self.config.shard_id).await {
            eprintln!(
                "walleye.storage open_tail stream={} outcome=uncleared error={error}",
                self.config.stream
            );
            return;
        }
        self.tail_noted = false;
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
        match self.warmer().await? {
            Some(warmer) => warmer.run().await,
            None => Ok(0),
        }
    }
    /// What [`Self::warm`] does, as something to run without the table: the
    /// generations to open are read here, and opening them needs neither the
    /// writer nor whatever lock the caller holds it under. Warming a table
    /// just taken over can take seconds; a write waiting behind it should
    /// not.
    pub async fn warmer(&self) -> lance::Result<Option<Warmer>> {
        let Some(manifest) = self.writer.manifest().await? else {
            return Ok(None);
        };
        Ok(Some(Warmer {
            dataset: self.dataset.clone(),
            snapshot: shard_snapshot(self.writer.shard_id(), &manifest),
            sstables: self.sstables.clone(),
        }))
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
    /// Freeze the memtable for flushing and return what to wait on for it to
    /// reach a generation. Freezing is quick; the flush that follows is not,
    /// and it needs no hold on the table: appends go on into a fresh memtable
    /// while it runs. What [`Self::checkpoint`] does not: take the open-tail
    /// note back, because rows appended meanwhile may still need it.
    pub async fn seal(&self) -> lance::Result<Sealed> {
        Ok(Sealed(self.writer.force_seal_active().await?))
    }
    pub async fn checkpoint(&mut self) -> lance::Result<()> {
        self.writer.checkpoint().await?;
        self.clear_open_tail().await;
        Ok(())
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
        if let Some(text) = &request.text {
            let mut query = FullTextSearchQuery::new(text.query.clone());
            if !text.columns.is_empty() {
                query = query.with_columns(&text.columns)?;
            }
            scanner = scanner.full_text_search(query)?;
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

/// Whether the tail the last writer left is one this writer can actually read.
///
/// The note in shared storage says acknowledged rows exist that no flush has
/// covered, and names the write-ahead log they are in, as the writer that left
/// it named its own. Equal names mean one log and an ordinary replay; any
/// other means the rows are somewhere this writer cannot reach. The engine
/// names every log it opens, so an unnamed writer only meets unnamed notes
/// where one process's tables share one log.
///
/// An unreachable tail is refused rather than replayed-as-empty, because the
/// alternative is a stream that silently loses rows a client was told were
/// stored. The fix is to drain it where it lives - check point the writer that
/// holds it - after which any writer may take the stream.
async fn reachable_tail(dataset: &Dataset, config: &TableConfig) -> lance::Result<()> {
    let store = dataset.object_store(None).await?;
    let base = dataset.branch_location().path;
    let note = crate::open_tail::read(&store, &base, config.shard_id, &config.stream).await?;
    let Some(note) = note else {
        return Ok(());
    };
    if note.log == config.log {
        return Ok(());
    }
    Err(lance::Error::invalid_input(format!(
        "stream {} has rows a writer acknowledged and no flush has covered, and they are {}. \
         Opening here would serve the stream without them. Check point the writer that holds \
         them - it is the one whose log has the tail - and any writer may take the stream \
         afterwards.",
        config.stream, "not in the write-ahead log this writer reads"
    )))
}

/// Whether this error is two openers racing for the same writer epoch rather
/// than anything being wrong.
///
/// Claiming is a compare-and-swap on the shard manifest. When two opens
/// overlap, as a reconfigure reopening a stream does with the warm-up opening
/// it, one commits the epoch and the other finds it taken. The loser has not
/// been fenced and nothing is damaged: the manifest simply moved under it, and
/// reading it again gives a claim that works. Worth retrying, and no use
/// reporting.
#[must_use]
pub fn is_claim_race(error: &lance::Error) -> bool {
    let said = error.to_string();
    said.contains("another writer claimed epoch")
        || (said.contains("Failed to claim shard") && said.contains("already exists"))
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
    let store = ShardManifestStore::new(object_store, &base_path, shard_id, MANIFEST_SCAN_BATCH);
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
    // Whether Bitr ever held the stream is the log's extent, not its history:
    // reading the records to find out would cost every claim the table's
    // whole life.
    let extent = writer
        .extent(stream)
        .await
        .map_err(|e| lance::Error::io(format!("Bitr extent for {stream}: {e}")))?;
    if extent.ever_written() {
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

/// How many manifest versions past the version hint one probe checks at
/// once, as the writer's own manifest reads do. The hint is kept current, so
/// the newest version is almost always the hint or the one after it: a wider
/// probe finds nothing more and pays a HEAD per version for it, which under
/// several opens at once is most of what reading the epoch costs.
const MANIFEST_SCAN_BATCH: usize = 2;

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
    let store = ShardManifestStore::new(object_store, &base_path, shard_id, MANIFEST_SCAN_BATCH);
    Ok(store
        .read_latest()
        .await?
        .map(|m| m.writer_epoch + 1)
        .unwrap_or(1))
}

fn shard_snapshot(shard_id: Uuid, manifest: &lance_index::mem_wal::ShardManifest) -> ShardSnapshot {
    manifest.sstables.iter().fold(
        ShardSnapshot::new(shard_id)
            .with_spec_id(manifest.shard_spec_id)
            .with_current_generation(manifest.current_generation),
        |s, t| s.with_sstable(t.generation, t.path.clone()),
    )
}

/// A memtable frozen by [`Table::seal`], and everything frozen before it.
pub struct Sealed(lance::dataset::mem_wal::SealFence);
impl Sealed {
    /// Until every one of them is a generation.
    pub async fn flushed(self) -> lance::Result<()> {
        self.0.wait().await
    }
}

/// Flushed generations to open into a table's caches, captured from its
/// writer by [`Table::warmer`].
pub struct Warmer {
    dataset: Arc<Dataset>,
    snapshot: ShardSnapshot,
    sstables: Arc<SsTableCache>,
}
impl Warmer {
    /// Open every generation and load its indexes. Returns how many.
    pub async fn run(self) -> lance::Result<usize> {
        let generations = self.snapshot.sstables.len();
        let cache: Arc<dyn DatasetCache> = self.sstables;
        self.dataset
            .prewarm_mem_wal(&[self.snapshot], Some(&cache))
            .await?;
        Ok(generations)
    }
}

/// A table loaded read-only by a process that does not own it yet, so that
/// taking it over costs the claim and nothing else.
///
/// Loading reads the dataset, its indexes and every flushed generation into
/// this process's caches; it writes nothing and claims nothing, so the owner
/// is untouched. [`Table::open_prepared`] then opens the writer on it.
pub struct Prepared {
    dataset: Dataset,
    sstables: Arc<SsTableCache>,
}
impl Prepared {
    /// The table's dataset, or `None` when it has never been written.
    pub async fn load(
        config: &TableConfig,
        storage: &LanceStorageOptions,
    ) -> lance::Result<Option<Self>> {
        match storage.open_dataset(&config.uri).await {
            Ok(dataset) => Ok(Some(Self {
                dataset,
                sstables: Arc::new(SsTableCache::new(256)),
            })),
            Err(lance::Error::DatasetNotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }
    /// Load the dataset's indexes and open every flushed generation the
    /// shard's manifest names now, into the caches the writer will read
    /// through. Returns how many generations.
    pub async fn warm(&self, shard_id: Uuid) -> lance::Result<usize> {
        // The MemWAL's own index is metadata with no file to load.
        let mut seen = std::collections::HashSet::new();
        for index in self.dataset.load_indices().await?.iter() {
            if !lance_index::is_system_index(index) && seen.insert(index.name.clone()) {
                self.dataset.prewarm_index(&index.name).await?;
            }
        }
        let Some(manifest) = self.shard_manifests(shard_id).await?.read_latest().await? else {
            return Ok(0);
        };
        Warmer {
            dataset: Arc::new(self.dataset.clone()),
            snapshot: shard_snapshot(shard_id, &manifest),
            sstables: self.sstables.clone(),
        }
        .run()
        .await
    }
    /// Move to the dataset's latest version. A compaction may have committed
    /// since the load.
    pub async fn refresh(&mut self) -> lance::Result<()> {
        self.dataset.checkout_latest().await
    }
    /// As [`next_writer_epoch`], read through this dataset.
    pub async fn next_writer_epoch(&self, shard_id: Uuid) -> lance::Result<u64> {
        Ok(self
            .shard_manifests(shard_id)
            .await?
            .read_latest()
            .await?
            .map(|m| m.writer_epoch + 1)
            .unwrap_or(1))
    }
    async fn shard_manifests(
        &self,
        shard_id: Uuid,
    ) -> lance::Result<lance::dataset::mem_wal::ShardManifestStore> {
        let object_store = self.dataset.object_store(None).await?;
        let base_path = self.dataset.branch_location().path;
        Ok(lance::dataset::mem_wal::ShardManifestStore::new(
            object_store,
            &base_path,
            shard_id,
            MANIFEST_SCAN_BATCH,
        ))
    }
}
