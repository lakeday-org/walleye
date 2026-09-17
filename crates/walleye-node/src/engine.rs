//! One deployment's stream registry. Definitions use object-store create-if-absent;
//! one designated ingress owns a separately locked memshard for each stream.
use crate::cluster::{Cluster, NotOwner};
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine as _;
use futures::{StreamExt, TryStreamExt};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use object_store::ObjectStoreExt;
use object_store::{PutMode, PutOptions, path::Path};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard, RwLock};
use walleye_bitr::{HttpReplica, QuorumWriter};
use walleye_lance::{
    BitrWalBackend, CachedStorage, CompactionResult, JsonSchema, LanceDurability,
    LanceStorageOptions, LsmStats, SearchRequest, SnapshotSource, Table, TableConfig,
    TableSnapshot, VectorIndexSpec,
};
use walleye_ring::Node;

/// Merge flushed generations once this many exist.
pub const COMPACT_MIN_SSTABLES: usize = 8;
/// The largest stream one member will hand to another for a query that spans
/// owners. Beyond it the query must run where the data lives.
pub const GATHER_ROW_LIMIT: usize = 1_000_000;
/// A table's rows taken from its owner, with the schema they arrived under.
type GatheredTable = (String, Arc<Schema>, Vec<RecordBatch>);
/// Minimum spacing between automatic compaction attempts per table.
const COMPACT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Server-managed primary key for tables created without one. It is an xxh3
/// hash of the row's full contents, so an identical row (including a retried
/// insert) collapses to one visible row and every memshard stays idempotent.
pub const HIDDEN_PK: &str = "_walleye_pk";
/// Every row's arrival order within its stream, assigned on append and never
/// reused. A consumer remembers the last one it processed, which is what lets
/// a tier be rebuilt from where it stopped instead of from the beginning.
pub const HIDDEN_SEQ: &str = "_walleye_seq";
/// Field metadata that marks a user-supplied primary key column.
pub const PK_METADATA_KEY: &str = "lance-schema:unenforced-primary-key";

#[derive(Debug)]
pub struct TableNotFound(pub String);
impl std::fmt::Display for TableNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table {} not found", self.0)
    }
}
impl std::error::Error for TableNotFound {}

#[derive(Debug)]
pub struct TableExists(pub String);
impl std::fmt::Display for TableExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table {} already exists", self.0)
    }
}
impl std::error::Error for TableExists {}

/// Catalog requests are small, but they are still remote object-store
/// operations. Emit bounded stage markers so a process killed by its startup
/// watchdog leaves evidence of the operation that was waiting. Do not include
/// the root URI or error text: either can contain tenant-specific or secret
/// material in an object-store configuration.
fn catalog_stage_start(stage: &str, stream: &str) -> Instant {
    let started = Instant::now();
    eprintln!(
        "walleye.storage catalog stream={} stage={} phase=start",
        stream, stage
    );
    started
}

fn catalog_stage_finish(stage: &str, stream: &str, started: Instant, outcome: &str) {
    eprintln!(
        "walleye.storage catalog stream={} stage={} phase=finish elapsed_ms={} outcome={}",
        stream,
        stage,
        started.elapsed().as_millis(),
        outcome,
    );
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    pub root_uri: String,
    /// Omit for single-node S3 CAS. Set to the local Bitr gateway for cluster mode.
    pub bitr_url: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub nullable: bool,
}
/// A stream definition as a client sends it. Strict on purpose: a misspelled
/// field in a request is a mistake worth reporting, and a client may not set
/// the fields the server derives.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamRequest {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
}
impl From<StreamRequest> for StreamDefinition {
    fn from(request: StreamRequest) -> Self {
        Self {
            name: request.name,
            columns: request.columns,
            primary_key: request.primary_key,
            schema: None,
            vector_indexes: Vec::new(),
        }
    }
}

/// A stream definition as the catalog stores it. Deliberately tolerant of
/// fields it does not know: a definition written by a later version must
/// still open here, or an upgrade could not be rolled back.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StreamDefinition {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    /// Full Arrow schema in Lance JSON form, for tables created through the
    /// LanceDB API. Includes the hidden primary key when one was added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// Vector indexes maintained on the memtable and every generation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector_indexes: Vec<VectorIndexSpec>,
}
/// Columns the node maintains and clients neither write nor see.
pub fn hidden(column: &str) -> bool {
    column == HIDDEN_PK || column == HIDDEN_SEQ
}
fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros() as u64)
        .unwrap_or(0)
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
}
impl StreamDefinition {
    /// Build a definition from a LanceDB `create_table` schema. Fields tagged
    /// with [`PK_METADATA_KEY`] form the primary key; otherwise a hidden
    /// content-hash key is appended so the memshard stays idempotent.
    pub fn from_arrow(name: &str, schema: &Schema) -> Result<Self, Error> {
        if !valid_name(name) {
            return Err("invalid table name".into());
        }
        if schema.fields().is_empty() {
            return Err("table requires at least one column".into());
        }
        let mut fields = Vec::with_capacity(schema.fields().len() + 1);
        let mut primary_key = Vec::new();
        for field in schema.fields() {
            if field.name().starts_with('_') {
                return Err(format!("column {} uses a reserved name", field.name()).into());
            }
            let marked = field.metadata().get(PK_METADATA_KEY).map(|v| v == "true") == Some(true);
            if marked {
                primary_key.push(field.name().clone());
                fields.push(field.as_ref().clone().with_nullable(false));
            } else {
                fields.push(field.as_ref().clone());
            }
        }
        if primary_key.is_empty() {
            fields.push(Field::new(HIDDEN_PK, DataType::UInt64, false));
            primary_key.push(HIDDEN_PK.into());
        }
        // Last, so the arrival order is never part of a row's identity: two
        // appends of the same content must still collapse on the key.
        fields.push(Field::new(HIDDEN_SEQ, DataType::UInt64, false));
        let schema = Schema::new(fields);
        // Every vector column gets an HNSW index from the first row; the
        // metric can be changed later through create_index.
        let vector_indexes = schema
            .fields()
            .iter()
            .filter(|f| {
                matches!(f.data_type(), DataType::FixedSizeList(inner, _)
                    if inner.data_type() == &DataType::Float32)
            })
            .map(|f| VectorIndexSpec {
                name: format!("{}_idx", f.name()),
                column: f.name().clone(),
                metric: "l2".into(),
            })
            .collect();
        let json = JsonSchema::try_from(&schema)?;
        Ok(Self {
            name: name.into(),
            columns: Vec::new(),
            primary_key,
            schema: Some(serde_json::to_value(json)?),
            vector_indexes,
        })
    }
    /// Same table shape: everything except the index configuration.
    fn same_shape(&self, other: &Self) -> bool {
        self.name == other.name
            && self.columns == other.columns
            && self.primary_key == other.primary_key
            && self.schema == other.schema
    }
    fn hidden_pk(&self) -> bool {
        self.primary_key.len() == 1 && self.primary_key[0] == HIDDEN_PK
    }
    fn arrow_schema(&self) -> Result<Schema, Error> {
        let Some(value) = &self.schema else {
            return Err("definition has no Arrow schema".into());
        };
        let json: JsonSchema = serde_json::from_value(value.clone())?;
        Ok(Schema::try_from(json)?)
    }
    /// Schema as clients see it: without the hidden primary key.
    pub fn user_schema(&self, full: &Schema) -> Schema {
        Schema::new(
            full.fields()
                .iter()
                .filter(|f| !hidden(f.name()))
                .cloned()
                .collect::<Vec<_>>(),
        )
    }
    fn table_config(&self, root: &str) -> Result<TableConfig, Error> {
        if self.schema.is_some() {
            if !self.columns.is_empty() {
                return Err("definition must use either columns or schema".into());
            }
            let schema = self.arrow_schema()?;
            return Ok(TableConfig::new(
                &self.name,
                format!("{}/data/{}", root.trim_end_matches('/'), self.name),
                Arc::new(schema),
                self.primary_key.clone(),
            )?
            .with_vector_indexes(self.vector_indexes.clone())?);
        }
        let mut seen = std::collections::HashSet::new();
        let fields = self
            .columns
            .iter()
            .map(|c| {
                if c.name.is_empty() || !seen.insert(&c.name) {
                    return Err("invalid or duplicate column".into());
                }
                if c.name.starts_with('_') {
                    return Err(format!("column {} uses a reserved name", c.name).into());
                }
                let t = match c.kind.as_str() {
                    "string" => DataType::Utf8,
                    "int64" => DataType::Int64,
                    "float64" => DataType::Float64,
                    "boolean" => DataType::Boolean,
                    _ => return Err("type must be string, int64, float64, or boolean".into()),
                };
                if c.nullable && self.primary_key.contains(&c.name) {
                    return Err("primary key must be non-nullable".into());
                }
                Ok(Field::new(&c.name, t, c.nullable))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        if fields.is_empty()
            || self
                .primary_key
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.primary_key.len()
        {
            return Err("columns and distinct primary keys are required".into());
        }
        // Every stream carries arrival order, however it was declared. A
        // consumer's cursor is useless on a stream that does not have it.
        let mut fields = fields;
        fields.push(Field::new(HIDDEN_SEQ, DataType::UInt64, false));
        Ok(TableConfig::new(
            &self.name,
            format!("{}/data/{}", root.trim_end_matches('/'), self.name),
            Arc::new(Schema::new(fields)),
            self.primary_key.clone(),
        )?)
    }
}
struct Stream {
    definition: StreamDefinition,
    config: TableConfig,
    storage: LanceStorageOptions,
    resources: Arc<walleye_cache::QueryResources>,
    /// Memory held for this stream's memtables and memtable indexes while
    /// its writer is open.
    lease: Mutex<Option<walleye_cache::MemoryLease>>,
    /// Bitr quorum writer in cluster mode; the WAL backend is minted per open
    /// so its epoch matches the MemWAL claim.
    bitr: Option<Arc<QuorumWriter>>,
    table: Mutex<Option<Table>>,
    /// Monotonic write version reported to LanceDB clients.
    version: AtomicU64,
    /// When this stream's table was last used, for closing idle tables and
    /// returning their memory to the budget.
    last_used: Mutex<Instant>,
    /// One automatic compaction in flight at a time, spaced by COMPACT_INTERVAL.
    compacting: std::sync::atomic::AtomicBool,
    last_compaction: Mutex<Option<Instant>>,
    /// Next arrival number to hand out, once the stream's high-water mark is
    /// known. Unset until the first append after opening.
    seq: Mutex<Option<u64>>,
}
impl Stream {
    /// Bytes this stream's writer may hold in memory: the memtable size and
    /// unflushed bound the table is opened with, plus every vector index's
    /// graph and storage at the memtable's row capacity.
    fn memory_footprint(&self) -> usize {
        const MEMTABLE_BYTES: usize = 16 * 1024 * 1024 + 32 * 1024 * 1024;
        const MEMTABLE_ROWS: usize = 100_000;
        let vectors: usize = self
            .config
            .vector_indexes
            .iter()
            .filter_map(|spec| self.config.schema.field_with_name(&spec.column).ok())
            .map(|field| match field.data_type() {
                DataType::FixedSizeList(_, dim) => MEMTABLE_ROWS * (*dim as usize * 4 + 128),
                _ => 0,
            })
            .sum();
        MEMTABLE_BYTES + vectors
    }
    /// Drop a writer Lance has fenced, so the next use opens a fresh one that
    /// claims the next epoch and replays the WAL. A fenced writer is a dead
    /// handle, not a dead stream, and keeping it would leave the stream
    /// unreadable and unwritable until the process restarted. Returns whether
    /// it discarded one.
    async fn discard_fenced_writer(&self, reason: walleye_lance::FenceReason) -> bool {
        let taken = self.table.lock().await.take();
        let Some(table) = taken else {
            return false;
        };
        // A fenced writer cannot flush; never let closing it hold a request.
        let closing = tokio::time::timeout(std::time::Duration::from_secs(10), table.close());
        if closing.await.is_err() {
            eprintln!(
                "walleye.storage writer_fenced stream={} reason={reason} close=timeout",
                self.definition.name
            );
        }
        // Release the budget the dead writer held. Reopening reserves before
        // it replaces the lease, so keeping this one would make the stream
        // pay for itself twice and, once that reservation fails, strand the
        // lease for good: the idle sweeper skips a stream with no table.
        *self.lease.lock().await = None;
        eprintln!(
            "walleye.storage writer_fenced stream={} reason={reason} outcome=discarded",
            self.definition.name
        );
        true
    }
    async fn table(&self) -> Result<MappedMutexGuard<'_, Table>, Error> {
        *self.last_used.lock().await = Instant::now();
        let mut table = self.table.lock().await;
        if table.is_none() {
            let durability = match &self.bitr {
                Some(writer) => {
                    if walleye_lance::prepare_bitr_takeover(
                        &self.storage,
                        &self.config.uri,
                        self.config.shard_id,
                        &self.config.stream,
                        writer,
                    )
                    .await?
                    {
                        eprintln!(
                            "walleye.storage takeover stream={} outcome=reset_wal_positions",
                            self.definition.name
                        );
                    }
                    let epoch = walleye_lance::next_writer_epoch(
                        &self.storage,
                        &self.config.uri,
                        self.config.shard_id,
                    )
                    .await?;
                    LanceDurability::Bitr(Arc::new(BitrWalBackend::new(
                        writer.clone(),
                        &self.config.stream,
                        self.config.shard_id,
                        epoch,
                    )?))
                }
                None => LanceDurability::ObjectStore,
            };
            // Fail closed before opening: a writer we cannot afford must not
            // exist. Memtable plus unflushed bound, plus the in-memory vector
            // graph sized for the memtable's row capacity.
            let lease = self.resources.reserve_memory(
                &format!("table {}", self.definition.name),
                self.memory_footprint(),
            )?;
            *self.lease.lock().await = Some(lease);
            *table =
                Some(Table::open(self.config.clone(), self.storage.clone(), durability).await?);
        }
        Ok(MutexGuard::map(table, |table| {
            table.as_mut().expect("initialized writer")
        }))
    }
}
#[async_trait::async_trait]
impl SnapshotSource for Stream {
    fn schema(&self) -> Arc<Schema> {
        // Dataset ownership metadata is not part of the SQL result schema.
        Arc::new(Schema::new(self.config.schema.fields().clone()))
    }
    async fn snapshot(&self) -> Result<TableSnapshot, walleye_lance::LanceError> {
        for attempt in 0..2 {
            let outcome = async {
                let table = self.table().await.map_err(|e| {
                    match walleye_lance::writer_fence_reason(&*e) {
                        // Keep the typed fence: a reader must be able to tell
                        // a dead handle from a dead stream too.
                        Some(walleye_lance::FenceReason::PeerClaimedEpoch) => {
                            walleye_lance::LanceError::fenced_by_peer(e.to_string())
                        }
                        Some(walleye_lance::FenceReason::PersistenceFailure) => {
                            walleye_lance::LanceError::writer_poisoned(e.to_string())
                        }
                        None => walleye_lance::LanceError::io(e.to_string()),
                    }
                })?;
                table.snapshot().await
            }
            .await;
            let error = match outcome {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) => error,
            };
            match error.fence_reason() {
                Some(reason) if attempt == 0 && self.discard_fenced_writer(reason).await => {
                    continue;
                }
                _ => return Err(error),
            }
        }
        unreachable!("the loop returns on both outcomes")
    }
}
pub struct Engine {
    config: ApiConfig,
    cache: CachedStorage,
    catalog: Arc<ObjectStore>,
    catalog_path: Path,
    views_path: Path,
    data_path: Path,
    streams: Mutex<BTreeMap<String, Arc<Stream>>>,
    // Names whose drop is still in flight. A catalog load that read the
    // definition object before `drop_table` deleted it would otherwise
    // register a stream that outlives the drop, and `register` only inserts
    // when the entry is absent, so that stale handle would go on to win
    // against the definition a later create publishes. Locked after
    // `streams`, never before it.
    dropping: Mutex<BTreeSet<String>>,
    // The catalog is immutable within one deployment authority except for
    // definitions admitted through this Engine. Avoid listing object storage
    // for every SQL request, while still allowing the query path to refresh
    // once when another node has published a previously unknown stream.
    catalog_loaded: Mutex<bool>,
    // Requests share this read lock; shutdown waits for all active requests.
    closed: RwLock<bool>,
    writer: Option<Arc<QuorumWriter>>,
    cluster: Option<Cluster>,
}
impl Engine {
    pub async fn open(
        config: ApiConfig,
        cache: CachedStorage,
        params: ObjectStoreParams,
        cluster: Option<Cluster>,
    ) -> Result<Self, Error> {
        let (catalog, prefix) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            &config.root_uri,
            &params,
        )
        .await?;
        let writer = if let Some(url) = &config.bitr_url {
            use hmac::{Hmac, Mac};
            use sha2::Sha256;
            let root = base64::engine::general_purpose::STANDARD
                .decode(std::env::var("LAKEDAY_DATAPLANE_ROOT_KEY")?)?;
            if root.len() != 32 {
                return Err("Bitr root key must contain 32 bytes".into());
            }
            let mut hmac = Hmac::<Sha256>::new_from_slice(&root)?;
            hmac.update(
                b"lakeday-cloud/deployment-identity/v1\0walleye\0replica-gateway-authentication",
            );
            let token = hex::encode(hmac.finalize().into_bytes());
            let key: [u8; 32] = base64::engine::general_purpose::STANDARD
                .decode(std::env::var("WALLEYE_DATA_KEY")?)?
                .try_into()
                .map_err(|_| "data key must contain 32 bytes")?;
            Some(Arc::new(QuorumWriter::new(
                Arc::new(HttpReplica::new(url, token)),
                key,
            )))
        } else {
            None
        };
        let engine = Self {
            config,
            cache,
            catalog,
            catalog_path: prefix.clone().join("streams"),
            views_path: prefix.clone().join("views"),
            data_path: prefix.join("data"),
            streams: Mutex::new(BTreeMap::new()),
            dropping: Mutex::new(BTreeSet::new()),
            catalog_loaded: Mutex::new(false),
            closed: RwLock::new(false),
            writer,
            cluster,
        };
        // Open writers lazily: the combined Bitr service starts after configuration loads.
        Ok(engine)
    }
    async fn register(&self, definition: StreamDefinition) -> Result<Arc<Stream>, Error> {
        let config = definition.table_config(&self.config.root_uri)?;
        let mut streams = self.streams.lock().await;
        // A drop in flight owns this name until it finishes. Reinstating it
        // here would resurrect the stream the drop is removing.
        if self.dropping.lock().await.contains(&definition.name) {
            return Err(Box::new(TableNotFound(definition.name)));
        }
        let stream = streams.entry(definition.name.clone()).or_insert_with(|| {
            Arc::new(Stream {
                definition,
                config,
                bitr: self.writer.clone(),
                storage: self.cache.storage.clone(),
                table: Mutex::new(None),
                version: AtomicU64::new(1),
                last_used: Mutex::new(Instant::now()),
                compacting: std::sync::atomic::AtomicBool::new(false),
                resources: self.cache.resources.clone(),
                lease: Mutex::new(None),
                last_compaction: Mutex::new(None),
                seq: Mutex::new(None),
            })
        });
        Ok(stream.clone())
    }
    /// The member that owns `name`, or `None` when this node does. Single
    /// node deployments own everything.
    pub fn owner(&self, name: &str) -> Option<Node> {
        self.cluster.as_ref().and_then(|c| c.owner(name))
    }
    pub fn cluster(&self) -> Option<&Cluster> {
        self.cluster.as_ref()
    }
    /// The one memory and disk budget every allocation in this process
    /// borrows from.
    pub fn resources(&self) -> &Arc<walleye_cache::QueryResources> {
        &self.cache.resources
    }
    /// A stream this node may write: its definition, after confirming
    /// ownership. A stream that moved to another member has its local writer
    /// closed so the new owner's epoch claim is the only live writer.
    async fn stream(&self, name: &str) -> Result<Arc<Stream>, Error> {
        let stream = self.definition(name).await?;
        if let Some(owner) = self.owner(name) {
            if let Some(mut table) = stream.table.lock().await.take() {
                let _ = table.checkpoint().await;
                let _ = table.close().await;
            }
            *stream.lease.lock().await = None;
            return Err(Box::new(NotOwner(owner)));
        }
        Ok(stream)
    }
    /// The registered definition, loading it from the catalog if needed.
    /// Does not open a writer and does not check ownership.
    async fn definition(&self, name: &str) -> Result<Arc<Stream>, Error> {
        if let Some(stream) = self.streams.lock().await.get(name).cloned() {
            return Ok(stream);
        }
        // Validate before constructing an object key from a request path.
        if !valid_name(name) {
            return Err("invalid stream name".into());
        }
        let path = self.catalog_path.clone().join(format!("{name}.json"));
        let get_started = catalog_stage_start("catalog_get", name);
        let data = match self.catalog.inner.get(&path).await {
            Ok(data) => match data.bytes().await {
                Ok(data) => {
                    catalog_stage_finish("catalog_get", name, get_started, "ok");
                    data
                }
                Err(error) => {
                    catalog_stage_finish("catalog_get", name, get_started, "error");
                    return Err(error.into());
                }
            },
            Err(object_store::Error::NotFound { .. }) => {
                catalog_stage_finish("catalog_get", name, get_started, "missing");
                return Err(Box::new(TableNotFound(name.into())));
            }
            Err(error) => {
                catalog_stage_finish("catalog_get", name, get_started, "error");
                return Err(error.into());
            }
        };
        let definition: StreamDefinition = serde_json::from_slice(&data)?;
        if definition.name != name {
            return Err("stream definition name does not match its path".into());
        }
        self.register(definition).await
    }
    async fn load_catalog(&self) -> Result<(), Error> {
        let mut loaded = self.catalog_loaded.lock().await;
        if *loaded {
            return Ok(());
        }
        self.load_catalog_objects().await?;
        *loaded = true;
        Ok(())
    }

    async fn refresh_catalog(&self) -> Result<(), Error> {
        let mut loaded = self.catalog_loaded.lock().await;
        self.load_catalog_objects().await?;
        *loaded = true;
        Ok(())
    }

    async fn load_catalog_objects(&self) -> Result<(), Error> {
        let list_started = catalog_stage_start("catalog_list", "*");
        let objects: Vec<_> = match self
            .catalog
            .inner
            .list(Some(&self.catalog_path))
            .try_collect()
            .await
        {
            Ok(objects) => {
                catalog_stage_finish("catalog_list", "*", list_started, "ok");
                objects
            }
            Err(error) => {
                catalog_stage_finish("catalog_list", "*", list_started, "error");
                return Err(error.into());
            }
        };
        for obj in objects {
            if obj.location.extension() != Some("json") {
                continue;
            }
            if let Some(name) = obj
                .location
                .filename()
                .and_then(|s| s.strip_suffix(".json"))
            {
                match self.definition(name).await {
                    Ok(_) => {}
                    // The listing is a snapshot. A table dropped before this
                    // fetch reached it must not fail the load for every
                    // other table in the catalog.
                    Err(error) if error.downcast_ref::<TableNotFound>().is_some() => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
    pub async fn define(&self, definition: StreamDefinition) -> Result<(), Error> {
        self.define_with(definition, true).await
    }
    /// Create a table. An identical existing definition is accepted when
    /// `exist_ok` is set and rejected with [`TableExists`] otherwise; a
    /// different existing definition is always rejected.
    pub async fn define_with(
        &self,
        definition: StreamDefinition,
        exist_ok: bool,
    ) -> Result<(), Error> {
        definition.table_config(&self.config.root_uri)?;
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        let path = self
            .catalog_path
            .clone()
            .join(format!("{}.json", definition.name));
        let bytes = serde_json::to_vec(&definition)?;
        let put_started = catalog_stage_start("catalog_put", &definition.name);
        match self
            .catalog
            .inner
            .put_opts(
                &path,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => catalog_stage_finish("catalog_put", &definition.name, put_started, "created"),
            Err(object_store::Error::AlreadyExists { .. }) => {
                catalog_stage_finish(
                    "catalog_put",
                    &definition.name,
                    put_started,
                    "already_exists",
                );
                let get_started = catalog_stage_start("catalog_get", &definition.name);
                let existing_bytes = match self.catalog.inner.get(&path).await {
                    Ok(data) => match data.bytes().await {
                        Ok(data) => data,
                        Err(error) => {
                            catalog_stage_finish(
                                "catalog_get",
                                &definition.name,
                                get_started,
                                "error",
                            );
                            return Err(error.into());
                        }
                    },
                    Err(error) => {
                        catalog_stage_finish("catalog_get", &definition.name, get_started, "error");
                        return Err(error.into());
                    }
                };
                let existing: StreamDefinition = match serde_json::from_slice(&existing_bytes) {
                    Ok(existing) => existing,
                    Err(error) => {
                        catalog_stage_finish(
                            "catalog_get",
                            &definition.name,
                            get_started,
                            "invalid_json",
                        );
                        return Err(error.into());
                    }
                };
                catalog_stage_finish("catalog_get", &definition.name, get_started, "ok");
                if !existing.same_shape(&definition) {
                    return Err(format!(
                        "table {} already exists with a different definition",
                        definition.name
                    )
                    .into());
                }
                if !exist_ok {
                    return Err(Box::new(TableExists(definition.name.clone())));
                }
                // The stored definition carries any index changes made since creation.
                let stream = self.register(existing).await?;
                drop(stream.table().await?);
                return Ok(());
            }
            Err(error) => {
                catalog_stage_finish("catalog_put", &definition.name, put_started, "error");
                return Err(error.into());
            }
        }
        let stream = self.register(definition).await?;
        drop(stream.table().await?);
        Ok(())
    }
    pub async fn ingest(&self, name: &str, rows: Vec<serde_json::Value>) -> Result<usize, Error> {
        if rows.is_empty() {
            return Err("rows must not be empty".into());
        }
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        let stream = self.stream(name).await?;
        let allowed: std::collections::HashSet<_> = stream
            .definition
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        for row in &rows {
            let object = row.as_object().ok_or("each row must be an object")?;
            if object.keys().any(|k| !allowed.contains(k.as_str())) {
                return Err("row contains unknown columns".into());
            }
            for c in &stream.definition.columns {
                let value = object.get(&c.name).unwrap_or(&serde_json::Value::Null);
                let valid = if value.is_null() {
                    c.nullable
                } else {
                    match c.kind.as_str() {
                        "string" => value.is_string(),
                        "int64" => value.as_i64().is_some(),
                        "float64" => value.is_number(),
                        "boolean" => value.is_boolean(),
                        _ => false,
                    }
                };
                if !valid {
                    return Err(format!("invalid value for column {}", c.name).into());
                }
            }
        }
        let count = rows.len();
        let mut ndjson = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut ndjson, &row)?;
            ndjson.push(b'\n');
        }
        // Decode against the columns a client declared, then let the shared
        // path add what the node maintains. Decoding against the full schema
        // would leave the arrival order null, and it is not nullable.
        let declared = Arc::new(Schema::new(
            stream
                .config
                .schema
                .fields()
                .iter()
                .filter(|field| !hidden(field.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let decoded = arrow_json::ReaderBuilder::new(declared)
            .with_batch_size(1024)
            .build(Cursor::new(ndjson))?
            .collect::<Result<Vec<RecordBatch>, _>>()?;
        let mut next = self
            .reserve_seq(
                name,
                &stream,
                decoded.iter().map(|b| b.num_rows() as u64).sum(),
            )
            .await?;
        let mut batches = Vec::with_capacity(decoded.len());
        for batch in decoded {
            let rows = batch.num_rows() as u64;
            batches.push(conform_batch(
                &stream.definition,
                &stream.config.schema,
                batch,
                next,
            )?);
            next = next.saturating_add(rows);
        }
        self.append_prepared(&stream, batches).await?;
        stream.version.fetch_add(1, Ordering::AcqRel);
        Ok(count)
    }
    /// Append Arrow batches from a LanceDB client. Returns the new table version.
    pub async fn append(&self, name: &str, batches: Vec<RecordBatch>) -> Result<u64, Error> {
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        let stream = self.stream(name).await?;
        let full = stream.config.schema.clone();
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut next = self.reserve_seq(name, &stream, rows).await?;
        let mut prepared = Vec::with_capacity(batches.len());
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let rows = batch.num_rows() as u64;
            prepared.push(conform_batch(&stream.definition, &full, batch, next)?);
            next = next.saturating_add(rows);
        }
        if prepared.is_empty() {
            return Ok(stream.version.load(Ordering::Acquire));
        }
        self.append_prepared(&stream, prepared).await?;
        self.maybe_compact(&stream).await;
        Ok(stream.version.fetch_add(1, Ordering::AcqRel) + 1)
    }
    /// Claim `rows` consecutive arrival numbers for a stream.
    ///
    /// The first claim after opening has to learn where the stream left off,
    /// because the counter lives in memory and the rows outlive the process.
    /// It takes the larger of the stored high-water mark and the current
    /// clock, so a clock that steps backwards can still not hand out a number
    /// a consumer has already passed. Handing out a number below a cursor
    /// would hide those rows from it for good.
    async fn reserve_seq(&self, name: &str, stream: &Arc<Stream>, rows: u64) -> Result<u64, Error> {
        let mut seq = stream.seq.lock().await;
        let next = match *seq {
            Some(next) => next,
            None => self.highest_seq(name).await?.max(now_micros()),
        };
        *seq = Some(next.saturating_add(rows.max(1)));
        Ok(next)
    }
    /// The largest arrival number the stream already holds, or zero when it
    /// holds none. Paid once per stream per process.
    async fn highest_seq(&self, name: &str) -> Result<u64, Error> {
        let sql = format!("SELECT max({HIDDEN_SEQ}) AS high FROM \"{name}\"");
        let bytes = match self.query_loaded(&sql, &[]).await {
            Ok(bytes) => bytes,
            // A stream with no rows yet, or one whose writer is not open,
            // simply has no mark to beat.
            Err(_) => return Ok(0),
        };
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap_or_default();
        Ok(rows
            .first()
            .and_then(|row| row.get("high"))
            .and_then(serde_json::Value::as_u64)
            .map_or(0, |high| high.saturating_add(1)))
    }
    /// Append, reopening once if the writer turns out to be fenced. Lance
    /// fences a writer whose WAL append failed or whose epoch a peer took;
    /// the handle is dead, the shard is not, and a fresh writer replays the
    /// WAL. Rows the fenced attempt did persist come back in that replay and
    /// collapse against these on the primary key.
    async fn append_prepared(
        &self,
        stream: &Arc<Stream>,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Error> {
        for attempt in 0..2 {
            let outcome = async {
                stream.table().await?.append(batches.clone()).await?;
                Ok::<(), Error>(())
            }
            .await;
            let error = match outcome {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            let reason = walleye_lance::writer_fence_reason(&*error);
            match reason {
                Some(reason) if attempt == 0 && stream.discard_fenced_writer(reason).await => {
                    continue;
                }
                _ => return Err(error),
            }
        }
        unreachable!("the loop returns on both outcomes")
    }
    /// Spawn one background merge per table when enough generations exist.
    async fn maybe_compact(&self, stream: &Arc<Stream>) {
        {
            let last = stream.last_compaction.lock().await;
            if last.is_some_and(|t| t.elapsed() < COMPACT_INTERVAL) {
                return;
            }
        }
        if stream.compacting.swap(true, Ordering::AcqRel) {
            return;
        }
        let stream = stream.clone();
        let query_timeout = self.cache.storage.query_timeout();
        tokio::spawn(async move {
            let outcome = compact_stream(&stream, COMPACT_MIN_SSTABLES, query_timeout, false).await;
            *stream.last_compaction.lock().await = Some(Instant::now());
            stream.compacting.store(false, Ordering::Release);
            if let Err(error) = outcome {
                eprintln!(
                    "walleye.storage compaction stream={} outcome=error error={}",
                    stream.definition.name, error
                );
            }
        });
    }
    /// Close tables left untouched for `idle`, returning their memory to the
    /// budget so another table can open. A table is reopened on its next use,
    /// so this costs a reopen, never data: the memtable is checkpointed
    /// first. Returns the number closed.
    pub async fn close_idle(&self, idle: std::time::Duration) -> usize {
        let streams: Vec<_> = self.streams.lock().await.values().cloned().collect();
        let mut closed = 0;
        for stream in streams {
            // Never wait: a stream in use is by definition not idle.
            let Ok(mut table) = stream.table.try_lock() else {
                continue;
            };
            if table.is_none() {
                continue;
            }
            let Ok(last_used) = stream.last_used.try_lock() else {
                continue;
            };
            if last_used.elapsed() < idle {
                continue;
            }
            let held = stream
                .lease
                .try_lock()
                .ok()
                .and_then(|lease| lease.as_ref().map(|lease| lease.bytes()))
                .unwrap_or(0);
            if let Some(mut open) = table.take() {
                if let Err(error) = open.checkpoint().await {
                    eprintln!(
                        "walleye.storage idle_close stream={} stage=checkpoint outcome=error error={error}",
                        stream.definition.name
                    );
                }
                let _ = open.close().await;
            }
            if let Ok(mut lease) = stream.lease.try_lock() {
                *lease = None;
            }
            closed += 1;
            eprintln!(
                "walleye.storage idle_close stream={} released_bytes={held}",
                stream.definition.name
            );
        }
        closed
    }
    /// Merge flushed generations now. Returns what was merged, if anything.
    pub async fn compact(&self, name: &str) -> Result<Option<CompactionResult>, Error> {
        let stream = self.stream(name).await?;
        compact_stream(&stream, 2, self.cache.storage.query_timeout(), false).await
    }
    /// Flush the memtable into a new generation.
    pub async fn flush(&self, name: &str) -> Result<(), Error> {
        let stream = self.stream(name).await?;
        stream.table().await?.checkpoint().await?;
        Ok(())
    }
    pub async fn lsm_stats(&self, name: &str) -> Result<LsmStats, Error> {
        let stream = self.stream(name).await?;
        Ok(stream.table().await?.lsm_stats().await?)
    }
    /// Open every stream this node owns and warm its generations, so the
    /// first request after startup does not pay the writer open, WAL replay,
    /// and index loads. Errors are logged per stream and never fatal.
    pub async fn warm(&self) {
        let names = match self.table_names().await {
            Ok(names) => names,
            Err(error) => {
                eprintln!("walleye.storage warm stage=catalog outcome=error error={error}");
                return;
            }
        };
        let owned = names.into_iter().filter(|n| self.owner(n).is_none());
        // Warming is an optimization: it opens streams before their first
        // request. It must leave room for the streams a client opens next, so
        // it plans against what each stream will hold and takes at most half
        // of what may be held. Planning beforehand, rather than checking as it
        // goes, is what keeps concurrent opens from overshooting together.
        // Streams it skips open on use, and idle ones are closed again.
        let budget = self.cache.resources.leasable() / 2;
        let mut planned = 0usize;
        let mut warming = Vec::new();
        let mut skipped = 0usize;
        for name in owned {
            let Ok(stream) = self.definition(&name).await else {
                continue;
            };
            let cost = stream.memory_footprint();
            if planned.saturating_add(cost) > budget {
                skipped += 1;
                continue;
            }
            planned += cost;
            warming.push(name);
        }
        if skipped > 0 {
            eprintln!(
                "walleye.storage warm outcome=partial warmed={} skipped={skipped} planned_bytes={planned} budget_bytes={budget}",
                warming.len()
            );
        }
        // Each open is a handful of object-storage round trips; overlap them.
        futures::stream::iter(warming)
            .for_each_concurrent(8, |name| async move {
                let started = Instant::now();
                let outcome = async {
                    let stream = self.stream(&name).await?;
                    let generations = stream.table().await?.warm().await?;
                    Ok::<usize, Error>(generations)
                }
                .await;
                match outcome {
                    Ok(generations) => eprintln!(
                        "walleye.storage warm stream={name} generations={generations} elapsed_ms={}",
                        started.elapsed().as_millis()
                    ),
                    Err(error) => eprintln!(
                        "walleye.storage warm stream={name} outcome=error elapsed_ms={} error={error}",
                        started.elapsed().as_millis()
                    ),
                }
            })
            .await;
    }
    pub async fn table_names(&self) -> Result<Vec<String>, Error> {
        self.refresh_catalog().await?;
        Ok(self
            .streams
            .lock()
            .await
            .keys()
            .filter(|name| !name.starts_with('_'))
            .cloned()
            .collect())
    }
    /// Current version and client-visible schema.
    pub async fn describe(&self, name: &str) -> Result<(u64, Schema), Error> {
        let stream = self.stream(name).await?;
        Ok((
            stream.version.load(Ordering::Acquire),
            stream.definition.user_schema(&stream.config.schema),
        ))
    }
    pub async fn search(
        &self,
        name: &str,
        request: &SearchRequest,
    ) -> Result<Vec<RecordBatch>, Error> {
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        let stream = self.stream(name).await?;
        let snapshot = SnapshotSource::snapshot(stream.as_ref()).await?;
        let result = snapshot.search(&self.cache.storage, request).await?;
        result.iter().map(strip_hidden).collect()
    }
    pub async fn count(&self, name: &str, filter: Option<&str>) -> Result<u64, Error> {
        let stream = self.stream(name).await?;
        let snapshot = SnapshotSource::snapshot(stream.as_ref()).await?;
        Ok(snapshot.count(&self.cache.storage, filter).await?)
    }
    /// Remove the catalog entry and every object under the table's data prefix.
    pub async fn drop_table(&self, name: &str) -> Result<(), Error> {
        let stream = self.stream(name).await?;
        if let Some(mut table) = stream.table.lock().await.take() {
            let _ = table.checkpoint().await;
            let _ = table.close().await;
        }
        // Claim the name before releasing the map, and hold the claim across
        // both deletes. A concurrent catalog load that already read the
        // definition object is either wiped by this removal or refused by
        // `register`; without the claim it could land in between and leave a
        // stale stream that no later create can displace.
        {
            let mut streams = self.streams.lock().await;
            self.dropping.lock().await.insert(name.to_owned());
            streams.remove(name);
        }
        let dropped = self.drop_objects(name).await;
        self.dropping.lock().await.remove(name);
        dropped
    }
    /// Delete the catalog entry and every data object for a claimed name.
    async fn drop_objects(&self, name: &str) -> Result<(), Error> {
        let path = self.catalog_path.clone().join(format!("{name}.json"));
        match self.catalog.inner.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        let prefix = self.data_path.clone().join(name);
        let objects: Vec<_> = self.catalog.inner.list(Some(&prefix)).try_collect().await?;
        for object in objects {
            match self.catalog.inner.delete(&object.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    /// Set the vector index for a column: the spec is stored in the catalog and
    /// the table is reopened so the memtable graph and every later generation
    /// use it. Existing generations pick it up at their next compaction.
    pub async fn configure_vector_index(
        &self,
        name: &str,
        spec: VectorIndexSpec,
    ) -> Result<(), Error> {
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        let stream = self.stream(name).await?;
        let mut definition = stream.definition.clone();
        definition
            .vector_indexes
            .retain(|v| v.column != spec.column);
        definition.vector_indexes.push(spec);
        definition
            .vector_indexes
            .sort_by(|a, b| a.column.cmp(&b.column));
        // Validate against the schema before touching the catalog.
        definition.table_config(&self.config.root_uri)?;
        // A definition that already matches proves the catalog was updated,
        // not that the generations were rewritten: the catalog is written
        // first, so a retry after a failed rewrite lands here. Skip the
        // catalog write, never the rewrite.
        let stream = if definition.vector_indexes == stream.definition.vector_indexes {
            stream
        } else {
            let path = self.catalog_path.clone().join(format!("{name}.json"));
            let bytes = serde_json::to_vec(&definition)?;
            let put_started = catalog_stage_start("catalog_put", name);
            match self.catalog.inner.put(&path, bytes.into()).await {
                Ok(_) => catalog_stage_finish("catalog_put", name, put_started, "updated"),
                Err(error) => {
                    catalog_stage_finish("catalog_put", name, put_started, "error");
                    return Err(error.into());
                }
            }
            if let Some(mut table) = stream.table.lock().await.take() {
                let _ = table.checkpoint().await;
                table.close().await?;
            }
            self.streams.lock().await.remove(name);
            self.register(definition).await?
        };
        drop(stream.table().await?);
        // Rewrite every flushed generation with the new index before
        // returning, so no query sees an index built with the old metric.
        compact_stream(&stream, 1, self.cache.storage.query_timeout(), true).await?;
        Ok(())
    }
    pub async fn vector_indexes(&self, name: &str) -> Result<Vec<VectorIndexSpec>, Error> {
        let stream = self.stream(name).await?;
        Ok(stream.definition.vector_indexes.clone())
    }
    /// Rows of every referenced stream this node does not own, taken from the
    /// member that does. Each gathered table is leased against the memory
    /// budget for the life of the query.
    async fn gather_remote(
        &self,
        sql: &str,
    ) -> Result<(Vec<GatheredTable>, Vec<walleye_cache::MemoryLease>), Error> {
        let Some(cluster) = &self.cluster else {
            return Ok((Vec::new(), Vec::new()));
        };
        let mut gathered = Vec::new();
        let mut leases = Vec::new();
        for name in walleye_lance::sql_table_names(sql)? {
            let Some(owner) = cluster.owner(&name) else {
                continue;
            };
            if !self.streams.lock().await.contains_key(&name) {
                continue;
            }
            let bytes = cluster.fetch_snapshot(&owner, &name).await?;
            leases.push(self.cache.resources.reserve_memory(
                &format!("gathered table {name}"),
                bytes.len().saturating_mul(2),
            )?);
            let reader =
                arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes.as_ref()), None)?;
            let schema = reader.schema();
            let batches = reader.collect::<Result<Vec<_>, _>>()?;
            gathered.push((name, schema, batches));
        }
        Ok((gathered, leases))
    }
    /// Every row of a stream this node owns, for a peer running SQL that spans
    /// owners. Refused when the stream is larger than one query may gather.
    pub async fn snapshot_batches(
        &self,
        name: &str,
    ) -> Result<(Arc<Schema>, Vec<RecordBatch>), Error> {
        let stream = self.stream(name).await?;
        let snapshot = SnapshotSource::snapshot(stream.as_ref()).await?;
        let result = snapshot
            .search(
                &self.cache.storage,
                &SearchRequest {
                    limit: Some(GATHER_ROW_LIMIT + 1),
                    ..Default::default()
                },
            )
            .await?;
        let rows: usize = result.iter().map(RecordBatch::num_rows).sum();
        if rows > GATHER_ROW_LIMIT {
            return Err(format!(
                "stream {name} has more than {GATHER_ROW_LIMIT} rows; query it on its owner \
                 rather than joining it across members"
            )
            .into());
        }
        Ok((
            stream.config.schema.clone(),
            result.iter().cloned().collect(),
        ))
    }
    /// Run SQL and keep the Arrow result, which is what a view needs: its
    /// output is written on, not rendered. The result carries the memory it
    /// was admitted under, so the caller holds it for as long as it reads the
    /// batches rather than taking them out from under the accounting.
    async fn query_batches(
        &self,
        sql: &str,
        gathered: &[GatheredTable],
    ) -> Result<walleye_lance::ScanResult, Error> {
        let tables: Vec<_> = self
            .streams
            .lock()
            .await
            .iter()
            .map(|(name, s)| (name.clone(), s.clone() as Arc<dyn SnapshotSource>))
            .collect();
        Ok(walleye_lance::query_with_gathered(&self.cache.storage, &tables, gathered, sql).await?)
    }
    async fn query_loaded(&self, sql: &str, gathered: &[GatheredTable]) -> Result<Vec<u8>, Error> {
        let tables: Vec<_> = self
            .streams
            .lock()
            .await
            .iter()
            .map(|(name, s)| (name.clone(), s.clone() as Arc<dyn SnapshotSource>))
            .collect();
        let result =
            walleye_lance::query_with_gathered(&self.cache.storage, &tables, gathered, sql).await?;
        // The columns the node maintains are its own business. A query that
        // names one gets it; `SELECT *` does not hand it out.
        let shown = result
            .iter()
            .map(strip_hidden)
            .collect::<Result<Vec<_>, _>>()?;
        let mut writer = arrow_json::ArrayWriter::new(BoundedOutput(Vec::new()));
        writer.write_batches(&shown.iter().collect::<Vec<_>>())?;
        writer.finish()?;
        Ok(writer.into_inner().0)
    }

    pub async fn query(&self, sql: &str) -> Result<Vec<u8>, Error> {
        if sql.len() > 64 * 1024 {
            return Err("query text exceeds 64 KiB".into());
        }
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        self.load_catalog().await?;
        let (gathered, _leases) = self.gather_remote(sql).await?;
        match self.query_loaded(sql, &gathered).await {
            Ok(result) => Ok(result),
            Err(first) if likely_unknown_relation(&first) => {
                // A sibling node may have admitted a stream after this
                // process's initial catalog load. Refresh only on an unknown
                // relation error; malformed SQL and execution failures keep
                // their original error without paying for another LIST.
                self.refresh_catalog().await?;
                let (gathered, _leases) = self.gather_remote(sql).await?;
                self.query_loaded(sql, &gathered).await.or(Err(first))
            }
            Err(error) => Err(error),
        }
    }
    pub async fn close(&self) {
        let mut closed = self.closed.write().await;
        *closed = true;
        let streams = std::mem::take(&mut *self.streams.lock().await);
        futures::future::join_all(streams.into_values().map(|stream| async move {
            if let Some(mut table) = stream.table.lock().await.take() {
                let _ = table.checkpoint().await;
                let _ = table.close().await;
            }
        }))
        .await;
    }
}
/// Merge generations without holding the table lock, then delete the replaced
/// directories once every snapshot taken before the swap has timed out.
/// `required` decides what a budget that cannot afford the merge means. A
/// background sweep defers and tries again later; a caller that is rewriting
/// generations to match a new index must fail instead, because returning
/// success would leave queries on the old index.
async fn compact_stream(
    stream: &Arc<Stream>,
    min_sstables: usize,
    query_timeout: std::time::Duration,
    required: bool,
) -> Result<Option<CompactionResult>, Error> {
    let (compactor, stats) = {
        let table = stream.table().await?;
        (table.compactor(), table.lsm_stats().await?)
    };
    let Some(compactor) = compactor else {
        return Ok(None);
    };
    // Merged rows plus the rebuilt vector index pass through memory once.
    let rows: usize = stats.sstables.iter().map(|s| s.rows as usize).sum();
    let row_bytes: usize = stream
        .config
        .schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::FixedSizeList(_, dim) => *dim as usize * 4 * 2,
            DataType::Utf8 => 64,
            _ => 8,
        })
        .sum::<usize>()
        + 64;
    let _lease = match stream.resources.reserve_memory(
        &format!("compaction of {}", stream.definition.name),
        rows * row_bytes,
    ) {
        Ok(lease) => lease,
        Err(error) => {
            if required {
                return Err(Box::new(error));
            }
            eprintln!(
                "walleye.storage compaction stream={} outcome=deferred error={error}",
                stream.definition.name
            );
            return Ok(None);
        }
    };
    let started = Instant::now();
    let Some(result) = compactor.compact(min_sstables).await? else {
        return Ok(None);
    };
    eprintln!(
        "walleye.storage compaction stream={} merged={} rows={} elapsed_ms={}",
        stream.definition.name,
        result.merged.len(),
        result.rows,
        started.elapsed().as_millis()
    );
    let merged = result.merged.clone();
    let name = stream.definition.name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(query_timeout * 2).await;
        if let Err(error) = compactor.delete_generations(&merged).await {
            eprintln!(
                "walleye.storage compaction stream={} stage=delete outcome=error error={}",
                name, error
            );
        }
    });
    Ok(Some(result))
}
/// Reorder and validate a client batch against the table schema, adding the
/// hidden content-hash key when the table has one.
fn conform_batch(
    definition: &StreamDefinition,
    full: &Arc<Schema>,
    batch: RecordBatch,
    first_seq: u64,
) -> Result<RecordBatch, Error> {
    let mut columns = Vec::with_capacity(full.fields().len());
    for field in full.fields() {
        if hidden(field.name()) {
            continue;
        }
        let index = batch
            .schema()
            .index_of(field.name())
            .map_err(|_| format!("missing column {}", field.name()))?;
        let column = batch.column(index).clone();
        if column.data_type() != field.data_type() {
            return Err(format!(
                "column {} has type {} but the table expects {}",
                field.name(),
                column.data_type(),
                field.data_type()
            )
            .into());
        }
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(format!("column {} must not contain nulls", field.name()).into());
        }
        columns.push(column);
    }
    if batch.num_columns() != columns.len() {
        return Err("batch contains columns that are not in the table".into());
    }
    if definition.hidden_pk() {
        columns.push(Arc::new(content_hash(&columns)?));
    }
    let rows = batch.num_rows() as u64;
    columns.push(Arc::new(UInt64Array::from_iter_values(
        first_seq..first_seq.saturating_add(rows),
    )));
    Ok(RecordBatch::try_new(full.clone(), columns)?)
}
/// xxh3 of each row's Arrow row-format encoding across all user columns.
fn content_hash(columns: &[Arc<dyn Array>]) -> Result<UInt64Array, Error> {
    let fields: Vec<_> = columns
        .iter()
        .map(|c| arrow_row::SortField::new(c.data_type().clone()))
        .collect();
    if !arrow_row::RowConverter::supports_fields(&fields) {
        return Err("a column type cannot be hashed for the hidden primary key".into());
    }
    let converter = arrow_row::RowConverter::new(fields)?;
    let rows = converter.convert_columns(columns)?;
    Ok(UInt64Array::from_iter_values(
        rows.iter()
            .map(|row| xxhash_rust::xxh3::xxh3_64(row.as_ref())),
    ))
}
fn strip_hidden(batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let schema = batch.schema();
    let keep: Vec<usize> = (0..schema.fields().len())
        .filter(|&i| !hidden(schema.field(i).name()))
        .collect();
    if keep.len() == schema.fields().len() {
        return Ok(batch.clone());
    }
    Ok(batch.project(&keep)?)
}
struct BoundedOutput(Vec<u8>);
impl std::io::Write for BoundedOutput {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(b.len()) > 8 * 1024 * 1024 {
            return Err(std::io::Error::other(
                "JSON result exceeds 8 MiB; use a SQL LIMIT",
            ));
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn likely_unknown_relation(error: &Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    (message.contains("table") || message.contains("relation"))
        && (message.contains("not found")
            || message.contains("does not exist")
            || message.contains("unknown"))
}

fn storage_params_from(mut get: impl FnMut(&str) -> Option<String>) -> ObjectStoreParams {
    let mut options = HashMap::new();
    for (env, key) in [
        ("AWS_ACCESS_KEY_ID", "aws_access_key_id"),
        ("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key"),
        ("AWS_SESSION_TOKEN", "aws_session_token"),
        ("AWS_REGION", "aws_region"),
        ("AWS_ENDPOINT", "aws_endpoint"),
        ("AWS_ALLOW_HTTP", "allow_http"),
        // The cell boot script sets this to avoid reusing stale pooled S3
        // connections after a suspended tenant runtime. Keep the mapping here
        // explicit: Lance's static storage accessor otherwise drops it.
        ("AWS_POOL_MAX_IDLE_PER_HOST", "pool_max_idle_per_host"),
    ] {
        if let Some(value) = get(env) {
            options.insert(key.to_string(), value);
        }
    }
    ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(
            lance_io::object_store::StorageOptionsAccessor::with_static_options(options),
        )),
        ..Default::default()
    }
}

pub fn storage_params() -> ObjectStoreParams {
    storage_params_from(|name| std::env::var(name).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;
    use walleye_bitr::MemoryReplica;

    fn definition(name: &str) -> StreamDefinition {
        serde_json::from_value(json!({"name": name, "columns": [
            {"name":"id", "type":"int64"}, {"name":"value", "type":"int64"}
        ], "primary_key":["id"]}))
        .unwrap()
    }

    #[tokio::test]
    async fn storage_params_preserve_pool_idle_limit_for_lance() {
        let params = storage_params_from(|name| {
            (name == "AWS_POOL_MAX_IDLE_PER_HOST").then(|| "0".to_owned())
        });
        let options = params
            .storage_options_accessor
            .expect("storage accessor")
            .get_storage_options()
            .await
            .expect("static storage options");
        assert_eq!(
            options.0.get("pool_max_idle_per_host"),
            Some(&"0".to_owned())
        );

        // Lance's AWS provider parses the storage map into its S3 config map
        // before constructing object_store::AmazonS3Builder. This assertion
        // checks the actual provider key, rather than only the accessor map,
        // so a typo or an unrecognized option cannot silently pass the test.
        let s3_options = lance_io::object_store::StorageOptions::new(options.0).as_s3_options();
        assert_eq!(
            s3_options.get(&object_store::aws::AmazonS3ConfigKey::Client(
                object_store::ClientConfigKey::PoolMaxIdlePerHost,
            )),
            Some(&"0".to_owned())
        );
    }

    fn composite_definition(name: &str) -> StreamDefinition {
        serde_json::from_value(json!({"name": name, "columns": [
            {"name":"scope", "type":"string"},
            {"name":"entry", "type":"string"},
            {"name":"value", "type":"int64"}
        ], "primary_key":["scope", "entry"]}))
        .unwrap()
    }
    async fn engine(
        dir: &std::path::Path,
        writer: Option<Arc<QuorumWriter>>,
        cache_id: &str,
    ) -> Engine {
        let cache = CachedStorage::open(
            dir.join(cache_id),
            "test",
            1024 * 1024 * 1024,
            64 * 1024 * 1024,
            ObjectStoreParams::default(),
            None,
        )
        .await
        .unwrap();
        let mut engine = Engine::open(
            ApiConfig {
                root_uri: format!("file://{}/store", dir.display()),
                bitr_url: None,
            },
            cache,
            ObjectStoreParams::default(),
            None,
        )
        .await
        .unwrap();
        engine.writer = writer;
        engine
    }
    async fn check_independent_streams(bitr: bool) {
        let dir = tempfile::tempdir().unwrap();
        let writer = bitr.then(|| {
            Arc::new(QuorumWriter::new(
                Arc::new(MemoryReplica::healthy()),
                [7; 32],
            ))
        });
        let e = engine(dir.path(), writer.clone(), "cache").await;
        e.define(definition("a")).await.unwrap();
        let a = e.stream("a").await.unwrap();
        // Hold A at its writer boundary, then queue two writes to the same key.
        // They must remain pending and execute in lock acquisition order.
        let guard = a.table().await.unwrap();
        let first = e.ingest("a", vec![json!({"id":0,"value":1})]);
        let second = e.ingest("a", vec![json!({"id":0,"value":2})]);
        tokio::pin!(first, second);
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        tokio::time::timeout(Duration::from_secs(10), async {
            // Creation, ingestion, and SQL on B must all finish while A is blocked.
            e.define(definition("b")).await.unwrap();
            e.ingest("b", vec![json!({"id":0,"value":9})])
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(
                    &e.query("SELECT * FROM b").await.unwrap()
                )
                .unwrap(),
                json!([{"id":0,"value":9}])
            );
        })
        .await
        .expect("a blocked stream must not block another stream");
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        drop(guard);
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap(), 1);
        assert_eq!(second.unwrap(), 1);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &e.query("SELECT value FROM a WHERE id = 0").await.unwrap()
            )
            .unwrap(),
            json!([{"value":2}])
        );

        // An owned SQL snapshot stays readable after new writes and checkpoint.
        struct Captured {
            schema: Arc<Schema>,
            snapshot: TableSnapshot,
            calls: std::sync::atomic::AtomicUsize,
        }
        #[async_trait::async_trait]
        impl SnapshotSource for Captured {
            fn schema(&self) -> Arc<Schema> {
                self.schema.clone()
            }
            async fn snapshot(&self) -> Result<TableSnapshot, walleye_lance::LanceError> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(self.snapshot.clone())
            }
        }
        let captured = Arc::new(Captured {
            schema: a.schema(),
            snapshot: a.snapshot().await.unwrap(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        e.ingest("a", vec![json!({"id":0,"value":3})])
            .await
            .unwrap();
        a.table().await.unwrap().checkpoint().await.unwrap();
        let result = walleye_lance::query(
            &e.cache.storage,
            &[("a".into(), captured.clone() as Arc<dyn SnapshotSource>)],
            "SELECT l.value AS before, r.value AS also_before FROM a l JOIN a r ON l.id = r.id",
        )
        .await
        .unwrap();
        let mut output = arrow_json::ArrayWriter::new(Vec::new());
        output
            .write_batches(&result.iter().collect::<Vec<_>>())
            .unwrap();
        output.finish().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.into_inner()).unwrap(),
            json!([{"before":2,"also_before":2}])
        );
        assert_eq!(captured.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Concurrent idempotent definitions must share the same writer. Multiple
        // requests per stream and across streams must preserve every unique row.
        let definitions =
            futures::future::join_all((0..8).map(|_| e.define(definition("c")))).await;
        for result in definitions {
            result.unwrap();
        }
        let mut conflicting = definition("c");
        conflicting.columns[1].kind = "string".into();
        assert!(e.define(conflicting).await.is_err());
        let writes =
            futures::future::join_all(["a", "b", "c"].into_iter().flat_map(|name| {
                (1..=16).map(|id| e.ingest(name, vec![json!({"id":id,"value":id})]))
            }))
            .await;
        for result in writes {
            assert_eq!(result.unwrap(), 1);
        }
        let expected =
            json!([{"stream":"a", "n":17}, {"stream":"b", "n":17}, {"stream":"c", "n":16}]);
        let sql = "SELECT 'a' AS stream, count(*) AS n FROM a UNION ALL SELECT 'b', count(*) FROM b UNION ALL SELECT 'c', count(*) FROM c ORDER BY stream";
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&e.query(sql).await.unwrap()).unwrap(),
            expected
        );
        e.close().await;
        assert!(
            e.ingest("a", vec![json!({"id":99,"value":0})])
                .await
                .is_err()
        );
        assert!(e.define(definition("d")).await.is_err());
        assert!(e.query(sql).await.is_err());
        e.cache.backend.close().await.unwrap();
        let reopened = engine(dir.path(), writer, "cache-reopened").await;
        // Concurrent first access after restart must also open just one writer.
        let writes = futures::future::join_all(
            (20..28).map(|id| reopened.ingest("a", vec![json!({"id":id,"value":id})])),
        )
        .await;
        for result in writes {
            result.unwrap();
        }
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &reopened.query("SELECT count(*) AS n FROM a").await.unwrap()
            )
            .unwrap(),
            json!([{"n":25}])
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &reopened.query("SELECT count(*) AS n FROM b").await.unwrap()
            )
            .unwrap(),
            json!([{"n":17}])
        );
        reopened.close().await;
        reopened.cache.backend.close().await.unwrap();
    }
    #[tokio::test]
    async fn object_store_streams_progress_independently_and_reopen() {
        check_independent_streams(false).await;
    }
    #[tokio::test]
    async fn bitr_streams_progress_independently_and_reopen() {
        check_independent_streams(true).await;
    }

    #[tokio::test]
    async fn composite_key_filters_push_down_and_keep_captured_reads_stable() {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(dir.path(), None, "cache").await;
        e.define(composite_definition("entries")).await.unwrap();
        e.ingest(
            "entries",
            vec![
                json!({"scope":"a","entry":"one","value":1}),
                json!({"scope":"a","entry":"two","value":2}),
                json!({"scope":"b","entry":"one","value":10}),
            ],
        )
        .await
        .unwrap();

        let entries = e.stream("entries").await.unwrap();

        // Capture a point-in-time view before either a checkpoint or the
        // overwrite. The active memtable is still mutable at this point; the
        // snapshot must freeze its visible batch prefix rather than retaining
        // a live index watermark.
        struct Captured {
            schema: Arc<Schema>,
            snapshot: TableSnapshot,
        }
        #[async_trait::async_trait]
        impl SnapshotSource for Captured {
            fn schema(&self) -> Arc<Schema> {
                self.schema.clone()
            }
            async fn snapshot(&self) -> Result<TableSnapshot, walleye_lance::LanceError> {
                Ok(self.snapshot.clone())
            }
        }
        let captured_active = Arc::new(Captured {
            schema: entries.schema(),
            snapshot: entries.table().await.unwrap().snapshot().await.unwrap(),
        });

        // Also capture after the initial generation is on disk. This leaves an
        // empty active generation in the view and exercises the older/base-arm
        // block-list when the later overwrite is flushed.
        entries.table().await.unwrap().checkpoint().await.unwrap();
        let captured_base = Arc::new(Captured {
            schema: entries.schema(),
            snapshot: entries.table().await.unwrap().snapshot().await.unwrap(),
        });

        e.ingest(
            "entries",
            vec![json!({"scope":"a","entry":"one","value":99})],
        )
        .await
        .unwrap();
        // Force the deterministic flush boundary. The captured source must
        // still use exactly one representation of the old generation when its
        // frozen handle is concurrently replaced by the manifest SSTable.
        entries.table().await.unwrap().checkpoint().await.unwrap();

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &e.query(
                    "SELECT entry, value FROM entries \
                     WHERE scope = 'a' AND entry IN ('one', 'two') ORDER BY entry",
                )
                .await
                .unwrap(),
            )
            .unwrap(),
            json!([{"entry":"one","value":99},{"entry":"two","value":2}])
        );

        // A non-PK predicate keeps the normal filtered LSM path and cannot be
        // accidentally dropped by the composite point-lookup optimization.
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &e.query(
                    "SELECT entry, value FROM entries \
                     WHERE scope = 'a' AND entry = 'one' AND value = 1",
                )
                .await
                .unwrap(),
            )
            .unwrap(),
            json!([])
        );

        let result = walleye_lance::query(
            &e.cache.storage,
            &[(
                "entries".into(),
                captured_active.clone() as Arc<dyn SnapshotSource>,
            )],
            "SELECT entry, value FROM entries WHERE scope = 'a' AND entry = 'one'",
        )
        .await
        .unwrap();
        let mut output = arrow_json::ArrayWriter::new(Vec::new());
        output
            .write_batches(&result.iter().collect::<Vec<_>>())
            .unwrap();
        output.finish().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.into_inner()).unwrap(),
            json!([{"entry":"one","value":1}])
        );

        // A non-PK predicate forces the general LSM plan. Its block-list must
        // use the captured active-batch watermark too: a live overwrite may
        // not shadow the old row and then be filtered away, which would make
        // this query incorrectly return no rows.
        let result = walleye_lance::query(
            &e.cache.storage,
            &[(
                "entries".into(),
                captured_active.clone() as Arc<dyn SnapshotSource>,
            )],
            "SELECT entry, value FROM entries WHERE value = 1",
        )
        .await
        .unwrap();
        let mut output = arrow_json::ArrayWriter::new(Vec::new());
        output
            .write_batches(&result.iter().collect::<Vec<_>>())
            .unwrap();
        output.finish().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.into_inner()).unwrap(),
            json!([{"entry":"one","value":1}])
        );

        // The snapshot whose active generation was empty must still expose
        // the old on-disk row after the overwrite was flushed into a newer
        // generation. The bounded empty active membership cannot shadow it.
        let result = walleye_lance::query(
            &e.cache.storage,
            &[("entries".into(), captured_base as Arc<dyn SnapshotSource>)],
            "SELECT entry, value FROM entries WHERE value = 1",
        )
        .await
        .unwrap();
        let mut output = arrow_json::ArrayWriter::new(Vec::new());
        output
            .write_batches(&result.iter().collect::<Vec<_>>())
            .unwrap();
        output.finish().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&output.into_inner()).unwrap(),
            json!([{"entry":"one","value":1}])
        );

        e.close().await;
        e.cache.backend.close().await.unwrap();
    }
}

/// Where a tier's rows come from, what turns them into the next tier, and
/// where they land. A view is maintained forward from where it stopped: it
/// reads the rows that arrived after its cursor, runs its query over only
/// those, writes the result, and remembers how far it reached.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ViewDefinition {
    pub name: String,
    /// The stream this view reads. Inside the query this name means the rows
    /// that are new, not the whole stream.
    pub source: String,
    /// The query that turns source rows into target rows. A view has this or
    /// a worker, and a worker sees whatever the query left.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    /// JavaScript that turns a batch of rows into the rows to write, written
    /// as a module with a default export taking an array and returning one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    /// Where the result is written. Created from the view's own output shape
    /// the first time it produces rows. A view with only an alert needs no
    /// target: it is watching, not materialising.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Where to send rows this view produced. Delivery is part of the pass:
    /// a batch that could not be delivered does not advance the cursor, so
    /// it is tried again rather than lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alert: Option<Alert>,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    /// Heap the worker may use, in MiB. Taken from the node's one budget
    /// like everything else that allocates, so a worker cannot be sized
    /// beyond what the machine has left.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_heap_mb: Option<usize>,
    /// Wall clock one batch may take, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_seconds: Option<u64>,
}
impl ViewDefinition {
    /// What this view's worker may spend. A deployment sizes its own
    /// workers; the node only decides whether it can afford them.
    fn limits(&self) -> walleye_v8::Limits {
        let fallback = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|value| *value > 0)
                .unwrap_or(default)
        };
        walleye_v8::Limits {
            heap_bytes: self
                .worker_heap_mb
                .map(|mb| mb as u64)
                .unwrap_or_else(|| fallback("WALLEYE_WORKER_HEAP_MB", 128))
                as usize
                * 1024
                * 1024,
            deadline: std::time::Duration::from_secs(
                self.worker_seconds
                    .unwrap_or_else(|| fallback("WALLEYE_WORKER_SECONDS", 15)),
            ),
        }
    }
}
/// Where a view sends what it found.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Alert {
    /// The endpoint that receives the rows, as a JSON array.
    pub url: String,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub headers: std::collections::HashMap<String, String>,
}
fn default_batch_rows() -> usize {
    512
}
/// What one pass over a view did.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Progress {
    /// Source rows read.
    pub rows: usize,
    /// Rows written to the target.
    pub written: usize,
    /// Rows delivered to the alert endpoint.
    pub delivered: usize,
    /// Arrival number the view has now consumed through.
    pub through: u64,
    /// Whether the source had nothing further at the end of the pass.
    pub caught_up: bool,
}

/// Where each consumer stopped. One row per consumer, superseded in place,
/// so the newest row for a name is its position.
pub const CURSORS: &str = "_walleye_cursors";

fn cursor_schema() -> Schema {
    let mut consumer = Field::new("consumer", DataType::Utf8, false);
    consumer.set_metadata(
        [(PK_METADATA_KEY.to_owned(), "true".to_owned())]
            .into_iter()
            .collect(),
    );
    Schema::new(vec![
        consumer,
        Field::new("position", DataType::UInt64, true),
        Field::new("updated_at", DataType::Int64, true),
    ])
}

impl Engine {
    /// Register a view. The source must exist; the target is created when the
    /// view first produces rows, from the shape its own query returns.
    pub async fn define_view(&self, view: ViewDefinition) -> Result<(), Error> {
        for name in [Some(&view.name), Some(&view.source), view.target.as_ref()]
            .into_iter()
            .flatten()
        {
            if !valid_name(name) {
                return Err(format!("invalid name {name}").into());
            }
        }
        let filled = |text: &Option<String>| text.as_ref().is_some_and(|t| !t.trim().is_empty());
        if !filled(&view.sql) && !filled(&view.worker) {
            return Err("a view needs a query, a worker, or both".into());
        }
        if view.target.is_none() && view.alert.is_none() {
            return Err("a view needs a target to write to, an alert to send to, or both".into());
        }
        if view.batch_rows == 0 || view.batch_rows > 100_000 {
            return Err("batch_rows must be between 1 and 100000".into());
        }
        if view.target.as_deref() == Some(view.source.as_str()) {
            return Err("a view cannot write back into its own source".into());
        }
        if let Some(alert) = &view.alert {
            let url = reqwest::Url::parse(&alert.url).map_err(|e| format!("alert url: {e}"))?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err("an alert url must be http or https".into());
            }
        }
        // The source need not exist yet. A pipeline is declared as a whole,
        // and a tier's source is usually the target of the tier above it,
        // which will not exist until that tier first produces rows.
        let path = self.views_path.clone().join(format!("{}.json", view.name));
        let bytes = serde_json::to_vec(&view)?;
        self.catalog.inner.put(&path, bytes.into()).await?;
        Ok(())
    }
    pub async fn view(&self, name: &str) -> Result<ViewDefinition, Error> {
        if !valid_name(name) {
            return Err("invalid view name".into());
        }
        let path = self.views_path.clone().join(format!("{name}.json"));
        let data = match self.catalog.inner.get(&path).await {
            Ok(data) => data.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Box::new(TableNotFound(name.into())));
            }
            Err(error) => return Err(error.into()),
        };
        Ok(serde_json::from_slice(&data)?)
    }
    pub async fn view_names(&self) -> Result<Vec<String>, Error> {
        let objects: Vec<_> = self
            .catalog
            .inner
            .list(Some(&self.views_path))
            .try_collect()
            .await?;
        let mut names: Vec<String> = objects
            .iter()
            .filter(|object| object.location.extension() == Some("json"))
            .filter_map(|object| object.location.filename())
            .filter_map(|file| file.strip_suffix(".json"))
            .map(str::to_owned)
            .collect();
        names.sort();
        Ok(names)
    }
    pub async fn drop_view(&self, name: &str) -> Result<(), Error> {
        let view = self.view(name).await?;
        let path = self.views_path.clone().join(format!("{name}.json"));
        match self.catalog.inner.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        // Leave the target and its rows: dropping a definition is not a
        // licence to delete data somebody may still be reading.
        let _ = view;
        Ok(())
    }

    /// Advance a view by at most one batch.
    ///
    /// The pass is safe to repeat. Rows are written before the cursor moves,
    /// so a crash in between replays the same source rows; the target's
    /// content key collapses the identical output rows that result. That
    /// holds only while the query is a deterministic function of its input,
    /// which is the contract a view signs.
    pub async fn refresh_view(&self, name: &str) -> Result<Progress, Error> {
        let view = self.view(name).await?;
        let consumer = format!("view:{name}");
        let cursor = self.cursor(&consumer).await?;
        let pick = format!(
            "SELECT * FROM \"{}\" WHERE {HIDDEN_SEQ} > {cursor} ORDER BY {HIDDEN_SEQ} LIMIT {}",
            view.source, view.batch_rows
        );
        // A source that does not exist yet is a tier whose upstream has not
        // produced anything. That is idleness, not failure: the view waits.
        if self.definition(&view.source).await.is_err() {
            return Ok(Progress {
                rows: 0,
                written: 0,
                delivered: 0,
                through: cursor,
                caught_up: true,
            });
        }
        let fresh = self.query_batches(&pick, &[]).await?;
        let rows: usize = fresh.iter().map(RecordBatch::num_rows).sum();
        if rows == 0 {
            return Ok(Progress {
                rows: 0,
                written: 0,
                delivered: 0,
                through: cursor,
                caught_up: true,
            });
        }
        let through = highest_in(&fresh)?.unwrap_or(cursor);
        // Inside the view's query the source name means these rows only,
        // which is what makes the pass incremental without the query having
        // to know about cursors at all.
        let schema = fresh[0].schema();
        let gathered = vec![(view.source.clone(), schema, fresh.to_vec())];
        let mut produced: Vec<RecordBatch> = match &view.sql {
            Some(sql) if !sql.trim().is_empty() => self
                .query_batches(sql, &gathered)
                .await?
                .iter()
                .map(strip_hidden)
                .collect::<Result<Vec<_>, _>>()?,
            _ => fresh
                .iter()
                .map(strip_hidden)
                .collect::<Result<Vec<_>, _>>()?,
        };
        if let Some(worker) = &view.worker
            && !worker.trim().is_empty()
        {
            let limits = view.limits();
            // A worker's heap is part of the machine's memory, not extra to
            // it. Reserving it here means a worker too large for what is
            // left is refused before it runs, and that the cache gives up
            // the room while it does run.
            let _heap = self
                .cache
                .resources
                .reserve_memory(&format!("worker {}", view.name), limits.heap_bytes)
                .map_err(|error| -> Error { Box::new(error) })?;
            let expected = match &view.target {
                Some(target) => self.definition(target).await.ok().map(|stream| {
                    Arc::new(stream.definition.user_schema(stream.config.schema.as_ref()))
                }),
                None => None,
            };
            let worker = worker.clone();
            let handed = produced;
            produced =
                tokio::task::spawn_blocking(move || run_worker(&worker, &handed, limits, expected))
                    .await
                    .map_err(|error| -> Error { error.to_string().into() })??;
        }
        let made: usize = produced.iter().map(RecordBatch::num_rows).sum();

        // Deliver before the cursor moves. A batch that could not be sent is
        // offered again on the next pass rather than quietly skipped, which
        // makes an alert at least once and never at most once.
        let mut delivered = 0;
        if let Some(alert) = &view.alert
            && made > 0
        {
            self.deliver(alert, &produced).await?;
            delivered = made;
        }
        let mut written = 0;
        if let Some(target) = &view.target
            && made > 0
        {
            if self.definition(target).await.is_err() {
                let definition = StreamDefinition::from_arrow(target, &produced[0].schema())?;
                self.define_with(definition, true).await?;
            }
            self.append(target, produced).await?;
            written = made;
        }
        self.set_cursor(&consumer, through).await?;
        Ok(Progress {
            rows,
            written,
            delivered,
            through,
            caught_up: rows < view.batch_rows,
        })
    }
    /// Send produced rows to an alert endpoint as a JSON array.
    async fn deliver(&self, alert: &Alert, batches: &[RecordBatch]) -> Result<(), Error> {
        let mut writer = arrow_json::ArrayWriter::new(Vec::new());
        writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
        writer.finish()?;
        let body = writer.into_inner();
        let mut request = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?
            .post(&alert.url)
            .header("content-type", "application/json");
        for (name, value) in &alert.headers {
            request = request.header(name, value);
        }
        let response = request.body(body).send().await?;
        if !response.status().is_success() {
            return Err(format!("alert endpoint answered {}", response.status()).into());
        }
        Ok(())
    }
    /// Advance a view until its source has nothing further, or until `passes`
    /// batches have been done. Bounded so one call cannot run forever.
    pub async fn drain_view(&self, name: &str, passes: usize) -> Result<Progress, Error> {
        let mut total = Progress {
            rows: 0,
            written: 0,
            delivered: 0,
            through: 0,
            caught_up: false,
        };
        for _ in 0..passes.clamp(1, 1000) {
            let pass = self.refresh_view(name).await?;
            total.rows += pass.rows;
            total.written += pass.written;
            total.delivered += pass.delivered;
            total.through = pass.through;
            total.caught_up = pass.caught_up;
            if pass.caught_up {
                break;
            }
        }
        Ok(total)
    }

    /// Drive every view this node owns until none of them can make progress.
    ///
    /// The rule is progress, not fullness. A pass over all the views repeats
    /// while any of them committed a batch, so a tier that fills its
    /// downstream tier feeds it on the next pass without anyone having to
    /// notice that a batch came back full. When a whole pass moves nothing,
    /// the work is done and the caller can go quiet rather than poll.
    ///
    /// A view whose source this node does not own is left alone: its owner
    /// drives it, and two nodes driving one cursor would do the same work
    /// twice.
    pub async fn advance_views(&self, budget: usize) -> Vec<(String, Result<Progress, Error>)> {
        let mut reports: Vec<(String, Result<Progress, Error>)> = Vec::new();
        let names = match self.view_names().await {
            Ok(names) => names,
            Err(error) => return vec![("*".to_owned(), Err(error))],
        };
        let mut spent = 0;
        while spent < budget.max(1) {
            let mut moved = false;
            for name in &names {
                if spent >= budget.max(1) {
                    break;
                }
                let Ok(view) = self.view(name).await else {
                    continue;
                };
                if self.owner(&view.source).is_some() {
                    continue;
                }
                let outcome = self.refresh_view(name).await;
                spent += 1;
                if let Ok(progress) = &outcome {
                    if progress.rows == 0 {
                        continue;
                    }
                    moved = true;
                }
                let failed = outcome.is_err();
                reports.push((name.clone(), outcome));
                if failed {
                    // A failing view stops being retried this round rather
                    // than starving its siblings out of the budget.
                    continue;
                }
            }
            if !moved {
                break;
            }
        }
        reports
    }
    /// Where a consumer stopped, or zero when it has never run.
    pub async fn cursor(&self, consumer: &str) -> Result<u64, Error> {
        if self.definition(CURSORS).await.is_err() {
            return Ok(0);
        }
        let sql = format!(
            "SELECT position FROM \"{CURSORS}\" WHERE consumer = '{}'",
            consumer.replace('\'', "''")
        );
        let batches = self.query_batches(&sql, &[]).await?;
        for batch in batches.iter() {
            if batch.num_rows() == 0 {
                continue;
            }
            if let Some(column) = batch.column(0).as_any().downcast_ref::<UInt64Array>()
                && !column.is_null(0)
            {
                return Ok(column.value(0));
            }
        }
        Ok(0)
    }
    /// Record where a consumer reached. The newest row for a consumer wins,
    /// so this supersedes rather than accumulates.
    pub async fn set_cursor(&self, consumer: &str, position: u64) -> Result<(), Error> {
        self.define_with(
            StreamDefinition::from_arrow(CURSORS, &cursor_schema())?,
            true,
        )
        .await?;
        let schema = Arc::new(cursor_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::StringArray::from(vec![consumer.to_owned()])),
                Arc::new(UInt64Array::from(vec![position])),
                Arc::new(arrow_array::Int64Array::from(vec![now_micros() as i64])),
            ],
        )?;
        self.append(CURSORS, vec![batch]).await?;
        Ok(())
    }
}

/// The largest arrival number in a set of batches.
fn highest_in(batches: &[RecordBatch]) -> Result<Option<u64>, Error> {
    let mut highest: Option<u64> = None;
    for batch in batches {
        let Ok(index) = batch.schema().index_of(HIDDEN_SEQ) else {
            return Err("source rows carry no arrival order".into());
        };
        let column = batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or("arrival order is not a number")?;
        for row in 0..column.len() {
            if !column.is_null(row) {
                highest =
                    Some(highest.map_or(column.value(row), |seen| seen.max(column.value(row))));
            }
        }
    }
    Ok(highest)
}

/// Hand a batch of rows to a worker and take back the rows it returns.
///
/// Rows cross as JSON in both directions. A worker therefore never holds an
/// Arrow buffer and the node never holds a JavaScript value, which is the
/// whole of the isolation at this boundary.
///
/// The returned shape is whatever the worker produced. It is read back by
/// inference rather than forced into the source's schema, because a transform
/// that could not change the shape of a row would not be much of a transform.
fn run_worker(
    worker: &str,
    batches: &[RecordBatch],
    limits: walleye_v8::Limits,
    expected: Option<Arc<Schema>>,
) -> Result<Vec<RecordBatch>, Error> {
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    let rows = String::from_utf8(writer.into_inner())?;

    let produced = walleye_v8::run(worker, &rows, limits)?;
    let values: Vec<serde_json::Value> = serde_json::from_str(&produced)?;
    if values.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(index) = values.iter().position(|row| !row.is_object()) {
        return Err(
            format!("worker returned a row that is not an object at position {index}").into(),
        );
    }
    // Once a target exists its schema is the authority. Inferring afresh from
    // each batch would let a column that happened to be whole numbers in the
    // first batch reject a fractional one in the second.
    let schema = match expected {
        Some(schema) => schema,
        None => Arc::new(arrow_json::reader::infer_json_schema_from_iterator(
            values
                .iter()
                .map(Ok::<&serde_json::Value, arrow_schema::ArrowError>),
        )?),
    };
    if schema.fields().iter().any(|f| hidden(f.name())) {
        return Err("worker returned a column using a reserved name".into());
    }
    let mut decoder = arrow_json::ReaderBuilder::new(schema).build_decoder()?;
    decoder.serialize(&values)?;
    Ok(decoder.flush()?.into_iter().collect())
}
