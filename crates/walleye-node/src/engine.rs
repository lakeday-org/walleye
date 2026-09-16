//! One deployment's stream registry. Definitions use object-store create-if-absent;
//! one designated ingress owns a separately locked memshard for each stream.
use crate::cluster::{Cluster, NotOwner};
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine as _;
use futures::TryStreamExt;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use object_store::ObjectStoreExt;
use object_store::{PutMode, PutOptions, path::Path};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
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
/// Minimum spacing between automatic compaction attempts per table.
const COMPACT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Server-managed primary key for tables created without one. It is an xxh3
/// hash of the row's full contents, so an identical row (including a retried
/// insert) collapses to one visible row and every memshard stays idempotent.
pub const HIDDEN_PK: &str = "_walleye_pk";
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
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
                .filter(|f| f.name() != HIDDEN_PK)
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
    /// Bitr quorum writer in cluster mode; the WAL backend is minted per open
    /// so its epoch matches the MemWAL claim.
    bitr: Option<Arc<QuorumWriter>>,
    table: Mutex<Option<Table>>,
    /// Monotonic write version reported to LanceDB clients.
    version: AtomicU64,
    /// One automatic compaction in flight at a time, spaced by COMPACT_INTERVAL.
    compacting: std::sync::atomic::AtomicBool,
    last_compaction: Mutex<Option<Instant>>,
}
impl Stream {
    async fn table(&self) -> Result<MappedMutexGuard<'_, Table>, Error> {
        let mut table = self.table.lock().await;
        if table.is_none() {
            let durability = match &self.bitr {
                Some(writer) => {
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
        let table = self
            .table()
            .await
            .map_err(|e| walleye_lance::LanceError::io(e.to_string()))?;
        table.snapshot().await
    }
}
pub struct Engine {
    config: ApiConfig,
    cache: CachedStorage,
    catalog: Arc<ObjectStore>,
    catalog_path: Path,
    data_path: Path,
    streams: Mutex<BTreeMap<String, Arc<Stream>>>,
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
            data_path: prefix.join("data"),
            streams: Mutex::new(BTreeMap::new()),
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
        let stream = streams.entry(definition.name.clone()).or_insert_with(|| {
            Arc::new(Stream {
                definition,
                config,
                bitr: self.writer.clone(),
                storage: self.cache.storage.clone(),
                table: Mutex::new(None),
                version: AtomicU64::new(1),
                compacting: std::sync::atomic::AtomicBool::new(false),
                last_compaction: Mutex::new(None),
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
                self.definition(name).await?;
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
        let batches = arrow_json::ReaderBuilder::new(stream.config.schema.clone())
            .with_batch_size(1024)
            .build(Cursor::new(ndjson))?
            .collect::<Result<Vec<RecordBatch>, _>>()?;
        stream.table().await?.append(batches).await?;
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
        let mut prepared = Vec::with_capacity(batches.len());
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            prepared.push(conform_batch(&stream.definition, &full, batch)?);
        }
        if prepared.is_empty() {
            return Ok(stream.version.load(Ordering::Acquire));
        }
        stream.table().await?.append(prepared).await?;
        self.maybe_compact(&stream).await;
        Ok(stream.version.fetch_add(1, Ordering::AcqRel) + 1)
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
            let outcome = compact_stream(&stream, COMPACT_MIN_SSTABLES, query_timeout).await;
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
    /// Merge flushed generations now. Returns what was merged, if anything.
    pub async fn compact(&self, name: &str) -> Result<Option<CompactionResult>, Error> {
        let stream = self.stream(name).await?;
        compact_stream(&stream, 2, self.cache.storage.query_timeout()).await
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
    pub async fn table_names(&self) -> Result<Vec<String>, Error> {
        self.refresh_catalog().await?;
        Ok(self.streams.lock().await.keys().cloned().collect())
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
        Ok(result
            .iter()
            .map(strip_hidden_pk)
            .collect::<Result<Vec<_>, _>>()?)
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
        self.streams.lock().await.remove(name);
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
        if definition.vector_indexes == stream.definition.vector_indexes {
            return Ok(());
        }
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
        let stream = self.register(definition).await?;
        drop(stream.table().await?);
        // Rewrite every flushed generation with the new index before
        // returning, so no query sees an index built with the old metric.
        compact_stream(&stream, 1, self.cache.storage.query_timeout()).await?;
        Ok(())
    }
    pub async fn vector_indexes(&self, name: &str) -> Result<Vec<VectorIndexSpec>, Error> {
        let stream = self.stream(name).await?;
        Ok(stream.definition.vector_indexes.clone())
    }
    async fn query_loaded(&self, sql: &str) -> Result<Vec<u8>, Error> {
        let tables: Vec<_> = self
            .streams
            .lock()
            .await
            .iter()
            .map(|(name, s)| (name.clone(), s.clone() as Arc<dyn SnapshotSource>))
            .collect();
        let result = walleye_lance::query(&self.cache.storage, &tables, sql).await?;
        let mut writer = arrow_json::ArrayWriter::new(BoundedOutput(Vec::new()));
        writer.write_batches(&result.iter().collect::<Vec<_>>())?;
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
        match self.query_loaded(sql).await {
            Ok(result) => Ok(result),
            Err(first) if likely_unknown_relation(&first) => {
                // A sibling node may have admitted a stream after this
                // process's initial catalog load. Refresh only on an unknown
                // relation error; malformed SQL and execution failures keep
                // their original error without paying for another LIST.
                self.refresh_catalog().await?;
                self.query_loaded(sql).await.or(Err(first))
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
async fn compact_stream(
    stream: &Arc<Stream>,
    min_sstables: usize,
    query_timeout: std::time::Duration,
) -> Result<Option<CompactionResult>, Error> {
    let compactor = stream.table().await?.compactor();
    let Some(compactor) = compactor else {
        return Ok(None);
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
) -> Result<RecordBatch, Error> {
    let mut columns = Vec::with_capacity(full.fields().len());
    for field in full.fields() {
        if field.name() == HIDDEN_PK {
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
fn strip_hidden_pk(batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let schema = batch.schema();
    let keep: Vec<usize> = (0..schema.fields().len())
        .filter(|&i| schema.field(i).name() != HIDDEN_PK)
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
            32 * 1024 * 1024,
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
