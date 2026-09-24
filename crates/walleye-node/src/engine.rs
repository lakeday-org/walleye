//! One deployment's stream registry. Definitions use object-store create-if-absent;
//! one designated ingress owns a separately locked memshard for each stream.
use crate::cluster::{Cluster, NoOwner, NotOwner, StaleOwner};
use crate::ownership::{LeaseConfig, Ownership, Refusal, Route};
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
    TableSnapshot, TextIndexSpec, VectorIndexSpec,
};

/// Merge flushed generations once this many exist.
pub const COMPACT_MIN_SSTABLES: usize = 8;
/// The largest stream one member will hand to another for a query that spans
/// owners. Beyond it the query must run where the data lives.
pub const GATHER_ROW_LIMIT: usize = 1_000_000;
/// A table's rows taken from its owner, with the schema they arrived under.
type GatheredTable = (String, Arc<Schema>, Vec<RecordBatch>);
/// Minimum spacing between automatic compaction attempts per table.
/// A number that differs between two processes racing for the same stream and
/// between one attempt and the next, without taking a dependency on a random
/// number generator for a backoff nobody is betting on.
/// How long a table loaded ahead of a takeover stays as loaded before it is
/// loaded again. Past this, what the owner has flushed since is worth
/// warming too.
const PREPARED_FOR: std::time::Duration = std::time::Duration::from_secs(10);
fn jitter(stream: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::process::id().hash(&mut hasher);
    stream.hash(&mut hasher);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0)
        .hash(&mut hasher);
    hasher.finish()
}
const COMPACT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
/// How long a claim on a writer is worth having. A writer a peer fences sooner
/// than this never got a turn: the epoch was taken off it before it had
/// finished replaying the log behind it, which is what two eager writers do to
/// each other when nothing makes them wait.
const MIN_HOLD: std::time::Duration = std::time::Duration::from_millis(250);
/// How long to leave a peer alone before claiming the writer back, after a
/// fence that arrived inside [`MIN_HOLD`]. Doubles per consecutive fast fence,
/// so a stream nobody is fighting over pays nothing and one two processes are
/// racing settles into turns instead of thrash.
const RECLAIM_BACKOFF: std::time::Duration = std::time::Duration::from_millis(60);
const RECLAIM_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_millis(2_000);

type Error = Box<dyn std::error::Error + Send + Sync>;

/// An alarm by the key that owns it and its name.
type AlarmSlot = (String, String);
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
            text_indexes: Vec::new(),
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
    /// Full-text indexes maintained on the memtable and every generation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub text_indexes: Vec<TextIndexSpec>,
}
/// Columns the node maintains and clients neither write nor see.
pub fn hidden(column: &str) -> bool {
    column == HIDDEN_PK || column == HIDDEN_SEQ
}
pub(crate) fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_micros() as u64)
        .unwrap_or(0)
}
/// The Arrow type a column kind is stored as. `json` is text holding the value
/// exactly as it arrived; `decimal` keeps nine places, which is every
/// currency; `timestamp` is microseconds in UTC; `vector:N` is N floats.
pub(crate) fn storage_type(kind: &str) -> Option<DataType> {
    Some(match kind {
        "string" | "json" => DataType::Utf8,
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "decimal" => DataType::Decimal128(
            crate::ingest::literal::DECIMAL_PRECISION,
            crate::ingest::literal::DECIMAL_SCALE as i8,
        ),
        "timestamp" => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        // A vector of this many floats, the shape a vector index searches.
        vector if vector.starts_with("vector:") => {
            let width: i32 = vector["vector:".len()..].parse().ok().filter(|w| *w > 0)?;
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), width)
        }
        _ => return None,
    })
}
/// Whether `key` can own things: a table name, or `view.<name>` for a view
/// with no source table.
pub(crate) fn valid_key(key: &str) -> bool {
    valid_name(key) || key.strip_prefix(VIEW_KEY).is_some_and(valid_name)
}
/// The prefix of the ownership key of a view that has no source table. A dot
/// cannot appear in a table name, so no table shares it.
pub(crate) const VIEW_KEY: &str = "view.";
pub(crate) fn valid_name(name: &str) -> bool {
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
            text_indexes: Vec::new(),
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
            .with_vector_indexes(self.vector_indexes.clone())?
            .with_text_indexes(self.text_indexes.clone())?);
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
                let t = storage_type(&c.kind).ok_or(
                    "type must be string, int64, float64, boolean, decimal, timestamp, json, or \
                     vector:N",
                )?;
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
        // A table declared by columns takes its indexes from the catalog the
        // same way one declared by schema does. It did not, so a stream-API
        // table could hold an index spec that nothing ever built.
        Ok(TableConfig::new(
            &self.name,
            format!("{}/data/{}", root.trim_end_matches('/'), self.name),
            Arc::new(Schema::new(fields)),
            self.primary_key.clone(),
        )?
        .with_vector_indexes(self.vector_indexes.clone())?
        .with_text_indexes(self.text_indexes.clone())?)
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
    /// The table loaded read-only while the process this one replaces still
    /// owns it, and when, so the claim that follows its release opens only
    /// the writer.
    prepared: Mutex<Option<(Instant, walleye_lance::Prepared)>>,
    /// When the writer this stream holds claimed its epoch. A fence can then
    /// tell a claim that was used from one taken away before it was worth
    /// anything.
    claimed_at: Mutex<Option<Instant>>,
    /// Consecutive fences that arrived before the claim had been held for
    /// [`MIN_HOLD`]. Two writers racing drive this up and back each other off;
    /// one turn that lasts puts it back to nothing.
    contention: std::sync::atomic::AtomicU32,
    /// Who owns the table. A writer opens only while this process holds the
    /// table's ownership record, at the epoch that record names.
    owners: Arc<Ownership>,
    /// The name of the log this writer appends to.
    log: Arc<LogName>,
}
impl Stream {
    /// Bytes this stream's writer may hold in memory: the memtable size and
    /// unflushed bound the table is opened with, plus every vector index's
    /// graph at the memtable's row capacity. Not the vectors: those are in the
    /// memtable already.
    fn memory_footprint(&self) -> usize {
        const MEMTABLE_BYTES: usize = 16 * 1024 * 1024 + 32 * 1024 * 1024;
        const MEMTABLE_ROWS: usize = 100_000;
        /// A graph node's neighbour lists and the store's row lookup, per row
        /// of capacity: 50 MiB measured over 100,000 rows, rounded up to cover
        /// the widest full memtable measured.
        const PER_VECTOR_SLOT: usize = 576;
        let vectors: usize = self
            .config
            .vector_indexes
            .iter()
            .filter_map(|spec| self.config.schema.field_with_name(&spec.column).ok())
            // The graph preallocates a node per row of capacity, and the store
            // a lookup entry per row; the vectors themselves are referenced in
            // the memtable's own batches, which MEMTABLE_BYTES already counts.
            // Measured, each in its own process, at 128, 768, 1536 and 3072
            // dimensions: the graph is 50 MiB however wide the vectors, and a
            // full memtable with it 82 to 102 MiB. Charging the vectors again
            // per row of capacity, as this used to, put a 3072-wide column at
            // 1184 MiB against 101 real, and a node could hold a handful.
            .map(|field| match field.data_type() {
                DataType::FixedSizeList(_, _) => MEMTABLE_ROWS * PER_VECTOR_SLOT,
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
    ///
    /// Discarding is all that happens, whoever did the fencing. A peer holding
    /// the writer is not a reason to stay out of the stream: the claim is a
    /// compare-and-swap that any live process may win, so the next write here
    /// takes it back the same way the peer took it. That is what lets several
    /// ingestors share one stream, each paying an open and a WAL replay for
    /// its turn, and it is why a fenced write is worth retrying.
    ///
    /// The cost is that two processes both writing steadily will trade the
    /// writer on every append, and an open is not free. Whoever wants one
    /// writer rather than a rota gets it by routing, not by a process
    /// refusing to open: see `Engine::owner`.
    async fn discard_fenced_writer(&self, reason: walleye_lance::FenceReason) -> bool {
        if reason == walleye_lance::FenceReason::PeerClaimedEpoch {
            // A claim taken away inside MIN_HOLD is a peer racing us rather
            // than a handover: it did not last long enough to replay the log
            // and write anything. Count those, and only those, because a
            // writer that had its turn should claim straight back.
            let held = self.claimed_at.lock().await.map(|at| at.elapsed());
            match held {
                Some(held) if held < MIN_HOLD => {
                    self.contention.fetch_add(1, Ordering::AcqRel);
                }
                _ => self.contention.store(0, Ordering::Release),
            }
        }
        *self.claimed_at.lock().await = None;
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
    /// Leave the peer that fenced us alone for a moment before claiming the
    /// writer back.
    ///
    /// Claiming is a compare-and-swap nobody can refuse, so the only way to
    /// give a writer a turn is for the other one to wait. Without this, two
    /// processes writing at once take the epoch off each other faster than
    /// either can replay the WAL behind it, and most writes are refused while
    /// the epoch climbs: livelock, not deadlock, and not corruption, but a
    /// stream that mostly says no.
    ///
    /// The wait doubles per consecutive fast fence and is jittered, because
    /// two processes that back off by the same amount collide again on the
    /// far side of it. A stream nobody is competing for never gets here.
    async fn wait_before_reclaiming(&self) {
        let rounds = self.contention.load(Ordering::Acquire);
        if rounds == 0 {
            return;
        }
        let doubled = RECLAIM_BACKOFF.saturating_mul(1 << rounds.min(5));
        let capped = doubled.min(RECLAIM_BACKOFF_MAX);
        // Half to full, the shape that keeps two backoffs from lining up.
        let spread = capped.as_millis() as u64;
        let wait = spread / 2 + jitter(&self.definition.name) % spread.max(1);
        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
    }
    /// How many times an open will re-read the manifest and claim again after
    /// losing the race to another opener. Overlapping opens are ordinary - a
    /// reconfigure reopening a stream while the warm-up opens it, for one - and
    /// failing the request over one is a flake, not a fault.
    const CLAIM_ATTEMPTS: usize = 5;

    /// Build this writer's durability and claim the table's writer epoch.
    ///
    /// The ownership record names the epoch first and the writer claims it
    /// second, so the record and the MemWAL manifest agree on one fencing
    /// epoch. Only the record's holder moves it, which is what makes the
    /// writer's claim - a compare-and-swap anybody could win - land on the
    /// number the record already says.
    async fn claim_table(&self) -> Result<Table, Error> {
        if let Some(writer) = &self.bitr
            && walleye_lance::prepare_bitr_takeover(
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
        let started = Instant::now();
        // What was loaded ahead is brought up to date, not loaded again: one
        // read of the latest version rather than the whole dataset.
        let mut dataset = self.prepared.lock().await.take().map(|(_, loaded)| loaded);
        let ahead = dataset.is_some();
        match &mut dataset {
            Some(loaded) => loaded.refresh().await?,
            None => {
                dataset = walleye_lance::Prepared::load(&self.config, &self.storage).await?;
            }
        }
        let load_ms = started.elapsed().as_millis();
        let epoch = match &dataset {
            Some(loaded) => loaded.next_writer_epoch(self.config.shard_id).await?,
            None => 1,
        };
        let epoch_ms = started.elapsed().as_millis() - load_ms;
        if !self.owners.set_epoch(&self.definition.name, epoch).await? {
            return Err(Box::new(NotOwner {
                table: self.definition.name.clone(),
                owner: None,
            }));
        }
        let recorded = Instant::now();
        let durability = match &self.bitr {
            Some(writer) => LanceDurability::Bitr(Arc::new(BitrWalBackend::new(
                writer.clone(),
                &self.config.stream,
                self.config.shard_id,
                epoch,
            )?)),
            None => LanceDurability::ObjectStore,
        };
        let config = self.config.clone().with_log(self.log.get().await?);
        let record_ms = recorded.duration_since(started).as_millis() - load_ms - epoch_ms;
        let table = Table::open_prepared(config, self.storage.clone(), durability, dataset).await?;
        eprintln!(
            "walleye.storage claim stream={} prepared={ahead} load_ms={load_ms} \
             epoch_ms={epoch_ms} record_ms={record_ms} open_ms={}",
            self.definition.name,
            recorded.elapsed().as_millis()
        );
        // An open that overlapped another moved the manifest in between. The
        // writer holds the later epoch and has fenced whatever held the
        // earlier one, so the record follows it rather than the writer being
        // thrown away after its claim.
        let claimed = table.writer_epoch();
        if claimed != epoch
            && !self
                .owners
                .set_epoch(&self.definition.name, claimed)
                .await?
        {
            let _ = table.close().await;
            return Err(Box::new(NotOwner {
                table: self.definition.name.clone(),
                owner: None,
            }));
        }
        Ok(table)
    }

    async fn table(&self) -> Result<MappedMutexGuard<'_, Table>, Error> {
        *self.last_used.lock().await = Instant::now();
        let mut table = self.table.lock().await;
        if table.is_none() {
            // Fail closed before opening: a writer we cannot afford must not
            // exist. Memtable plus unflushed bound, plus the in-memory vector
            // graph sized for the memtable's row capacity.
            let lease = self.resources.reserve_memory(
                &format!("table {}", self.definition.name),
                self.memory_footprint(),
            )?;
            *self.lease.lock().await = Some(lease);
            let mut opened = None;
            for attempt in 0..Self::CLAIM_ATTEMPTS {
                match self.claim_table().await {
                    Ok(table) => {
                        opened = Some(table);
                        break;
                    }
                    // Two opens overlapping, one of which committed the epoch
                    // first. Nothing is wrong and nothing was fenced; the
                    // manifest moved under this one, and reading it again
                    // gives a claim that works.
                    Err(error)
                        if attempt + 1 < Self::CLAIM_ATTEMPTS
                            && error
                                .downcast_ref::<walleye_lance::LanceError>()
                                .is_some_and(walleye_lance::is_claim_race) =>
                    {
                        // Count it as contention so the wait is a real one:
                        // two opens that retry the instant they lose simply
                        // race again, and the backoff exists to stagger them.
                        self.contention.fetch_add(1, Ordering::AcqRel);
                        self.wait_before_reclaiming().await;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            let Some(opened) = opened else {
                *self.lease.lock().await = None;
                return Err(Box::new(walleye_lance::LanceError::io(format!(
                    "could not claim the writer for {} after {} attempts; another opener keeps \
                     winning the race",
                    self.definition.name,
                    Self::CLAIM_ATTEMPTS
                ))));
            };
            *table = Some(opened);
            *self.claimed_at.lock().await = Some(Instant::now());
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
        for attempt in 0..Engine::RECLAIM_ATTEMPTS {
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
                Some(reason)
                    if attempt + 1 < Engine::RECLAIM_ATTEMPTS
                        && self.discard_fenced_writer(reason).await =>
                {
                    self.wait_before_reclaiming().await;
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
    /// Where the ingest path keeps its routes and table rules.
    ingest_path: Path,
    ingest: crate::ingest::State,
    /// One pass per view at a time. A pass reads the cursor, does its work and
    /// only then moves the cursor, so two overlapping passes both start from
    /// the same place and both deliver the same rows. The node's processor
    /// and a caller's refresh are two such passes.
    view_passes: Mutex<HashMap<String, Arc<Mutex<()>>>>,
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
    /// View definitions by name with the object version they were read at,
    /// so the ownership sweep reads only the ones that changed.
    view_defs: Mutex<HashMap<String, (String, ViewDefinition)>>,
    /// One firing of an alarm at a time on this process.
    firing_locks: Mutex<HashMap<AlarmSlot, Arc<Mutex<()>>>>,
    /// Held by every firing for its length, and taken outright by a release,
    /// so a process hands a key over only between firings, never during one.
    firings: RwLock<()>,
    // Requests share this read lock; shutdown waits for all active requests.
    closed: RwLock<bool>,
    writer: Option<Arc<QuorumWriter>>,
    cluster: Cluster,
    owners: Arc<Ownership>,
    log: Arc<LogName>,
    /// The lease renewer and the bucket sampler, stopped with the engine.
    background: Vec<tokio::task::AbortHandle>,
    /// The one crossing out of a worker's isolate.
    reach: Arc<dyn walleye_v8::Host>,
}
impl Engine {
    /// How many times a request will claim the writer back before it gives up
    /// and answers retryably. One is enough for a handover, where the peer has
    /// finished and gone; it is not enough when the peer is still writing,
    /// because the reclaim is fenced mid-replay and the caller sees a refusal
    /// for what is only a queue.
    const RECLAIM_ATTEMPTS: usize = 5;
    pub async fn open(
        config: ApiConfig,
        cache: CachedStorage,
        params: ObjectStoreParams,
        cluster: Cluster,
        lease: LeaseConfig,
    ) -> Result<Self, Error> {
        lease.validate()?;
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
        let owners = Arc::new(Ownership::new(
            catalog.inner.clone(),
            prefix.clone(),
            &cluster.node_id,
            cluster.endpoint.clone(),
            lease,
        ));
        // A lease before anything else: nothing may be claimed without one.
        let mut published = false;
        for _ in 0..5 {
            if owners.renew().await {
                published = true;
                break;
            }
        }
        if !published {
            return Err("could not publish this process's lease in the bucket".into());
        }
        let background = vec![
            tokio::spawn({
                let owners = owners.clone();
                async move { owners.renew_forever().await }
            })
            .abort_handle(),
            tokio::spawn({
                let owners = owners.clone();
                async move { owners.sample_forever().await }
            })
            .abort_handle(),
        ];
        eprintln!(
            "walleye.ownership start node={} addr={}",
            owners.node(),
            cluster.endpoint
        );
        let log = Arc::new(LogName::new(config.bitr_url.clone()));
        let engine = Self {
            config,
            cache,
            catalog,
            catalog_path: prefix.clone().join("streams"),
            views_path: prefix.clone().join("views"),
            ingest_path: prefix.clone().join("ingest"),
            ingest: crate::ingest::State::default(),
            view_passes: Mutex::new(HashMap::new()),
            data_path: prefix.join("data"),
            streams: Mutex::new(BTreeMap::new()),
            dropping: Mutex::new(BTreeSet::new()),
            catalog_loaded: Mutex::new(false),
            view_defs: Mutex::new(HashMap::new()),
            firing_locks: Mutex::new(HashMap::new()),
            firings: RwLock::new(()),
            closed: RwLock::new(false),
            log,
            writer,
            cluster,
            owners,
            background,
            reach: crate::reach::Reach::new(
                crate::reach::Allowed::from_env(),
                tokio::runtime::Handle::current(),
            ),
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
                prepared: Mutex::new(None),
                claimed_at: Mutex::new(None),
                contention: std::sync::atomic::AtomicU32::new(0),
                owners: self.owners.clone(),
                log: self.log.clone(),
            })
        });
        Ok(stream.clone())
    }
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }
    pub fn ownership(&self) -> &Ownership {
        &self.owners
    }
    /// Where requests for `name` go, claiming it here when nobody live owns
    /// it and it belongs here. `fresh` reads the bucket rather than the last
    /// sample. Never
    /// answers [`Route::Unowned`]: a table this process cannot take is a
    /// [`NoOwner`] error with the wait before trying again.
    pub async fn route(&self, name: &str, fresh: bool) -> Result<Route, Error> {
        self.route_request(name, fresh, false).await
    }
    /// As [`Self::route`], for a request that arrived here. One another
    /// process forwarded is taken here if nobody owns it, wherever it belongs:
    /// that process sent it because it belongs here, and sending it on again
    /// could go round in circles while two views of the leases disagree.
    pub async fn route_request(
        &self,
        name: &str,
        fresh: bool,
        forwarded: bool,
    ) -> Result<Route, Error> {
        match self.owners.resolve(name, fresh).await? {
            Route::Unowned => {}
            route => return Ok(route),
        }
        if self.owners.draining() {
            // A process on its way out takes nothing. Send the request to the
            // live process that should, which claims it on arrival.
            let me = self.owners.node();
            return match self.owners.preferred(name) {
                Some(peer) if peer.node != me => Ok(Route::Remote {
                    peer,
                    verdict_in: self.owners.config().sample(),
                }),
                _ => Err(Box::new(NoOwner {
                    table: name.to_owned(),
                    retry_after: self.owners.config().sample(),
                })),
            };
        }
        // A key nobody owns goes to the process it belongs to, which takes
        // it on arrival; only a request already sent here is taken here.
        let me = self.owners.node();
        if !forwarded
            && let Some(peer) = self.owners.preferred(name)
            && peer.node != me
            // The last sample can be a lease behind: one that is draining,
            // retired or lapsed since is no place to send the key.
            && let Some(verdict_in) = self.owners.accepting(&peer.node).await?
        {
            return Ok(Route::Remote { peer, verdict_in });
        }
        match self.owners.claim(name).await? {
            Ok(epoch) => {
                self.open_claimed(name).await;
                Ok(Route::Local { epoch })
            }
            Err(Refusal::Owned { peer, verdict_in }) => Ok(Route::Remote { peer, verdict_in }),
            Err(Refusal::NotNow { retry_after }) => Err(Box::new(NoOwner {
                table: name.to_owned(),
                retry_after,
            })),
        }
    }
    /// Open a table just claimed, replaying whatever its previous owner left
    /// in the log, and learn where its arrival numbers stand - before the
    /// request that claimed it takes the process's write lock, so tables
    /// taken over together are not opened one after another under it. A
    /// table not in the catalog yet is being created, and opens then.
    async fn open_claimed(&self, name: &str) {
        let Ok(stream) = self.definition(name).await else {
            return;
        };
        let started = Instant::now();
        if let Err(error) = stream.table().await.map(drop) {
            eprintln!("walleye.ownership open table={name} outcome=error error={error}");
            return;
        }
        let opened_ms = started.elapsed().as_millis();
        let mut seq = stream.seq.lock().await;
        let from = self.seed_seq(name, &mut seq).await.unwrap_or("unknown");
        eprintln!(
            "walleye.ownership open table={name} elapsed_ms={} writer_ms={opened_ms} seq={from}",
            started.elapsed().as_millis()
        );
    }
    /// Resolves once `node` has gone a whole lease verdict without renewing,
    /// on this process's clock. A lease that disappears counts from when it
    /// did: a holder that retires it is still answering what it was already
    /// sent, and one whose lease was collected lapsed long ago.
    pub async fn lease_lapses(&self, node: String) {
        let mut gone_since: Option<Instant> = None;
        loop {
            tokio::time::sleep(self.owners.config().sample()).await;
            match self.owners.liveness(&node, true).await {
                Ok(crate::ownership::Liveness::Lapsed) => return,
                Ok(crate::ownership::Liveness::Gone) => {
                    let since = *gone_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= self.owners.config().verdict() {
                        return;
                    }
                }
                Ok(crate::ownership::Liveness::Live { .. }) | Err(_) => gone_since = None,
            }
        }
    }

    /// The one memory and disk budget every allocation in this process
    /// borrows from.
    pub fn resources(&self) -> &Arc<walleye_cache::QueryResources> {
        &self.cache.resources
    }
    /// A stream this process owns, claiming it if nobody live does. One that
    /// another process owns has any local writer closed, so the owner's is
    /// the only live one.
    async fn stream(&self, name: &str) -> Result<Arc<Stream>, Error> {
        let stream = self.definition(name).await?;
        if self.owners.holds(name).is_some() {
            return Ok(stream);
        }
        match self.route(name, false).await {
            Ok(Route::Local { .. }) => Ok(stream),
            Ok(Route::Remote { peer, .. }) => {
                close_writer(&stream).await;
                Err(Box::new(NotOwner {
                    table: name.to_owned(),
                    owner: Some(peer),
                }))
            }
            Ok(Route::Unowned) => Err(Box::new(NoOwner {
                table: name.to_owned(),
                retry_after: self.owners.config().sample(),
            })),
            Err(error) => {
                close_writer(&stream).await;
                Err(error)
            }
        }
    }
    /// Close the writers of tables this process stopped owning without
    /// releasing them. They are not flushed: another process may have their
    /// epoch by now, and a fenced writer cannot flush anyway.
    pub async fn close_lost(&self) {
        for name in self.owners.take_lost() {
            let stream = self.streams.lock().await.get(&name).cloned();
            if let Some(stream) = stream {
                close_writer(&stream).await;
                eprintln!("walleye.ownership lost table={name} writer=closed");
            }
        }
    }
    /// Claim what nobody live owns and this process should, by rendezvous
    /// over the live leases, and open what it claims so a dead owner's log
    /// is replayed now rather than on the first request. Returns the tables
    /// claimed.
    pub async fn sweep(&self) -> Vec<String> {
        self.close_lost().await;
        if !self.owners.settled() || self.owners.draining() {
            return Vec::new();
        }
        if let Err(error) = self.refresh_catalog().await {
            eprintln!("walleye.ownership sweep stage=catalog outcome=error error={error}");
            return Vec::new();
        }
        let mut names: Vec<String> = self.streams.lock().await.keys().cloned().collect();
        let views = match self.view_definitions().await {
            Ok(views) => views,
            Err(error) => {
                eprintln!("walleye.ownership sweep stage=views outcome=error error={error}");
                return Vec::new();
            }
        };
        // A view's key is owned like a table, so its schedule and its worker's
        // alarm have exactly one process to run them.
        for view in &views {
            let key = driver_key(view);
            if !names.contains(&key) {
                names.push(key);
            }
        }
        self.owners.plan(&names);
        let mut claimed = Vec::new();
        for name in self.owners.orphaned(&names) {
            match self.owners.claim(&name).await {
                Ok(Ok(_)) => claimed.push(name),
                Ok(Err(_)) => {}
                Err(error) => {
                    eprintln!("walleye.ownership sweep table={name} outcome=error error={error}")
                }
            }
        }
        if !claimed.is_empty() {
            self.warm_tables(claimed.clone()).await;
        }
        self.reconcile_views(&views).await;
        // One key at a time, so a rebalance is a trickle a sweep apart rather
        // than a stampede, and keys only ever move to where they belong, so it
        // stops once nobody holds more than their share.
        if let Some(key) = self.owners.surplus(&names) {
            self.hand_back(&key).await;
        }
        claimed
    }
    /// Load, read-only, every table the process this one is replacing holds,
    /// so that when it stops and releases them each claim here opens only
    /// the writer. Nothing is claimed and nothing is written: the owner
    /// keeps serving them untouched. A table is loaded again once what was
    /// loaded is [`PREPARED_FOR`] old, so what it warms stays close to what
    /// the release leaves.
    pub async fn prepare_successor(&self) {
        if self.owners.draining() {
            return;
        }
        let known: Vec<String> = self.streams.lock().await.keys().cloned().collect();
        if self.owners.held_by_predecessor(&known).is_empty() {
            // The catalog may not be loaded yet, or may have grown.
            if !self.owners.has_predecessor() || self.refresh_catalog().await.is_err() {
                return;
            }
        }
        let known: Vec<String> = self.streams.lock().await.keys().cloned().collect();
        let due: Vec<Arc<Stream>> = {
            let streams = self.streams.lock().await;
            let mut due = Vec::new();
            for name in self.owners.held_by_predecessor(&known) {
                let Some(stream) = streams.get(&name) else {
                    continue;
                };
                let fresh = stream
                    .prepared
                    .try_lock()
                    .map(|p| {
                        p.as_ref()
                            .is_some_and(|(at, _)| at.elapsed() < PREPARED_FOR)
                    })
                    .unwrap_or(true);
                if !fresh {
                    due.push(stream.clone());
                }
            }
            due
        };
        futures::stream::iter(due)
            .for_each_concurrent(4, |stream| async move {
                let name = &stream.definition.name;
                let started = Instant::now();
                let loaded =
                    match walleye_lance::Prepared::load(&stream.config, &stream.storage).await {
                        Ok(Some(loaded)) => loaded,
                        Ok(None) => return,
                        Err(error) => {
                            eprintln!(
                                "walleye.ownership prepare table={name} outcome=error error={error}"
                            );
                            return;
                        }
                    };
                match loaded.warm(stream.config.shard_id).await {
                    Ok(generations) => {
                        *stream.prepared.lock().await = Some((Instant::now(), loaded));
                        eprintln!(
                            "walleye.ownership prepare table={name} generations={generations} \
                             elapsed_ms={}",
                            started.elapsed().as_millis()
                        );
                    }
                    Err(error) => eprintln!(
                        "walleye.ownership prepare table={name} outcome=error error={error}"
                    ),
                }
            })
            .await;
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
                self.register(existing).await?;
                let stream = self.stream(&definition.name).await?;
                drop(stream.table().await?);
                return Ok(());
            }
            Err(error) => {
                catalog_stage_finish("catalog_put", &definition.name, put_started, "error");
                return Err(error.into());
            }
        }
        let name = definition.name.clone();
        self.register(definition).await?;
        let stream = self.stream(&name).await?;
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
                        "string" | "json" => value.is_string(),
                        "int64" => value.as_i64().is_some(),
                        "float64" => value.is_number(),
                        "boolean" => value.is_boolean(),
                        "decimal" => value.is_number() || value.is_string(),
                        "timestamp" => value.is_string() || value.as_i64().is_some(),
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
    /// Append Arrow batches. Returns the new table version. A table another
    /// process owns is appended to through that process, which is how a view,
    /// a worker or an alarm handler running here writes wherever it names.
    pub async fn append(&self, name: &str, batches: Vec<RecordBatch>) -> Result<u64, Error> {
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        let mut fresh = false;
        loop {
            let Route::Remote { peer, .. } = self.route(name, fresh).await? else {
                break;
            };
            match self.cluster.insert(&peer, name, &batches).await {
                Err(error) if !fresh && error.downcast_ref::<NotOwner>().is_some() => {
                    fresh = true;
                }
                answer => return answer,
            }
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
        self.seed_seq(name, &mut seq).await?;
        let next = seq.expect("seeded");
        *seq = Some(next.saturating_add(rows.max(1)));
        Ok(next)
    }
    /// Make sure `seq` holds the stream's next arrival number. A key claimed
    /// from a release starts where its last owner stopped, which that owner
    /// wrote into the release; anything else scans the table for its
    /// highest, once per process. Returns which it was.
    async fn seed_seq(&self, name: &str, seq: &mut Option<u64>) -> Result<&'static str, Error> {
        if let Some(next) = self.owners.take_handed_seq(name) {
            *seq = Some(seq.unwrap_or(0).max(next).max(now_micros()));
            return Ok("handed");
        }
        if seq.is_none() {
            *seq = Some(self.highest_seq(name).await?.max(now_micros()));
            return Ok("scanned");
        }
        Ok("known")
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
        // More than one reclaim, because under contention the reopen is itself
        // what gets fenced: the writer is taken back while it is still
        // replaying the log. Each attempt waits longer than the last, so a
        // caller sees a slower write rather than a refused one.
        for attempt in 0..Self::RECLAIM_ATTEMPTS {
            let outcome = async {
                let mut table = stream.table().await?;
                let epoch = table.writer_epoch();
                table.append(batches.clone()).await?;
                // Acknowledge only as the owner, at the epoch this writer
                // holds, checked after the rows are durable rather than only
                // before they were sent.
                if !self.owners.confirm(&stream.definition.name, epoch) {
                    drop(table);
                    close_writer(stream).await;
                    return Err(Box::new(StaleOwner(stream.definition.name.clone())) as Error);
                }
                Ok::<(), Error>(())
            }
            .await;
            let error = match outcome {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            let reason = walleye_lance::writer_fence_reason(&*error);
            let Some(reason) = reason.filter(|_| attempt + 1 < Self::RECLAIM_ATTEMPTS) else {
                return Err(error);
            };
            // A fence means somebody else claimed this stream's writer.
            // Reopening claims it straight back, because claiming is the only
            // way an epoch ever moves, so a process that reopens a table it no
            // longer owns takes the writer from the one that does.
            //
            // Only the owner may reopen. The rows of the fenced attempt did
            // not reach the log, since the fence refused them.
            if self.owners.holds(&stream.definition.name).is_none() {
                close_writer(stream).await;
                return Err(Box::new(NotOwner {
                    table: stream.definition.name.clone(),
                    owner: None,
                }));
            }
            if !stream.discard_fenced_writer(reason).await {
                return Err(error);
            }
            stream.wait_before_reclaiming().await;
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
        if let Err(error) = self.load_catalog().await {
            eprintln!("walleye.storage warm stage=catalog outcome=error error={error}");
            return;
        }
        self.warm_tables(self.owners.held_tables()).await;
    }
    /// Open the named tables this process owns, within half the budget.
    async fn warm_tables(&self, names: Vec<String>) {
        let owned = names.into_iter().filter(|n| self.owners.holds(n).is_some());
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
                    // Read what to open under the writer, open it without:
                    // a write that arrives meanwhile does not wait for it.
                    let warmer = stream.table().await?.warmer().await?;
                    let generations = match warmer {
                        Some(warmer) => warmer.run().await?,
                        None => 0,
                    };
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
    /// The object store and prefix the ingest path keeps its rules under.
    pub(crate) fn ingest_store(&self) -> (&Arc<ObjectStore>, &Path, &crate::ingest::State) {
        (&self.catalog, &self.ingest_path, &self.ingest)
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
    /// Every table's client-visible schema, read from the catalog rather than
    /// by opening the table.
    ///
    /// [`Self::describe`] goes through [`Self::stream`], which refuses a table
    /// this node does not own. That is right for reading rows and wrong for
    /// describing a schema: the catalog is shared, so a node that cannot say
    /// what columns a table has cannot answer a question about it.
    pub async fn schemas(&self) -> Result<Vec<(String, Schema)>, Error> {
        let mut out = Vec::new();
        for name in self.table_names().await? {
            if let Ok(stream) = self.definition(&name).await {
                out.push((name, stream.definition.user_schema(&stream.config.schema)));
            }
        }
        Ok(out)
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
        // The name is free for anyone to create again.
        if let Err(error) = self.owners.release(name, None).await {
            eprintln!("walleye.ownership release table={name} outcome=error error={error}");
        }
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
    /// Set the full-text index for a column. As with a vector index the spec
    /// is stored in the catalog and the table reopened, so the memtable and
    /// every later generation maintain it, and existing generations pick it up
    /// at their next compaction.
    pub async fn configure_text_index(&self, name: &str, spec: TextIndexSpec) -> Result<(), Error> {
        let closed = self.closed.read().await;
        if *closed {
            return Err("engine is closed".into());
        }
        let stream = self.stream(name).await?;
        let mut definition = stream.definition.clone();
        definition.text_indexes.retain(|t| t.column != spec.column);
        definition.text_indexes.push(spec);
        definition
            .text_indexes
            .sort_by(|a, b| a.column.cmp(&b.column));
        definition.table_config(&self.config.root_uri)?;
        let stream = if definition.text_indexes == stream.definition.text_indexes {
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
        compact_stream(&stream, 1, self.cache.storage.query_timeout(), true).await?;
        Ok(())
    }
    pub async fn text_indexes(&self, name: &str) -> Result<Vec<TextIndexSpec>, Error> {
        let stream = self.stream(name).await?;
        Ok(stream.definition.text_indexes.clone())
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
        let mut gathered = Vec::new();
        let mut leases = Vec::new();
        for name in walleye_lance::sql_table_names(sql)? {
            if !self.streams.lock().await.contains_key(&name) {
                continue;
            }
            let owner = match self.route(&name, false).await? {
                Route::Remote { peer, .. } => peer,
                Route::Local { .. } | Route::Unowned => continue,
            };
            let bytes = self.cluster.fetch_snapshot(&owner, &name).await?;
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
    /// Hand every table back and leave: stop claiming, flush and close each
    /// owned table's writer, release its record at the same epoch so a peer
    /// or a replacement claims it at once, then remove the lease. A request
    /// for a released table is forwarded to the process that should claim it.
    pub async fn release_all(&self) {
        if self.owners.draining() {
            return;
        }
        let started = Instant::now();
        self.owners.drain().await;
        self.quiesce_alarms().await;
        let held = self.owners.held_tables();
        let count = held.len();
        futures::stream::iter(held)
            .for_each_concurrent(8, |name| async move { self.release_key(&name).await })
            .await;
        self.owners.retire().await;
        eprintln!(
            "walleye.ownership released tables={count} elapsed_ms={}",
            started.elapsed().as_millis()
        );
    }
    /// Flush a key's writer and release its record at the same epoch, holding
    /// the writer slot across both so no write opens a writer in between. A
    /// write that was waiting finds the key released and is sent on to
    /// whoever takes it.
    async fn release_key(&self, name: &str) {
        let stream = self.streams.lock().await.get(name).cloned();
        let guard = match &stream {
            Some(stream) => {
                let mut guard = stream.table.lock().await;
                if let Some(mut table) = guard.take() {
                    if let Err(error) = table.checkpoint().await {
                        eprintln!(
                            "walleye.ownership release table={name} stage=flush outcome=error error={error}"
                        );
                    }
                    let _ = table.close().await;
                }
                *stream.lease.lock().await = None;
                Some(guard)
            }
            None => None,
        };
        // Read with the writer slot held, so no row takes a number after it.
        let next_seq = match &stream {
            Some(stream) => *stream.seq.lock().await,
            None => None,
        };
        if let Err(error) = self.owners.release(name, next_seq).await {
            eprintln!("walleye.ownership release table={name} outcome=error error={error}");
        }
        drop(guard);
    }

    /// Give one key back to the process it belongs to, because this one holds
    /// more than its share. Waits out any alarm firing in flight and holds
    /// new ones off until the key is released, as a stopping process does.
    async fn hand_back(&self, key: &str) {
        let started = Instant::now();
        let _firings = self.firings.write().await;
        if self.owners.holds(key).is_none() {
            return;
        }
        self.release_key(key).await;
        eprintln!(
            "walleye.ownership hand_back key={key} to={} elapsed_ms={}",
            self.owners
                .preferred(key)
                .map(|peer| peer.node)
                .unwrap_or_default(),
            started.elapsed().as_millis()
        );
    }

    pub async fn close(&self) {
        self.release_all().await;
        let mut closed = self.closed.write().await;
        *closed = true;
        let streams = std::mem::take(&mut *self.streams.lock().await);
        futures::future::join_all(streams.into_values().map(|stream| async move {
            close_writer(&stream).await;
        }))
        .await;
        for task in &self.background {
            task.abort();
        }
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        for task in &self.background {
            task.abort();
        }
    }
}
/// Close a table's writer without flushing it, and return its memory.
async fn close_writer(stream: &Stream) {
    let taken = stream.table.lock().await.take();
    if let Some(table) = taken {
        let closing = tokio::time::timeout(std::time::Duration::from_secs(10), table.close());
        if closing.await.is_err() {
            eprintln!(
                "walleye.storage writer_close stream={} outcome=timeout",
                stream.definition.name
            );
        }
    }
    *stream.lease.lock().await = None;
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
        engine_with(dir, writer, cache_id, LeaseConfig::default()).await
    }
    async fn engine_with(
        dir: &std::path::Path,
        writer: Option<Arc<QuorumWriter>>,
        cache_id: &str,
        lease: LeaseConfig,
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
            Cluster::new(cache_id.into(), "http://127.0.0.1:1".into(), "token".into()).unwrap(),
            lease,
        )
        .await
        .unwrap();
        engine.writer = writer;
        engine
    }
    /// A table whose name has a capital in it is reachable by the quoted name
    /// the catalog reports, and by nothing else.
    ///
    /// Registering it with a `&str` filed it under a folded `pets`, so `FROM
    /// "Pets"` — the only form available once a name holds a space or a
    /// reserved word — could not resolve, while `/v1/table/Pets/` and the
    /// cluster's own routing both still called it `Pets`.
    #[tokio::test]
    async fn a_table_is_queried_by_the_name_the_catalog_reports() {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(dir.path(), None, "cache").await;
        e.define(definition("Pets")).await.unwrap();
        assert_eq!(e.table_names().await.unwrap(), ["Pets"]);

        let answered = e.query(r#"SELECT count(*) AS n FROM "Pets""#).await;
        assert!(answered.is_ok(), "quoted exact name: {:?}", answered.err());

        // Folding happens on the reference in the statement, as SQL says it
        // should, so neither of these names the table that exists.
        for missing in [
            r#"SELECT count(*) FROM "pets""#,
            "SELECT count(*) FROM Pets",
        ] {
            assert!(
                e.query(missing).await.is_err(),
                "{missing} names no table here"
            );
        }
        e.close().await;
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
    /// Two engines over one root, the first holding table `t` with one row
    /// and a second write waiting inside it - past its ownership check, at the
    /// writer - when it stops renewing its lease and the second takes `t`.
    /// The first is leaked, so the write in flight can outlive this function.
    async fn a_write_in_flight_across_a_takeover(
        dir: &std::path::Path,
    ) -> (
        &'static Engine,
        Engine,
        impl std::future::Future<Output = Result<usize, Error>>,
    ) {
        let lease = LeaseConfig {
            ttl_ms: 600,
            skew_ms: 100,
            sample_ms: 100,
        };
        let a: &'static Engine =
            Box::leak(Box::new(engine_with(dir, None, "a", lease.clone()).await));
        let b = engine_with(dir, None, "b", lease).await;
        // A takes the table outright, wherever it would be placed.
        assert!(a.owners.claim("t").await.unwrap().is_ok());
        a.define(definition("t")).await.unwrap();
        a.ingest("t", vec![json!({"id":1,"value":1})])
            .await
            .unwrap();
        let stream = a.stream("t").await.unwrap();
        let guard = stream.table().await.unwrap();
        let mut pending = Box::pin(a.ingest("t", vec![json!({"id":2,"value":2})]));
        assert!(futures::poll!(&mut pending).is_pending());
        // A stops renewing, as a paused or partitioned process does.
        for task in &a.background {
            task.abort();
        }
        let started = Instant::now();
        loop {
            if let Ok(Ok(_)) = b.owners.claim("t").await {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "b never took t"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(a.owners.holds("t"), None, "a stopped acting as owner first");
        drop(guard);
        (a, b, pending)
    }

    /// The write reached the log before the new owner opened its writer, but
    /// the old owner no longer owned the table when it would have answered, so
    /// it does not acknowledge it. The new owner replays the row: the outcome
    /// the client was told is unknown turns out to be stored, once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_is_acknowledged_only_while_its_writer_still_owns_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let (_a, b, pending) = a_write_in_flight_across_a_takeover(dir.path()).await;
        let refused = pending.await.unwrap_err();
        assert!(
            refused.downcast_ref::<StaleOwner>().is_some(),
            "refused at acknowledgement: {refused}"
        );
        let rows: serde_json::Value = serde_json::from_slice(
            &b.query("SELECT count(*) AS n, count(DISTINCT id) AS d FROM t")
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rows, json!([{"n":2,"d":2}]));
    }

    /// The new owner opened its writer first, which fences the old one: the
    /// write is refused by the log itself, not written, and the old owner does
    /// not take the writer back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_behind_the_new_owners_writer_is_fenced_and_not_retaken() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, pending) = a_write_in_flight_across_a_takeover(dir.path()).await;
        b.ingest("t", vec![json!({"id":3,"value":3})])
            .await
            .unwrap();
        let refused = pending.await.unwrap_err();
        assert!(
            refused.downcast_ref::<NotOwner>().is_some()
                || refused.downcast_ref::<StaleOwner>().is_some(),
            "refused rather than retaken: {refused}"
        );
        assert_eq!(a.owners.holds("t"), None);
        let rows: serde_json::Value =
            serde_json::from_slice(&b.query("SELECT id FROM t ORDER BY id").await.unwrap())
                .unwrap();
        assert_eq!(
            rows,
            json!([{"id":1},{"id":3}]),
            "the fenced write is not stored"
        );
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
    ///
    /// A view with no source reads nothing: it is a worker that goes and gets
    /// its own rows, run on `every_seconds` rather than when rows arrive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
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
    /// How often a sourceless worker runs. Required when there is no source
    /// and meaningless when there is one, because a view with a source runs
    /// when its source has rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every_seconds: Option<u64>,
    /// When a sourceless worker runs, as five cron fields. An alternative to
    /// `every_seconds` for work that belongs at a time rather than at an
    /// interval, such as once an hour on the hour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// A socket the node holds open, handing what arrives to the worker.
    ///
    /// The connection lives in the node, not in the isolate. A worker is
    /// still a bounded turn over a batch of frames, which is what keeps it
    /// stoppable and replaceable while the stream stays connected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket: Option<Socket>,
}
/// Where to connect, and what to say on connecting.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Socket {
    pub url: String,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub headers: std::collections::HashMap<String, String>,
    /// Sent once the socket opens, usually a subscription. `{{env:NAME}}` is
    /// filled in by the node, so a key never has to live in the definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<String>,
    /// Frames to gather before handing them to the worker.
    #[serde(default = "default_frames")]
    pub frames: usize,
    /// How long to wait for that many, in milliseconds, before handing over
    /// whatever has arrived.
    #[serde(default = "default_window_ms")]
    pub window_ms: u64,
}
fn default_frames() -> usize {
    256
}
fn default_window_ms() -> u64 {
    1000
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
        for name in [Some(&view.name), view.source.as_ref(), view.target.as_ref()]
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
        if view.target.is_some() && view.target == view.source {
            return Err("a view cannot write back into its own source".into());
        }
        if let Some(socket) = &view.websocket {
            let url = reqwest::Url::parse(&socket.url)
                .map_err(|error| -> Error { format!("websocket url: {error}").into() })?;
            if !matches!(url.scheme(), "ws" | "wss") {
                return Err("a websocket url must be ws or wss".into());
            }
            if !filled(&view.worker) {
                return Err("a websocket view needs a worker to read its frames".into());
            }
            if view.source.is_some() {
                return Err("a websocket view has no source: the socket is its source".into());
            }
            if socket.frames == 0 || socket.frames > 10_000 {
                return Err("frames must be between 1 and 10000".into());
            }
        }
        if let Some(expression) = &view.cron {
            crate::cron::Schedule::parse(expression)
                .map_err(|reason| -> Error { reason.into() })?;
            if view.every_seconds.is_some() {
                return Err("a view runs on a cron or on an interval, not both".into());
            }
        }
        if view.source.is_none() && view.websocket.is_none() {
            if view.cron.is_none() && view.every_seconds.is_none() {
                return Err(
                    "a view with no source needs a cron or every_seconds: it runs on a clock, \
                     not on rows"
                        .into(),
                );
            }
            if view.every_seconds == Some(0) {
                return Err("every_seconds must be at least one".into());
            }
            if !filled(&view.worker) {
                return Err("a view with no source needs a worker to go and get its rows".into());
            }
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
        // Arm its schedule now if this process owns its key; its owner's next
        // sweep does otherwise.
        if self.owners.holds(&driver_key(&view)).is_some() {
            self.reconcile_view(&view).await?;
        }
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
        // licence to delete data somebody may still be reading. Its alarms go
        // with it, here if this process owns its key and otherwise at the
        // owner's next sweep.
        self.clear_view_alarms(&view).await;
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
        if view.source.is_none() && view.websocket.is_none() {
            // A view on a clock runs when its schedule is due, and asking
            // runs it now only if it is: the schedule is the one thing that
            // decides, whoever asks.
            self.reconcile_view(&view).await?;
            let fired = self
                .fire(&driver_key(&view), &schedule_alarm(&view.name))
                .await?;
            return Ok(fired.unwrap_or(Progress {
                rows: 0,
                written: 0,
                delivered: 0,
                through: 0,
                caught_up: true,
            }));
        }
        self.refresh_rows(&view).await
    }

    /// One pass of a view driven by rows: its source's, or its socket's.
    async fn refresh_rows(&self, view: &ViewDefinition) -> Result<Progress, Error> {
        let name = view.name.as_str();
        // One pass at a time per view, however it started - a caller's
        // refresh, a retry alarm, a worker's alarm catching its view up. Held
        // from before the cursor is read until after it moves, so a pass that
        // waited here starts from wherever the one before it finished. Every
        // pass is here: `advance_from` is reached only through this.
        let pass = self
            .view_passes
            .lock()
            .await
            .entry(name.to_owned())
            .or_default()
            .clone();
        let _only_pass = pass.lock().await;
        let consumer = format!("view:{name}");
        let cursor = self.cursor(&consumer).await?;
        match (&view.source, &view.websocket) {
            (Some(source), None) => self.advance_from(view, source, &consumer, cursor).await,
            // A socket view is driven by what arrives on its socket.
            _ => Ok(Progress {
                rows: 0,
                written: 0,
                delivered: 0,
                through: cursor,
                caught_up: true,
            }),
        }
    }

    /// Passes of a view driven by rows until it is caught up.
    async fn drain_rows(&self, view: &ViewDefinition, passes: usize) -> Result<Progress, Error> {
        let mut total = Progress {
            rows: 0,
            written: 0,
            delivered: 0,
            through: 0,
            caught_up: false,
        };
        for _ in 0..passes.clamp(1, 1000) {
            let pass = self.refresh_rows(view).await?;
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

    /// One occurrence of a view on a clock: its worker goes and gets its own
    /// rows, told which scheduled time this run stands for and how many
    /// earlier ones it also covers.
    async fn run_scheduled(
        &self,
        view: &ViewDefinition,
        firing: &crate::alarms::Firing,
    ) -> Result<Progress, Error> {
        let Some(worker) = view.worker.as_deref().filter(|w| !w.trim().is_empty()) else {
            return Err("a view with no source needs a worker".into());
        };
        let scheduled = serde_json::json!({
            "scheduledTime": firing.scheduled_ms,
            "missed": firing.missed,
            "attempt": firing.attempt,
        })
        .to_string();
        // It is handed an empty batch: its rows come from wherever it goes.
        let (produced, elsewhere, alarm) = self
            .invoke_worker(view, worker, &[], Some(scheduled))
            .await?;
        let mut written = self.land(view, elsewhere).await?;
        let made: usize = produced.iter().map(RecordBatch::num_rows).sum();
        let mut delivered = 0;
        if let Some(alert) = &view.alert
            && made > 0
        {
            self.deliver(alert, &produced).await?;
            delivered = made;
        }
        if let Some(target) = &view.target
            && made > 0
        {
            self.ensure_target(target, &produced).await?;
            self.append(target, produced).await?;
            written += made;
        }
        self.apply_worker_alarm(view, alarm).await?;
        Ok(Progress {
            rows: 0,
            written,
            delivered,
            through: firing.scheduled_ms.saturating_mul(1000),
            caught_up: true,
        })
    }

    /// One batch of whatever arrived in the source after the cursor.
    async fn advance_from(
        &self,
        view: &ViewDefinition,
        source: &str,
        consumer: &str,
        cursor: u64,
    ) -> Result<Progress, Error> {
        let idle = Progress {
            rows: 0,
            written: 0,
            delivered: 0,
            through: cursor,
            caught_up: true,
        };
        // A source that does not exist yet is a tier whose upstream has not
        // produced anything. That is idleness, not failure: the view waits.
        if self.definition(source).await.is_err() {
            return Ok(idle);
        }
        // Reading the source opens its writer, which only its owner may do.
        self.stream(source).await?;
        let pick = format!(
            "SELECT * FROM \"{source}\" WHERE {HIDDEN_SEQ} > {cursor} \
             ORDER BY {HIDDEN_SEQ} LIMIT {}",
            view.batch_rows
        );
        let fresh = self.query_batches(&pick, &[]).await?;
        let rows: usize = fresh.iter().map(RecordBatch::num_rows).sum();
        if rows == 0 {
            return Ok(idle);
        }
        let through = highest_in(&fresh)?.unwrap_or(cursor);
        // Inside the view's work the source name means these rows only, which
        // is what makes the pass incremental without the query or the worker
        // having to know about cursors at all.
        let schema = fresh[0].schema();
        let gathered = vec![(source.to_owned(), schema, fresh.to_vec())];
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
        let mut written = 0;
        let mut alarm = None;
        if let Some(worker) = view.worker.as_deref().filter(|w| !w.trim().is_empty()) {
            let (target_rows, elsewhere, change) =
                self.invoke_worker(view, worker, &produced, None).await?;
            produced = target_rows;
            alarm = change;
            written += self.land(view, elsewhere).await?;
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
        if let Some(target) = &view.target
            && made > 0
        {
            self.ensure_target(target, &produced).await?;
            self.append(target, produced).await?;
            written += made;
        }
        // The alarm before the cursor: a pass that dies between the two is
        // replayed, and sets the same alarm again, rather than moving on
        // without it.
        self.apply_worker_alarm(view, alarm).await?;
        self.set_cursor(consumer, through).await?;
        Ok(Progress {
            rows,
            written,
            delivered,
            through,
            caught_up: rows < view.batch_rows,
        })
    }

    /// Run a view's worker over a batch, with the heap it asked for taken out
    /// of the machine's budget first.
    async fn invoke_worker(
        &self,
        view: &ViewDefinition,
        worker: &str,
        batch: &[RecordBatch],
        scheduled: Option<String>,
    ) -> Result<WorkerOutput, Error> {
        let limits = view.limits();
        // A worker's heap is part of the machine's memory, not extra to it.
        // Reserving it here means a worker too large for what is left is
        // refused before it runs, and the cache gives up the room while it
        // does run.
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
        let host = self.reach.clone();
        let worker = worker.to_owned();
        let handed = batch.to_vec();
        let turn = walleye_v8::Turn {
            alarm: self.worker_alarm_at(view).await,
            scheduled,
        };
        tokio::task::spawn_blocking(move || {
            run_worker(&worker, &handed, limits, expected, host, turn)
        })
        .await
        .map_err(|error| -> Error { error.to_string().into() })?
    }

    /// Write the rows a worker sent to streams it named itself.
    ///
    /// These land before the cursor moves, so a pass that fails anywhere is
    /// replayed whole. A worker chooses where rows go; the host still owns
    /// the transaction they go in.
    async fn land(
        &self,
        view: &ViewDefinition,
        elsewhere: Vec<(String, Vec<RecordBatch>)>,
    ) -> Result<usize, Error> {
        let mut written = 0;
        for (stream, batches) in elsewhere {
            if batches.is_empty() {
                continue;
            }
            if Some(&stream) == view.source.as_ref() {
                return Err(format!("a worker wrote back into its own source, {stream}").into());
            }
            self.ensure_target(&stream, &batches).await?;
            let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            self.append(&stream, batches).await?;
            written += rows;
        }
        Ok(written)
    }

    /// Create a stream from the shape of what is about to be written, if it
    /// is not there yet.
    async fn ensure_target(&self, name: &str, batches: &[RecordBatch]) -> Result<(), Error> {
        let definition = StreamDefinition::from_arrow(name, &batches[0].schema())?;
        self.ensure_table(definition).await
    }

    /// Create a table unless it is in the catalog. One that another process
    /// created and owns meanwhile exists, which is all this asks.
    async fn ensure_table(&self, definition: StreamDefinition) -> Result<(), Error> {
        if self.definition(&definition.name).await.is_ok() {
            return Ok(());
        }
        let name = definition.name.clone();
        match self.define_with(definition, true).await {
            Err(error)
                if (error.downcast_ref::<NotOwner>().is_some()
                    || error.downcast_ref::<NoOwner>().is_some())
                    && self.definition(&name).await.is_ok() =>
            {
                Ok(())
            }
            other => other,
        }
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

    /// Hand a batch of socket frames to a view's worker.
    ///
    /// Each frame becomes one row with a `data` column holding the text as it
    /// arrived. Parsing it is the worker's job, because only the worker knows
    /// what the other end sends.
    pub async fn handle_frames(&self, name: &str, frames: Vec<String>) -> Result<usize, Error> {
        if frames.is_empty() {
            return Ok(0);
        }
        let view = self.view(name).await?;
        let Some(worker) = view.worker.as_deref().filter(|w| !w.trim().is_empty()) else {
            return Err(format!("{name} has no worker to read its frames").into());
        };
        let rows: Vec<serde_json::Value> = frames
            .into_iter()
            .map(|data| serde_json::json!({ "data": data }))
            .collect();
        let batch = rows_to_batches(rows, None)?;
        let (produced, elsewhere, alarm) = self.invoke_worker(&view, worker, &batch, None).await?;
        let mut written = self.land(&view, elsewhere).await?;
        let made: usize = produced.iter().map(RecordBatch::num_rows).sum();
        if let Some(alert) = &view.alert
            && made > 0
        {
            self.deliver(alert, &produced).await?;
        }
        if let Some(target) = &view.target
            && made > 0
        {
            self.ensure_target(target, &produced).await?;
            self.append(target, produced).await?;
            written += made;
        }
        self.apply_worker_alarm(&view, alarm).await?;
        Ok(written)
    }

    /// Hand one request to a view's worker and take back its answer.
    ///
    /// Whatever the worker wrote lands before the answer goes out, so a
    /// caller that got a 200 can rely on the rows being durable. A worker
    /// that throws answers nothing and writes nothing.
    pub async fn handle_request(
        &self,
        name: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let view = self.view(name).await?;
        let Some(worker) = view.worker.as_deref().filter(|w| !w.trim().is_empty()) else {
            return Err(format!("{name} has no worker to answer with").into());
        };
        let limits = view.limits();
        let _heap = self
            .cache
            .resources
            .reserve_memory(&format!("worker {}", view.name), limits.heap_bytes)
            .map_err(|error| -> Error { Box::new(error) })?;
        let host = self.reach.clone();
        let body = serde_json::to_string(&request)?;
        let source = worker.to_owned();
        let turn = walleye_v8::Turn {
            alarm: self.worker_alarm_at(&view).await,
            scheduled: None,
        };
        let outcome = tokio::task::spawn_blocking(move || {
            walleye_v8::run_request(&source, &body, limits, host, turn)
        })
        .await
        .map_err(|error| -> Error { error.to_string().into() })??;
        let alarm = outcome.alarm;

        let mut landed = Vec::new();
        for (stream, written) in outcome.writes {
            if !valid_name(&stream) {
                return Err(format!("a worker wrote to an invalid stream name: {stream}").into());
            }
            let value: serde_json::Value = serde_json::from_str(&written)?;
            let rows = match value {
                serde_json::Value::Array(rows) => rows,
                row => vec![row],
            };
            landed.push((stream, rows));
        }
        for (stream, rows) in landed {
            let batches = rows_to_batches(rows, None)?;
            if batches.is_empty() {
                continue;
            }
            self.ensure_target(&stream, &batches).await?;
            self.append(&stream, batches).await?;
        }
        self.apply_worker_alarm(&view, alarm).await?;
        if outcome.returned.is_empty() {
            return Ok(serde_json::json!({ "status": 204 }));
        }
        Ok(serde_json::from_str(&outcome.returned)?)
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
                // The owner of a view's source drives it, so exactly one
                // process runs each. A view on a clock or a socket is not
                // driven by rows arriving, and is not driven here.
                let Some(source) = view.source.as_deref() else {
                    continue;
                };
                if self.owners.holds(source).is_none() {
                    continue;
                }
                let outcome = self.refresh_view(name).await;
                spent += 1;
                if let Ok(progress) = &outcome {
                    if progress.rows == 0 && progress.written == 0 {
                        continue;
                    }
                    moved = true;
                }
                let failed = outcome.is_err();
                if failed {
                    // The retry is an alarm, so it survives this process and
                    // backs off, rather than waiting for the next row.
                    if let Err(error) = self.arm_pass_retry(&view).await {
                        eprintln!("walleye.alarm arm view={name} outcome=error error={error}");
                    }
                }
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
        // The cursors table may be owned anywhere; a query gathers it from
        // its owner.
        let sql = format!(
            "SELECT position FROM \"{CURSORS}\" WHERE consumer = '{}'",
            consumer.replace('\'', "''")
        );
        let rows: Vec<serde_json::Value> = serde_json::from_slice(&self.query(&sql).await?)?;
        Ok(rows
            .first()
            .and_then(|row| row["position"].as_u64())
            .unwrap_or(0))
    }
    /// Record where a consumer reached. The newest row for a consumer wins,
    /// so this supersedes rather than accumulates.
    pub async fn set_cursor(&self, consumer: &str, position: u64) -> Result<(), Error> {
        self.ensure_table(StreamDefinition::from_arrow(CURSORS, &cursor_schema())?)
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

/// Which write-ahead log a writer appends to, as a name every writer on the
/// same log spells the same way and no writer on another log does. The
/// object store's own WAL is shared by everyone writing to the bucket. A Bitr
/// log is named by where it archives, which the gateway reports: the address
/// a process reaches its gateway at says nothing about which log is behind
/// it, and the replicas holding the log can change without it becoming
/// another log.
pub(crate) struct LogName {
    gateway: Option<String>,
    name: tokio::sync::OnceCell<String>,
}
impl LogName {
    fn new(gateway: Option<String>) -> Self {
        Self {
            gateway,
            name: tokio::sync::OnceCell::new(),
        }
    }
    async fn get(&self) -> Result<String, Error> {
        let Some(gateway) = &self.gateway else {
            return Ok("object-store".to_owned());
        };
        self.name
            .get_or_try_init(|| async {
                let ready: serde_json::Value = reqwest::Client::new()
                    .get(format!("{}/readyz", gateway.trim_end_matches('/')))
                    .timeout(std::time::Duration::from_secs(10))
                    .send()
                    .await?
                    .json()
                    .await?;
                match ready["log"].as_str().filter(|log| !log.is_empty()) {
                    Some(log) => Ok(format!("bitr:{log}")),
                    None => Err(Error::from(
                        "the replica gateway is not ready to say which log it serves",
                    )),
                }
            })
            .await
            .cloned()
    }
}

/// The alarm that runs a view on its clock.
pub(crate) fn schedule_alarm(view: &str) -> String {
    format!("schedule:{view}")
}
/// The alarm a view's worker sets for itself.
pub(crate) fn worker_alarm(view: &str) -> String {
    format!("worker:{view}")
}
/// The alarm that retries a view's failed pass.
pub(crate) fn pass_alarm(view: &str) -> String {
    format!("pass:{view}")
}
/// The ownership key whose owner runs a view: its source table, or the view
/// itself when it has none. Its alarms live in that key's record.
pub(crate) fn driver_key(view: &ViewDefinition) -> String {
    match &view.source {
        Some(source) => source.clone(),
        None => format!("{VIEW_KEY}{}", view.name),
    }
}
fn wall_ms() -> u64 {
    now_micros() / 1000
}
fn alarm_id() -> u64 {
    uuid::Uuid::new_v4().as_u64_pair().0
}

impl Engine {
    /// Every view definition, reading only those whose object changed.
    async fn view_definitions(&self) -> Result<Vec<ViewDefinition>, Error> {
        let objects: Vec<_> = self
            .catalog
            .inner
            .list(Some(&self.views_path))
            .try_collect()
            .await?;
        let mut listed = HashMap::new();
        for object in objects {
            let Some(name) = object
                .location
                .filename()
                .and_then(|file| file.strip_suffix(".json"))
            else {
                continue;
            };
            let version = object
                .e_tag
                .clone()
                .unwrap_or_else(|| object.last_modified.timestamp_micros().to_string());
            listed.insert(name.to_owned(), version);
        }
        let mut cache = self.view_defs.lock().await;
        cache.retain(|name, _| listed.contains_key(name));
        for (name, version) in listed {
            if cache.get(&name).is_some_and(|(seen, _)| *seen == version) {
                continue;
            }
            match self.view(&name).await {
                Ok(view) => {
                    cache.insert(name, (version, view));
                }
                Err(error) if error.downcast_ref::<TableNotFound>().is_some() => {}
                Err(error) => return Err(error),
            }
        }
        let mut views: Vec<ViewDefinition> = cache.values().map(|(_, view)| view.clone()).collect();
        views.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(views)
    }

    /// Make a view's schedule alarm match its definition, on the process that
    /// owns its key. A view on a clock gets a repeating alarm; changing the
    /// clock replaces it; a view without one has none.
    pub(crate) async fn reconcile_view(&self, view: &ViewDefinition) -> Result<(), Error> {
        let key = driver_key(view);
        if !matches!(self.route(&key, false).await?, Route::Local { .. }) {
            return Ok(());
        }
        let name = schedule_alarm(&view.name);
        let wanted = match (
            &view.source,
            &view.websocket,
            &view.cron,
            view.every_seconds,
        ) {
            (None, None, Some(cron), _) => Some(crate::alarms::Repeat::Cron(cron.clone())),
            (None, None, None, Some(every)) => Some(crate::alarms::Repeat::Every(every)),
            _ => None,
        };
        let now = wall_ms();
        let updated = self
            .owners
            .update_alarms(&key, |alarms| match (&wanted, alarms.get(&name)) {
                (Some(repeat), Some(alarm)) if alarm.repeat.as_ref() == Some(repeat) => None,
                (Some(repeat), _) => {
                    // An interval runs at once, then on its interval; a cron
                    // at its next named time, so declaring one does not fire
                    // it early.
                    let first = match repeat {
                        crate::alarms::Repeat::Every(_) => Some(now),
                        crate::alarms::Repeat::Cron(_) => repeat.next_after(now),
                    }?;
                    alarms.insert(
                        name.clone(),
                        crate::alarms::Alarm::repeating(repeat.clone(), first, alarm_id()),
                    );
                    Some(())
                }
                (None, Some(_)) => alarms.remove(&name).map(|_| ()),
                (None, None) => None,
            })
            .await?;
        if let crate::ownership::AlarmUpdate::Applied(()) = updated {
            eprintln!("walleye.alarm schedule view={} key={key}", view.name);
        }
        Ok(())
    }

    /// Arm every view whose key this process holds, and drop alarms that
    /// belong to views gone or moved to another key.
    async fn reconcile_views(&self, views: &[ViewDefinition]) {
        for view in views {
            if self.owners.holds(&driver_key(view)).is_some()
                && let Err(error) = self.reconcile_view(view).await
            {
                eprintln!(
                    "walleye.alarm reconcile view={} outcome=error error={error}",
                    view.name
                );
            }
        }
        let by_name: HashMap<&str, &ViewDefinition> = views
            .iter()
            .map(|view| (view.name.as_str(), view))
            .collect();
        let mut keys: Vec<String> = self
            .owners
            .held_alarms()
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        keys.dedup();
        for key in keys {
            let _ = self
                .owners
                .update_alarms(&key, |alarms| {
                    let stale: Vec<String> = alarms
                        .keys()
                        .filter(|name| {
                            let view = name.split_once(':').map(|(_, view)| view);
                            view.and_then(|view| by_name.get(view))
                                .is_none_or(|view| driver_key(view) != key)
                        })
                        .cloned()
                        .collect();
                    if stale.is_empty() {
                        return None;
                    }
                    for name in stale {
                        alarms.remove(&name);
                    }
                    Some(())
                })
                .await;
        }
    }

    async fn clear_view_alarms(&self, view: &ViewDefinition) {
        let key = driver_key(view);
        let names = [
            schedule_alarm(&view.name),
            worker_alarm(&view.name),
            pass_alarm(&view.name),
        ];
        let _ = self
            .owners
            .update_alarms(&key, |alarms| {
                let before = alarms.len();
                for name in &names {
                    alarms.remove(name);
                }
                (alarms.len() != before).then_some(())
            })
            .await;
    }

    /// When a view's worker alarm is set to fire, as this process sees it.
    async fn worker_alarm_at(&self, view: &ViewDefinition) -> Option<u64> {
        let key = driver_key(view);
        let name = worker_alarm(&view.name);
        self.owners
            .held_alarms()
            .into_iter()
            .find(|(k, n, _)| *k == key && *n == name)
            .map(|(_, _, alarm)| alarm.at_ms)
    }

    /// Apply what a worker asked of its alarm, once the turn that asked has
    /// landed everything it wrote.
    async fn apply_worker_alarm(
        &self,
        view: &ViewDefinition,
        change: Option<walleye_v8::AlarmChange>,
    ) -> Result<(), Error> {
        let Some(change) = change else {
            return Ok(());
        };
        let key = driver_key(view);
        let name = worker_alarm(&view.name);
        for _ in 0..2 {
            let updated = self
                .owners
                .update_alarms(&key, |alarms| match change {
                    walleye_v8::AlarmChange::Set(at) => {
                        alarms.insert(name.clone(), crate::alarms::Alarm::once(at, alarm_id()));
                        Some(())
                    }
                    walleye_v8::AlarmChange::Delete => alarms.remove(&name).map(|_| ()),
                })
                .await?;
            match updated {
                crate::ownership::AlarmUpdate::NotOwner => {
                    // A turn on a key nobody owned yet takes it.
                    if !matches!(self.route(&key, false).await?, Route::Local { .. }) {
                        break;
                    }
                }
                _ => return Ok(()),
            }
        }
        Err(Box::new(NotOwner {
            table: key,
            owner: None,
        }))
    }

    /// After a view's pass failed, try it again on an alarm with backoff.
    async fn arm_pass_retry(&self, view: &ViewDefinition) -> Result<(), Error> {
        let name = pass_alarm(&view.name);
        let at = wall_ms() + crate::alarms::BACKOFF_BASE_MS;
        self.owners
            .update_alarms(&driver_key(view), |alarms| {
                if alarms.contains_key(&name) {
                    return None;
                }
                alarms.insert(name.clone(), crate::alarms::Alarm::once(at, alarm_id()));
                Some(())
            })
            .await?;
        Ok(())
    }

    /// Fire one alarm if it is due, as the owner of its key. The firing is
    /// begun by a write to the key's record, which only the owner can make,
    /// and is completed by another. `None` when it was not due.
    pub(crate) async fn fire(&self, key: &str, name: &str) -> Result<Option<Progress>, Error> {
        let _firing = self.firings.read().await;
        if self.owners.draining() {
            return Ok(None);
        }
        let lock = self
            .firing_locks
            .lock()
            .await
            .entry((key.to_owned(), name.to_owned()))
            .or_default()
            .clone();
        let _one = lock.lock().await;
        // A worker's alarm is set by its view's pass, and written before the
        // pass moves its cursor, so an owner that died between the two left a
        // pass to replay that sets the alarm again. Replay it before firing:
        // its setAlarm then replaces this alarm, as setting one again does,
        // and `begin` below finds it not yet due. Fired first, the old alarm
        // ran and the replay set a second one, and the worker woke twice.
        if let Some(view_name) = name.strip_prefix("worker:")
            && let Ok(view) = self.view(view_name).await
            && view.source.is_some()
            && driver_key(&view) == key
            && let Err(error) = self.drain_rows(&view, 1000).await
        {
            // The view's own retry takes it from here; the alarm still fires.
            eprintln!("walleye.alarm fire key={key} alarm={name} catch_up=error error={error}");
        }
        let now = wall_ms();
        let begun = self
            .owners
            .update_alarms(key, |alarms| {
                crate::alarms::begin(alarms.get_mut(name)?, now)
            })
            .await?;
        let firing = match begun {
            crate::ownership::AlarmUpdate::Applied(firing) => firing,
            crate::ownership::AlarmUpdate::Skipped => return Ok(None),
            crate::ownership::AlarmUpdate::NotOwner => {
                return Err(Box::new(NotOwner {
                    table: key.to_owned(),
                    owner: None,
                }));
            }
        };
        eprintln!(
            "walleye.alarm fire key={key} alarm={name} scheduled_ms={} attempt={} missed={}",
            firing.scheduled_ms, firing.attempt, firing.missed
        );
        let outcome = self.run_alarm(key, name, &firing).await;
        if let Err(error) = &outcome {
            eprintln!(
                "walleye.alarm fire key={key} alarm={name} attempt={} outcome=error error={error}",
                firing.attempt
            );
        }
        let ok = outcome.is_ok();
        let done = wall_ms();
        let settled = self
            .owners
            .update_alarms(key, |alarms| {
                let alarm = alarms.get_mut(name)?;
                let before = alarm.clone();
                match crate::alarms::complete(alarm, &firing, ok, done) {
                    crate::alarms::Settled::Remove => {
                        alarms.remove(name);
                        Some(())
                    }
                    crate::alarms::Settled::Keep => (*alarm != before).then_some(()),
                }
            })
            .await?;
        if settled == crate::ownership::AlarmUpdate::NotOwner {
            eprintln!("walleye.alarm fire key={key} alarm={name} outcome=lost_before_completion");
        }
        outcome.map(Some)
    }

    /// What an alarm is for, by its name.
    async fn run_alarm(
        &self,
        key: &str,
        name: &str,
        firing: &crate::alarms::Firing,
    ) -> Result<Progress, Error> {
        let idle = Progress {
            rows: 0,
            written: 0,
            delivered: 0,
            through: 0,
            caught_up: true,
        };
        let Some((kind, view_name)) = name.split_once(':') else {
            return Err(format!("no handler for alarm {name}").into());
        };
        let view = match self.view(view_name).await {
            Ok(view) => view,
            // Its view is gone; the sweep removes what is left of it.
            Err(error) if error.downcast_ref::<TableNotFound>().is_some() => return Ok(idle),
            Err(error) => return Err(error),
        };
        if driver_key(&view) != key {
            return Ok(idle);
        }
        match kind {
            "schedule" => self.run_scheduled(&view, firing).await,
            "pass" => self.drain_rows(&view, 1000).await,
            "worker" => self.run_worker_alarm(&view, firing).await,
            _ => Err(format!("no handler for alarm {name}").into()),
        }
    }

    /// Wake a view's worker with its `alarm` handler.
    async fn run_worker_alarm(
        &self,
        view: &ViewDefinition,
        firing: &crate::alarms::Firing,
    ) -> Result<Progress, Error> {
        let Some(worker) = view.worker.as_deref().filter(|w| !w.trim().is_empty()) else {
            return Err(format!("{} has no worker to wake", view.name).into());
        };
        let limits = view.limits();
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
        let info = serde_json::json!({
            "scheduledTime": firing.scheduled_ms,
            "attempt": firing.attempt,
            "retryCount": firing.attempt.saturating_sub(1),
        })
        .to_string();
        let host = self.reach.clone();
        let source = worker.to_owned();
        let (produced, elsewhere, alarm) = tokio::task::spawn_blocking(move || {
            let outcome =
                walleye_v8::run_alarm(&source, &info, limits, host, walleye_v8::Turn::default())?;
            worker_output(outcome, expected)
        })
        .await
        .map_err(|error| -> Error { error.to_string().into() })??;
        let mut written = self.land(view, elsewhere).await?;
        let made: usize = produced.iter().map(RecordBatch::num_rows).sum();
        if let Some(target) = &view.target
            && made > 0
        {
            self.ensure_target(target, &produced).await?;
            self.append(target, produced).await?;
            written += made;
        }
        self.apply_worker_alarm(view, alarm).await?;
        Ok(Progress {
            rows: 0,
            written,
            delivered: 0,
            through: firing.scheduled_ms.saturating_mul(1000),
            caught_up: true,
        })
    }

    /// Every pending alarm on the keys this process owns, as the inspection
    /// route shows them.
    pub fn pending_alarms(&self) -> serde_json::Value {
        let alarms: Vec<serde_json::Value> = self
            .owners
            .held_alarms()
            .into_iter()
            .map(|(key, name, alarm)| {
                serde_json::json!({
                    "key": key,
                    "alarm": name,
                    "at_ms": alarm.at_ms,
                    "scheduled_ms": alarm.scheduled_ms,
                    "attempt": alarm.attempt,
                    "repeat": alarm.repeat,
                })
            })
            .collect();
        serde_json::json!({ "node": self.owners.node(), "alarms": alarms })
    }

    /// A view's pending alarms, read from its key's record wherever it is
    /// owned: its next scheduled run, its worker's alarm, a pass waiting to
    /// be retried.
    pub async fn view_alarms(&self, view: &ViewDefinition) -> Result<serde_json::Value, Error> {
        let alarms = self.owners.read_alarms(&driver_key(view)).await?;
        let mut shown = serde_json::Map::new();
        for (name, alarm) in alarms {
            let Some((kind, owner)) = name.split_once(':') else {
                continue;
            };
            if owner != view.name {
                continue;
            }
            let label = match kind {
                "schedule" => "next_run",
                "worker" => "worker_alarm",
                "pass" => "retry",
                _ => continue,
            };
            shown.insert(
                label.into(),
                serde_json::json!({
                    "at_ms": alarm.at_ms,
                    "scheduled_ms": alarm.scheduled_ms,
                    "attempt": alarm.attempt,
                }),
            );
        }
        Ok(serde_json::Value::Object(shown))
    }

    /// Stop firing and wait out any firing in flight, so the keys can be
    /// handed over between firings rather than during one.
    async fn quiesce_alarms(&self) {
        drop(self.firings.write().await);
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

/// What one worker turn produced: rows for the view's own target, and rows it
/// wrote to streams it named itself.
/// What one worker turn produced: rows for the view's target, rows for
/// streams it named itself, and what it asked of its alarm.
type WorkerOutput = (
    Vec<RecordBatch>,
    Vec<(String, Vec<RecordBatch>)>,
    Option<walleye_v8::AlarmChange>,
);

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
    host: Arc<dyn walleye_v8::Host>,
    turn: walleye_v8::Turn,
) -> Result<WorkerOutput, Error> {
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    let rows = String::from_utf8(writer.into_inner())?;

    let outcome = walleye_v8::run_with_host(worker, &rows, limits, host, turn)?;
    worker_output(outcome, expected)
}

/// Rows a worker returned and wrote, grouped for landing, and its alarm.
fn worker_output(
    outcome: walleye_v8::Outcome,
    expected: Option<Arc<Schema>>,
) -> Result<WorkerOutput, Error> {
    let alarm = outcome.alarm;

    // Rows the worker sent elsewhere, grouped by stream and kept in the order
    // it wrote them.
    let mut elsewhere: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
    for (stream, written) in outcome.writes {
        if !valid_name(&stream) {
            return Err(format!("a worker wrote to an invalid stream name: {stream}").into());
        }
        let value: serde_json::Value = serde_json::from_str(&written)?;
        let rows = match value {
            serde_json::Value::Array(rows) => rows,
            row => vec![row],
        };
        match elsewhere.iter_mut().find(|(name, _)| name == &stream) {
            Some((_, held)) => held.extend(rows),
            None => elsewhere.push((stream, rows)),
        }
    }
    let mut written = Vec::with_capacity(elsewhere.len());
    for (stream, rows) in elsewhere {
        let batches = rows_to_batches(rows, None)?;
        written.push((stream, batches));
    }

    if outcome.returned.is_empty() {
        return Ok((Vec::new(), written, alarm));
    }
    let values: Vec<serde_json::Value> = serde_json::from_str(&outcome.returned)?;
    if values.is_empty() {
        return Ok((Vec::new(), written, alarm));
    }
    Ok((rows_to_batches(values, expected)?, written, alarm))
}

/// Turn JSON rows into Arrow batches, against a known schema when there is
/// one and by inference otherwise.
fn rows_to_batches(
    values: Vec<serde_json::Value>,
    expected: Option<Arc<Schema>>,
) -> Result<Vec<RecordBatch>, Error> {
    if values.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(index) = values.iter().position(|row| !row.is_object()) {
        return Err(
            format!("a worker produced a row that is not an object at position {index}").into(),
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
        return Err("a worker produced a column using a reserved name".into());
    }
    let mut decoder = arrow_json::ReaderBuilder::new(schema).build_decoder()?;
    decoder.serialize(&values)?;
    Ok(decoder.flush()?.into_iter().collect())
}
