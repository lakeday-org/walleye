//! One deployment's stream registry. Definitions use object-store create-if-absent;
//! one designated ingress owns a separately locked memshard for each stream.
use arrow_array::RecordBatch;
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
    sync::Arc,
    time::Instant,
};
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard, RwLock};
use walleye_bitr::{HttpReplica, QuorumWriter};
use walleye_lance::{
    BitrWalBackend, CachedStorage, LanceDurability, LanceStorageOptions, SnapshotSource, Table,
    TableConfig, TableSnapshot,
};

type Error = Box<dyn std::error::Error + Send + Sync>;

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
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
}
impl StreamDefinition {
    fn table_config(&self, root: &str) -> Result<TableConfig, Error> {
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
    durability: LanceDurability,
    table: Mutex<Option<Table>>,
}
impl Stream {
    async fn table(&self) -> Result<MappedMutexGuard<'_, Table>, Error> {
        let mut table = self.table.lock().await;
        if table.is_none() {
            *table = Some(
                Table::open(
                    self.config.clone(),
                    self.storage.clone(),
                    self.durability.clone(),
                )
                .await?,
            );
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
    streams: Mutex<BTreeMap<String, Arc<Stream>>>,
    // Requests share this read lock; shutdown waits for all active requests.
    closed: RwLock<bool>,
    writer: Option<Arc<QuorumWriter>>,
}
impl Engine {
    pub async fn open(
        config: ApiConfig,
        cache: CachedStorage,
        params: ObjectStoreParams,
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
            catalog_path: prefix.join("streams"),
            streams: Mutex::new(BTreeMap::new()),
            closed: RwLock::new(false),
            writer,
        };
        // Open writers lazily: the combined Bitr service starts after configuration loads.
        Ok(engine)
    }
    fn durability(&self, config: &TableConfig) -> Result<LanceDurability, Error> {
        Ok(match &self.writer {
            Some(w) => LanceDurability::Bitr(Arc::new(BitrWalBackend::new(
                w.clone(),
                &config.stream,
                config.shard_id,
                1,
            )?)),
            None => LanceDurability::ObjectStore,
        })
    }
    async fn register(&self, definition: StreamDefinition) -> Result<Arc<Stream>, Error> {
        let config = definition.table_config(&self.config.root_uri)?;
        let durability = self.durability(&config)?;
        let mut streams = self.streams.lock().await;
        let stream = streams.entry(definition.name.clone()).or_insert_with(|| {
            Arc::new(Stream {
                definition,
                config,
                durability,
                storage: self.cache.storage.clone(),
                table: Mutex::new(None),
            })
        });
        Ok(stream.clone())
    }
    async fn stream(&self, name: &str) -> Result<Arc<Stream>, Error> {
        if let Some(stream) = self.streams.lock().await.get(name).cloned() {
            return Ok(stream);
        }
        // Validate before constructing an object key from a request path.
        if name.is_empty()
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        {
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
                self.stream(name).await?;
            }
        }
        Ok(())
    }
    pub async fn define(&self, definition: StreamDefinition) -> Result<(), Error> {
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
                if existing != definition {
                    return Err("stream already exists with a different definition".into());
                }
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
        Ok(count)
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
}
