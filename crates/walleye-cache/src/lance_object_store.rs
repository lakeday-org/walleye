//! Versioned ordinary object reads through the same backend as Lance metadata
//! and indexes. Query scopes pin object versions; every fill is conditional on
//! that exact version, while writes and listings remain origin operations.

use std::{borrow::Cow, collections::HashMap, fmt, sync::Arc};

use bytes::Bytes;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use lance_core::cache::{
    CacheBackend, CacheCodec, CacheCodecImpl, CacheEntryReader, CacheEntryWriter, CacheKey,
    CacheKeySchema, Context, DeepSizeOf, KeyBuilder, LanceCache,
};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    path::Path,
};
use tokio::sync::OnceCell;

use crate::LanceCacheError;

#[derive(Clone, Debug)]
struct Version {
    meta: ObjectMeta,
    attributes: Attributes,
}

type Versions = std::sync::Mutex<HashMap<(Path, Option<String>), Arc<OnceCell<Version>>>>;

#[derive(Clone, Debug)]
struct BlockKey {
    identity: Arc<str>,
    path: Path,
    e_tag: Option<String>,
    version: Option<String>,
    size: u64,
    geometry: u64,
    start: u64,
}

#[derive(Debug)]
struct Block(Bytes);

impl DeepSizeOf for Block {
    /// Charge the byte payload held by this value.
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        self.0.len()
    }
}

impl CacheCodecImpl for Block {
    const TYPE_ID: &'static str = "lakeday.lance.object-block";
    const CURRENT_VERSION: u32 = 1;

    /// Write one bounded raw byte block into Lance's codec envelope.
    fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> lance_core::Result<()> {
        writer.write_raw(&self.0)
    }

    /// Recover the immutable byte block without copying its payload.
    fn deserialize(reader: &mut CacheEntryReader<'_>) -> lance_core::Result<Self> {
        Ok(Self(reader.read_raw()?))
    }
}

impl CacheKey for BlockKey {
    type ValueType = Block;

    /// Supply the diagnostic logical key required by Lance's cache interface.
    fn key(&self) -> Cow<'_, str> {
        format!("{}:{}", self.path, self.start).into()
    }

    /// Separate raw blocks from every decoded Lance value type.
    fn type_name() -> &'static str {
        "lakeday.lance.object-block"
    }

    /// Declare the exact field layout streamed into the opaque key.
    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lakeday.lance.object-block", 1)
    }

    /// Include binding, version, length, geometry and range without textual aliases.
    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_str(&self.identity);
        builder.write_str(self.path.as_ref());
        for value in [&self.e_tag, &self.version] {
            match value {
                Some(value) => {
                    builder.write_some();
                    builder.write_str(value);
                }
                None => builder.write_none(),
            }
        }
        builder.write_u64(self.size);
        builder.write_u64(self.geometry);
        builder.write_u64(self.start);
    }

    /// Allow ordinary byte blocks to survive process restart.
    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<Block>())
    }
}

/// An object-store view backed by the process-wide Lance/Foyer cache.
#[derive(Debug)]
pub struct LanceCachedObjectStore {
    origin: Arc<dyn ObjectStore>,
    cache: LanceCache,
    identity: Arc<str>,
    geometry: u64,
    immutable_lance_files: bool,
    /// Versions pinned by one query, like its catalog snapshot and query plan.
    /// This state is never shared between query invocations or persisted.
    versions: Option<Versions>,
}

impl LanceCachedObjectStore {
    /// Attach byte reads to an existing cache authority; never open another device.
    pub fn new(
        origin: Arc<dyn ObjectStore>,
        backend: Arc<dyn CacheBackend>,
        identity: &str,
        data_block_bytes: usize,
    ) -> std::result::Result<Self, LanceCacheError> {
        if !(4096..=8 * 1024 * 1024).contains(&data_block_bytes)
            || !data_block_bytes.is_power_of_two()
        {
            return Err(LanceCacheError::BlockSizeInvalid(data_block_bytes));
        }
        Ok(Self {
            origin,
            cache: LanceCache::with_backend(backend),
            identity: Arc::from(identity),
            geometry: data_block_bytes as u64,
            versions: None,
            immutable_lance_files: false,
        })
    }

    /// Trust the host-owned Lance data/index naming contract across queries and restarts.
    /// Callers must use this only for managed Lance storage whose files are never overwritten.
    /// Mutable manifests, pointers, WAL objects and unrecognized paths always revalidate.
    pub fn with_immutable_lance_files(mut self) -> Self {
        self.immutable_lance_files = true;
        self.versions = None;
        self
    }

    /// Pin each object's validated version for one read-only Query invocation.
    /// The host must create a new view for the next query. Long-lived writers
    /// use the normal view, which validates current metadata on every read.
    pub fn for_query_snapshot(mut self) -> Self {
        self.versions = Some(std::sync::Mutex::new(HashMap::new()));
        self
    }

    /// Resolve one version before touching cached bytes, coalescing query HEADs.
    async fn version(&self, path: &Path, options: &GetOptions) -> Result<Version> {
        let load = || async {
            let result = self
                .origin
                .get_opts(
                    path,
                    GetOptions {
                        head: true,
                        version: options.version.clone(),
                        extensions: options.extensions.clone(),
                        ..Default::default()
                    },
                )
                .await?;
            Ok::<_, object_store::Error>(Version {
                meta: result.meta,
                attributes: result.attributes,
            })
        };
        let version = if self.immutable_lance_files && immutable_lance_path(path) {
            let key = VersionKey {
                identity: Arc::clone(&self.identity),
                path: path.clone(),
                version: options.version.clone(),
            };
            (*self
                .cache
                .get_or_insert_with_key(key, || async {
                    load()
                        .await
                        .map_err(|error| lance_core::Error::io(error.to_string()))
                })
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "LanceCachedObjectStore",
                    source: Box::new(error),
                })?)
            .clone()
        } else if let Some(versions) = &self.versions {
            let cell = {
                let mut versions = versions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                Arc::clone(
                    versions
                        .entry((path.clone(), options.version.clone()))
                        .or_default(),
                )
            };
            cell.get_or_try_init(load).await?.clone()
        } else {
            load().await?
        };
        options.check_preconditions(&version.meta)?;
        Ok(version)
    }

    /// Fill one exact block conditionally, so a HEAD/GET race cannot poison it.
    async fn block(
        cache: LanceCache,
        origin: Arc<dyn ObjectStore>,
        key: BlockKey,
    ) -> Result<Bytes> {
        let end = key.size.min(key.start.saturating_add(key.geometry));
        let expected = key.clone();
        let value = cache
            .get_or_insert_with_key(key, move || async move {
                let response = origin
                    .get_opts(
                        &expected.path,
                        GetOptions {
                            range: Some((expected.start..end).into()),
                            if_match: expected.e_tag.clone(),
                            version: expected.version.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| lance_core::Error::io(error.to_string()))?;
                if response.meta.size != expected.size
                    || response.meta.e_tag != expected.e_tag
                    || response.range != (expected.start..end)
                {
                    return Err(lance_core::Error::io(
                        "origin changed while filling a Lance cache block",
                    ));
                }
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| lance_core::Error::io(error.to_string()))?;
                if bytes.len() as u64 != end - expected.start {
                    return Err(lance_core::Error::io(
                        "origin returned an incomplete Lance cache block",
                    ));
                }
                Ok(Block(bytes))
            })
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "LanceCachedObjectStore",
                source: Box::new(error),
            })?;
        Ok(value.0.clone())
    }
}

impl fmt::Display for LanceCachedObjectStore {
    /// Avoid exposing the storage binding or object paths in display output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Lance cached object store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LanceCachedObjectStore {
    /// Mutations retain the origin's conditional and durability semantics.
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.origin.put_opts(path, payload, options).await
    }

    /// Multipart completion stays entirely with the authoritative origin.
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.origin.put_multipart_opts(path, options).await
    }

    /// Stream bounded cache blocks for the validated requested object version.
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        let version = self.version(path, &options).await?;
        let pinned_version = options
            .version
            .clone()
            .or_else(|| version.meta.version.clone());
        if !options.head && version.meta.e_tag.is_none() && pinned_version.is_none() {
            // An origin without a stable validator cannot safely populate this cache.
            return self.origin.get_opts(path, options).await;
        }
        let range = match &options.range {
            Some(range) if !options.head => {
                range
                    .as_range(version.meta.size)
                    .map_err(|error| object_store::Error::Generic {
                        store: "LanceCachedObjectStore",
                        source: Box::new(error),
                    })?
            }
            _ => 0..version.meta.size,
        };
        let payload = if options.head || range.is_empty() {
            stream::empty().boxed()
        } else {
            let geometry = self.geometry;
            let first = range.start / geometry;
            let last = range.end.div_ceil(geometry);
            let requested = range.clone();
            let template = BlockKey {
                identity: Arc::clone(&self.identity),
                path: path.clone(),
                e_tag: version.meta.e_tag.clone(),
                version: pinned_version,
                size: version.meta.size,
                geometry,
                start: 0,
            };
            let cache = self.cache.clone();
            let origin = Arc::clone(&self.origin);
            stream::iter(first..last)
                .map(move |number| {
                    let mut key = template.clone();
                    key.start = number * geometry;
                    let start = key.start;
                    let requested = requested.clone();
                    let cache = cache.clone();
                    let origin = Arc::clone(&origin);
                    async move {
                        let bytes = Self::block(cache, origin, key).await?;
                        let begin = requested.start.saturating_sub(start) as usize;
                        let end = bytes.len().min((requested.end - start) as usize);
                        Ok(bytes.slice(begin..end))
                    }
                })
                .buffered(4)
                .boxed()
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta: version.meta,
            range,
            attributes: version.attributes,
        })
    }

    /// Deletes do not mutate immutable version keys; the next normal read revalidates.
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.origin.delete_stream(paths)
    }

    /// Listings always reflect authoritative origin state.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.origin.list(prefix)
    }

    /// Preserve the origin's directory listing semantics.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.origin.list_with_delimiter(prefix).await
    }

    /// Copies preserve the origin's conditional write semantics.
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.origin.copy_opts(from, to, options).await
    }
}

/// Only UUID-scoped index directories and Lance's UUID-derived data filenames are immutable.
fn immutable_lance_path(path: &Path) -> bool {
    let parts: Vec<_> = path.as_ref().split('/').collect();
    parts.windows(2).enumerate().any(|(index, pair)| {
        if pair[0] == "data" && index + 2 == parts.len() {
            pair[1].strip_suffix(".lance").is_some_and(|name| {
                uuid_name(name)
                    || (name.len() == 50
                        && name.bytes().take(24).all(|b| b == b'0' || b == b'1')
                        && name.bytes().skip(24).all(|b| b.is_ascii_hexdigit()))
            })
        } else {
            pair[0] == "_indices" && index + 2 < parts.len() && uuid_name(pair[1])
        }
    })
}

/// Accept the UUID spelling used by older data files and index directories.
fn uuid_name(name: &str) -> bool {
    (name.len() == 32 && name.bytes().all(|b| b.is_ascii_hexdigit()))
        || (name.len() == 36
            && name.bytes().enumerate().all(|(i, b)| {
                if matches!(i, 8 | 13 | 18 | 23) {
                    b == b'-'
                } else {
                    b.is_ascii_hexdigit()
                }
            }))
}

#[derive(Clone, Debug)]
struct VersionKey {
    identity: Arc<str>,
    path: Path,
    version: Option<String>,
}
impl CacheKey for VersionKey {
    type ValueType = Version;
    /// Identify immutable metadata separately from cached data blocks.
    fn key(&self) -> Cow<'_, str> {
        self.path.as_ref().into()
    }
    /// Keep metadata records separate from other Lance cache types.
    fn type_name() -> &'static str {
        "lakeday.lance.immutable-metadata"
    }
    /// Define the persistent metadata key shape.
    fn schema() -> CacheKeySchema {
        CacheKeySchema::new(Self::type_name(), 1)
    }
    /// Scope immutable file identities to their origin binding and explicit version request.
    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_str(&self.identity);
        builder.write_str(self.path.as_ref());
        match &self.version {
            Some(version) => {
                builder.write_some();
                builder.write_str(version);
            }
            None => builder.write_none(),
        }
    }
    /// Preserve validated immutable metadata in the same Foyer disk allocation as data.
    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<Version>())
    }
}

impl DeepSizeOf for Version {
    /// Charge metadata strings as well as the outer value.
    fn deep_size_of_children(&self, _: &mut Context) -> usize {
        self.meta.location.as_ref().len()
            + self.meta.e_tag.as_ref().map_or(0, String::len)
            + self.meta.version.as_ref().map_or(0, String::len)
            + self
                .attributes
                .iter()
                .map(|(key, value)| format!("{key:?}").len() + value.as_ref().len())
                .sum::<usize>()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct VersionWire {
    path: String,
    modified: String,
    size: u64,
    e_tag: Option<String>,
    version: Option<String>,
    attributes: Vec<(String, String)>,
}
impl CacheCodecImpl for Version {
    const TYPE_ID: &'static str = "lakeday.lance.immutable-metadata";
    const CURRENT_VERSION: u32 = 1;
    /// Store complete origin metadata so cached HEAD responses preserve attributes and conditions.
    fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> lance_core::Result<()> {
        use object_store::Attribute;
        let attributes = self
            .attributes
            .iter()
            .map(|(key, value)| {
                let key = match key {
                    Attribute::ContentDisposition => "disposition".into(),
                    Attribute::ContentEncoding => "encoding".into(),
                    Attribute::ContentLanguage => "language".into(),
                    Attribute::ContentType => "type".into(),
                    Attribute::CacheControl => "cache".into(),
                    Attribute::StorageClass => "storage".into(),
                    Attribute::Metadata(key) => format!("metadata:{key}"),
                    _ => {
                        return Err(lance_core::Error::io(
                            "unsupported immutable object attribute",
                        ));
                    }
                };
                Ok((key, value.as_ref().to_owned()))
            })
            .collect::<lance_core::Result<Vec<_>>>()?;
        let wire = VersionWire {
            path: self.meta.location.to_string(),
            modified: self.meta.last_modified.to_rfc3339(),
            size: self.meta.size,
            e_tag: self.meta.e_tag.clone(),
            version: self.meta.version.clone(),
            attributes,
        };
        writer.write_raw(
            &serde_json::to_vec(&wire).map_err(|error| lance_core::Error::io(error.to_string()))?,
        )
    }
    /// Decode the original headers; corrupt entries become ordinary cache misses.
    fn deserialize(reader: &mut CacheEntryReader<'_>) -> lance_core::Result<Self> {
        use object_store::Attribute;
        let wire: VersionWire = serde_json::from_slice(&reader.read_raw()?)
            .map_err(|error| lance_core::Error::io(error.to_string()))?;
        let mut attributes = Attributes::new();
        for (key, value) in wire.attributes {
            let attribute = match key.as_str() {
                "disposition" => Attribute::ContentDisposition,
                "encoding" => Attribute::ContentEncoding,
                "language" => Attribute::ContentLanguage,
                "type" => Attribute::ContentType,
                "cache" => Attribute::CacheControl,
                "storage" => Attribute::StorageClass,
                _ => Attribute::Metadata(
                    key.strip_prefix("metadata:")
                        .ok_or_else(|| lance_core::Error::io("invalid immutable object attribute"))?
                        .to_owned()
                        .into(),
                ),
            };
            attributes.insert(attribute, value.into());
        }
        Ok(Self {
            meta: ObjectMeta {
                location: Path::parse(wire.path)
                    .map_err(|error| lance_core::Error::io(error.to_string()))?,
                last_modified: chrono::DateTime::parse_from_rfc3339(&wire.modified)
                    .map_err(|error| lance_core::Error::io(error.to_string()))?
                    .with_timezone(&chrono::Utc),
                size: wire.size,
                e_tag: wire.e_tag,
                version: wire.version,
            },
            attributes,
        })
    }
}
