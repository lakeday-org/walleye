//! Optional Lance cache backend backed by one Foyer hybrid cache.
//!
//! Lance hands a backend opaque keys and a type-erased value codec. This module
//! keeps those keys opaque, stores codec envelopes in Foyer's persistent tier,
//! and keeps values without a codec in the Foyer memory tier only.
//!
//! One backend is intended to be shared by every Lance session in a process.
//! Lance derives dataset and binding prefixes into each `InternalCacheKey`,
//! while this adapter adds only the cache-instance namespace needed to keep a
//! physical Foyer directory safe across cache format/runtime identities.

use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use foyer::{
    BlockEngineConfig, Code, DeviceBuilder, Error as FoyerError, FileDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridCachePolicy, HybridCacheProperties, Location, RecoverMode,
};
use lance_core::Result;
use lance_core::cache::{CacheBackend, CacheCodec, CacheEntry, InternalCacheKey};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::directory::DirectoryOwner;

const FOYER_PAGE_BYTES: usize = 4 * 1024;
const MIN_BLOCK_BYTES: usize = 64 * 1024;
const MAX_BLOCK_BYTES: usize = 16 * 1024 * 1024;
const MIN_BLOCKS: usize = 4;
const DEFAULT_MEMORY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_CAPACITY_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Configuration for a persistent Lance cache backend.
#[derive(Debug, Clone)]
pub struct LanceCacheConfig {
    /// Directory owned by this Foyer cache instance.
    pub dir: PathBuf,
    /// Hard memory-tier budget in bytes.
    pub memory_bytes: usize,
    /// Hard persistent-tier budget in bytes.
    pub capacity_bytes: usize,
    /// Foyer's physical eviction block size in bytes.
    pub block_bytes: usize,
}

impl LanceCacheConfig {
    /// Creates a configuration with a block size derived from the disk budget.
    pub fn new(dir: impl AsRef<Path>, memory_bytes: usize, capacity_bytes: usize) -> Self {
        Self {
            dir: dir.as_ref().to_owned(),
            memory_bytes,
            capacity_bytes,
            block_bytes: default_block_bytes(capacity_bytes),
        }
    }

    /// Creates a configuration using the normal host defaults.
    pub fn defaults(dir: impl AsRef<Path>) -> Self {
        Self::new(dir, DEFAULT_MEMORY_BYTES, DEFAULT_CAPACITY_BYTES)
    }

    /// Sets the physical Foyer block size used for persistent entries.
    pub fn with_block_bytes(mut self, block_bytes: usize) -> Self {
        self.block_bytes = block_bytes;
        self
    }

    /// Validates hard budgets before Foyer opens the device.
    fn validate(&self) -> std::result::Result<(), LanceCacheError> {
        if self.memory_bytes == 0 {
            return Err(LanceCacheError::MemoryBudgetTooSmall(self.memory_bytes));
        }
        if self.block_bytes < FOYER_PAGE_BYTES || self.block_bytes > MAX_BLOCK_BYTES {
            return Err(LanceCacheError::BlockSizeInvalid(self.block_bytes));
        }
        let block_bytes = self.block_bytes.div_ceil(FOYER_PAGE_BYTES) * FOYER_PAGE_BYTES;
        let minimum_capacity = block_bytes.saturating_mul(MIN_BLOCKS);
        if self.capacity_bytes < minimum_capacity {
            return Err(LanceCacheError::DiskBudgetTooSmall(self.capacity_bytes));
        }
        Ok(())
    }
}

impl Default for LanceCacheConfig {
    /// Uses the persistent host defaults rooted at the current directory.
    fn default() -> Self {
        Self::defaults(".")
    }
}

/// A persistent cache construction or Foyer operation failure.
#[derive(Debug, thiserror::Error)]
pub enum LanceCacheError {
    /// Another live engine owns the configured directory, or its lock failed.
    #[error("Lance cache directory ownership failed: {0}")]
    Directory(#[from] crate::directory::DirectoryError),
    /// The memory tier cannot hold one useful cache entry.
    #[error("Lance cache memory budget is too small: {0} bytes")]
    MemoryBudgetTooSmall(usize),
    /// The disk tier cannot hold Foyer's minimum number of physical blocks.
    #[error("Lance cache persistent budget is too small: {0} bytes")]
    DiskBudgetTooSmall(usize),
    /// The configured physical block size is outside the supported range.
    #[error("Lance cache block size is invalid: {0} bytes")]
    BlockSizeInvalid(usize),
    /// Foyer failed to build, flush, or close the cache.
    #[error("Lance Foyer cache failed: {0}")]
    Foyer(String),
}

/// The Foyer key includes a stable cache-instance namespace and Lance's opaque
/// key.
///
/// Lance's `InternalCacheKey` is intentionally only 16 bytes. Keeping the
/// cache-instance namespace beside it prevents entries from an incompatible
/// cache format/runtime from aliasing when one physical Foyer directory is
/// reused. Lance's own dataset/binding prefixes remain inside the opaque key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FoyerKey {
    cache_namespace: [u8; 32],
    key: InternalCacheKey,
}

impl Code for FoyerKey {
    /// Encodes the fixed-size namespace and Lance key without a logical key
    /// string, preserving Lance's opaque-key contract on disk.
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer
            .write_all(&self.cache_namespace)
            .and_then(|()| writer.write_all(self.key.as_bytes()))
            .map_err(FoyerError::io_error)
    }

    /// Decodes the exact inverse of [`Self::encode`].
    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut cache_namespace = [0; 32];
        reader
            .read_exact(&mut cache_namespace)
            .map_err(FoyerError::io_error)?;
        let mut key = [0; 16];
        reader.read_exact(&mut key).map_err(FoyerError::io_error)?;
        Ok(Self {
            cache_namespace,
            key: InternalCacheKey::from_bytes(key),
        })
    }

    /// Returns the fixed serialized key size used by Foyer's accounting.
    fn estimated_size(&self) -> usize {
        self.cache_namespace.len() + self.key.as_bytes().len()
    }
}

/// A typed memory value or a recovered codec envelope. Only one representation
/// is retained after promotion, so encoded payloads do not duplicate RAM values.
#[derive(Clone)]
struct FoyerEntry {
    /// Type-erased value available on a memory hit. Recovered disk entries are
    /// materialized after their codec is supplied by the Lance caller.
    entry: Option<CacheEntry>,
    /// Only recovered disk entries carry an encoded envelope.
    encoded: Option<bytes::Bytes>,
    /// Codec retained with typed values so serialization does not duplicate RAM payloads.
    codec: Option<CacheCodec>,
    /// Declared Lance value size used for memory eviction accounting.
    size_bytes: usize,
}

impl fmt::Debug for FoyerEntry {
    /// Omits type-erased values while exposing enough state for diagnostics.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FoyerEntry")
            .field("has_entry", &self.entry.is_some())
            .field(
                "encoded_bytes",
                &self.encoded.as_ref().map(bytes::Bytes::len),
            )
            .field("size_bytes", &self.size_bytes)
            .finish()
    }
}

impl Code for FoyerEntry {
    /// Serializes the typed value directly into the storage buffer, retaining
    /// its decoded weight for promotion after restart. No second payload is kept in RAM.
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer
            .write_all(&(self.size_bytes as u64).to_le_bytes())
            .map_err(FoyerError::io_error)?;
        if let (Some(entry), Some(codec)) = (&self.entry, self.codec) {
            return codec
                .serialize(entry, writer)
                .map_err(|error| FoyerError::io_error(std::io::Error::other(error.to_string())));
        }
        match &self.encoded {
            Some(encoded) => writer.write_all(encoded).map_err(FoyerError::io_error),
            None => Err(FoyerError::new(
                foyer::ErrorKind::Unsupported,
                "Lance cache entry has no codec",
            )),
        }
    }

    /// Recovers the codec envelope. The type-erased value is reconstructed by
    /// the Lance codec on the subsequent backend `get` call.
    fn decode(reader: &mut impl Read) -> foyer::Result<Self> {
        let mut size = [0_u8; 8];
        reader.read_exact(&mut size).map_err(FoyerError::io_error)?;
        let size_bytes = usize::try_from(u64::from_le_bytes(size))
            .map_err(|error| FoyerError::io_error(std::io::Error::other(error.to_string())))?;
        let mut encoded = Vec::new();
        reader
            .read_to_end(&mut encoded)
            .map_err(FoyerError::io_error)?;
        Ok(Self {
            entry: None,
            encoded: Some(bytes::Bytes::from(encoded)),
            codec: None,
            size_bytes,
        })
    }

    /// Charges the representation actually retained in memory plus its header.
    fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(if self.entry.is_some() {
                self.size_bytes
            } else {
                self.encoded.as_ref().map_or(0, bytes::Bytes::len)
            })
            .max(1)
    }
}

/// Ephemeral per-key coordination for Lance's borrowed miss loader.
///
/// This map contains no cache values and is removed when the guard drops; it
/// only lets unrelated keys load in parallel while preserving Lance's
/// at-most-once loader contract for one key.
pub(crate) struct LoadGate {
    /// Opaque key whose loader currently owns the gate.
    key: FoyerKey,
    /// The per-key lock retained while another waiter may still hold it.
    lock: Arc<Mutex<()>>,
    /// Shared registry from which this temporary gate is removed.
    gates: Arc<std::sync::Mutex<HashMap<FoyerKey, Arc<Mutex<()>>>>>,
    /// Held permit that serializes this key's miss loaders.
    _permit: OwnedMutexGuard<()>,
}

impl Drop for LoadGate {
    /// Removes the temporary coordination entry after the loader completes or
    /// is cancelled, without affecting any cache value.
    fn drop(&mut self) {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let is_current = gates
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.lock));
        if is_current {
            gates.remove(&self.key);
        }
    }
}

/// A serialized physical resize request; completion means the file has released its tail blocks.
struct DiskRequest {
    bytes: usize,
    reply: std::sync::mpsc::Sender<std::result::Result<usize, LanceCacheError>>,
}

type LiveValues = HashMap<usize, (Weak<dyn std::any::Any + Send + Sync>, usize)>;

/// Exclusive ownership of the spill directory and shared query accounting.
#[derive(Debug)]
pub(crate) struct QueryOwner(Arc<AtomicBool>);
impl Drop for QueryOwner {
    /// Allow a new resource manager only after the previous one and all spill leases are gone.
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Lance's `CacheBackend` implemented by one bounded Foyer memory/NVMe cache.
pub struct LanceFoyerCacheBackend {
    /// Shared ownership is released by explicit close, even while views remain.
    owner: Arc<std::sync::Mutex<Option<DirectoryOwner>>>,
    cache: HybridCache<FoyerKey, FoyerEntry>,
    cache_namespace: [u8; 32],
    dir: Arc<PathBuf>,
    query_owner: Arc<AtomicBool>,
    memory_bytes: Arc<AtomicUsize>,
    capacity_bytes: Arc<AtomicUsize>,
    memory_ceiling: usize,
    disk_ceiling: usize,
    disk_floor: usize,
    disk_requests: std::sync::mpsc::Sender<DiskRequest>,
    retained_estimate: Arc<AtomicUsize>,
    live_values: Arc<std::sync::Mutex<LiveValues>>,
    /// Temporary per-key gates deduplicate borrowed miss loaders without
    /// storing cache values outside Foyer. Cache hits never take a gate.
    load_gates: Arc<std::sync::Mutex<HashMap<FoyerKey, Arc<Mutex<()>>>>>,
}

impl fmt::Debug for LanceFoyerCacheBackend {
    /// Reports budgets and the opaque namespace while omitting cache contents.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LanceFoyerCacheBackend")
            .field("memory_bytes", &self.memory_bytes)
            .field("capacity_bytes", &self.capacity_bytes)
            .field("cache_namespace", &self.cache_namespace)
            .finish_non_exhaustive()
    }
}

impl LanceFoyerCacheBackend {
    /// Opens a persistent Foyer backend for one cache-instance identity.
    ///
    /// The resulting backend is intended to be shared by every Lance dataset
    /// and session in the process. Dataset and binding identities are already
    /// included by Lance in each `InternalCacheKey`.
    pub async fn new(
        dir: impl AsRef<Path>,
        memory_bytes: usize,
        capacity_bytes: usize,
        cache_instance_identity: impl AsRef<str>,
    ) -> std::result::Result<Self, LanceCacheError> {
        Self::open(
            LanceCacheConfig::new(dir, memory_bytes, capacity_bytes),
            cache_instance_identity,
        )
        .await
    }

    /// Opens a backend from a complete host cache configuration.
    pub async fn open(
        config: LanceCacheConfig,
        cache_instance_identity: impl AsRef<str>,
    ) -> std::result::Result<Self, LanceCacheError> {
        config.validate()?;
        let owner = DirectoryOwner::acquire(&config.dir)?;
        // The old multi-file cache is disposable. Its partitions must not survive
        // alongside the new backing file and consume the same allocation twice.
        for entry in std::fs::read_dir(&config.dir)
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))?
        {
            let entry = entry.map_err(|error| LanceCacheError::Foyer(error.to_string()))?;
            let name = entry.file_name();
            if name
                .to_str()
                .and_then(|name| name.strip_prefix("foyer-storage-direct-fs-"))
                .is_some_and(|suffix| {
                    suffix.len() == 8 && suffix.bytes().all(|b| b.is_ascii_digit())
                })
            {
                std::fs::remove_file(entry.path())
                    .map_err(|error| LanceCacheError::Foyer(error.to_string()))?;
            }
        }
        let capacity = Arc::new(AtomicUsize::new(config.capacity_bytes));
        let (disk_requests, requests) = std::sync::mpsc::channel::<DiskRequest>();
        let (ready, receiver) = tokio::sync::oneshot::channel();
        let background_config = config.clone();
        let background_capacity = Arc::clone(&capacity);
        std::thread::Builder::new()
            .name("lance-cache-control".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready.send(Err(LanceCacheError::Foyer(error.to_string())));
                        return;
                    }
                };
                let cache = match runtime.block_on(Self::build_cache(&background_config)) {
                    Ok(cache) => cache,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(cache.clone())).is_err() {
                    return;
                }
                for request in requests {
                    let result = runtime
                        .block_on(cache.resize_disk(request.bytes as u64))
                        .map(|bytes| bytes as usize)
                        .map_err(|error| LanceCacheError::Foyer(error.to_string()));
                    if let Ok(bytes) = result.as_ref() {
                        background_capacity.store(*bytes, Ordering::Release);
                    }
                    let _ = request.reply.send(result);
                }
            })
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))?;
        let cache = receiver
            .await
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))??;
        Ok(Self {
            owner: Arc::new(std::sync::Mutex::new(Some(owner))),
            cache,
            cache_namespace: cache_namespace(cache_instance_identity.as_ref()),
            dir: Arc::new(config.dir.clone()),
            query_owner: Arc::new(AtomicBool::new(false)),
            memory_bytes: Arc::new(AtomicUsize::new(config.memory_bytes)),
            capacity_bytes: capacity,
            memory_ceiling: config.memory_bytes,
            disk_ceiling: config.capacity_bytes,
            disk_floor: config.block_bytes * 2,
            disk_requests,
            retained_estimate: Arc::new(AtomicUsize::new(0)),
            live_values: Arc::new(std::sync::Mutex::new(HashMap::new())),
            load_gates: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
    }

    /// Build all disk tasks on a dedicated runtime so synchronous spill requests cannot starve them.
    async fn build_cache(
        config: &LanceCacheConfig,
    ) -> std::result::Result<HybridCache<FoyerKey, FoyerEntry>, LanceCacheError> {
        let device = FileDeviceBuilder::new(config.dir.join("cache.bin"))
            .with_capacity(config.capacity_bytes)
            .build()
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))?;
        HybridCacheBuilder::<FoyerKey, FoyerEntry>::new()
            .with_name("lakeday-lance-cache")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(config.memory_bytes)
            .with_weighter(|key: &FoyerKey, value: &FoyerEntry| {
                key.estimated_size()
                    .saturating_add(value.estimated_size())
                    .max(1)
            })
            .storage()
            // Explicit thresholds preserve the queue-buffer fix independently
            // of the fork's default-resolution behavior.
            .with_engine_config(
                BlockEngineConfig::new(device)
                    .with_block_size(config.block_bytes)
                    .with_clean_block_threshold(1)
                    .with_buffer_pool_size(8 * 1024 * 1024)
                    .with_submit_queue_size_threshold(16 * 1024 * 1024),
            )
            .with_recover_mode(RecoverMode::Quiet)
            .build()
            .await
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))
    }

    /// Change the shared memory target and evict excess entries before returning.
    pub fn resize_memory(&self, bytes: usize) -> std::result::Result<(), LanceCacheError> {
        let bytes = bytes.min(self.memory_ceiling);
        if self.memory_capacity() != bytes {
            self.cache
                .memory()
                .resize(bytes)
                .map_err(|error| LanceCacheError::Foyer(error.to_string()))?;
            self.memory_bytes.store(bytes, Ordering::Release);
        }
        Ok(())
    }

    /// Runtime-owned cache directory; spill shares its filesystem and owner lock.
    pub fn directory(&self) -> &Path {
        self.dir.as_path()
    }

    /// Prevent duplicate resource managers from resetting an active spill directory.
    pub(crate) fn acquire_query_owner(&self) -> Option<QueryOwner> {
        self.query_owner
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| QueryOwner(Arc::clone(&self.query_owner)))
    }

    /// Minimum working footprint retained by the block engine during spilling.
    pub fn disk_floor(&self) -> usize {
        self.disk_floor
    }

    /// Reclaim physical disk blocks on the cache's independent executor before granting spill space.
    pub fn resize_disk_blocking(
        &self,
        bytes: usize,
    ) -> std::result::Result<usize, LanceCacheError> {
        let (reply, receiver) = std::sync::mpsc::channel();
        self.disk_requests
            .send(DiskRequest {
                bytes: bytes.min(self.disk_ceiling),
                reply,
            })
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))?;
        receiver
            .recv()
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))?
    }

    /// Resize without blocking the caller's async executor.
    pub async fn resize_disk(&self, bytes: usize) -> std::result::Result<usize, LanceCacheError> {
        let backend = self.scoped("disk-control");
        tokio::task::spawn_blocking(move || backend.resize_disk_blocking(bytes))
            .await
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))?
    }

    /// Count decoded allocations still alive, including values retained by queries after eviction.
    pub fn retained_value_bytes(&self) -> usize {
        let mut values = self
            .live_values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        values.retain(|_, (value, _)| value.strong_count() != 0);
        let bytes = values
            .values()
            .fold(0usize, |sum, (_, bytes)| sum.saturating_add(*bytes));
        self.retained_estimate.store(bytes, Ordering::Release);
        bytes
    }

    /// Conservative constant-time bound; dead weak references are swept only under pressure.
    pub fn retained_value_bytes_estimate(&self) -> usize {
        self.retained_estimate.load(Ordering::Acquire)
    }

    /// Observe a decoded allocation without retaining it or changing Lance's concrete value type.
    fn track_value(&self, entry: &CacheEntry, bytes: usize) {
        let mut values = self
            .live_values
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if values.len().is_multiple_of(1024) {
            values.retain(|_, (value, _)| value.strong_count() != 0);
            self.retained_estimate.store(
                values.values().map(|(_, bytes)| *bytes).sum(),
                Ordering::Release,
            );
        }
        let previous = values.insert(
            Arc::as_ptr(entry) as *const () as usize,
            (Arc::downgrade(entry), bytes),
        );
        let prior_bytes = previous.map_or(0, |(_, bytes)| bytes);
        let estimate = self
            .retained_estimate
            .load(Ordering::Relaxed)
            .saturating_sub(prior_bytes)
            .saturating_add(bytes);
        self.retained_estimate.store(estimate, Ordering::Release);
    }

    /// Opens a backend from a complete host cache configuration.
    pub async fn from_config(
        config: LanceCacheConfig,
        cache_instance_identity: impl AsRef<str>,
    ) -> std::result::Result<Self, LanceCacheError> {
        Self::open(config, cache_instance_identity).await
    }

    /// Returns the configured memory budget.
    pub fn memory_capacity(&self) -> usize {
        self.memory_bytes.load(Ordering::Acquire)
    }

    /// Returns the configured persistent budget.
    pub fn persistent_capacity(&self) -> usize {
        self.capacity_bytes.load(Ordering::Acquire)
    }

    /// Waits for queued codec-backed writes to reach Foyer's persistent tier.
    pub async fn flush(&self) {
        self.cache.storage().wait().await;
    }

    /// Closes Foyer and waits for its background work to finish.
    pub async fn close(&self) -> std::result::Result<(), LanceCacheError> {
        self.cache
            .close()
            .await
            .map_err(|error| LanceCacheError::Foyer(error.to_string()))?;
        self.owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Ok(())
    }

    /// Creates a binding namespace over the same physical engine and budget.
    /// Closing any view closes that shared engine; the runtime retains its root owner.
    pub fn scoped(&self, identity: &str) -> Arc<Self> {
        let mut hash = Sha256::new();
        hash.update(self.cache_namespace);
        hash.update((identity.len() as u64).to_le_bytes());
        hash.update(identity.as_bytes());
        Arc::new(Self {
            owner: Arc::clone(&self.owner),
            cache: self.cache.clone(),
            cache_namespace: hash.finalize().into(),
            dir: Arc::clone(&self.dir),
            query_owner: Arc::clone(&self.query_owner),
            memory_bytes: Arc::clone(&self.memory_bytes),
            capacity_bytes: Arc::clone(&self.capacity_bytes),
            memory_ceiling: self.memory_ceiling,
            disk_ceiling: self.disk_ceiling,
            disk_floor: self.disk_floor,
            disk_requests: self.disk_requests.clone(),
            retained_estimate: Arc::clone(&self.retained_estimate),
            live_values: Arc::clone(&self.live_values),
            load_gates: Arc::clone(&self.load_gates),
        })
    }

    /// Returns the live Foyer memory-tier usage.
    pub fn memory_usage(&self) -> usize {
        self.cache.memory().usage()
    }

    /// Translates an opaque Lance key to this cache instance's Foyer key.
    fn foyer_key(&self, key: &InternalCacheKey) -> FoyerKey {
        FoyerKey {
            cache_namespace: self.cache_namespace,
            key: *key,
        }
    }

    /// Acquires the temporary gate for one key while preserving parallelism
    /// between unrelated Lance cache misses.
    async fn acquire_load_gate(&self, key: FoyerKey) -> LoadGate {
        let lock = {
            let mut gates = self
                .load_gates
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(gates.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let permit = Arc::clone(&lock).lock_owned().await;
        LoadGate {
            key,
            lock,
            gates: Arc::clone(&self.load_gates),
            _permit: permit,
        }
    }

    /// Materializes one Foyer value using the codec supplied by Lance.
    fn materialize(value: &FoyerEntry, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        if let Some(entry) = value.entry.as_ref() {
            return Some(Arc::clone(entry));
        }
        let encoded = value.encoded.as_ref()?;
        codec?.deserialize(encoded).hit()
    }

    /// Converts a Lance value to its Foyer representation and chooses whether
    /// it may leave the memory tier.
    fn foyer_entry(entry: CacheEntry, size_bytes: usize, codec: Option<CacheCodec>) -> FoyerEntry {
        FoyerEntry {
            entry: Some(entry),
            encoded: None,
            codec,
            size_bytes: size_bytes.max(1),
        }
    }
}

#[async_trait::async_trait]
impl CacheBackend for LanceFoyerCacheBackend {
    /// Reads one entry from Foyer and decodes a recovered persistent envelope.
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        let foyer_key = self.foyer_key(key);
        let value = match self.cache.get(&foyer_key).await {
            Ok(Some(entry)) => entry.value().clone(),
            Ok(None) => return None,
            Err(error) => {
                tracing::debug!(error = %error, "Lance Foyer cache read failed");
                return None;
            }
        };
        let Some(entry) = Self::materialize(&value, codec) else {
            // A malformed, incompatible, or codec-less recovered value must
            // behave as a Lance cache miss, not poison future lookups.
            self.cache.remove(&foyer_key);
            return None;
        };
        self.track_value(&entry, value.size_bytes);
        if value.entry.is_none() {
            // Promote a disk hit to a typed memory hit without enqueueing a
            // duplicate disk write. The value remains codec-backed on disk.
            let promoted = FoyerEntry {
                entry: Some(Arc::clone(&entry)),
                encoded: None,
                codec,
                size_bytes: value.size_bytes,
            };
            self.cache.insert_with_properties(
                foyer_key,
                promoted,
                HybridCacheProperties::default().with_location(Location::InMem),
            );
        }
        Some(entry)
    }

    /// Inserts an entry into Foyer, keeping entries without a codec RAM-only.
    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        codec: Option<CacheCodec>,
    ) {
        let foyer_key = self.foyer_key(key);
        self.track_value(&entry, size_bytes);
        let value = Self::foyer_entry(entry, size_bytes, codec);
        let properties = if value.codec.is_some() {
            HybridCacheProperties::default()
        } else {
            // A key may have been populated with a codec before this insert.
            // Remove that persistent record before replacing it with the
            // explicitly RAM-only value; otherwise a later restart could
            // resurrect stale bytes for a key whose current value cannot be
            // serialized.
            self.cache.remove(&foyer_key);
            HybridCacheProperties::default().with_location(Location::InMem)
        };
        self.cache
            .insert_with_properties(foyer_key, value, properties);
    }

    /// Deduplicates concurrent miss loaders, then inserts the resulting value.
    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: std::pin::Pin<
            Box<dyn futures::Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>,
        >,
        codec: Option<CacheCodec>,
    ) -> Result<(CacheEntry, bool)> {
        if let Some(entry) = self.get(key, codec).await {
            return Ok((entry, true));
        }
        let _gate = self.acquire_load_gate(self.foyer_key(key)).await;
        if let Some(entry) = self.get(key, codec).await {
            return Ok((entry, true));
        }
        let (entry, size_bytes) = loader.await?;
        self.insert(key, Arc::clone(&entry), size_bytes, codec)
            .await;
        Ok((entry, false))
    }

    /// Clears both Foyer tiers.
    async fn clear(&self) {
        if let Err(error) = self.cache.clear().await {
            tracing::debug!(error = %error, "Lance Foyer cache clear failed");
        }
    }

    /// Returns entries currently present in Foyer's memory index.
    async fn num_entries(&self) -> usize {
        self.cache.memory().entries()
    }

    /// Returns the weighted memory-tier usage.
    async fn size_bytes(&self) -> usize {
        self.cache.memory().usage()
    }

    /// Returns the current memory-tier entry count without awaiting a flush.
    fn approx_num_entries(&self) -> usize {
        self.cache.memory().entries()
    }

    /// Returns the current weighted memory-tier usage without awaiting a flush.
    fn approx_size_bytes(&self) -> usize {
        self.cache.memory().usage()
    }
}

/// Hashes the caller's stable cache-instance identity into a fixed namespace.
fn cache_namespace(identity: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"lakeday-lance-cache");
    hasher.update((identity.len() as u64).to_le_bytes());
    hasher.update(identity.as_bytes());
    hasher.finalize().into()
}

/// Chooses a block size that leaves at least four physical eviction blocks.
fn default_block_bytes(capacity_bytes: usize) -> usize {
    (capacity_bytes / 16).clamp(MIN_BLOCK_BYTES, MAX_BLOCK_BYTES)
}

impl LanceFoyerCacheBackend {
    /// Account values held by a query even when their cache owner is a remote node.
    pub(crate) fn track_external(&self, entry: &CacheEntry, size: usize) {
        self.track_value(entry, size);
    }
    pub(crate) async fn load_gate(&self, key: &InternalCacheKey) -> LoadGate {
        self.acquire_load_gate(self.foyer_key(key)).await
    }
    /// Serve resident bytes only. A peer miss never executes an origin loader.
    pub async fn export_entry(&self, key: &InternalCacheKey) -> Option<bytes::Bytes> {
        let handle = self.cache.get(&self.foyer_key(key)).await.ok()??;
        let entry = handle.value();
        if entry.size_bytes > crate::distributed::MAX_PEER_BYTES {
            return None;
        }
        let mut bytes = Vec::new();
        entry.encode(&mut bytes).ok()?;
        (bytes.len() <= crate::distributed::MAX_PEER_BYTES).then(|| bytes.into())
    }
    /// Admit a bounded envelope; the consuming Lance codec validates its type and contents.
    pub async fn import_entry(&self, key: &InternalCacheKey, data: bytes::Bytes) -> bool {
        let Some(size) = crate::distributed::envelope_size(&data) else {
            return false;
        };
        if size > self.memory_capacity() {
            return false;
        }
        let value = FoyerEntry {
            entry: None,
            encoded: Some(data.slice(8..)),
            codec: None,
            size_bytes: size,
        };
        self.cache.insert_with_properties(
            self.foyer_key(key),
            value,
            HybridCacheProperties::default(),
        );
        true
    }
}

impl LanceFoyerCacheBackend {
    /// Carry the complete storage-binding namespace into a peer's opaque key.
    pub fn peer_key(&self, key: &InternalCacheKey) -> InternalCacheKey {
        let mut hash = Sha256::new();
        hash.update(self.cache_namespace);
        hash.update(key.as_bytes());
        let digest = hash.finalize();
        let mut key = [0; 16];
        key.copy_from_slice(&digest[..16]);
        InternalCacheKey::from_bytes(key)
    }
}
