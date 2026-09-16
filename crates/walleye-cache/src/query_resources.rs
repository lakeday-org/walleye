//! Give query operators first claim on the runtime's RAM and disk allocation.
//! Disk leases start before spill creation and end after file deletion. Foyer's
//! minimum working footprint remains reserved; cache contents are disposable.

use crate::LanceFoyerCacheBackend;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::{
    disk_manager::{DiskManager, DiskManagerMode, SpillFileGuard, SpillFileObserver},
    memory_pool::{FairSpillPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
    runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};

/// Shared query and cache accounting for one runtime, including concurrent queries.
#[derive(Debug)]
pub struct QueryResources {
    backend: Arc<LanceFoyerCacheBackend>,
    pool: FairSpillPool,
    memory_bytes: usize,
    disk_bytes: usize,
    cache_memory_ceiling: usize,
    cache_disk_ceiling: usize,
    spill_dir: PathBuf,
    state: Mutex<State>,
    /// Bytes held by long-lived leases (tables, bodies, index builds), as
    /// opposed to query reservations that come and go batch by batch.
    leased: std::sync::atomic::AtomicUsize,
    this: Weak<Self>,
    _owner: crate::lance_backend::QueryOwner,
}

#[derive(Debug, Default)]
struct State {
    spill_files: usize,
    /// Disk consumed beside the cache file by named users (the Bitr log,
    /// for one), reported by their owners. The cache's disk ceiling is the
    /// budget minus these.
    disk_usage: std::collections::BTreeMap<String, usize>,
}

/// Memory held by a named non-query user (an open table's memtables, a
/// decoded request body, an index build). Dropping it returns the memory to
/// the cache. Obtained from [`QueryResources::reserve_memory`].
#[derive(Debug)]
pub struct MemoryLease {
    resources: Arc<QueryResources>,
    bytes: usize,
    /// Held for its drop: releasing it shrinks the pool.
    _reservation: MemoryReservation,
}
impl MemoryLease {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for MemoryLease {
    fn drop(&mut self) {
        // Leave the leased counter first; the reservation's own drop then
        // shrinks the pool and lets the cache grow back by this amount.
        self.resources
            .leased
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
    }
}

impl QueryResources {
    /// Bind one instance allocation to the cache, leaving host overhead outside these budgets.
    pub fn new(
        backend: Arc<LanceFoyerCacheBackend>,
        spill_dir: PathBuf,
        memory_bytes: usize,
        disk_bytes: usize,
    ) -> Result<Arc<Self>> {
        if memory_bytes == 0
            || memory_bytes < backend.memory_capacity()
            || disk_bytes < backend.persistent_capacity()
            || disk_bytes <= backend.disk_floor()
        {
            return Err(DataFusionError::Configuration(
                "query allocation must contain the cache ceilings and its disk working floor"
                    .into(),
            ));
        }
        if spill_dir != backend.directory().join("spill") {
            return Err(DataFusionError::Configuration(
                "spill must use the owned cache directory's spill subdirectory".into(),
            ));
        }
        let owner = backend.acquire_query_owner().ok_or_else(|| {
            DataFusionError::Configuration("this cache already has a query resource manager".into())
        })?;
        // This directory is exclusively runtime-owned and protected by the cache
        // directory owner. Files left by a crashed process are never durable state.
        if spill_dir.exists() {
            std::fs::remove_dir_all(&spill_dir)?;
        }
        std::fs::create_dir_all(&spill_dir)?;
        Ok(Arc::new_cyclic(|this| Self {
            cache_memory_ceiling: backend.memory_capacity(),
            cache_disk_ceiling: backend.persistent_capacity(),
            backend,
            pool: FairSpillPool::new(memory_bytes),
            memory_bytes,
            disk_bytes,
            spill_dir,
            state: Mutex::new(State::default()),
            leased: std::sync::atomic::AtomicUsize::new(0),
            this: this.clone(),
            _owner: owner,
        }))
    }

    /// Build the one shared execution environment used by all runtime query sessions.
    /// Reserve memory for a non-query user. Every reservation, query or not,
    /// comes out of the one budget: the cache's memory ceiling shrinks by the
    /// same amount while the lease lives and grows back when it drops. Fails
    /// closed with `ResourcesExhausted` when the budget cannot cover it.
    pub fn reserve_memory(self: &Arc<Self>, name: &str, bytes: usize) -> Result<MemoryLease> {
        let pool: Arc<dyn MemoryPool> = self.clone();
        let mut reservation = MemoryConsumer::new(name).register(&pool);
        self.leased
            .fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
        if reservation.try_grow(bytes).is_err() {
            self.leased
                .fetch_sub(bytes, std::sync::atomic::Ordering::AcqRel);
            return Err(DataFusionError::ResourcesExhausted(format!(
                "{name} needs {} MiB but only {} MiB of the {} MiB memory budget is free",
                bytes / (1024 * 1024),
                self.memory_available() / (1024 * 1024),
                self.memory_bytes / (1024 * 1024)
            )));
        }
        Ok(MemoryLease {
            resources: self.clone(),
            bytes,
            _reservation: reservation,
        })
    }
    /// Memory budget for everything that is not the fixed runtime floor.
    pub fn memory_budget(&self) -> usize {
        self.memory_bytes
    }
    /// Memory not currently held by queries or leases.
    pub fn memory_available(&self) -> usize {
        self.memory_bytes.saturating_sub(self.pool.reserved())
    }
    pub fn memory_reserved(&self) -> usize {
        self.pool.reserved()
    }
    /// The whole disk budget; the cache's working floor is carved out of it
    /// only while a query spills.
    pub fn disk_budget(&self) -> usize {
        self.disk_bytes
    }
    /// Report how much disk a named user occupies beside the cache file. The
    /// cache's disk ceiling becomes the budget minus every reported usage,
    /// applied at once; it grows back as usages fall.
    pub async fn set_disk_usage(self: &Arc<Self>, name: &str, bytes: usize) -> Result<()> {
        let this = self.clone();
        let name = name.to_string();
        tokio::task::spawn_blocking(move || this.set_disk_usage_blocking(&name, bytes))
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
    }
    pub fn set_disk_usage_blocking(&self, name: &str, bytes: usize) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bytes == 0 {
            state.disk_usage.remove(name);
        } else {
            state.disk_usage.insert(name.to_string(), bytes);
        }
        if state.spill_files > 0 {
            return Ok(());
        }
        let target = self.cache_disk_target(&state);
        self.backend
            .resize_disk_blocking(target)
            .map_err(resource_error)?;
        Ok(())
    }
    /// Disk used beside the cache file, as reported by its owners.
    pub fn disk_used_elsewhere(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .disk_usage
            .values()
            .sum()
    }
    fn cache_disk_target(&self, state: &State) -> usize {
        let elsewhere: usize = state.disk_usage.values().sum();
        self.cache_disk_ceiling
            .min(self.disk_bytes.saturating_sub(elsewhere))
            .max(self.backend.disk_floor())
    }

    pub fn runtime(self: &Arc<Self>) -> Result<Arc<RuntimeEnv>> {
        let manager = DiskManager::builder()
            .with_mode(DiskManagerMode::Directories(vec![self.spill_dir.clone()]))
            .with_max_temp_directory_size((self.disk_bytes - self.backend.disk_floor()) as u64)
            .with_spill_file_observer(self.clone());
        RuntimeEnvBuilder::new()
            .with_memory_pool(self.clone())
            .with_disk_manager_builder(manager)
            .with_metadata_cache_limit(0)
            .with_file_statistics_cache_limit(0)
            .build_arc()
    }

    /// Trim before granting query memory; externally held values remain charged after eviction.
    fn make_memory_available(&self) -> Result<()> {
        let available = self.memory_bytes.saturating_sub(self.pool.reserved());
        let target = available.min(self.cache_memory_ceiling);
        if self.backend.memory_capacity() > target {
            // Round down so small operator reservations do not repeatedly spawn eviction workers.
            let quantum = (8 * 1024 * 1024).min(self.cache_memory_ceiling);
            self.backend
                .resize_memory(target / quantum * quantum)
                .map_err(resource_error)?;
        }
        if self.backend.retained_value_bytes_estimate() > available
            && self.backend.retained_value_bytes() > available
        {
            self.backend.resize_memory(0).map_err(resource_error)?;
            if self.backend.retained_value_bytes() > available {
                return Err(DataFusionError::ResourcesExhausted(
                    "query memory is still held by live Lance values after cache eviction".into(),
                ));
            }
        }
        Ok(())
    }

    /// Restore admission after the last query releases its buffers, avoiding per-batch resize churn.
    /// Give the cache back what the budget no longer needs: the ceiling minus
    /// live leases, once no query holds memory. Queries release batch by
    /// batch, and regrowing between batches would make the cache oscillate.
    fn return_memory(&self) {
        let leased = self.leased.load(std::sync::atomic::Ordering::Acquire);
        if self.pool.reserved() > leased {
            return;
        }
        let quantum = (8 * 1024 * 1024).min(self.cache_memory_ceiling).max(1);
        let target = self
            .memory_bytes
            .saturating_sub(leased)
            .min(self.cache_memory_ceiling)
            / quantum
            * quantum;
        if self.backend.memory_capacity() != target
            && let Err(error) = self.backend.resize_memory(target)
        {
            tracing::warn!(%error, "could not restore cache memory capacity");
        }
    }
}

impl std::fmt::Display for QueryResources {
    /// Identify the shared allocation in DataFusion resource errors.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lakeday query/cache pool ({} bytes)", self.memory_bytes)
    }
}

impl MemoryPool for QueryResources {
    /// Name this pool for DataFusion diagnostics.
    fn name(&self) -> &str {
        "LakedayQueryResources"
    }

    /// Preserve DataFusion's fairness between concurrent spilling operators.
    fn register(&self, consumer: &MemoryConsumer) {
        self.pool.register(consumer);
    }
    /// Release DataFusion's consumer bookkeeping.
    fn unregister(&self, consumer: &MemoryConsumer) {
        self.pool.unregister(consumer);
    }
    /// Account DataFusion's infallible reservations and yield all available cache RAM.
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let _state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pool.grow(reservation, additional);
        if let Err(error) = self.make_memory_available() {
            // DataFusion uses this infallible API for already-owned buffers. It cannot
            // reject those bytes; normal growing operators must use try_grow to spill.
            tracing::warn!(%error, "infallible query reservation exceeded reclaimable memory");
        }
    }
    /// Return memory to the shared pool and expand cache admission when useful.
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let _state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pool.shrink(reservation, shrink);
        self.return_memory();
    }
    /// Grant memory only after reclaiming cache allocations; failure asks the operator to spill.
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let _state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pool.try_grow(reservation, additional)?;
        if let Err(error) = self.make_memory_available() {
            self.pool.shrink(reservation, additional);
            return Err(error);
        }
        Ok(())
    }
    /// Report total query reservations across all sessions.
    fn reserved(&self) -> usize {
        self.pool.reserved()
    }
    /// Expose the shared operator ceiling to query planning.
    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.memory_bytes)
    }
}

impl SpillFileObserver for QueryResources {
    /// Release the cache's disk tail before the first spill file can be created.
    fn before_create(&self) -> Result<Arc<dyn SpillFileGuard>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.spill_files == 0 {
            let actual = self
                .backend
                .resize_disk_blocking(0)
                .map_err(resource_error)?;
            if actual > self.backend.disk_floor() {
                return Err(DataFusionError::ResourcesExhausted(
                    "cache did not release the required spill allocation".into(),
                ));
            }
        }
        state.spill_files += 1;
        let resources = self
            .this
            .upgrade()
            .ok_or_else(|| DataFusionError::Internal("query resources lost their owner".into()))?;
        Ok(Arc::new(SpillLease { resources }))
    }
}

#[derive(Debug)]
struct SpillLease {
    resources: Arc<QueryResources>,
}
impl SpillFileGuard for SpillLease {}
impl Drop for SpillLease {
    /// Expand cache only after the last live spill file has physically disappeared.
    fn drop(&mut self) {
        let mut state = self
            .resources
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.spill_files -= 1;
        if state.spill_files == 0 {
            let target = self.resources.cache_disk_target(&state);
            if let Err(error) = self.resources.backend.resize_disk_blocking(target) {
                tracing::warn!(%error, "could not restore cache disk capacity");
            }
        }
    }
}

/// Report failed reclamation as resource exhaustion so no spill allocation is granted.
fn resource_error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("cache reclamation failed: {error}"))
}
