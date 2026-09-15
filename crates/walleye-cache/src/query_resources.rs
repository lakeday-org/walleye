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
    this: Weak<Self>,
    _owner: crate::lance_backend::QueryOwner,
}

#[derive(Debug, Default)]
struct State {
    spill_files: usize,
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
            this: this.clone(),
            _owner: owner,
        }))
    }

    /// Build the one shared execution environment used by all runtime query sessions.
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
    fn return_memory(&self) {
        if self.pool.reserved() == 0
            && let Err(error) = self.backend.resize_memory(self.cache_memory_ceiling)
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
        if state.spill_files == 0
            && let Err(error) = self
                .resources
                .backend
                .resize_disk_blocking(self.resources.cache_disk_ceiling)
        {
            tracing::warn!(%error, "could not restore cache disk capacity");
        }
    }
}

/// Report failed reclamation as resource exhaustion so no spill allocation is granted.
fn resource_error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("cache reclamation failed: {error}"))
}
