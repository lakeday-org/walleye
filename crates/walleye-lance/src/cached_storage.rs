//! Assemble deployment-local Lance sessions, Foyer, and shared query resources without a runtime dependency.
use crate::LanceStorageOptions;
use lance::session::{CacheSpec, Session};
use lance_core::cache::CacheBackend;
use lance_io::object_store::{ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc, time::Duration};
use walleye_cache::{
    DistributedCache, LanceCachedObjectStore, LanceFoyerCacheBackend, PeerConfig, PeerStats,
    QueryResources,
};

/// An explicit node allocation. Host and ingestion memory must be budgeted outside it.
pub struct CachedStorage {
    pub storage: LanceStorageOptions,
    pub backend: Arc<LanceFoyerCacheBackend>,
    pub resources: Arc<QueryResources>,
    pub peers: Option<Arc<PeerStats>>,
}
impl CachedStorage {
    /// Bind every dataset, metadata, index, and ordinary data read to the same node allocation.
    pub async fn open(
        dir: impl AsRef<Path>,
        identity: &str,
        memory: usize,
        disk: usize,
        params: ObjectStoreParams,
        peer: Option<PeerConfig>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut binding = Sha256::new();
        binding.update((identity.len() as u64).to_le_bytes());
        binding.update(identity.as_bytes());
        if let Some(accessor) = &params.storage_options_accessor {
            let options = accessor.get_storage_options().await?;
            let ordered: std::collections::BTreeMap<_, _> = options.0.into_iter().collect();
            for (key, value) in ordered {
                for part in [key, value] {
                    binding.update((part.len() as u64).to_le_bytes());
                    binding.update(part.as_bytes());
                }
            }
        }
        let identity = format!("{:x}", binding.finalize());
        let backend = Arc::new(LanceFoyerCacheBackend::new(&dir, memory, disk, &identity).await?);
        Self::from_backend(backend, &identity, params, peer).await
    }
    /// Share the node's sole cache allocation with SQL, including peer-resident entries.
    pub async fn from_backend(
        backend: Arc<LanceFoyerCacheBackend>,
        identity: &str,
        mut params: ObjectStoreParams,
        peer: Option<PeerConfig>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = backend.directory();
        let memory = backend.memory_capacity();
        let disk = backend.persistent_capacity();
        let identity = identity.to_string();
        let resources = QueryResources::new(backend.clone(), dir.join("spill"), memory, disk)?;
        let (cache, peers): (Arc<dyn CacheBackend>, _) = match peer {
            Some(config) => {
                let distributed = Arc::new(DistributedCache::new(backend.clone(), config)?);
                let stats = distributed.stats();
                (distributed, Some(stats))
            }
            None => (backend.clone(), None),
        };
        params.object_store_wrapper = Some(Arc::new(CacheWrapper {
            backend: cache.clone(),
            identity,
            permits: Arc::new(tokio::sync::Semaphore::new(16)),
        }));
        let session = Arc::new(Session::with_cache_backends(
            CacheSpec::Backend(cache.clone()),
            CacheSpec::Backend(cache),
            Arc::new(ObjectStoreRegistry::default()),
        ));
        let storage = LanceStorageOptions::new(Some(params), session).with_query_runtime(
            resources.runtime()?,
            2,
            Duration::from_secs(60),
        );
        Ok(Self {
            storage,
            backend,
            resources,
            peers,
        })
    }
}
#[derive(Debug)]
struct CacheWrapper {
    backend: Arc<dyn CacheBackend>,
    identity: String,
    permits: Arc<tokio::sync::Semaphore>,
}
impl WrappingObjectStore for CacheWrapper {
    fn wrap(
        &self,
        prefix: &str,
        origin: Arc<dyn object_store::ObjectStore>,
    ) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(
            LanceCachedObjectStore::new(
                Arc::new(walleye_cache::LanceReadLimiter::new(
                    origin,
                    self.permits.clone(),
                )),
                self.backend.clone(),
                &format!("{}:{prefix}", self.identity),
                64 * 1024,
            )
            .expect("fixed valid block size")
            .with_immutable_lance_files(),
        )
    }
}
