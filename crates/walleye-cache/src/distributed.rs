//! Deployment-local peer cache over stable rendezvous ownership. Peers exchange only
//! serialized Lance entries; loaders and authoritative credentials never cross this boundary.
use crate::LanceFoyerCacheBackend;
use lance_core::{
    Result,
    cache::{CacheBackend, CacheCodec, CacheEntry, InternalCacheKey},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use walleye_ring::Membership;

/// Authenticated identity and stable placement shared by one deployment's sessions.
#[derive(Clone)]
pub struct PeerConfig {
    pub token: String,
    pub ring: Arc<Membership>,
    /// Sent with every request to a peer, beside the token.
    pub headers: reqwest::header::HeaderMap,
}
impl std::fmt::Debug for PeerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConfig")
            .field("ring", &self.ring)
            .finish_non_exhaustive()
    }
}
/// Observable evidence of local hits, peer hits, and successful owner population.
#[derive(Debug, Default)]
pub struct PeerStats {
    pub local_hits: AtomicU64,
    pub peer_hits: AtomicU64,
    pub peer_misses: AtomicU64,
    pub peer_errors: AtomicU64,
    pub peer_stores: AtomicU64,
    /// Inserts that were never offered to a peer: no codec to serialize with,
    /// or larger than one peer envelope.
    pub peer_skipped: AtomicU64,
    pub loads: AtomicU64,
}
impl PeerStats {
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"local_hits":self.local_hits.load(Ordering::Relaxed),"peer_hits":self.peer_hits.load(Ordering::Relaxed),"peer_misses":self.peer_misses.load(Ordering::Relaxed),"peer_errors":self.peer_errors.load(Ordering::Relaxed),"peer_stores":self.peer_stores.load(Ordering::Relaxed),"peer_skipped":self.peer_skipped.load(Ordering::Relaxed),"loads":self.loads.load(Ordering::Relaxed)})
    }
}
/// A Lance backend whose serializable entries are offered to their ring owner.
#[derive(Debug)]
pub struct DistributedCache {
    local: Arc<LanceFoyerCacheBackend>,
    config: PeerConfig,
    client: reqwest::Client,
    stats: Arc<PeerStats>,
}
impl DistributedCache {
    pub fn new(
        local: Arc<LanceFoyerCacheBackend>,
        config: PeerConfig,
    ) -> std::result::Result<Self, reqwest::Error> {
        Ok(Self {
            local,
            client: reqwest::Client::builder()
                .default_headers(config.headers.clone())
                .connect_timeout(Duration::from_millis(100))
                .timeout(Duration::from_secs(2))
                .build()?,
            config,
            stats: Arc::default(),
        })
    }
    pub fn stats(&self) -> Arc<PeerStats> {
        self.stats.clone()
    }
    fn url(&self, endpoint: &str, key: &InternalCacheKey) -> String {
        format!("{endpoint}/internal/cache/{}", hex::encode(key.as_bytes()))
    }
    async fn peer_get(
        &self,
        endpoint: &str,
        key: &InternalCacheKey,
        codec: CacheCodec,
    ) -> Option<CacheEntry> {
        let response = match self
            .client
            .get(self.url(endpoint, key))
            .bearer_auth(&self.config.token)
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => {
                self.stats.peer_errors.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.stats.peer_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        if !response.status().is_success()
            || response
                .content_length()
                .is_none_or(|n| n > MAX_PEER_BYTES as u64)
        {
            self.stats.peer_errors.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let data = response.bytes().await.ok()?;
        let size = envelope_size(&data)?;
        let entry = codec.deserialize(&data.slice(8..)).hit()?;
        self.local.track_external(&entry, size);
        self.stats.peer_hits.fetch_add(1, Ordering::Relaxed);
        Some(entry)
    }
}
pub(crate) const MAX_PEER_BYTES: usize = 8 * 1024 * 1024;
pub(crate) fn envelope_size(data: &[u8]) -> Option<usize> {
    if data.len() <= 8 || data.len() > MAX_PEER_BYTES {
        return None;
    }
    let size = usize::try_from(u64::from_le_bytes(data[..8].try_into().ok()?)).ok()?;
    (size > 0 && size <= MAX_PEER_BYTES).then_some(size)
}
#[async_trait::async_trait]
impl CacheBackend for DistributedCache {
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        if let Some(entry) = self.local.get(key, codec).await {
            self.stats.local_hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry);
        }
        let codec = codec?;
        let key = self.local.peer_key(key);
        let ring = self.config.ring.snapshot();
        let owner = ring.owner(key.as_bytes());
        if let Some(value) = self.peer_get(&owner.endpoint, &key, codec).await {
            return Some(value);
        }
        if let Some(donor) = ring.donor(key.as_bytes(), Instant::now()) {
            return self.peer_get(&donor.endpoint, &key, codec).await;
        }
        None
    }
    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size: usize,
        codec: Option<CacheCodec>,
    ) {
        self.local.track_external(&entry, size);
        let wire_key = self.local.peer_key(key);
        let ring = self.config.ring.snapshot();
        let owner = ring.owner(wire_key.as_bytes());
        if size <= MAX_PEER_BYTES
            && let Some(codec) = codec
        {
            let mut bytes = (size as u64).to_le_bytes().to_vec();
            if codec.serialize(&entry, &mut bytes).is_ok() && bytes.len() <= MAX_PEER_BYTES {
                match self
                    .client
                    .put(self.url(&owner.endpoint, &wire_key))
                    .bearer_auth(&self.config.token)
                    .body(bytes)
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        self.stats.peer_stores.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    other => {
                        // A silent peer error is why a broken distributed
                        // cache looks like a slow one; say what happened.
                        if self.stats.peer_errors.fetch_add(1, Ordering::Relaxed) < 3 {
                            match other {
                                Ok(response) => eprintln!(
                                    "walleye.peer op=store owner={} outcome=refused status={}",
                                    owner.id,
                                    response.status()
                                ),
                                Err(error) => eprintln!(
                                    "walleye.peer op=store owner={} outcome=error error={error}",
                                    owner.id
                                ),
                            }
                        }
                    }
                }
            } else {
                self.stats.peer_skipped.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            self.stats.peer_skipped.fetch_add(1, Ordering::Relaxed);
        }
        self.local.insert(key, entry, size, codec).await;
    }
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
        let _gate = self.local.load_gate(key).await;
        if let Some(entry) = self.get(key, codec).await {
            return Ok((entry, true));
        }
        let (entry, size) = loader.await?;
        self.stats.loads.fetch_add(1, Ordering::Relaxed);
        self.insert(key, entry.clone(), size, codec).await;
        Ok((entry, false))
    }
    async fn clear(&self) {
        self.local.clear().await;
    }
    async fn num_entries(&self) -> usize {
        self.local.num_entries().await
    }
    async fn size_bytes(&self) -> usize {
        self.local.size_bytes().await
    }
    fn approx_num_entries(&self) -> usize {
        self.local.approx_num_entries()
    }
    fn approx_size_bytes(&self) -> usize {
        self.local.approx_size_bytes()
    }
}
