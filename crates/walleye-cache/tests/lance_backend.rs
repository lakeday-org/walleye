//! Contract tests for the optional Lance cache backend.

use std::borrow::Cow;
use std::sync::Arc;

use lance_core::cache::{
    CacheBackend, CacheCodec, CacheCodecImpl, CacheEntry, CacheEntryReader, CacheEntryWriter,
    CacheKey, CacheKeySchema, Context, DeepSizeOf, InternalCacheKey, KeyBuilder, LanceCache,
};
use lance_core::{Error, Result};
use tempfile::tempdir;

use walleye_cache::LanceFoyerCacheBackend;

/// A compact codec whose decoded value occupies substantially more memory.
struct ExpandedValue(Vec<u8>);

impl CacheCodecImpl for ExpandedValue {
    const TYPE_ID: &'static str = "lakeday.test.ExpandedValue";
    const CURRENT_VERSION: u32 = 1;

    /// Store the length of this all-zero fixture, like a compressed index page.
    fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> Result<()> {
        writer.write_raw(&(self.0.len() as u64).to_le_bytes())
    }

    /// Reconstruct the full allocation so persistent promotion must account it.
    fn deserialize(reader: &mut CacheEntryReader<'_>) -> Result<Self> {
        let bytes = reader.read_raw()?;
        let length =
            u64::from_le_bytes(bytes.as_ref().try_into().map_err(|_| Error::io("length"))?);
        if length > 32768 {
            return Err(Error::io("fixture length exceeds allocation"));
        }
        Ok(Self(vec![0; length as usize]))
    }
}

#[tokio::test]
async fn recovered_compressed_values_keep_their_decoded_memory_weight() {
    let directory = tempdir().expect("directory");
    let backend = LanceFoyerCacheBackend::new(&directory, 128 * 1024, 8 * 1024 * 1024, "weight")
        .await
        .expect("backend");
    let codec = Some(CacheCodec::from_impl::<ExpandedValue>());
    backend
        .insert(
            &key(91),
            Arc::new(ExpandedValue(vec![0; 32768])),
            32792,
            codec,
        )
        .await;
    backend.flush().await;
    backend.close().await.expect("close");
    let reopened = LanceFoyerCacheBackend::new(&directory, 128 * 1024, 8 * 1024 * 1024, "weight")
        .await
        .expect("reopen");
    let value = reopened.get(&key(91), codec).await.expect("disk value");
    assert_eq!(
        value.downcast_ref::<ExpandedValue>().expect("type").0.len(),
        32768
    );
    assert!(
        reopened.memory_usage() >= 32792,
        "decoded allocation must be charged after restart"
    );
    reopened.close().await.expect("close reopened");
}

#[tokio::test]
async fn a_lance_cache_directory_has_one_live_owner() {
    let directory = tempdir().expect("directory");
    let owner = LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "owner")
        .await
        .expect("owner");
    let second = LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "other").await;
    assert!(
        second.is_err(),
        "a second engine must not write the same Foyer files"
    );
    owner.close().await.expect("close owner");
    let next = LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "owner")
        .await
        .expect("close releases ownership");
    next.close().await.expect("close next");
}

#[derive(Debug, PartialEq)]
struct SerializableValue {
    value: u64,
}

impl DeepSizeOf for SerializableValue {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

impl CacheCodecImpl for SerializableValue {
    const TYPE_ID: &'static str = "lakeday.test.SerializableValue";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, writer: &mut CacheEntryWriter<'_>) -> Result<()> {
        writer.write_raw(&self.value.to_le_bytes())
    }

    fn deserialize(reader: &mut CacheEntryReader<'_>) -> Result<Self> {
        let value = reader.read_raw()?;
        let value = value
            .as_ref()
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| Error::io("invalid test value"))?;
        Ok(Self { value })
    }
}

#[derive(Debug)]
struct RejectedValue;

impl CacheCodecImpl for RejectedValue {
    const TYPE_ID: &'static str = "lakeday.test.RejectedValue";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, _writer: &mut CacheEntryWriter<'_>) -> Result<()> {
        Err(Error::io("test codec rejection"))
    }

    fn deserialize(_reader: &mut CacheEntryReader<'_>) -> Result<Self> {
        Err(Error::io("test codec rejection"))
    }
}

#[derive(Debug)]
struct RamOnlyValue;

impl DeepSizeOf for RamOnlyValue {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        0
    }
}

#[derive(Debug, Clone, Copy)]
struct SerializableKey(u64);

impl CacheKey for SerializableKey {
    type ValueType = SerializableValue;

    fn key(&self) -> Cow<'_, str> {
        self.0.to_string().into()
    }

    fn type_name() -> &'static str {
        "lakeday.test.SerializableValue"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lakeday.test.serializable-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u64(self.0);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<SerializableValue>())
    }
}

#[derive(Debug, Clone, Copy)]
struct RamOnlyKey(u64);

impl CacheKey for RamOnlyKey {
    type ValueType = RamOnlyValue;

    fn key(&self) -> Cow<'_, str> {
        self.0.to_string().into()
    }

    fn type_name() -> &'static str {
        "lakeday.test.RamOnlyValue"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lakeday.test.ram-only-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u64(self.0);
    }
}

fn key(byte: u8) -> InternalCacheKey {
    InternalCacheKey::from_bytes([byte; 16])
}

#[tokio::test]
async fn serializable_entries_survive_backend_reopen() {
    let directory = tempdir().expect("cache directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("open backend"),
    );
    let cache = LanceCache::with_backend(backend.clone());
    let dataset = cache
        .with_key_prefix("dataset-a")
        .with_key_prefix("metadata");
    dataset
        .insert_with_key(
            &SerializableKey(7),
            Arc::new(SerializableValue { value: 42 }),
        )
        .await;
    backend.flush().await;
    backend.close().await.expect("close backend");
    drop(cache);

    let reopened = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("reopen backend"),
    );
    let reopened_cache = LanceCache::with_backend(reopened.clone());
    let reopened_dataset = reopened_cache
        .with_key_prefix("dataset-a")
        .with_key_prefix("metadata");
    let recovered = reopened_dataset
        .get_with_key(&SerializableKey(7))
        .await
        .expect("serialized entry survives reopen");
    assert_eq!(*recovered, SerializableValue { value: 42 });
    reopened.close().await.expect("close reopened backend");
}

#[tokio::test]
async fn entries_without_codecs_are_ram_only_and_miss_after_reopen() {
    let directory = tempdir().expect("cache directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("open backend"),
    );
    let cache = LanceCache::with_backend(backend.clone());
    let dataset = cache.with_key_prefix("dataset-a");
    dataset
        .insert_with_key(&RamOnlyKey(8), Arc::new(RamOnlyValue))
        .await;
    assert!(dataset.get_with_key(&RamOnlyKey(8)).await.is_some());

    backend.flush().await;
    backend.close().await.expect("close backend");
    drop(cache);

    let reopened = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("reopen backend"),
    );
    let reopened_cache = LanceCache::with_backend(reopened.clone());
    let reopened_dataset = reopened_cache.with_key_prefix("dataset-a");
    assert!(
        reopened_dataset
            .get_with_key(&RamOnlyKey(8))
            .await
            .is_none()
    );
    reopened.close().await.expect("close reopened backend");
}

#[tokio::test]
async fn codec_rejection_keeps_entry_in_memory_only() {
    let directory = tempdir().expect("cache directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("open backend"),
    );
    let cache_key = key(11);
    let codec = Some(CacheCodec::from_impl::<RejectedValue>());
    let value: CacheEntry = Arc::new(RejectedValue);
    backend.insert(&cache_key, value, 32, codec).await;
    assert!(backend.get(&cache_key, codec).await.is_some());
    backend.flush().await;
    backend.close().await.expect("close backend");

    let reopened = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("reopen backend"),
    );
    assert!(reopened.get(&cache_key, codec).await.is_none());
    reopened.close().await.expect("close reopened backend");
}

#[tokio::test]
async fn lance_prefixes_share_one_backend_without_aliasing() {
    let directory = tempdir().expect("cache directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("open backend"),
    );
    let cache = LanceCache::with_backend(backend.clone());
    let dataset_a = cache
        .with_key_prefix("dataset-a")
        .with_key_prefix("binding");
    let dataset_b = cache
        .with_key_prefix("dataset-b")
        .with_key_prefix("binding");

    dataset_a
        .insert_with_key(
            &SerializableKey(9),
            Arc::new(SerializableValue { value: 1 }),
        )
        .await;
    dataset_b
        .insert_with_key(
            &SerializableKey(9),
            Arc::new(SerializableValue { value: 2 }),
        )
        .await;

    assert_eq!(
        dataset_a
            .get_with_key(&SerializableKey(9))
            .await
            .expect("dataset a value")
            .value,
        1
    );
    assert_eq!(
        dataset_b
            .get_with_key(&SerializableKey(9))
            .await
            .expect("dataset b value")
            .value,
        2
    );
    backend.close().await.expect("close backend");
}

#[tokio::test]
async fn get_or_insert_deduplicates_and_uses_foyer_capacity() {
    let directory = tempdir().expect("cache directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache-format-v1")
            .await
            .expect("open backend"),
    );
    let codec = Some(CacheCodec::from_impl::<SerializableValue>());
    let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first_load = {
        let loads = Arc::clone(&loads);
        Box::pin(async move {
            loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::task::yield_now().await;
            let value: CacheEntry = Arc::new(SerializableValue { value: 5 });
            Ok((value, 64))
        })
    };
    let second_load = {
        let loads = Arc::clone(&loads);
        Box::pin(async move {
            loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let value: CacheEntry = Arc::new(SerializableValue { value: 5 });
            Ok((value, 64))
        })
    };
    let cache_key = key(10);
    let first = backend.get_or_insert(&cache_key, first_load, codec);
    let second = backend.get_or_insert(&cache_key, second_load, codec);
    let (first, second) = tokio::join!(first, second);
    let (first_value, first_cached) = first.expect("first load value");
    let (second_value, second_cached) = second.expect("second load value");
    assert_ne!(first_cached, second_cached);
    assert_eq!(
        first_value.downcast_ref::<SerializableValue>(),
        Some(&SerializableValue { value: 5 })
    );
    assert_eq!(
        second_value.downcast_ref::<SerializableValue>(),
        Some(&SerializableValue { value: 5 })
    );
    assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 1);
    let (value, was_cached) = backend
        .get_or_insert(
            &cache_key,
            Box::pin(async { Err(Error::io("loader should not run")) }),
            codec,
        )
        .await
        .expect("cached value");
    assert!(was_cached);
    assert_eq!(
        value.downcast_ref::<SerializableValue>(),
        Some(&SerializableValue { value: 5 })
    );
    assert!(backend.num_entries().await <= 1);
    assert!(backend.size_bytes().await <= 64 * 1024);
    backend.close().await.expect("close backend");
}

#[tokio::test]
async fn live_budgets_are_shared_and_disk_space_is_really_released() {
    let directory = tempdir().expect("directory");
    let backend = LanceFoyerCacheBackend::new(&directory, 128 * 1024, 8 * 1024 * 1024, "resize")
        .await
        .expect("backend");
    let view = backend.scoped("view");
    backend.resize_memory(0).expect("yield RAM");
    assert_eq!(view.memory_capacity(), 0);
    let size = backend.resize_disk(0).await.expect("yield disk");
    assert!(size < 8 * 1024 * 1024);
    assert_eq!(
        std::fs::metadata(directory.path().join("cache.bin"))
            .expect("file")
            .len(),
        size as u64
    );
    assert_eq!(view.persistent_capacity(), size);
    backend
        .resize_disk(8 * 1024 * 1024)
        .await
        .expect("restore disk");
    view.resize_memory(128 * 1024).expect("restore RAM");
    assert_eq!(backend.memory_capacity(), 128 * 1024);
    backend.close().await.expect("close");
}

#[tokio::test]
async fn eviction_does_not_claim_memory_still_held_by_a_query() {
    let directory = tempdir().expect("directory");
    let backend = LanceFoyerCacheBackend::new(&directory, 128 * 1024, 8 * 1024 * 1024, "pinned")
        .await
        .expect("backend");
    let value: CacheEntry = Arc::new(ExpandedValue(vec![0; 32768]));
    backend.insert(&key(93), value.clone(), 32768, None).await;
    backend.resize_memory(0).expect("evict");
    assert!(backend.retained_value_bytes() >= 32768);
    drop(value);
    assert_eq!(backend.retained_value_bytes(), 0);
    backend.close().await.expect("close");
}

#[tokio::test]
async fn old_disposable_partitions_do_not_consume_the_new_disk_allocation() {
    let directory = tempdir().expect("directory");
    let old = directory.path().join("foyer-storage-direct-fs-00000001");
    std::fs::write(&old, vec![1; 4096]).expect("old cache");
    let unrelated = directory.path().join("keep-me");
    std::fs::write(&unrelated, b"owned elsewhere").expect("other file");
    let backend = LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cleanup")
        .await
        .expect("backend");
    assert!(!old.exists());
    assert!(unrelated.exists());
    backend.close().await.expect("close");
}
