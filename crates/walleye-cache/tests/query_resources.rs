//! Queries borrow from cache; spill ownership prevents cache growth until files are deleted.
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool};
use std::sync::Arc;
use tempfile::tempdir;
use walleye_cache::{LanceFoyerCacheBackend, QueryResources};

#[tokio::test]
async fn query_reservations_reclaim_ram_and_return_it_after_release() {
    let dir = tempdir().expect("directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(
            dir.path().join("cache"),
            128 * 1024,
            8 * 1024 * 1024,
            "pool",
        )
        .await
        .expect("backend"),
    );
    let resources = QueryResources::new(
        backend.clone(),
        dir.path().join("cache/spill"),
        128 * 1024,
        8 * 1024 * 1024,
    )
    .expect("resources");
    let pool: Arc<dyn MemoryPool> = resources.clone();
    let first = MemoryConsumer::new("first").register(&pool);
    let second = MemoryConsumer::new("second").register(&pool);
    first.try_grow(96 * 1024).expect("query gets RAM");
    assert!(backend.memory_capacity() <= 32 * 1024);
    assert!(
        second.try_grow(64 * 1024).is_err(),
        "queries share one ceiling"
    );
    first.free();
    second
        .try_grow(128 * 1024)
        .expect("released query RAM is reusable");
    second.free();
    assert_eq!(backend.memory_capacity(), 128 * 1024);
    backend.close().await.expect("close");
}

#[tokio::test]
async fn spill_reclaims_disk_before_creation_and_restores_after_last_file() {
    let dir = tempdir().expect("directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(
            dir.path().join("cache"),
            128 * 1024,
            8 * 1024 * 1024,
            "spill",
        )
        .await
        .expect("backend"),
    );
    let resources = QueryResources::new(
        backend.clone(),
        dir.path().join("cache/spill"),
        128 * 1024,
        8 * 1024 * 1024,
    )
    .expect("resources");
    let runtime = resources.runtime().expect("runtime");
    let first = runtime
        .disk_manager
        .create_tmp_file("first spill")
        .expect("spill");
    let floor = backend.persistent_capacity();
    assert!(floor < 8 * 1024 * 1024);
    assert!(runtime.disk_manager.max_temp_directory_size() + floor as u64 <= 8 * 1024 * 1024);
    assert!(first.path().starts_with(dir.path().join("cache/spill")));
    let second = runtime
        .disk_manager
        .create_tmp_file("second spill")
        .expect("spill");
    drop(first);
    assert_eq!(backend.persistent_capacity(), floor);
    drop(second);
    assert_eq!(backend.persistent_capacity(), 8 * 1024 * 1024);
    backend.close().await.expect("close");
}

#[tokio::test]
async fn active_queries_keep_reclaimed_ram_until_the_last_reservation_is_released() {
    let dir = tempdir().expect("directory");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(
            dir.path().join("cache"),
            64 * 1024 * 1024,
            8 * 1024 * 1024,
            "active query",
        )
        .await
        .expect("backend"),
    );
    let resources = QueryResources::new(
        backend.clone(),
        dir.path().join("cache/spill"),
        64 * 1024 * 1024,
        8 * 1024 * 1024,
    )
    .expect("resources");
    let pool: Arc<dyn MemoryPool> = resources;
    let input = MemoryConsumer::new("scan").register(&pool);
    let operator = MemoryConsumer::new("operator").register(&pool);
    input.try_grow(1024 * 1024).expect("input");
    operator.try_grow(32 * 1024 * 1024).expect("operator");
    let reclaimed = backend.memory_capacity();
    operator.free();
    assert_eq!(
        backend.memory_capacity(),
        reclaimed,
        "partial release must not oscillate Foyer capacity between batches"
    );
    input.free();
    assert_eq!(backend.memory_capacity(), 64 * 1024 * 1024);
    backend.close().await.expect("close");
}
