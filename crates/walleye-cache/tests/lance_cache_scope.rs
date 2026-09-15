//! Binding isolation and shared capacity for the runtime Lance cache.
use lance_core::cache::{CacheBackend, InternalCacheKey};
use std::sync::Arc;
use tempfile::tempdir;
use walleye_cache::LanceFoyerCacheBackend;

/// Produce the same opaque Lance key in separate storage bindings.
fn key(byte: u8) -> InternalCacheKey {
    InternalCacheKey::from_bytes([byte; 16])
}

#[tokio::test]
async fn scoped_backends_isolate_bindings_and_share_one_memory_budget() {
    let directory = tempdir().expect("directory");
    let backend = LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "root")
        .await
        .expect("backend");
    let first = backend.scoped("account-a/bucket");
    let second = backend.scoped("account-b/bucket");
    assert_ne!(
        first.peer_key(&key(92)),
        second.peer_key(&key(92)),
        "peer transport must preserve binding identity"
    );
    assert_eq!(
        first.peer_key(&key(92)),
        backend.scoped("account-a/bucket").peer_key(&key(92)),
        "binding peer keys must be stable"
    );
    first.insert(&key(92), Arc::new(1_u64), 32, None).await;
    assert!(second.get(&key(92), None).await.is_none());
    second.insert(&key(92), Arc::new(2_u64), 32, None).await;
    let value = first.get(&key(92), None).await.expect("first binding");
    assert_eq!(*value.downcast_ref::<u64>().expect("type"), 1);
    assert_eq!(
        backend.num_entries().await,
        2,
        "both scopes populate the same engine"
    );
    assert_eq!(first.approx_size_bytes(), backend.memory_usage());
    for number in 0..100_u8 {
        let scope = if number % 2 == 0 { &first } else { &second };
        scope
            .insert(&key(number), Arc::new(vec![0_u8; 4096]), 4120, None)
            .await;
    }
    assert!(backend.memory_usage() <= backend.memory_capacity());
    assert_eq!(first.approx_size_bytes(), second.approx_size_bytes());
    backend.close().await.expect("close");
}
