//! Peer transport only exchanges bounded immutable cache entries, never origin fills.
use lance_core::cache::{CacheBackend, InternalCacheKey};
use walleye_cache::LanceFoyerCacheBackend;
#[tokio::test]
async fn missing_peer_entry_never_materializes_a_value() {
    let d = tempfile::tempdir().unwrap();
    let b = LanceFoyerCacheBackend::new(&d, 65536, 8388608, "test")
        .await
        .unwrap();
    assert!(
        b.export_entry(&InternalCacheKey::from_bytes([1; 16]))
            .await
            .is_none()
    );
    assert_eq!(b.num_entries().await, 0);
    b.close().await.unwrap();
}
#[tokio::test]
async fn malformed_or_oversized_peer_entries_are_rejected() {
    let d = tempfile::tempdir().unwrap();
    let b = LanceFoyerCacheBackend::new(&d, 65536, 8388608, "test")
        .await
        .unwrap();
    let k = InternalCacheKey::from_bytes([1; 16]);
    assert!(!b.import_entry(&k, bytes::Bytes::from_static(b"bad")).await);
    let mut bytes = vec![0; 16];
    bytes[..8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(!b.import_entry(&k, bytes.into()).await);
    assert_eq!(b.num_entries().await, 0);
    b.close().await.unwrap();
}
