//! An origin request keeps its permit until its response body is consumed or dropped.
use bytes::Bytes;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use walleye_cache::LanceReadLimiter;

#[tokio::test]
async fn concurrent_store_views_share_limits_through_body_consumption() {
    let origin = Arc::new(InMemory::new());
    let path = Path::from("test");
    origin
        .put(&path, Bytes::from_static(b"body").into())
        .await
        .expect("put");
    let permits = Arc::new(Semaphore::new(1));
    let first = LanceReadLimiter::new(origin.clone(), permits.clone());
    let second = LanceReadLimiter::new(origin, permits.clone());
    let body = first.get(&path).await.expect("get");
    assert_eq!(permits.available_permits(), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), second.get(&path))
            .await
            .is_err()
    );
    body.bytes().await.expect("body");
    let body = second.get(&path).await.expect("released permit");
    drop(body);
    assert_eq!(permits.available_permits(), 1);
    assert!(first.get(&Path::from("missing")).await.is_err());
    assert_eq!(permits.available_permits(), 1);
}
