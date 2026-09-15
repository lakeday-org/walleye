//! Origin-version, binding and recovery checks for ordinary Lance byte reads.

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    memory::InMemory, path::Path,
};
use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tempfile::tempdir;
use walleye_cache::{LanceCachedObjectStore, LanceFoyerCacheBackend};

#[derive(Debug, Default)]
struct Origin {
    inner: InMemory,
    gets: AtomicUsize,
    heads: AtomicUsize,
    fail_get: AtomicBool,
    replace_on_get: AtomicBool,
}

impl fmt::Display for Origin {
    /// Identify the test origin without describing stored contents.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test origin")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Origin {
    /// Preserve object-store mutation semantics for cache freshness checks.
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    /// Delegate multipart writes without involving the cache.
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    /// Distinguish metadata validation from data fills and simulate a write race.
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head {
            self.heads.fetch_add(1, Ordering::SeqCst);
        } else {
            self.gets.fetch_add(1, Ordering::SeqCst);
            if self.fail_get.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "test",
                    source: "GET unavailable".into(),
                });
            }
            if self.replace_on_get.swap(false, Ordering::SeqCst) {
                self.inner
                    .put(path, Bytes::from(vec![9; 8192]).into())
                    .await?;
            }
        }
        self.inner.get_opts(path, options).await
    }

    /// Keep deletion authoritative in the origin.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    /// Lists always describe current origin objects.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    /// Delegate directory-style listing.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    /// Delegate conditional copies to the origin.
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn a_query_pins_one_object_version_but_a_new_query_revalidates_it() {
    let directory = tempdir().expect("directory");
    let origin = Arc::new(Origin::default());
    let path = Path::from("data/file.lance");
    origin
        .put(&path, Bytes::from(vec![1; 8192]).into())
        .await
        .expect("put");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache")
            .await
            .expect("backend"),
    );
    let query = LanceCachedObjectStore::new(origin.clone(), backend.clone(), "binding-a", 4096)
        .expect("store")
        .for_query_snapshot();
    assert_eq!(
        query.get_range(&path, 0..100).await.expect("first"),
        Bytes::from(vec![1; 100])
    );
    assert_eq!(
        query.get_range(&path, 200..300).await.expect("same object"),
        Bytes::from(vec![1; 100])
    );
    assert_eq!(
        origin.heads.load(Ordering::SeqCst),
        1,
        "range reads in one query share the version check"
    );
    origin
        .put(&path, Bytes::from(vec![2; 8192]).into())
        .await
        .expect("overwrite");
    assert!(
        query.get_range(&path, 4096..4200).await.is_err(),
        "a miss cannot mix a newer object into the pinned version"
    );
    let next = LanceCachedObjectStore::new(origin.clone(), backend.clone(), "binding-a", 4096)
        .expect("next query")
        .for_query_snapshot();
    assert_eq!(
        next.get_range(&path, 0..100).await.expect("new version"),
        Bytes::from(vec![2; 100])
    );
    backend.close().await.expect("close");
}

#[tokio::test]
async fn ordinary_ranges_share_blocks_and_recover_without_data_gets() {
    let directory = tempdir().expect("directory");
    let origin = Arc::new(Origin::default());
    let path = Path::from("data/file.lance");
    let expected = Bytes::from((0..8192).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    origin
        .put(&path, expected.clone().into())
        .await
        .expect("put");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache")
            .await
            .expect("backend"),
    );
    let store = LanceCachedObjectStore::new(origin.clone(), backend.clone(), "binding-a", 4096)
        .expect("store");
    assert_eq!(
        store.get_range(&path, 100..5000).await.expect("cold range"),
        expected.slice(100..5000)
    );
    let cold_gets = origin.gets.load(Ordering::SeqCst);
    assert_eq!(
        store.get_range(&path, 200..4500).await.expect("overlap"),
        expected.slice(200..4500)
    );
    assert_eq!(
        origin.gets.load(Ordering::SeqCst),
        cold_gets,
        "overlapping ranges reuse physical blocks"
    );
    backend.flush().await;
    backend.close().await.expect("close");
    drop(store);
    drop(backend);
    let reopened = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache")
            .await
            .expect("reopen"),
    );
    origin.fail_get.store(true, Ordering::SeqCst);
    let store = LanceCachedObjectStore::new(origin.clone(), reopened.clone(), "binding-a", 4096)
        .expect("store");
    assert_eq!(
        store.get_range(&path, 100..5000).await.expect("disk range"),
        expected.slice(100..5000)
    );
    assert_eq!(origin.gets.load(Ordering::SeqCst), cold_gets);
    reopened.close().await.expect("close");
}

#[tokio::test]
async fn changed_or_deleted_origins_cannot_reuse_stale_bytes() {
    let directory = tempdir().expect("directory");
    let origin = Arc::new(Origin::default());
    let path = Path::from("data/file.lance");
    origin
        .put(&path, Bytes::from(vec![1; 8192]).into())
        .await
        .expect("put");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache")
            .await
            .expect("backend"),
    );
    let store = LanceCachedObjectStore::new(origin.clone(), backend.clone(), "binding-a", 4096)
        .expect("store");
    assert_eq!(
        store.get_range(&path, 0..100).await.expect("read"),
        Bytes::from(vec![1; 100])
    );
    origin
        .put(&path, Bytes::from(vec![2; 8192]).into())
        .await
        .expect("external overwrite");
    assert_eq!(
        store.get_range(&path, 0..100).await.expect("fresh read"),
        Bytes::from(vec![2; 100])
    );
    origin.delete(&path).await.expect("external delete");
    assert!(store.get_range(&path, 0..100).await.is_err());
    backend.close().await.expect("close");
}

#[tokio::test]
async fn an_origin_change_between_head_and_fill_is_rejected() {
    let directory = tempdir().expect("directory");
    let origin = Arc::new(Origin::default());
    let path = Path::from("data/file.lance");
    origin
        .put(&path, Bytes::from(vec![1; 8192]).into())
        .await
        .expect("put");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache")
            .await
            .expect("backend"),
    );
    let store = LanceCachedObjectStore::new(origin.clone(), backend.clone(), "binding-a", 4096)
        .expect("store");
    origin.replace_on_get.store(true, Ordering::SeqCst);
    assert!(
        store.get_range(&path, 0..100).await.is_err(),
        "the fill must use a version precondition"
    );
    assert_eq!(
        store.get_range(&path, 0..100).await.expect("new version"),
        Bytes::from(vec![9; 100])
    );
    backend.close().await.expect("close");
}

#[tokio::test]
async fn binding_and_geometry_are_part_of_every_persistent_key() {
    let directory = tempdir().expect("directory");
    let first = Arc::new(Origin::default());
    let second = Arc::new(Origin::default());
    let path = Path::from("data/file.lance");
    first
        .put(&path, Bytes::from(vec![1; 8192]).into())
        .await
        .expect("first");
    second
        .put(&path, Bytes::from(vec![2; 8192]).into())
        .await
        .expect("second");
    let backend = Arc::new(
        LanceFoyerCacheBackend::new(&directory, 64 * 1024, 8 * 1024 * 1024, "cache")
            .await
            .expect("backend"),
    );
    let a = LanceCachedObjectStore::new(first.clone(), backend.clone(), "account-a/bucket", 4096)
        .expect("first store");
    let b = LanceCachedObjectStore::new(second.clone(), backend.clone(), "account-b/bucket", 4096)
        .expect("second store");
    assert_eq!(
        a.get_range(&path, 0..100).await.expect("first read"),
        Bytes::from(vec![1; 100])
    );
    assert_eq!(
        b.get_range(&path, 0..100).await.expect("second read"),
        Bytes::from(vec![2; 100])
    );
    let larger = LanceCachedObjectStore::new(first, backend.clone(), "account-a/bucket", 8192)
        .expect("geometry");
    assert_eq!(
        larger
            .get_range(&path, 4000..8000)
            .await
            .expect("other geometry"),
        Bytes::from(vec![1; 4000])
    );
    backend.close().await.expect("close");
}

#[tokio::test]
async fn immutable_lance_headers_survive_restart_but_mutable_objects_revalidate() {
    let directory = tempdir().expect("directory");
    let origin = Arc::new(Origin::default());
    let path = Path::from("table/data/550e8400-e29b-41d4-a716-446655440000.lance");
    let mutable = Path::from("table/_latest.manifest");
    origin
        .put(&path, Bytes::from(vec![1; 8192]).into())
        .await
        .expect("put");
    origin
        .put(&mutable, Bytes::from(vec![2; 8192]).into())
        .await
        .expect("put");
    for iteration in 0..2 {
        let backend = Arc::new(
            LanceFoyerCacheBackend::new(&directory, 128 * 1024, 8 * 1024 * 1024, "immutable")
                .await
                .expect("backend"),
        );
        let store = LanceCachedObjectStore::new(origin.clone(), backend.clone(), "binding", 4096)
            .expect("store")
            .with_immutable_lance_files();
        assert_eq!(
            store.get_range(&path, 0..100).await.expect("read"),
            Bytes::from(vec![1; 100])
        );
        assert_eq!(
            origin.heads.load(Ordering::SeqCst),
            1 + iteration * 2,
            "immutable header is shared across cache restart"
        );
        store.get_range(&mutable, 0..100).await.expect("mutable");
        origin
            .put(&mutable, Bytes::from(vec![3; 8192]).into())
            .await
            .expect("update");
        assert_eq!(
            store.get_range(&mutable, 0..100).await.expect("fresh"),
            Bytes::from(vec![3; 100])
        );
        backend.flush().await;
        backend.close().await.expect("close");
    }
}

/// Capture the data-file path Lance actually emits at write time.
/// `Dataset::write` places one data file under `DATA_DIR = "data"` and names it
/// with `generate_random_filename()` (24 binary chars + 26 hex chars, plus the
/// `.lance` suffix). This is the production path shape `immutable_lance_path`
/// must classify as immutable; hand-built fixtures cannot prove that.
async fn lance_emitted_data_file_name() -> String {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::Dataset;
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
        .expect("batch");
    let uri = format!("memory://lakeday-cache-repro-{}", std::process::id());
    let dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        &uri,
        None,
    )
    .await
    .expect("dataset");
    let fragment = &dataset.fragments()[0];
    assert_eq!(
        fragment.files.len(),
        1,
        "single-row write produces one data file"
    );
    fragment.files[0].path.clone()
}

#[tokio::test]
async fn production_lance_data_path_is_immutable_across_cache_restart() {
    let directory = tempdir().expect("directory");
    let origin = Arc::new(Origin::default());
    let filename = lance_emitted_data_file_name().await;
    let path = Path::from(format!("table/data/{filename}"));
    origin
        .put(&path, Bytes::from(vec![1; 8192]).into())
        .await
        .expect("put");
    for iteration in 0..2 {
        let backend = Arc::new(
            LanceFoyerCacheBackend::new(&directory, 128 * 1024, 8 * 1024 * 1024, "immutable")
                .await
                .expect("backend"),
        );
        let store = LanceCachedObjectStore::new(origin.clone(), backend.clone(), "binding", 4096)
            .expect("store")
            .with_immutable_lance_files();
        assert_eq!(
            store.get_range(&path, 0..100).await.expect("read"),
            Bytes::from(vec![1; 100])
        );
        assert_eq!(
            origin.heads.load(Ordering::SeqCst),
            1,
            "real Lance data-file headers persist across cache restart (no re-HEAD); iteration {iteration}"
        );
        backend.flush().await;
        backend.close().await.expect("close");
    }
}
