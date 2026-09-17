//! Bound active origin reads across every Lance table and binding in an instance.
//! A permit covers response-body consumption, including streamed range fills.
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use std::{fmt, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Read concurrency shared by all wrappers over the runtime's origin transports.
#[derive(Debug)]
pub struct LanceReadLimiter {
    origin: Arc<dyn ObjectStore>,
    permits: Arc<Semaphore>,
}
/// Set `WALLEYE_TRACE_ORIGIN=1` to log every request that reaches object
/// storage. Everything Lance reads passes through here after the block cache,
/// so the log is the ground truth for what the cache did not absorb.
fn trace_origin(op: &str, path: &Path, detail: &str) {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ENABLED.get_or_init(|| std::env::var_os("WALLEYE_TRACE_ORIGIN").is_some()) {
        eprintln!("walleye.origin op={op} path={path} {detail}");
    }
}
impl LanceReadLimiter {
    /// Reuse one runtime semaphore instead of allocating a per-table limit.
    pub fn new(origin: Arc<dyn ObjectStore>, permits: Arc<Semaphore>) -> Self {
        Self { origin, permits }
    }
    /// Acquire a read slot without blocking an executor thread.
    async fn acquire(permits: Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
        permits
            .acquire_owned()
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "LanceReadLimiter",
                source: Box::new(error),
            })
    }
}
impl fmt::Display for LanceReadLimiter {
    /// Keep binding credentials out of diagnostics.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Lance bounded origin reads")
    }
}
#[async_trait::async_trait]
impl ObjectStore for LanceReadLimiter {
    /// Keep the permit alive until a streaming response is fully consumed or dropped.
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        trace_origin(
            if options.head { "head" } else { "get" },
            path,
            &format!("range={:?}", options.range),
        );
        let permit = Self::acquire(self.permits.clone()).await?;
        let mut result = self.origin.get_opts(path, options).await?;
        result.payload = match result.payload {
            GetResultPayload::Stream(body) => GetResultPayload::Stream(
                body.map(move |item| {
                    let _ = &permit;
                    item
                })
                .boxed(),
            ),
            payload => payload,
        };
        Ok(result)
    }
    /// Keep listing requests under the same shared bound.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        trace_origin("list", prefix.unwrap_or(&Path::default()), "");
        let origin = self.origin.clone();
        let permits = self.permits.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            match Self::acquire(permits).await {
                Ok(permit) => origin
                    .list(prefix.as_ref())
                    .map(move |item| {
                        let _ = &permit;
                        item
                    })
                    .boxed(),
                Err(error) => stream::once(async { Err(error) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
    /// Bound directory listings through completion.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        trace_origin("list_delim", prefix.unwrap_or(&Path::default()), "");
        let _permit = Self::acquire(self.permits.clone()).await?;
        self.origin.list_with_delimiter(prefix).await
    }
    /// Preserve authoritative writes and their conditional options.
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.origin.put_opts(path, payload, options).await
    }
    /// Preserve multipart writes at the origin boundary.
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.origin.put_multipart_opts(path, options).await
    }
    /// Preserve authoritative deletion semantics.
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.origin.delete_stream(paths)
    }
    /// Preserve conditional server-side copies.
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.origin.copy_opts(from, to, options).await
    }
}
