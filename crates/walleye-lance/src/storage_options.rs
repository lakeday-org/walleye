//! Carry one host-owned session through every dataset read and write. Storage
//! credentials and cache ownership must travel together, including catalog,
//! vector and MemWAL datasets opened below the initial runtime boundary.

use lance::deps::datafusion::execution::runtime_env::RuntimeEnv;
use lance::{
    dataset::{Dataset, WriteParams, builder::DatasetBuilder},
    io::ObjectStoreParams,
    session::Session,
};
use std::{sync::Arc, time::Duration};

/// Object-store parameters and the session selected by the host.
#[derive(Clone, Debug)]
pub struct LanceStorageOptions {
    params: Option<ObjectStoreParams>,
    session: Arc<Session>,
    query_runtime: Option<Arc<RuntimeEnv>>,
    query_partitions: usize,
    query_timeout: Duration,
}

impl Default for LanceStorageOptions {
    /// Provide standalone library storage; the daemon supplies its Foyer session.
    fn default() -> Self {
        Self::new(None, Arc::new(Session::default()))
    }
}

impl LanceStorageOptions {
    /// Bind storage access to a session whose cache ownership outlives datasets.
    pub fn new(params: Option<ObjectStoreParams>, session: Arc<Session>) -> Self {
        Self {
            params,
            session,
            query_runtime: None,
            query_partitions: 2,
            query_timeout: Duration::from_secs(10),
        }
    }

    /// Carry the host's shared query allocation alongside its shared cache session.
    pub fn with_query_runtime(
        mut self,
        runtime: Arc<RuntimeEnv>,
        partitions: usize,
        timeout: Duration,
    ) -> Self {
        self.query_runtime = Some(runtime);
        self.query_partitions = partitions.max(1);
        self.query_timeout = timeout;
        self
    }

    /// Obtain the process-wide query memory and spill authority.
    pub fn query_runtime(&self) -> Option<Arc<RuntimeEnv>> {
        self.query_runtime.clone()
    }
    /// Match query partitioning to the allocated CPU budget.
    pub fn query_partitions(&self) -> usize {
        self.query_partitions
    }
    /// Preserve the configured deadline when query handles are reused.
    pub fn query_timeout(&self) -> Duration {
        self.query_timeout
    }

    /// Open a dataset without creating an implicit per-dataset cache.
    pub async fn open_dataset(&self, uri: &str) -> lance::Result<Dataset> {
        let mut builder = DatasetBuilder::from_uri(uri).with_session(Arc::clone(&self.session));
        if let Some(params) = &self.params {
            builder = builder.with_store_params(params.clone());
        }
        builder.load().await
    }

    /// Preserve the host session for table creation, append and rewrite operations.
    pub fn write_params(&self) -> WriteParams {
        WriteParams {
            store_params: self.params.clone(),
            session: Some(Arc::clone(&self.session)),
            ..Default::default()
        }
    }

    /// Return the session shared by this storage capability's datasets.
    pub fn session(&self) -> Arc<Session> {
        Arc::clone(&self.session)
    }

    /// Supply the same origin wrapper and credentials to derived WAL stores.
    pub fn object_store_params(&self) -> Option<ObjectStoreParams> {
        self.params.clone()
    }
}
