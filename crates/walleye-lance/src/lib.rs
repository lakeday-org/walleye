//! Standalone Lance tables and a Bitr implementation of Lance's WAL boundary.
mod backend;
mod cached_storage;
mod error;
mod identity;
mod ipc;
mod result_memory;
mod sql;
mod storage_options;
mod table;
pub use backend::{BitrWalBackend, RecoveredEntry, WalReceipt};
pub use cached_storage::CachedStorage;
pub use error::WalBackendError;
pub use identity::{DoIdentity, NamespaceConfig, RECORDS_DATASET_NAME};
pub use ipc::{
    FENCE_SENTINEL_KEY, IpcEntry, OWNER_DO_ID_KEY, WRITER_EPOCH_KEY, decode_ipc_entry,
    encode_fence_sentinel, encode_ipc_batches, with_owner,
};
pub use lance::Error as LanceError;
pub use lance::arrow::json::JsonSchema;
pub use sql::{SnapshotSource, TableSnapshot, query};
pub use storage_options::LanceStorageOptions;
pub use table::{
    IndexInfo, ScanResult, SearchRequest, Table, TableConfig, VectorIndexRequest, VectorQuery,
};
pub type Error = WalBackendError;
pub type Result<T> = std::result::Result<T, WalBackendError>;
type WalResult<T> = Result<T>;
use std::sync::Arc;
/// Durable WAL authority selected before a Lance store admits events.
#[derive(Clone, Debug)]
pub enum LanceDurability {
    /// Store WAL entries through Lance's atomic object-store backend.
    ObjectStore,
    /// Store WAL entries through the configured Bitr quorum.
    Bitr(Arc<BitrWalBackend>),
}

impl From<Arc<BitrWalBackend>> for LanceDurability {
    /// Wraps an authenticated Bitr backend as an explicit durability choice.
    fn from(backend: Arc<BitrWalBackend>) -> Self {
        Self::Bitr(backend)
    }
}

impl LanceDurability {
    /// Returns the authority represented in downstream commit knowledge.
    #[must_use]
    pub const fn authority(&self) -> walleye_bitr::DurabilityAuthority {
        match self {
            Self::ObjectStore => walleye_bitr::DurabilityAuthority::ObjectStore,
            Self::Bitr(_) => walleye_bitr::DurabilityAuthority::Replica,
        }
    }
}
