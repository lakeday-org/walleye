//! Typed WAL corruption, fencing, and durability errors.
use crate::OWNER_DO_ID_KEY;
use thiserror::Error;
use walleye_bitr::ReplicaError;
/// Errors produced while validating namespace identity, IPC data, or Bitr
/// durability.  Structural errors poison a backend because replay can no
/// longer establish one unambiguous log prefix; transient replica errors do
/// not poison it and leave the exact LSN/payload pending for retry.
#[derive(Debug, Error)]
pub enum WalBackendError {
    /// A supplied identity component is empty or unsafe for a stream key.
    #[error("invalid identity component `{field}`: {reason}")]
    InvalidIdentity { field: String, reason: String },
    /// A namespace dataset URI is missing or malformed.
    #[error("invalid namespace dataset URI: {0}")]
    InvalidDatasetUri(String),
    /// The replica quorum rejected or could not durably acknowledge a write.
    #[error("replica durability failed: {0}")]
    Replica(#[source] ReplicaError),
    /// A stale writer epoch was rejected by the quorum gateway.
    #[error("writer epoch is fenced")]
    Fenced,
    /// The backend was permanently poisoned by a fence or structural error.
    #[error("Bitr WAL backend is poisoned: {0}")]
    Poisoned(String),
    /// An LSN was reused with different bytes or a caller proposed a gap.
    #[error("LSN conflict at position {position}: {reason}")]
    LsnConflict { position: u64, reason: String },
    /// The replica tail skipped an expected contiguous position.
    #[error("recovery expected LSN {expected}, received {received}")]
    RecoveryGap { expected: u64, received: u64 },
    /// The replica returned a stream other than this backend's stream.
    #[error("recovery stream mismatch: expected `{expected}`, received `{received}`")]
    RecoveryStreamMismatch { expected: String, received: String },
    /// A WAL entry was not a valid Arrow IPC stream with required metadata.
    #[error("corrupt Arrow IPC WAL entry: {0}")]
    CorruptIpc(String),
    /// A row batch had no immutable owner metadata.
    #[error("Arrow batch is missing `{OWNER_DO_ID_KEY}` metadata")]
    OwnerMissing,
    /// A row batch was owned by a different Durable Object.
    #[error("Arrow batch owner `{received}` does not match `{expected}`")]
    OwnerMismatch { expected: String, received: String },
    /// A backend operation received a zero or otherwise invalid position.
    #[error("invalid WAL position {0}")]
    InvalidPosition(u64),
    /// Lance returned an invalid commit receipt from a backend append.
    #[error("invalid backend commit receipt: {0}")]
    InvalidCommit(String),
    /// A restarted stream needs an authenticated commit certificate before
    /// its recovered tail may be accepted as durable.
    #[error("certified recovery requires a replica commit certificate")]
    CommitCertificateMissing,
    /// Lance failed while opening, writing, or closing the shared dataset.
    #[error("Lance dataset operation failed: {0}")]
    Lance(#[source] lance::Error),
    /// JSON command or outbox payload conversion failed.
    #[error("JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
}
