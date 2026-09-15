//! Serialized Bitr append, recovery cursors, and Lance WAL trait implementation.
use crate::{DoIdentity, NamespaceConfig, WalBackendError, WalResult, ipc::*};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use lance::dataset::mem_wal::{WalBackend, WalCommit, WalWrite, WalWriteResult};
use std::{collections::BTreeMap, fmt, sync::Arc};
use tokio::sync::Mutex;
use uuid::Uuid;
use walleye_bitr::{AppendRecord, CommitKnowledge, QuorumWriter, ReplicaError};
/// A decoded, ordered recovery result identified by its Bitr/Lance position.
#[derive(Debug)]
pub struct RecoveredEntry {
    position: u64,
    writer_epoch: u64,
    fence_sentinel: bool,
    batches: Vec<RecordBatch>,
    payload: Bytes,
}

impl RecoveredEntry {
    /// Returns the one-based WAL position.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Returns the writer epoch embedded in this entry.
    #[must_use]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns whether this entry is a fence sentinel.
    #[must_use]
    pub fn is_fence_sentinel(&self) -> bool {
        self.fence_sentinel
    }

    /// Returns the Arrow batches in this entry.
    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Returns the exact IPC payload recovered from the Bitr stream.
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// Receipt returned after one Bitr WAL entry reaches the replica quorum.
#[derive(Clone, Debug)]
pub struct WalReceipt {
    position: u64,
    knowledge: CommitKnowledge,
}

impl WalReceipt {
    /// Returns the one-based Bitr/Lance position.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Returns the replica commit knowledge for this acknowledgement.
    #[must_use]
    pub fn knowledge(&self) -> &CommitKnowledge {
        &self.knowledge
    }
}

/// One immutable write retained while a quorum response is uncertain.
#[derive(Clone, Debug)]
struct PendingWrite {
    position: u64,
    payload: Bytes,
}

/// A committed entry cached after a successful append or validated replay.
#[derive(Clone, Debug)]
struct CommittedEntry {
    payload: Bytes,
    knowledge: CommitKnowledge,
}

/// Mutable cursors for one Bitr stream.  The mutex serializes position
/// assignment and the network append, so concurrent callers cannot consume the
/// same LSN or observe a partially acknowledged write.
#[derive(Debug)]
struct BackendState {
    next_position: Option<u64>,
    pending: Option<PendingWrite>,
    committed: BTreeMap<u64, CommittedEntry>,
    checkpointed_position: u64,
    knowledge: CommitKnowledge,
    /// Highest WAL position validated while this backend opened/recovered.
    /// This excludes the predecessor fence written by the current open so a
    /// readiness receipt can distinguish recovered history from that fence.
    recovered_tail_position: u64,
    /// The fence sentinel written by this backend instance, if any.
    fence_sentinel_position: Option<u64>,
    poisoned: Option<String>,
}

/// Bitr implementation of Lance's MemWAL backend seam.
///
/// The backend persists Lance's complete Arrow IPC bytes as one encrypted Bitr
/// record.  Bitr's stream/epoch/LSN is the sole durability identity; there is
/// no object-store WAL path and no per-object bucket.
pub struct BitrWalBackend {
    writer: Arc<QuorumWriter>,
    stream: Arc<str>,
    shard_id: Uuid,
    writer_epoch: u64,
    owner_do_id: Arc<str>,
    state: Arc<Mutex<BackendState>>,
}

impl fmt::Debug for BitrWalBackend {
    /// Redacts the quorum encryption client while exposing stream identity.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BitrWalBackend")
            .field("stream", &self.stream)
            .field("shard_id", &self.shard_id)
            .field("writer_epoch", &self.writer_epoch)
            .field("owner_do_id", &self.owner_do_id)
            .finish_non_exhaustive()
    }
}

impl BitrWalBackend {
    /// Creates a backend whose owner identity is the stream key.
    pub fn new(
        writer: Arc<QuorumWriter>,
        stream: impl Into<String>,
        shard_id: Uuid,
        writer_epoch: u64,
    ) -> WalResult<Self> {
        let stream = stream.into();
        Self::new_with_owner(writer, stream.clone(), shard_id, writer_epoch, stream)
    }

    /// Creates a backend with an explicit immutable owner identity.
    pub fn new_with_owner(
        writer: Arc<QuorumWriter>,
        stream: impl Into<String>,
        shard_id: Uuid,
        writer_epoch: u64,
        owner_do_id: impl Into<String>,
    ) -> WalResult<Self> {
        let stream = validate_stream(stream.into())?;
        if shard_id.is_nil() {
            return Err(WalBackendError::InvalidIdentity {
                field: "shard_id".to_owned(),
                reason: "shard UUID must not be nil".to_owned(),
            });
        }
        if writer_epoch == 0 {
            return Err(WalBackendError::InvalidIdentity {
                field: WRITER_EPOCH_KEY.to_owned(),
                reason: "writer epoch must be positive".to_owned(),
            });
        }
        let owner_do_id = validate_stream(owner_do_id.into())?;
        Ok(Self {
            writer,
            stream: Arc::from(stream.clone()),
            shard_id,
            writer_epoch,
            owner_do_id: Arc::from(owner_do_id),
            state: Arc::new(Mutex::new(BackendState {
                next_position: None,
                pending: None,
                committed: BTreeMap::new(),
                checkpointed_position: 0,
                knowledge: CommitKnowledge::replica(stream, writer_epoch, 0),
                recovered_tail_position: 0,
                fence_sentinel_position: None,
                poisoned: None,
            })),
        })
    }

    /// Creates a backend from a namespace and full DO identity.
    pub fn for_do(
        writer: Arc<QuorumWriter>,
        namespace: &NamespaceConfig,
        identity: &DoIdentity,
        writer_epoch: u64,
    ) -> WalResult<Self> {
        let shard_id = namespace.shard_for_do(identity)?;
        let stream = namespace.stream_for_do(identity)?;
        Self::new_with_owner(writer, stream, shard_id, writer_epoch, identity.full_key())
    }

    /// Returns this backend's deterministic MemWAL shard UUID.
    #[must_use]
    pub fn shard_id(&self) -> Uuid {
        self.shard_id
    }

    /// Returns the Bitr stream key.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the claimed writer epoch.
    #[must_use]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns the immutable owner identity expected in every data batch.
    #[must_use]
    pub fn owner_do_id(&self) -> &str {
        &self.owner_do_id
    }

    /// Appends multiple Arrow batches as exactly one Bitr LSN.
    pub async fn append_batches(&self, batches: Vec<RecordBatch>) -> WalResult<WalReceipt> {
        let payload = encode_ipc_batches(self.writer_epoch, &batches)?;
        let result = self.append_payload(payload, false).await?;
        self.receipt_from_result(result).await
    }

    /// Appends an already encoded Lance IPC entry.  This is used by the Lance
    /// trait implementation; callers should prefer [`Self::append_batches`].
    pub async fn append_encoded(&self, payload: Bytes) -> WalResult<WalReceipt> {
        let result = self.append_payload(payload, false).await?;
        self.receipt_from_result(result).await
    }

    /// Writes the data-less predecessor fence sentinel at one Bitr position.
    pub async fn append_fence(&self) -> WalResult<WalReceipt> {
        let payload = encode_fence_sentinel(self.writer_epoch)?;
        let result = self.append_payload(payload, true).await?;
        self.receipt_from_result(result).await
    }

    /// Recovers and validates a contiguous tail after `after_lsn`.
    pub async fn recover(&self, after_lsn: u64) -> WalResult<Vec<RecoveredEntry>> {
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        let result = self.load_recovery(after_lsn).await;
        match result {
            Ok(entries) => {
                self.apply_recovery(&mut state, &entries, None)?;
                Ok(entries)
            }
            Err(error) => {
                self.poison_if_structural(&mut state, &error);
                Err(error)
            }
        }
    }

    /// Recovers a tail through an authenticated persisted commit watermark.
    ///
    /// A restarted backend must use this path when it has no in-memory receipt:
    /// ordinary recovery is deliberately rejected rather than manufacturing a
    /// certificate-less [`CommitKnowledge`] value.
    pub async fn recover_with_watermark(
        &self,
        after_lsn: u64,
        knowledge: &CommitKnowledge,
    ) -> WalResult<Vec<RecoveredEntry>> {
        if knowledge.stream != self.stream.as_ref() || knowledge.writer_epoch > self.writer_epoch {
            return Err(WalBackendError::InvalidCommit(
                "persisted commit knowledge does not match this stream or is from a future epoch"
                    .to_owned(),
            ));
        }
        let certificate = knowledge
            .certificate
            .as_deref()
            .filter(|certificate| !certificate.is_empty())
            .ok_or(WalBackendError::CommitCertificateMissing)?;
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        let result = self
            .load_recovery_with_watermark(after_lsn, knowledge.committed_lsn, certificate)
            .await;
        match result {
            Ok(entries) => {
                if knowledge.committed_lsn > after_lsn
                    && entries.last().map(RecoveredEntry::position) != Some(knowledge.committed_lsn)
                {
                    let error = WalBackendError::RecoveryGap {
                        expected: knowledge.committed_lsn,
                        received: entries.last().map_or(after_lsn, RecoveredEntry::position),
                    };
                    self.poison_if_structural(&mut state, &error);
                    return Err(error);
                }
                self.apply_recovery(&mut state, &entries, Some(knowledge))?;
                Ok(entries)
            }
            Err(error) => {
                self.poison_if_structural(&mut state, &error);
                Err(error)
            }
        }
    }

    /// Alias emphasizing that the supplied watermark must have been persisted.
    pub async fn recover_with_commit_knowledge(
        &self,
        after_lsn: u64,
        knowledge: &CommitKnowledge,
    ) -> WalResult<Vec<RecoveredEntry>> {
        self.recover_with_watermark(after_lsn, knowledge).await
    }

    /// Returns the next one-based position without advancing it.
    pub async fn next_position(&self) -> WalResult<u64> {
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        self.ensure_position(&mut state).await
    }

    /// Reads and validates one entry, returning `None` when it is absent.
    pub async fn read_entry(&self, position: u64) -> WalResult<Option<RecoveredEntry>> {
        if position == 0 {
            return Err(WalBackendError::InvalidPosition(position));
        }
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        if let Some(entry) = state.committed.get(&position) {
            let decoded = decode_ipc_entry(entry.payload.as_ref())?;
            return Ok(Some(RecoveredEntry {
                position,
                writer_epoch: decoded.writer_epoch,
                fence_sentinel: decoded.fence_sentinel,
                batches: decoded.batches,
                payload: entry.payload.clone(),
            }));
        }
        let result = self.load_recovery(position.saturating_sub(1)).await;
        match result {
            Ok(entries) => {
                self.apply_recovery(&mut state, &entries, None)?;
                Ok(entries.into_iter().find(|entry| entry.position == position))
            }
            Err(error) => {
                self.poison_if_structural(&mut state, &error);
                Err(error)
            }
        }
    }

    /// Bytes held locally for WAL entries not yet covered by a Lance checkpoint.
    pub async fn retained_wal_bytes(&self) -> usize {
        self.state
            .lock()
            .await
            .committed
            .values()
            .map(|e| e.payload.len())
            .sum()
    }

    /// Returns the most recent acknowledged replica commit knowledge.
    pub async fn commit_knowledge(&self) -> WalResult<CommitKnowledge> {
        let state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        Ok(state.knowledge.clone())
    }

    /// Returns whether a fence or structural failure has poisoned this backend.
    pub async fn is_poisoned(&self) -> bool {
        self.state.lock().await.poisoned.is_some()
    }

    /// Returns the first retained one-based position, or one for an empty log.
    async fn first_position_inner(&self) -> WalResult<u64> {
        let entries = self.recover(0).await?;
        Ok(entries.first().map_or(1, RecoveredEntry::position))
    }

    /// Validates a caller-provided payload before selecting an LSN.
    fn decode_for_write(
        &self,
        payload: &Bytes,
        fence: bool,
        writer_epoch: u64,
    ) -> WalResult<IpcEntry> {
        let entry = decode_ipc_entry(payload.as_ref())?;
        if writer_epoch == 0 || entry.writer_epoch != writer_epoch {
            return Err(WalBackendError::Fenced);
        }
        if entry.fence_sentinel != fence {
            return Err(WalBackendError::CorruptIpc(if fence {
                "fence write must carry a fence sentinel".to_owned()
            } else {
                "data write must not carry a fence sentinel".to_owned()
            }));
        }
        if !entry.fence_sentinel {
            validate_owner(&entry.batches, self.owner_do_id.as_ref())?;
        }
        Ok(entry)
    }

    /// Appends one payload while holding the stream's serialization lock.
    async fn append_payload(&self, payload: Bytes, fence: bool) -> WalResult<WalWriteResult> {
        let entry = self.decode_for_write(&payload, fence, self.writer_epoch)?;
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        let position = self.ensure_position(&mut state).await?;
        let write = WalWrite {
            shard_id: self.shard_id,
            writer_epoch: self.writer_epoch,
            entry_position: position,
            payload,
        };
        self.append_locked(&mut state, write, entry).await
    }

    /// Handles a write supplied by Lance at its chosen position.
    async fn append_at_position(&self, write: WalWrite, fence: bool) -> WalResult<WalWriteResult> {
        if write.shard_id != self.shard_id {
            return Err(WalBackendError::LsnConflict {
                position: write.entry_position,
                reason: "write targets a different shard".to_owned(),
            });
        }
        // Lance owns the per-open writer epoch through its conditional shard
        // manifest. The descriptor epoch authenticates the host but is not a
        // second MemWAL counter: a cached shard can be evicted and reopened
        // without a new descriptor. Preserve Lance's claimed epoch end to end
        // and let Bitr fence an older writer against the highest durable epoch.
        let entry = self.decode_for_write(&write.payload, fence, write.writer_epoch)?;
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        self.ensure_position(&mut state).await?;
        let position = write.entry_position;
        let writer_epoch = write.writer_epoch;
        match self.append_locked(&mut state, write, entry).await {
            Err(WalBackendError::LsnConflict { .. }) if fence && state.poisoned.is_none() => {
                self.recover_fence_position(&mut state, position, writer_epoch)
                    .await
            }
            result => result,
        }
    }

    /// Reports an occupied fence slot only after validated recovery proves an
    /// older writer owns it. Lance then retries its sentinel after that tail
    /// and replays the predecessor's records before admitting data writes.
    async fn recover_fence_position(
        &self,
        state: &mut BackendState,
        position: u64,
        writer_epoch: u64,
    ) -> WalResult<WalWriteResult> {
        let entries = self
            .load_recovery_for_lance(position.saturating_sub(1))
            .await
            .inspect_err(|error| self.poison_if_structural(state, error))?;
        if entries
            .iter()
            .any(|entry| entry.writer_epoch > writer_epoch)
        {
            self.poison_if_structural(state, &WalBackendError::Fenced);
            return Err(WalBackendError::Fenced);
        }
        // A rejection alone does not prove a committed record occupies this
        // position. An entry in our own epoch is also ambiguous; never skip it.
        // Retain the pending sentinel and cursor when that proof is absent.
        if entries
            .first()
            .is_none_or(|entry| entry.position != position)
            || entries
                .iter()
                .any(|entry| entry.writer_epoch == writer_epoch)
        {
            return Err(WalBackendError::LsnConflict {
                position,
                reason: "fence conflict is not backed by a recovered predecessor tail".to_owned(),
            });
        }
        self.apply_recovery_for_lance(state, &entries)
            .inspect_err(|error| self.poison_if_structural(state, error))?;
        state.pending = None;
        Ok(WalWriteResult::AlreadyExists)
    }

    /// Performs idempotency checks and one quorum append at the exact LSN.
    async fn append_locked(
        &self,
        state: &mut BackendState,
        write: WalWrite,
        entry: IpcEntry,
    ) -> WalResult<WalWriteResult> {
        let expected = state.next_position.unwrap_or(1);
        if write.entry_position != expected {
            if write.entry_position < expected
                && let Some(existing) = state.committed.get(&write.entry_position)
                && existing.payload == write.payload
            {
                return Ok(WalWriteResult::Committed(WalCommit {
                    entry_position: write.entry_position,
                    knowledge: encode_commit_knowledge(&existing.knowledge)?,
                }));
            }
            let error = WalBackendError::LsnConflict {
                position: write.entry_position,
                reason: format!("expected next position {expected}"),
            };
            self.poison_if_structural(state, &error);
            return Err(error);
        }
        if let Some(pending) = &state.pending
            && (pending.position != write.entry_position || pending.payload != write.payload)
        {
            let error = WalBackendError::LsnConflict {
                position: write.entry_position,
                reason: "pending retry has different bytes".to_owned(),
            };
            self.poison_if_structural(state, &error);
            return Err(error);
        }
        state.pending = Some(PendingWrite {
            position: write.entry_position,
            payload: write.payload.clone(),
        });
        let knowledge = match self
            .writer
            .append(AppendRecord::new(
                self.stream.as_ref(),
                write.writer_epoch,
                write.entry_position,
                write.entry_position.saturating_sub(1),
                write.payload.as_ref(),
            ))
            .await
        {
            Ok(knowledge) => knowledge,
            Err(error) => {
                let mapped = map_replica_error(error);
                if matches!(mapped, WalBackendError::Fenced) {
                    self.poison_if_structural(state, &mapped);
                }
                return Err(mapped);
            }
        };
        if knowledge.stream != self.stream.as_ref()
            || knowledge.writer_epoch != write.writer_epoch
            || knowledge.committed_lsn != write.entry_position
        {
            let error = WalBackendError::InvalidCommit(
                "replica acknowledgement does not match stream, epoch, or LSN".to_owned(),
            );
            self.poison_if_structural(state, &error);
            return Err(error);
        }
        let encoded_knowledge = encode_commit_knowledge(&knowledge)?;
        state.committed.insert(
            write.entry_position,
            CommittedEntry {
                payload: write.payload,
                knowledge: knowledge.clone(),
            },
        );
        state.next_position = Some(
            write
                .entry_position
                .checked_add(1)
                .ok_or(WalBackendError::InvalidPosition(write.entry_position))?,
        );
        state.pending = None;
        state.knowledge = knowledge;
        if entry.fence_sentinel {
            state.fence_sentinel_position = Some(write.entry_position);
        }
        let _ = entry;
        Ok(WalWriteResult::Committed(WalCommit {
            entry_position: write.entry_position,
            knowledge: encoded_knowledge,
        }))
    }

    /// Discovers the true next position through strict Bitr recovery.
    async fn ensure_position(&self, state: &mut BackendState) -> WalResult<u64> {
        if let Some(position) = state.next_position {
            return Ok(position);
        }
        let result = self.load_recovery(0).await;
        match result {
            Ok(entries) => {
                self.apply_recovery(state, &entries, None)?;
                let position = entries
                    .last()
                    .map_or(1, |entry| entry.position.saturating_add(1));
                state.next_position = Some(position);
                Ok(position)
            }
            Err(error) => {
                self.poison_if_structural(state, &error);
                Err(error)
            }
        }
    }

    /// Replays a Bitr tail for Lance's writer-open path without requiring a
    /// second authenticated commit certificate.  Lance's own manifest and
    /// MemWAL overlay are the recovery authority here; the public
    /// `recover_with_watermark` API remains strict for callers that need a
    /// certified cross-process handoff.
    async fn load_recovery_for_lance(&self, after_lsn: u64) -> WalResult<Vec<RecoveredEntry>> {
        let records = self
            .writer
            .recover(self.stream.as_ref(), after_lsn)
            .await
            .map_err(map_replica_error)?;
        self.decode_recovery_records_with_epoch_bound(after_lsn, records, None)
    }

    /// Applies fully decoded Lance-open recovery using a local synthetic
    /// watermark.  The bytes have already passed Bitr stream/epoch/owner
    /// validation, and the synthetic value is never exposed as a certified
    /// commit knowledge receipt.
    fn apply_recovery_for_lance(
        &self,
        state: &mut BackendState,
        entries: &[RecoveredEntry],
    ) -> WalResult<()> {
        for entry in entries {
            if let Some(existing) = state.committed.get(&entry.position)
                && existing.payload != entry.payload
            {
                return Err(WalBackendError::LsnConflict {
                    position: entry.position,
                    reason: "Lance recovery bytes differ from the cached entry".to_owned(),
                });
            }
            let knowledge = if state.knowledge.committed_lsn >= entry.position {
                state.knowledge.clone()
            } else {
                CommitKnowledge::replica(self.stream.as_ref(), entry.writer_epoch, entry.position)
            };
            if entry.position > state.checkpointed_position {
                state.committed.insert(
                    entry.position,
                    CommittedEntry {
                        payload: entry.payload.clone(),
                        knowledge: knowledge.clone(),
                    },
                );
            }
            state.knowledge = knowledge;
            state.recovered_tail_position = state.recovered_tail_position.max(entry.position);
        }
        Self::advance_next_position(state, entries);
        Ok(())
    }

    /// Returns a recovered entry through Lance's read seam, retaining the
    /// validated bytes in this backend so subsequent reads are local.
    async fn read_entry_for_lance(&self, position: u64) -> WalResult<Option<RecoveredEntry>> {
        if position == 0 {
            return Err(WalBackendError::InvalidPosition(position));
        }
        {
            let state = self.state.lock().await;
            self.ensure_not_poisoned(&state)?;
            if let Some(entry) = state.committed.get(&position) {
                let decoded = decode_ipc_entry(entry.payload.as_ref())?;
                return Ok(Some(RecoveredEntry {
                    position,
                    writer_epoch: decoded.writer_epoch,
                    fence_sentinel: decoded.fence_sentinel,
                    batches: decoded.batches,
                    payload: entry.payload.clone(),
                }));
            }
        }
        let entries = self
            .load_recovery_for_lance(position.saturating_sub(1))
            .await?;
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        self.apply_recovery_for_lance(&mut state, &entries)?;
        Ok(entries.into_iter().find(|entry| entry.position == position))
    }

    /// Discovers the next position for Lance while retaining a certless,
    /// validated hot tail in memory.  A caller that needs authenticated
    /// recovery must use the public strict methods above instead.
    async fn next_position_for_lance(&self, after_lsn: u64) -> WalResult<u64> {
        {
            let state = self.state.lock().await;
            self.ensure_not_poisoned(&state)?;
            if let Some(position) = state.next_position {
                return Ok(position);
            }
        }
        let entries = self.load_recovery_for_lance(after_lsn).await?;
        let mut state = self.state.lock().await;
        self.ensure_not_poisoned(&state)?;
        if let Some(position) = state.next_position {
            return Ok(position);
        }
        if entries.is_empty() {
            let next_position = after_lsn.saturating_add(1).max(1);
            state.recovered_tail_position = state
                .recovered_tail_position
                .max(next_position.saturating_sub(1));
            state.next_position = Some(next_position);
        } else {
            self.apply_recovery_for_lance(&mut state, &entries)?;
        }
        Ok(state.next_position.unwrap_or(1))
    }

    /// Fetches a Bitr tail and validates every record before state mutation.
    async fn load_recovery(&self, after_lsn: u64) -> WalResult<Vec<RecoveredEntry>> {
        let records = self
            .writer
            .recover(self.stream.as_ref(), after_lsn)
            .await
            .map_err(map_replica_error)?;
        self.decode_recovery_records(after_lsn, records)
    }

    /// Fetches a tail bounded by an authenticated commit certificate.
    async fn load_recovery_with_watermark(
        &self,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> WalResult<Vec<RecoveredEntry>> {
        let records = self
            .writer
            .recover_with_watermark(self.stream.as_ref(), after_lsn, committed_lsn, certificate)
            .await
            .map_err(map_replica_error)?;
        self.decode_recovery_records(after_lsn, records)
    }

    /// Decodes and validates a contiguous decrypted Bitr tail.
    fn decode_recovery_records(
        &self,
        after_lsn: u64,
        records: Vec<AppendRecord>,
    ) -> WalResult<Vec<RecoveredEntry>> {
        self.decode_recovery_records_with_epoch_bound(after_lsn, records, Some(self.writer_epoch))
    }

    /// Decodes a Bitr tail, optionally enforcing the fixed epoch used by the
    /// adapter's direct API. Lance recovery deliberately leaves the bound
    /// open: its shard manifest validates the epoch it claimed during reopen.
    fn decode_recovery_records_with_epoch_bound(
        &self,
        after_lsn: u64,
        records: Vec<AppendRecord>,
        maximum_writer_epoch: Option<u64>,
    ) -> WalResult<Vec<RecoveredEntry>> {
        let mut expected = after_lsn
            .checked_add(1)
            .ok_or(WalBackendError::InvalidPosition(after_lsn))?;
        let mut entries = Vec::with_capacity(records.len());
        for record in records {
            if record.stream() != self.stream.as_ref() {
                return Err(WalBackendError::RecoveryStreamMismatch {
                    expected: self.stream.to_string(),
                    received: record.stream().to_owned(),
                });
            }
            if record.lsn() != expected {
                return Err(WalBackendError::RecoveryGap {
                    expected,
                    received: record.lsn(),
                });
            }
            if record.committed_lsn() != expected.saturating_sub(1) {
                return Err(WalBackendError::RecoveryGap {
                    expected: expected.saturating_sub(1),
                    received: record.committed_lsn(),
                });
            }
            if maximum_writer_epoch.is_some_and(|epoch| record.writer_epoch() > epoch) {
                return Err(WalBackendError::Fenced);
            }
            let payload = Bytes::copy_from_slice(record.payload());
            let decoded = decode_ipc_entry(payload.as_ref())?;
            if decoded.writer_epoch != record.writer_epoch() {
                return Err(WalBackendError::CorruptIpc(
                    "IPC writer epoch differs from Bitr record epoch".to_owned(),
                ));
            }
            if !decoded.fence_sentinel {
                validate_owner(&decoded.batches, self.owner_do_id.as_ref())?;
            }
            entries.push(RecoveredEntry {
                position: record.lsn(),
                writer_epoch: decoded.writer_epoch,
                fence_sentinel: decoded.fence_sentinel,
                batches: decoded.batches,
                payload,
            });
            expected = expected
                .checked_add(1)
                .ok_or(WalBackendError::InvalidPosition(record.lsn()))?;
        }
        Ok(entries)
    }

    /// Applies a fully validated recovery result atomically to local cursors.
    fn apply_recovery(
        &self,
        state: &mut BackendState,
        entries: &[RecoveredEntry],
        persisted_knowledge: Option<&CommitKnowledge>,
    ) -> WalResult<()> {
        let recovered_knowledge = if let Some(knowledge) = persisted_knowledge {
            Some(knowledge.clone())
        } else if entries.is_empty() {
            None
        } else if state.knowledge.committed_lsn
            >= entries.last().map_or(0, RecoveredEntry::position)
        {
            Some(state.knowledge.clone())
        } else {
            return Err(WalBackendError::CommitCertificateMissing);
        };
        for entry in entries {
            let knowledge = recovered_knowledge
                .as_ref()
                .ok_or(WalBackendError::CommitCertificateMissing)?
                .clone();
            if entry.position > state.checkpointed_position {
                state.committed.insert(
                    entry.position,
                    CommittedEntry {
                        payload: entry.payload.clone(),
                        knowledge: knowledge.clone(),
                    },
                );
            }
            state.knowledge = knowledge;
            state.recovered_tail_position = state.recovered_tail_position.max(entry.position);
            if state
                .pending
                .as_ref()
                .is_some_and(|pending| pending.position == entry.position)
            {
                if state
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.payload == entry.payload)
                {
                    state.pending = None;
                } else {
                    return Err(WalBackendError::LsnConflict {
                        position: entry.position,
                        reason: "recovery bytes differ from pending retry".to_owned(),
                    });
                }
            }
        }
        Self::advance_next_position(state, entries);
        Ok(())
    }

    /// Moves the cached next position past a recovered tail, never backwards.
    ///
    /// A gateway recovery is a snapshot that can lag this backend's own
    /// acknowledged appends: a reopen commits its fence sentinel and, moments
    /// later, replays the tail from a position below its cache, and the
    /// recovery it fetches for that read may not report the sentinel yet.
    /// The entries this backend committed itself are the stronger evidence,
    /// so the cursor only ever advances. Regressing it once made every
    /// reopened writer refuse its own first append (`expected next position
    /// N` for the sentinel at `N`, with Lance correctly at `N + 1`), poison
    /// itself, and reopen again one position later.
    fn advance_next_position(state: &mut BackendState, entries: &[RecoveredEntry]) {
        let recovered = entries.last().map(|entry| entry.position.saturating_add(1));
        let committed = state
            .committed
            .keys()
            .next_back()
            .map(|position| position.saturating_add(1));
        state.next_position = Some(
            [state.next_position, recovered, committed]
                .into_iter()
                .flatten()
                .max()
                .unwrap_or(1),
        );
    }

    /// Returns a typed error if a prior terminal failure was latched.
    fn ensure_not_poisoned(&self, state: &BackendState) -> WalResult<()> {
        if let Some(reason) = &state.poisoned {
            return Err(WalBackendError::Poisoned(reason.clone()));
        }
        Ok(())
    }

    /// Latches errors that make this stream unsafe to advance.
    fn poison_if_structural(&self, state: &mut BackendState, error: &WalBackendError) {
        if matches!(
            error,
            WalBackendError::Fenced
                | WalBackendError::Poisoned(_)
                | WalBackendError::LsnConflict { .. }
                | WalBackendError::RecoveryGap { .. }
                | WalBackendError::RecoveryStreamMismatch { .. }
                | WalBackendError::CorruptIpc(_)
                | WalBackendError::OwnerMissing
                | WalBackendError::OwnerMismatch { .. }
                | WalBackendError::InvalidCommit(_)
        ) && state.poisoned.is_none()
        {
            state.poisoned = Some(error.to_string());
        }
    }

    /// Rebuilds a public receipt from a Lance backend result.
    async fn receipt_from_result(&self, result: WalWriteResult) -> WalResult<WalReceipt> {
        match result {
            WalWriteResult::Committed(commit) => {
                let knowledge = if commit.knowledge.is_empty() {
                    self.commit_knowledge().await?
                } else {
                    decode_commit_knowledge(commit.knowledge.as_ref())?
                };
                Ok(WalReceipt {
                    position: commit.entry_position,
                    knowledge,
                })
            }
            WalWriteResult::AlreadyExists => Err(WalBackendError::LsnConflict {
                position: 0,
                reason: "backend reported an unexpected occupied position".to_owned(),
            }),
        }
    }
}

#[async_trait]
impl WalBackend for BitrWalBackend {
    async fn checkpointed(&self, shard_id: Uuid, covered_position: u64) {
        if shard_id != self.shard_id {
            return;
        }
        let mut state = self.state.lock().await;
        state.checkpointed_position = state.checkpointed_position.max(covered_position);
        let covered = state.checkpointed_position;
        // Keep append/retry cursors and the latest commit certificate intact.
        // A concurrently appended suffix remains available for replay.
        state.committed.retain(|position, _| *position > covered);
    }

    /// Persists one Lance IPC entry through the Bitr quorum writer.
    async fn append(&self, write: WalWrite) -> lance::Result<WalWriteResult> {
        self.append_at_position(write, false)
            .await
            .map_err(to_lance_error)
    }

    /// Reads one exact IPC entry from the Bitr stream.
    async fn read(&self, shard_id: Uuid, entry_position: u64) -> lance::Result<Option<Bytes>> {
        if shard_id != self.shard_id {
            return Err(to_lance_error(WalBackendError::LsnConflict {
                position: entry_position,
                reason: "read targets a different shard".to_owned(),
            }));
        }
        self.read_entry_for_lance(entry_position)
            .await
            .map(|entry| entry.map(|entry| entry.payload))
            .map_err(to_lance_error)
    }

    /// Returns the next one-based position discovered from the strict Bitr tail.
    async fn next_position(&self, shard_id: Uuid, hint: Option<u64>) -> lance::Result<u64> {
        if shard_id != self.shard_id {
            return Err(to_lance_error(WalBackendError::LsnConflict {
                position: 0,
                reason: "position probe targets a different shard".to_owned(),
            }));
        }
        // Lance's manifest cursor is only a lower bound. Bitr may have a
        // longer committed tail (including records already compacted to S3),
        // so discover that tail before Lance writes its predecessor fence.
        self.next_position_for_lance(hint.unwrap_or(0))
            .await
            .map_err(to_lance_error)
    }

    /// Persists Lance's predecessor-fence sentinel through Bitr.
    async fn fence(&self, write: WalWrite) -> lance::Result<WalWriteResult> {
        self.append_at_position(write, true)
            .await
            .map_err(to_lance_error)
    }

    /// Returns the first retained position in this Bitr stream.
    async fn first_position(&self, shard_id: Uuid) -> lance::Result<u64> {
        if shard_id != self.shard_id {
            return Err(to_lance_error(WalBackendError::LsnConflict {
                position: 0,
                reason: "first-position probe targets a different shard".to_owned(),
            }));
        }
        self.first_position_inner().await.map_err(to_lance_error)
    }
}

/// Validates a stream/owner key before it enters the replica binary protocol.
fn validate_stream(value: String) -> WalResult<String> {
    if value.trim().is_empty() || value.len() > 4 * 1024 {
        return Err(WalBackendError::InvalidIdentity {
            field: "stream".to_owned(),
            reason: "stream must be non-empty and at most 4096 bytes".to_owned(),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(WalBackendError::InvalidIdentity {
            field: "stream".to_owned(),
            reason: "stream must not contain control characters".to_owned(),
        });
    }
    Ok(value)
}

/// Maps Bitr failures to the adapter's typed error taxonomy.
fn map_replica_error(error: ReplicaError) -> WalBackendError {
    if matches!(error, ReplicaError::WriterFenced) {
        WalBackendError::Fenced
    } else if matches!(error, ReplicaError::LsnConflict) {
        WalBackendError::LsnConflict {
            position: 0,
            reason: "Bitr replica rejected the append as out of order".to_owned(),
        }
    } else if let ReplicaError::RecoveryGap {
        expected_lsn,
        received_lsn,
    } = error
    {
        WalBackendError::RecoveryGap {
            expected: expected_lsn,
            received: received_lsn,
        }
    } else if let ReplicaError::RecoveryStreamMismatch {
        expected_stream,
        received_stream,
    } = error
    {
        WalBackendError::RecoveryStreamMismatch {
            expected: expected_stream,
            received: received_stream,
        }
    } else {
        WalBackendError::Replica(error)
    }
}

/// Serializes opaque commit knowledge carried through Lance's backend seam.
fn encode_commit_knowledge(knowledge: &CommitKnowledge) -> WalResult<Bytes> {
    serde_json::to_vec(knowledge)
        .map(Bytes::from)
        .map_err(|error| WalBackendError::InvalidCommit(error.to_string()))
}

/// Deserializes opaque commit knowledge from a Lance durable receipt.
fn decode_commit_knowledge(bytes: &[u8]) -> WalResult<CommitKnowledge> {
    serde_json::from_slice(bytes).map_err(|error| WalBackendError::InvalidCommit(error.to_string()))
}

/// Converts adapter failures to Lance errors while preserving typed fencing.
fn to_lance_error(error: WalBackendError) -> lance::Error {
    match error {
        WalBackendError::Fenced => lance::Error::fenced_by_peer("Bitr replica fenced this writer"),
        WalBackendError::Poisoned(reason) => lance::Error::writer_poisoned(reason),
        other => lance::Error::io(other.to_string()),
    }
}
