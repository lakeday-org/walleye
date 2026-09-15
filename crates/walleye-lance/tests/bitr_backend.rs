//! Contract tests for the Bitr-gateway Lance MemWAL adapter.
//!
//! The gateway in this suite is `MemoryReplica`, an in-process implementation
//! of the gateway trait. These tests cover encoding, fencing, recovery, and
//! adapter state transitions; they do not exercise the cloud gateway or real
//! replica processes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use lance::dataset::mem_wal::{WalBackend, WalWrite, WalWriteResult};
use tokio::sync::Mutex;
use uuid::Uuid;
use walleye_bitr::{
    AppendRecord, CommitKnowledge, EncryptedRecord, MemoryReplica, QuorumWriter, ReplicaError,
    ReplicaGateway,
};
use walleye_lance::{
    BitrWalBackend, NamespaceConfig, WalBackendError, decode_ipc_entry, encode_fence_sentinel,
    with_owner,
};

fn batch(values: &[i32], owner: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let values: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
    let batch = RecordBatch::try_new(schema, vec![values]).expect("test batch has a valid schema");
    with_owner(batch, owner).expect("test owner metadata is valid")
}

fn backend(gateway: Arc<dyn ReplicaGateway>, stream: &str, epoch: u64) -> BitrWalBackend {
    BitrWalBackend::new(
        Arc::new(QuorumWriter::new(gateway, [7_u8; 32])),
        stream,
        Uuid::new_v4(),
        epoch,
    )
    .expect("test backend identity is valid")
}

#[test]
fn namespace_shards_are_deterministic_and_do_isolated() {
    let namespace = NamespaceConfig::new("tenant-a", "s3://lake/tenant-a")
        .expect("test namespace must be valid");
    let do_id = namespace
        .do_identity("worker", "binding", "object-1")
        .expect("test Durable Object identity must be valid");
    let same = namespace
        .shard_for_do(&do_id)
        .expect("identity shard derives");
    assert_eq!(
        same,
        namespace.shard_for_do(&do_id).expect("same shard derives")
    );
    let other_identity = namespace
        .do_identity("worker", "binding", "object-2")
        .expect("other identity validates");
    assert_ne!(
        same,
        namespace
            .shard_for_do(&other_identity)
            .expect("other shard derives")
    );
    let other_namespace =
        NamespaceConfig::new("tenant-b", "s3://lake/tenant-b").expect("other namespace validates");
    let other_do = other_namespace
        .do_identity("worker", "binding", "object-1")
        .expect("other DO identity validates");
    assert_ne!(
        same,
        other_namespace
            .shard_for_do(&other_do)
            .expect("tenant shard derives")
    );
    assert_eq!(namespace.dataset_uri(), "s3://lake/tenant-a");
    assert_eq!(
        namespace.stream_for_do(&do_id).expect("stream derives"),
        "tenant-a/do:7:default6:worker7:bindingobject-1"
    );
}

#[test]
fn durable_namespace_identity_is_stable_across_worker_aliases() {
    let namespace = NamespaceConfig::new("tenant-a", "s3://lake/tenant-a")
        .expect("test namespace must be valid");
    let identity = namespace
        .do_identity_for_namespace("do-stream", "events")
        .expect("physical Durable Object identity must be valid");
    assert_eq!(identity.shard_key(), "9:do-streamevents");
    assert_eq!(
        namespace.stream_for_do(&identity).expect("stream derives"),
        "tenant-a/do:7:default9:do-streamevents"
    );
}

#[tokio::test]
async fn two_batches_are_one_arrow_ipc_record_and_one_lsn() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let backend = backend(gateway.clone(), "tenant-a/ns/table/do-1", 1);

    let receipt = backend
        .append_batches(vec![
            batch(&[1, 2], "tenant-a/ns/table/do-1"),
            batch(&[3], "tenant-a/ns/table/do-1"),
        ])
        .await
        .expect("durable append succeeds");
    assert_eq!(receipt.position(), 1);
    assert_eq!(receipt.knowledge().committed_lsn, 1);

    let entries = backend.recover(0).await.expect("recovery succeeds");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].position(), 1);
    assert_eq!(entries[0].batches().len(), 2);
    assert_eq!(entries[0].batches()[0].num_rows(), 2);
    assert_eq!(entries[0].batches()[1].num_rows(), 1);
}

#[tokio::test]
async fn failed_append_does_not_advance_position_or_commit_knowledge() {
    let gateway = Arc::new(MemoryReplica::unavailable());
    let backend = backend(gateway, "tenant-a/ns/table/do-1", 1);

    let error = backend
        .append_batches(vec![batch(&[1], "tenant-a/ns/table/do-1")])
        .await
        .expect_err("unavailable replica must reject the append");
    assert!(matches!(error, WalBackendError::Replica(_)));
    assert_eq!(
        backend.next_position().await.expect("position is readable"),
        1
    );
    assert_eq!(
        backend
            .commit_knowledge()
            .await
            .expect("initial commit knowledge is available")
            .committed_lsn,
        0
    );
    assert!(!backend.is_poisoned().await);
}

#[tokio::test]
async fn retry_after_unknown_ack_reuses_exact_lsn_and_payload() {
    let inner = Arc::new(MemoryReplica::healthy());
    let gateway = Arc::new(DropFirstAckGateway::new(inner));
    let backend = backend(gateway.clone(), "tenant-a/ns/table/do-1", 1);

    let first = backend
        .append_batches(vec![batch(&[9], "tenant-a/ns/table/do-1")])
        .await;
    assert!(matches!(first, Err(WalBackendError::Replica(_))));
    let second = backend
        .append_batches(vec![batch(&[9], "tenant-a/ns/table/do-1")])
        .await
        .expect("retry reuses the unknown acknowledgement");
    assert_eq!(second.position(), 1);
    assert_eq!(
        backend.next_position().await.expect("position is readable"),
        2
    );

    let writes = gateway.writes.lock().await;
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0], writes[1]);
}

#[tokio::test]
async fn recovery_rejects_gap_and_corrupt_ipc() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let corrupt_backend = backend(gateway.clone(), "tenant-a/ns/table/do-1", 1);
    let writer = QuorumWriter::new(gateway, [7_u8; 32]);
    writer
        .append(AppendRecord::new(
            "tenant-a/ns/table/do-1",
            1,
            1,
            0,
            b"not-arrow",
        ))
        .await
        .expect("fixture can seed corrupt durable bytes");
    let error = corrupt_backend
        .recover(0)
        .await
        .expect_err("corrupt IPC must reject recovery");
    assert!(matches!(error, WalBackendError::CorruptIpc(_)));

    let gap_gateway = Arc::new(MemoryReplica::healthy());
    let gap_writer = QuorumWriter::new(gap_gateway.clone(), [7_u8; 32]);
    gap_writer
        .append(AppendRecord::new("tenant-a/ns/table/do-1", 1, 1, 0, b"one"))
        .await
        .expect("fixture first record persists");
    gap_writer
        .append(AppendRecord::new(
            "tenant-a/ns/table/do-1",
            1,
            3,
            2,
            b"three",
        ))
        .await
        .expect("fixture can seed a sparse tail");
    let gap_backend = backend(gap_gateway, "tenant-a/ns/table/do-1", 1);
    let error = gap_backend
        .recover(0)
        .await
        .expect_err("recovery must reject a missing LSN");
    assert!(matches!(error, WalBackendError::RecoveryGap { .. }));
}

#[tokio::test]
async fn stale_epoch_is_fenced_and_poisoned() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let first = backend(gateway.clone(), "tenant-a/ns/table/do-1", 1);
    first
        .append_batches(vec![batch(&[1], "tenant-a/ns/table/do-1")])
        .await
        .expect("first writer append succeeds");

    let successor = backend(gateway.clone(), "tenant-a/ns/table/do-1", 2);
    successor
        .recover_with_watermark(
            0,
            &CommitKnowledge::replica_with_certificate(
                "tenant-a/ns/table/do-1",
                1,
                1,
                Some("test-commit-certificate".to_owned()),
            ),
        )
        .await
        .expect("successor can recover the certified predecessor tail");
    successor
        .append_batches(vec![batch(&[2], "tenant-a/ns/table/do-1")])
        .await
        .expect("successor append succeeds");

    let error = first
        .append_batches(vec![batch(&[3], "tenant-a/ns/table/do-1")])
        .await
        .expect_err("stale writer must be fenced");
    assert!(matches!(error, WalBackendError::Fenced));
    assert!(first.is_poisoned().await);
    assert!(matches!(
        first
            .append_batches(vec![batch(&[4], "tenant-a/ns/table/do-1")])
            .await,
        Err(WalBackendError::Poisoned(_))
    ));
}

#[tokio::test]
async fn cross_shard_owner_is_rejected_before_lsn_assignment() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let backend = backend(gateway, "tenant-a/ns/table/do-1", 1);

    let error = backend
        .append_batches(vec![batch(&[1], "tenant-a/ns/table/do-2")])
        .await
        .expect_err("a batch owned by another DO must be rejected");
    assert!(matches!(error, WalBackendError::OwnerMismatch { .. }));
    assert_eq!(
        backend.next_position().await.expect("position is readable"),
        1
    );
}

#[tokio::test]
async fn ipc_entry_preserves_epoch_and_sentinel_marker() {
    let payload = encode_fence_sentinel(9).expect("sentinel encoding succeeds");
    let entry = decode_ipc_entry(payload.as_ref()).expect("sentinel decoding succeeds");
    assert_eq!(entry.writer_epoch(), 9);
    assert!(entry.is_fence_sentinel());
    assert!(entry.batches().is_empty());
}

#[tokio::test]
async fn lance_manifest_hint_is_a_lower_bound_for_compacted_recovery() {
    let gateway = Arc::new(MemoryReplica::healthy());
    let writer = QuorumWriter::new(gateway.clone(), [7_u8; 32]);
    let stream = "tenant-a/ns/table/do-1";
    let payload = encode_fence_sentinel(42).expect("sentinel encoding succeeds");
    for lsn in 1..=11 {
        writer
            .append(AppendRecord::new(
                stream,
                42,
                lsn,
                lsn.saturating_sub(1),
                payload.as_ref(),
            ))
            .await
            .expect("fixture tail persists");
    }

    let backend = backend(gateway, stream, 59);
    let next = WalBackend::next_position(&backend, backend.shard_id(), Some(1))
        .await
        .expect("Lance discovers the durable tail beyond its stale hint");
    assert_eq!(next, 12);
}

/// An old recovery snapshot can hide a committed record without changing writes.
struct StaleProbeGateway {
    inner: Arc<MemoryReplica>,
    visible: AtomicBool,
}

#[async_trait]
impl ReplicaGateway for StaleProbeGateway {
    async fn append(&self, record: EncryptedRecord) -> Result<Option<String>, ReplicaError> {
        self.inner.append(record).await
    }

    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        if self.visible.load(Ordering::Acquire) {
            self.inner.recover(stream, after_lsn).await
        } else {
            Ok(vec![])
        }
    }

    async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.inner
            .recover_with_watermark(stream, after_lsn, committed_lsn, certificate)
            .await
    }
}

/// Seeds an occupied position after the new backend has cached an empty tail.
async fn occupied_probe(epoch: u64) -> (BitrWalBackend, Arc<StaleProbeGateway>) {
    let gateway = Arc::new(StaleProbeGateway {
        inner: Arc::new(MemoryReplica::healthy()),
        visible: AtomicBool::new(false),
    });
    let backend = backend(gateway.clone(), "fence-race", 2);
    assert_eq!(
        WalBackend::next_position(&backend, backend.shard_id(), None)
            .await
            .expect("empty probe"),
        1
    );
    let writer = QuorumWriter::new(gateway.inner.clone(), [7_u8; 32]);
    let payload = walleye_lance::encode_ipc_batches(epoch, &[batch(&[17], "fence-race")])
        .expect("data payload");
    writer
        .append(AppendRecord::new(
            "fence-race",
            epoch,
            1,
            0,
            payload.as_ref(),
        ))
        .await
        .expect("competing write");
    (backend, gateway)
}

/// Builds the new writer's fence without changing the cached append position.
fn fence_write(backend: &BitrWalBackend, position: u64) -> WalWrite {
    WalWrite {
        shard_id: backend.shard_id(),
        writer_epoch: 2,
        entry_position: position,
        payload: encode_fence_sentinel(2).expect("sentinel"),
    }
}

#[tokio::test]
async fn fence_conflict_refreshes_only_after_recovering_an_older_writer() {
    let (backend, gateway) = occupied_probe(1).await;
    let before = gateway
        .inner
        .recover("fence-race", 0)
        .await
        .expect("durable prefix");
    gateway.visible.store(true, Ordering::Release);
    assert!(matches!(
        WalBackend::fence(&backend, fence_write(&backend, 1))
            .await
            .expect("occupied position is recoverable"),
        WalWriteResult::AlreadyExists
    ));
    let next = WalBackend::next_position(&backend, backend.shard_id(), None)
        .await
        .expect("fresh tail");
    assert_eq!(next, 2);
    assert!(matches!(
        WalBackend::fence(&backend, fence_write(&backend, next))
            .await
            .expect("new fence commits"),
        WalWriteResult::Committed(_)
    ));
    let after = gateway
        .inner
        .recover("fence-race", 0)
        .await
        .expect("durable tail");
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], before[0], "the winning ciphertext is immutable");
}

#[tokio::test]
async fn fence_conflict_without_recovery_evidence_keeps_its_position() {
    let (backend, gateway) = occupied_probe(1).await;
    assert!(
        WalBackend::fence(&backend, fence_write(&backend, 1))
            .await
            .is_err()
    );
    assert_eq!(
        WalBackend::next_position(&backend, backend.shard_id(), None)
            .await
            .expect("unchanged position"),
        1
    );
    // Once the occupied record becomes recoverable, the identical fence retry
    // can prove the race. It never guesses past the gateway's rejected slot.
    gateway.visible.store(true, Ordering::Release);
    assert!(matches!(
        WalBackend::fence(&backend, fence_write(&backend, 1))
            .await
            .expect("evidence is now available"),
        WalWriteResult::AlreadyExists
    ));
}

#[tokio::test]
async fn fence_conflict_does_not_skip_a_same_epoch_write_or_a_successor() {
    for epoch in [2, 3] {
        let (backend, gateway) = occupied_probe(epoch).await;
        gateway.visible.store(true, Ordering::Release);
        assert!(
            WalBackend::fence(&backend, fence_write(&backend, 1))
                .await
                .is_err()
        );
        assert_eq!(
            gateway
                .inner
                .recover("fence-race", 0)
                .await
                .expect("unchanged log")
                .len(),
            1
        );
        if epoch == 3 {
            assert!(
                backend.is_poisoned().await,
                "a newer writer fences this backend"
            );
        }
    }
}

#[tokio::test]
async fn ordinary_append_conflict_never_uses_fence_position_recovery() {
    let (backend, gateway) = occupied_probe(1).await;
    gateway.visible.store(true, Ordering::Release);
    let payload =
        walleye_lance::encode_ipc_batches(2, &[batch(&[18], "fence-race")]).expect("data payload");
    let write = || WalWrite {
        shard_id: backend.shard_id(),
        writer_epoch: 2,
        entry_position: 1,
        payload: payload.clone(),
    };
    assert!(WalBackend::append(&backend, write()).await.is_err());
    assert_eq!(
        WalBackend::next_position(&backend, backend.shard_id(), None)
            .await
            .expect("unchanged position"),
        1
    );
    assert!(WalBackend::append(&backend, write()).await.is_err());
    assert_eq!(
        gateway
            .inner
            .recover("fence-race", 0)
            .await
            .expect("unchanged log")
            .len(),
        1
    );
}

struct DropFirstAckGateway {
    inner: Arc<MemoryReplica>,
    dropped: Mutex<bool>,
    writes: Mutex<Vec<AppendRecord>>,
}

impl DropFirstAckGateway {
    fn new(inner: Arc<MemoryReplica>) -> Self {
        Self {
            inner,
            dropped: Mutex::new(false),
            writes: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ReplicaGateway for DropFirstAckGateway {
    async fn append(&self, record: EncryptedRecord) -> Result<Option<String>, ReplicaError> {
        let plaintext = record.clone();
        self.writes.lock().await.push(AppendRecord::new(
            plaintext.stream().to_owned(),
            plaintext.writer_epoch(),
            plaintext.lsn(),
            plaintext.committed_lsn(),
            plaintext.ciphertext(),
        ));
        let result = self.inner.append(record).await?;
        let mut dropped = self.dropped.lock().await;
        if !*dropped {
            *dropped = true;
            return Err(ReplicaError::GatewayUnavailable);
        }
        Ok(result)
    }

    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.inner.recover(stream, after_lsn).await
    }

    async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.inner
            .recover_with_watermark(stream, after_lsn, committed_lsn, certificate)
            .await
    }
}
