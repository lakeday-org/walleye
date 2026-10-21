//! Durable Object client for the cloud-owned quorum gateway.
//!
//! The client authenticates and encrypts each record once, sends it to one
//! load-balanced gateway, and treats a successful response as durable only
//! under the gateway's server-side quorum contract. The gateway owns replica
//! fan-out and committed-tail recovery; this crate does not encode a replica
//! count or placement.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

/// Header carrying the gateway-issued certificate for an acknowledged append.
pub const COMMIT_CERTIFICATE_HEADER: &str = "x-lakeday-replica-commit-certificate";
/// Media type used for the bounded binary encrypted-record transport.
pub const ENCRYPTED_RECORD_CONTENT_TYPE: &str = "application/octet-stream";
/// Maximum UTF-8 stream-name length accepted by the binary record transport.
pub const MAX_ENCRYPTED_RECORD_STREAM_BYTES: usize = 4 * 1024;
/// Maximum opaque ciphertext length accepted by the binary record transport.
pub const MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES: usize = 8 * 1024 * 1024;
/// XChaCha20-Poly1305 appends this many authentication bytes to plaintext.
pub const XCHACHA20POLY1305_TAG_BYTES: usize = 16;
/// Maximum plaintext that can be encrypted without exceeding the bounded
/// ciphertext transport limit.
pub const MAX_ENCRYPTED_RECORD_PLAINTEXT_BYTES: usize =
    MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES - XCHACHA20POLY1305_TAG_BYTES;
/// Maximum number of records in one framed binary append batch.
pub const MAX_ENCRYPTED_RECORD_BATCH_RECORDS: usize = 1024;
/// Maximum framed body size for one append batch.
pub const MAX_ENCRYPTED_RECORD_BATCH_BYTES: usize = 32 * 1024 * 1024;
/// Maximum encoded encrypted-record body accepted by the binary transport.
pub const MAX_ENCRYPTED_RECORD_BYTES: usize = ENCRYPTED_RECORD_BINARY_FIXED_BYTES
    + MAX_ENCRYPTED_RECORD_STREAM_BYTES
    + MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES;
/// JSON code returned when the selected replica cohort cannot accept an
/// encrypted append of the requested size.
pub const CAPACITY_EXCEEDED_CODE: &str = "replica_capacity_exceeded";

/// Plain commit material supplied by a single Durable Object writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRecord {
    stream: String,
    writer_epoch: u64,
    lsn: u64,
    /// Highest contiguous LSN committed before this record was proposed.
    committed_lsn: u64,
    payload: Vec<u8>,
}

impl AppendRecord {
    /// Creates one stream record with an LSN and its preceding committed watermark.
    #[must_use]
    pub fn new(
        stream: impl Into<String>,
        writer_epoch: u64,
        lsn: u64,
        committed_lsn: u64,
        payload: &[u8],
    ) -> Self {
        Self {
            stream: stream.into(),
            writer_epoch,
            lsn,
            committed_lsn,
            payload: payload.to_vec(),
        }
    }

    /// Returns the stream identity carried by the record.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the writer epoch that fences earlier writers.
    #[must_use]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns the stream-local LSN proposed by this writer.
    #[must_use]
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Returns the committed watermark observed before this append.
    #[must_use]
    pub fn committed_lsn(&self) -> u64 {
        self.committed_lsn
    }

    /// Returns the decrypted durability payload.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Opaque authenticated record persisted by the cloud quorum service.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EncryptedRecord {
    stream: String,
    writer_epoch: u64,
    lsn: u64,
    /// cLSN observed before this nLSN was proposed.
    committed_lsn: u64,
    nonce: [u8; 24],
    ciphertext: Vec<u8>,
    authentication: [u8; 32],
}

impl EncryptedRecord {
    /// Returns the opaque stream identity used for gateway fencing.
    #[must_use]
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Returns the record's stream-local nLSN.
    #[must_use]
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Returns the fenced writer epoch attached to the record.
    #[must_use]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns the cLSN observed before this record was proposed.
    #[must_use]
    pub fn committed_lsn(&self) -> u64 {
        self.committed_lsn
    }

    /// Returns the opaque ciphertext stored by the gateway.
    #[must_use]
    pub fn ciphertext(&self) -> &[u8] {
        &self.ciphertext
    }

    /// Encodes this record using the bounded binary HTTP representation.
    ///
    /// The wire layout is little-endian and contains a `u32` stream length,
    /// the UTF-8 stream bytes, the three `u64` sequence fields, the 24-byte
    /// nonce, a `u32` ciphertext length, the ciphertext, and the 32-byte
    /// authentication tag. Lengths are bounded before allocation so malformed
    /// or oversized requests cannot cause an unbounded transport buffer.
    pub fn encode_binary(&self) -> Result<Vec<u8>, ReplicaError> {
        let encoded_len = self.binary_encoded_len()?;
        let stream = self.stream.as_bytes();
        let stream_len = u32::try_from(stream.len()).map_err(|_| {
            ReplicaError::Protocol("encrypted record stream length overflows wire type".to_owned())
        })?;
        let ciphertext_len = u32::try_from(self.ciphertext.len()).map_err(|_| {
            ReplicaError::Protocol(
                "encrypted record ciphertext length overflows wire type".to_owned(),
            )
        })?;
        let mut encoded = Vec::with_capacity(encoded_len);
        encoded.extend_from_slice(&stream_len.to_le_bytes());
        encoded.extend_from_slice(stream);
        encoded.extend_from_slice(&self.writer_epoch.to_le_bytes());
        encoded.extend_from_slice(&self.lsn.to_le_bytes());
        encoded.extend_from_slice(&self.committed_lsn.to_le_bytes());
        encoded.extend_from_slice(&self.nonce);
        encoded.extend_from_slice(&ciphertext_len.to_le_bytes());
        encoded.extend_from_slice(&self.ciphertext);
        encoded.extend_from_slice(&self.authentication);
        Ok(encoded)
    }

    fn binary_encoded_len(&self) -> Result<usize, ReplicaError> {
        let stream = self.stream.as_bytes();
        if stream.is_empty() || stream.len() > MAX_ENCRYPTED_RECORD_STREAM_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record stream length is out of bounds".to_owned(),
            ));
        }
        if self.ciphertext.len() > MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record ciphertext length is out of bounds".to_owned(),
            ));
        }

        let encoded_len = ENCRYPTED_RECORD_BINARY_FIXED_BYTES
            .checked_add(stream.len())
            .and_then(|length| length.checked_add(self.ciphertext.len()))
            .ok_or_else(|| ReplicaError::Protocol("encrypted record length overflow".to_owned()))?;
        if encoded_len > MAX_ENCRYPTED_RECORD_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record body is too large".to_owned(),
            ));
        }
        Ok(encoded_len)
    }

    /// Decodes one bounded binary encrypted-record body.
    pub fn decode_binary(mut bytes: &[u8]) -> Result<Self, ReplicaError> {
        if bytes.len() > MAX_ENCRYPTED_RECORD_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record body is too large".to_owned(),
            ));
        }

        let stream_len = usize::try_from(read_u32(&mut bytes)?).map_err(|_| {
            ReplicaError::Protocol("encrypted record stream length is invalid".to_owned())
        })?;
        if stream_len == 0 || stream_len > MAX_ENCRYPTED_RECORD_STREAM_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record stream length is out of bounds".to_owned(),
            ));
        }
        let stream =
            String::from_utf8(take_bytes(&mut bytes, stream_len)?.to_vec()).map_err(|_| {
                ReplicaError::Protocol("encrypted record stream is not valid UTF-8".to_owned())
            })?;
        let writer_epoch = read_u64(&mut bytes)?;
        let lsn = read_u64(&mut bytes)?;
        let committed_lsn = read_u64(&mut bytes)?;
        let nonce = read_fixed::<24>(&mut bytes)?;
        let ciphertext_len = usize::try_from(read_u32(&mut bytes)?).map_err(|_| {
            ReplicaError::Protocol("encrypted record ciphertext length is invalid".to_owned())
        })?;
        if ciphertext_len > MAX_ENCRYPTED_RECORD_CIPHERTEXT_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record ciphertext length is out of bounds".to_owned(),
            ));
        }

        let expected_tail = ciphertext_len
            .checked_add(ENCRYPTED_RECORD_AUTHENTICATION_BYTES)
            .ok_or_else(|| ReplicaError::Protocol("encrypted record length overflow".to_owned()))?;
        if bytes.len() != expected_tail {
            return Err(ReplicaError::Protocol(
                "encrypted record body is truncated or has trailing bytes".to_owned(),
            ));
        }
        let ciphertext = take_bytes(&mut bytes, ciphertext_len)?.to_vec();
        let authentication = read_fixed::<32>(&mut bytes)?;
        debug_assert!(bytes.is_empty());

        Ok(Self {
            stream,
            writer_epoch,
            lsn,
            committed_lsn,
            nonce,
            ciphertext,
            authentication,
        })
    }

    /// Encodes a non-empty sequence of records using bounded length-delimited
    /// frames. Each frame contains exactly one [`Self::encode_binary`] body,
    /// allowing a receiver to reject a malformed record without guessing at
    /// field offsets or allocating from an attacker-controlled count.
    pub fn encode_binary_batch(records: &[Self]) -> Result<Vec<u8>, ReplicaError> {
        if records.is_empty() {
            return Err(ReplicaError::Protocol(
                "encrypted record batch must not be empty".to_owned(),
            ));
        }
        if records.len() > MAX_ENCRYPTED_RECORD_BATCH_RECORDS {
            return Err(ReplicaError::Protocol(
                "encrypted record batch has too many records".to_owned(),
            ));
        }

        let mut encoded_len = ENCRYPTED_RECORD_BATCH_HEADER_BYTES;
        for record in records {
            let record_len = record.binary_encoded_len()?;
            encoded_len = encoded_len
                .checked_add(ENCRYPTED_RECORD_BATCH_FRAME_BYTES)
                .and_then(|length| length.checked_add(record_len))
                .ok_or_else(|| {
                    ReplicaError::Protocol("encrypted record batch length overflow".to_owned())
                })?;
        }
        if encoded_len > MAX_ENCRYPTED_RECORD_BATCH_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record batch body is too large".to_owned(),
            ));
        }

        let count = u32::try_from(records.len()).map_err(|_| {
            ReplicaError::Protocol("encrypted record batch count overflows wire type".to_owned())
        })?;
        let mut encoded = Vec::with_capacity(encoded_len);
        encoded.extend_from_slice(&count.to_le_bytes());
        for record in records {
            let record = record.encode_binary()?;
            let frame_len = u32::try_from(record.len()).map_err(|_| {
                ReplicaError::Protocol(
                    "encrypted record frame length overflows wire type".to_owned(),
                )
            })?;
            encoded.extend_from_slice(&frame_len.to_le_bytes());
            encoded.extend_from_slice(&record);
        }
        Ok(encoded)
    }

    /// Decodes a bounded sequence of length-delimited binary record frames.
    pub fn decode_binary_batch(mut bytes: &[u8]) -> Result<Vec<Self>, ReplicaError> {
        if bytes.len() > MAX_ENCRYPTED_RECORD_BATCH_BYTES {
            return Err(ReplicaError::Protocol(
                "encrypted record batch body is too large".to_owned(),
            ));
        }
        let count = usize::try_from(read_u32(&mut bytes)?).map_err(|_| {
            ReplicaError::Protocol("encrypted record batch count is invalid".to_owned())
        })?;
        if count == 0 || count > MAX_ENCRYPTED_RECORD_BATCH_RECORDS {
            return Err(ReplicaError::Protocol(
                "encrypted record batch count is out of bounds".to_owned(),
            ));
        }

        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let frame_len = usize::try_from(read_u32(&mut bytes)?).map_err(|_| {
                ReplicaError::Protocol("encrypted record frame length is invalid".to_owned())
            })?;
            if frame_len == 0 || frame_len > MAX_ENCRYPTED_RECORD_BYTES {
                return Err(ReplicaError::Protocol(
                    "encrypted record frame length is out of bounds".to_owned(),
                ));
            }
            let frame = take_bytes(&mut bytes, frame_len)?;
            records.push(Self::decode_binary(frame)?);
        }
        if !bytes.is_empty() {
            return Err(ReplicaError::Protocol(
                "encrypted record batch has trailing bytes".to_owned(),
            ));
        }
        Ok(records)
    }
}

const ENCRYPTED_RECORD_AUTHENTICATION_BYTES: usize = 32;
/// Fixed bytes in one binary record, excluding stream and ciphertext data.
pub const ENCRYPTED_RECORD_BINARY_FIXED_BYTES: usize =
    4 + 8 + 8 + 8 + 24 + 4 + ENCRYPTED_RECORD_AUTHENTICATION_BYTES;
const ENCRYPTED_RECORD_BATCH_HEADER_BYTES: usize = 4;
const ENCRYPTED_RECORD_BATCH_FRAME_BYTES: usize = 4;

fn take_bytes<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8], ReplicaError> {
    if bytes.len() < length {
        return Err(ReplicaError::Protocol(
            "encrypted record body is truncated".to_owned(),
        ));
    }
    let (head, tail) = bytes.split_at(length);
    *bytes = tail;
    Ok(head)
}

fn read_u32(bytes: &mut &[u8]) -> Result<u32, ReplicaError> {
    let raw = take_bytes(bytes, 4)?;
    let raw: [u8; 4] = raw
        .try_into()
        .map_err(|_| ReplicaError::Protocol("encrypted record body is truncated".to_owned()))?;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(bytes: &mut &[u8]) -> Result<u64, ReplicaError> {
    let raw = take_bytes(bytes, 8)?;
    let raw: [u8; 8] = raw
        .try_into()
        .map_err(|_| ReplicaError::Protocol("encrypted record body is truncated".to_owned()))?;
    Ok(u64::from_le_bytes(raw))
}

fn read_fixed<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], ReplicaError> {
    let raw = take_bytes(bytes, N)?;
    raw.try_into()
        .map_err(|_| ReplicaError::Protocol("encrypted record body is truncated".to_owned()))
}

/// Causal watermark delivered to pipelines before their effects are released.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitKnowledge {
    /// Durable Object stream that produced the mutation.
    pub stream: String,
    /// Epoch that fences every earlier writer.
    pub writer_epoch: u64,
    /// Highest LSN known to have reached the selected durability boundary.
    pub committed_lsn: u64,
    /// Gateway-issued proof for the acknowledged committed LSN, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,
    /// Boundary that made the commit durable.
    pub authority: DurabilityAuthority,
}

impl CommitKnowledge {
    /// Builds knowledge for a cloud quorum-gateway commit.
    #[must_use]
    pub fn replica(stream: impl Into<String>, writer_epoch: u64, committed_lsn: u64) -> Self {
        Self::replica_with_certificate(stream, writer_epoch, committed_lsn, None)
    }

    /// Builds replica knowledge with the certificate returned by the gateway.
    #[must_use]
    pub fn replica_with_certificate(
        stream: impl Into<String>,
        writer_epoch: u64,
        committed_lsn: u64,
        certificate: Option<String>,
    ) -> Self {
        Self {
            stream: stream.into(),
            writer_epoch,
            committed_lsn,
            certificate,
            authority: DurabilityAuthority::Replica,
        }
    }

    /// Builds knowledge for an isolated filesystem test store.
    #[must_use]
    pub fn local(stream: impl Into<String>, committed_lsn: u64) -> Self {
        Self {
            stream: stream.into(),
            writer_epoch: 0,
            committed_lsn,
            certificate: None,
            authority: DurabilityAuthority::Local,
        }
    }

    /// Builds knowledge for a WAL entry committed atomically to object storage.
    #[must_use]
    pub fn object_store(stream: impl Into<String>, committed_lsn: u64) -> Self {
        Self {
            stream: stream.into(),
            writer_epoch: 0,
            committed_lsn,
            certificate: None,
            authority: DurabilityAuthority::ObjectStore,
        }
    }
}

/// Authority selected before an event is admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DurabilityAuthority {
    /// The cloud gateway's configured synchronous quorum.
    Replica,
    /// An atomic conditional write in the configured object store.
    ObjectStore,
    /// Filesystem authority used only by isolated crate tests.
    Local,
}

/// Replica durability failures that are safe to expose at event admission.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ReplicaError {
    /// A quorum gateway reported that it could not durably commit the record.
    #[error("replica quorum unavailable")]
    QuorumUnavailable,
    /// The gateway endpoint could not be reached or did not establish quorum.
    #[error("replica quorum gateway unavailable")]
    GatewayUnavailable,
    /// The gateway rejected the caller's tenant credential.
    #[error("replica quorum gateway rejected credentials")]
    GatewayUnauthorized,
    /// The gateway returned an HTTP status outside the replica contract.
    #[error("replica quorum gateway returned HTTP {status}")]
    GatewayRejected { status: u16 },
    /// The node or gateway has already observed a newer writer epoch.
    #[error("writer epoch is fenced")]
    WriterFenced,
    /// Encryption or authentication failed before a node accepted the record.
    #[error("record encryption failed")]
    Encryption,
    /// A replica node is unavailable.
    #[error("replica node unavailable")]
    NodeUnavailable,
    /// An LSN was reused for different encrypted contents.
    #[error("LSN conflicts with an existing record")]
    LsnConflict,
    /// The proposed nLSN does not follow the supplied preceding cLSN.
    #[error("record LSN {lsn} is not after committed LSN {committed_lsn}")]
    InvalidWatermark { lsn: u64, committed_lsn: u64 },
    /// A node could not recover or durably append its local log.
    #[error("replica storage failed: {0}")]
    NodeStorage(String),
    /// The selected immutable placement cohort cannot accept this record's
    /// encoded payload under its configured append-size limit.
    #[error(
        "replica cohort {cohort_id} append is too large: {requested_bytes} bytes exceeds {max_append_bytes}"
    )]
    CapacityExceeded {
        cohort_id: u64,
        requested_bytes: u64,
        max_append_bytes: u64,
    },
    /// The gateway returned malformed or otherwise unusable protocol data.
    #[error("replica gateway protocol error: {0}")]
    Protocol(String),
    /// The gateway returned a tail with a gap in the committed LSN sequence.
    /// A recovery asked for records a durable checkpoint already released.
    #[error(
        "replica log was released through LSN {released_lsn}; recovery after {after_lsn} was asked"
    )]
    Released { after_lsn: u64, released_lsn: u64 },
    #[error("replica recovery expected LSN {expected_lsn}, received {received_lsn}")]
    RecoveryGap {
        expected_lsn: u64,
        received_lsn: u64,
    },
    /// The gateway returned a record for a different stream.
    #[error("replica recovery returned stream `{received_stream}` for `{expected_stream}`")]
    RecoveryStreamMismatch {
        expected_stream: String,
        received_stream: String,
    },
    /// A one-copy recovery request was made without a gateway-issued proof.
    #[error("replica commit certificate is required for watermark recovery")]
    CommitCertificateMissing,
    /// The gateway returned fewer contiguous records than the authenticated
    /// committed watermark requires.
    #[error(
        "replica recovery incomplete before committed LSN {committed_lsn}; expected LSN {expected_lsn}"
    )]
    RecoveryIncomplete {
        expected_lsn: u64,
        committed_lsn: u64,
    },
    /// Ordinary recovery observed a matching data quorum without the
    /// corresponding durable commit-marker quorum. The record may be a
    /// pre-acknowledgement partial append and must not be silently omitted,
    /// since doing so could fork the stream at a lower LSN.
    #[error(
        "replica recovery found ambiguous LSN {lsn}: {committed_nodes} durable commit markers, quorum is {quorum}"
    )]
    RecoveryAmbiguous {
        lsn: u64,
        committed_nodes: usize,
        quorum: usize,
    },
}

/// Validates the ordering fence shared by every append-many implementation.
///
/// A batch is one stream and writer epoch with adjacent nLSNs. Each record's
/// cLSN points to the preceding record, while the first cLSN may reference an
/// already durable prefix. Implementations perform their own idempotency and
/// placement checks after this common shape validation.
pub fn validate_append_batch(records: &[EncryptedRecord]) -> Result<(), ReplicaError> {
    let first = records.first().ok_or_else(|| {
        ReplicaError::Protocol("encrypted record batch must not be empty".to_owned())
    })?;
    if first.committed_lsn >= first.lsn {
        return Err(ReplicaError::InvalidWatermark {
            lsn: first.lsn,
            committed_lsn: first.committed_lsn,
        });
    }
    for pair in records.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if current.stream != first.stream {
            return Err(ReplicaError::Protocol(
                "encrypted record batch contains multiple streams".to_owned(),
            ));
        }
        if current.writer_epoch != first.writer_epoch {
            return Err(ReplicaError::Protocol(
                "encrypted record batch contains multiple writer epochs".to_owned(),
            ));
        }
        let expected_lsn = previous
            .lsn
            .checked_add(1)
            .ok_or(ReplicaError::LsnConflict)?;
        if current.lsn != expected_lsn {
            return Err(ReplicaError::LsnConflict);
        }
        if current.committed_lsn != previous.lsn {
            return Err(ReplicaError::InvalidWatermark {
                lsn: current.lsn,
                committed_lsn: current.committed_lsn,
            });
        }
        if current.committed_lsn >= current.lsn {
            return Err(ReplicaError::InvalidWatermark {
                lsn: current.lsn,
                committed_lsn: current.committed_lsn,
            });
        }
    }
    Ok(())
}

/// Durable append surface implemented by each independent cloud replica node.
///
/// This low-level seam is used by the cloud gateway implementation. Tenant
/// runtimes use [`ReplicaGateway`] and never select or fan out nodes.
#[async_trait]
pub trait Replica: Send + Sync {
    /// Persists and fsyncs one opaque record before returning success.
    async fn append(&self, record: EncryptedRecord) -> Result<(), ReplicaError>;

    /// Persists and fsyncs a bounded, ordered batch. Legacy implementations
    /// retain correctness through the one-record fallback; durable nodes
    /// override this to group their writes and one sync operation.
    async fn append_many(&self, records: Vec<EncryptedRecord>) -> Result<(), ReplicaError> {
        validate_append_batch(&records)?;
        for record in records {
            self.append(record).await?;
        }
        Ok(())
    }

    /// Returns a snapshot used by the cloud gateway's repair and test paths.
    async fn records(&self, stream: &str) -> Vec<EncryptedRecord>;
}

/// How much of a stream's log exists: everything through `released_lsn` was
/// covered by a durable checkpoint and let go, and `committed_lsn` is the
/// committed tail. A stream never written has both at zero. The records a
/// recovery can still return are those after `released_lsn`.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamExtent {
    pub released_lsn: u64,
    pub committed_lsn: u64,
}

impl StreamExtent {
    /// Whether the log has ever held a committed record of the stream.
    #[must_use]
    pub fn ever_written(&self) -> bool {
        self.committed_lsn > 0
    }

    /// The first position a recovery can return.
    #[must_use]
    pub fn first_retained(&self) -> u64 {
        self.released_lsn.saturating_add(1)
    }
}

/// One load-balanced cloud endpoint that owns quorum fan-out and recovery.
#[async_trait]
pub trait ReplicaGateway: Send + Sync {
    /// Sends one opaque record and returns the gateway certificate after quorum fsync.
    async fn append(&self, record: EncryptedRecord) -> Result<Option<String>, ReplicaError>;

    /// Sends a bounded ordered batch and returns the certificate for its last
    /// LSN. Legacy gateways retain correctness through a one-record fallback.
    async fn append_many(
        &self,
        records: Vec<EncryptedRecord>,
    ) -> Result<Option<String>, ReplicaError> {
        validate_append_batch(&records)?;
        let mut certificate = None;
        for record in records {
            certificate = self.append(record).await?;
        }
        Ok(certificate)
    }

    /// Returns the committed, contiguous opaque tail after the supplied cLSN.
    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError>;

    /// Returns exactly the contiguous tail through an authenticated committed LSN.
    /// Implementations must fail rather than return a shorter prefix.
    async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError>;

    /// Where the stream's log begins and ends, without reading its records.
    async fn extent(&self, stream: &str) -> Result<StreamExtent, ReplicaError>;

    /// Lets go of the stream's log through `through_lsn`: its writer has
    /// published a durable checkpoint covering it, so no recovery will ask
    /// for it again. Never moves backwards, and never past what is committed.
    async fn release(&self, stream: &str, through_lsn: u64) -> Result<(), ReplicaError>;
}

/// Shared handle for a single cloud quorum gateway.
pub type ReplicaGatewayHandle = Arc<dyn ReplicaGateway>;

/// Client-side encryptor and cloud-gateway coordinator.
pub struct QuorumWriter {
    gateway: ReplicaGatewayHandle,
    key: [u8; 32],
}

impl QuorumWriter {
    /// Creates a writer with a tenant-derived encryption and authentication key.
    #[must_use]
    pub fn new(gateway: ReplicaGatewayHandle, key: [u8; 32]) -> Self {
        Self { gateway, key }
    }

    /// Returns the configured single gateway handle for health and test inspection.
    #[must_use]
    pub fn gateway(&self) -> &ReplicaGatewayHandle {
        &self.gateway
    }

    /// Encrypts once, sends once, and acknowledges the gateway's quorum response.
    pub async fn append(&self, record: AppendRecord) -> Result<CommitKnowledge, ReplicaError> {
        if record.committed_lsn >= record.lsn {
            return Err(ReplicaError::InvalidWatermark {
                lsn: record.lsn,
                committed_lsn: record.committed_lsn,
            });
        }
        let encrypted = self.encrypt(&record)?;
        let certificate = self.gateway.append(encrypted).await?;
        Ok(CommitKnowledge::replica_with_certificate(
            record.stream,
            record.writer_epoch,
            record.lsn,
            certificate,
        ))
    }

    /// Encrypts an ordered batch once and sends it through one gateway request.
    /// The gateway's acknowledgement is the durability boundary for the final
    /// LSN in the batch.
    pub async fn append_many(
        &self,
        records: Vec<AppendRecord>,
    ) -> Result<CommitKnowledge, ReplicaError> {
        if records.is_empty() {
            return Err(ReplicaError::Protocol(
                "append batch must not be empty".to_owned(),
            ));
        }
        if records.len() > MAX_ENCRYPTED_RECORD_BATCH_RECORDS {
            return Err(ReplicaError::Protocol(
                "append batch has too many records".to_owned(),
            ));
        }
        let first = records.first().expect("non-empty append batch");
        if first.committed_lsn >= first.lsn {
            return Err(ReplicaError::InvalidWatermark {
                lsn: first.lsn,
                committed_lsn: first.committed_lsn,
            });
        }
        if first.stream.is_empty() || first.stream.len() > MAX_ENCRYPTED_RECORD_STREAM_BYTES {
            return Err(ReplicaError::Protocol(
                "append batch stream length is out of bounds".to_owned(),
            ));
        }
        let mut encoded_len = 4_usize;
        for record in &records {
            if record.payload.len() > MAX_ENCRYPTED_RECORD_PLAINTEXT_BYTES {
                return Err(ReplicaError::Protocol(
                    "append payload exceeds encrypted record plaintext limit".to_owned(),
                ));
            }
            encoded_len = encoded_len
                .checked_add(4)
                .and_then(|length| {
                    length
                        .checked_add(ENCRYPTED_RECORD_BINARY_FIXED_BYTES)
                        .and_then(|length| length.checked_add(first.stream.len()))
                        .and_then(|length| length.checked_add(XCHACHA20POLY1305_TAG_BYTES))
                        .and_then(|length| length.checked_add(record.payload.len()))
                })
                .ok_or_else(|| ReplicaError::Protocol("append batch length overflow".to_owned()))?;
        }
        if encoded_len > MAX_ENCRYPTED_RECORD_BATCH_BYTES {
            return Err(ReplicaError::Protocol(
                "append batch body is too large".to_owned(),
            ));
        }
        for pair in records.windows(2) {
            let previous = &pair[0];
            let current = &pair[1];
            if current.stream != previous.stream {
                return Err(ReplicaError::Protocol(
                    "append batch contains multiple streams".to_owned(),
                ));
            }
            if current.writer_epoch != previous.writer_epoch {
                return Err(ReplicaError::Protocol(
                    "append batch contains multiple writer epochs".to_owned(),
                ));
            }
            let expected_lsn = previous
                .lsn
                .checked_add(1)
                .ok_or(ReplicaError::LsnConflict)?;
            if current.lsn != expected_lsn {
                return Err(ReplicaError::LsnConflict);
            }
            if current.committed_lsn != previous.lsn {
                return Err(ReplicaError::InvalidWatermark {
                    lsn: current.lsn,
                    committed_lsn: current.committed_lsn,
                });
            }
        }
        let encrypted = records
            .iter()
            .map(|record| self.encrypt(record))
            .collect::<Result<Vec<_>, _>>()?;
        let certificate = self.gateway.append_many(encrypted).await?;
        let last = records.last().expect("non-empty append batch");
        Ok(CommitKnowledge::replica_with_certificate(
            last.stream.clone(),
            last.writer_epoch,
            last.lsn,
            certificate,
        ))
    }

    /// Recovers the gateway's committed contiguous encrypted tail and decrypts it.
    pub async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<AppendRecord>, ReplicaError> {
        let records = self.gateway.recover(stream, after_lsn).await?;
        self.decrypt_recovery(stream, after_lsn, None, records)
    }

    /// Where the stream's log begins and ends, without reading its records.
    pub async fn extent(&self, stream: &str) -> Result<StreamExtent, ReplicaError> {
        self.gateway.extent(stream).await
    }

    /// Lets go of the stream's log through a durable checkpoint.
    pub async fn release(&self, stream: &str, through_lsn: u64) -> Result<(), ReplicaError> {
        self.gateway.release(stream, through_lsn).await
    }

    /// Recovers one gateway tail using an authenticated committed watermark.
    pub async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<AppendRecord>, ReplicaError> {
        if certificate.is_empty() {
            return Err(ReplicaError::CommitCertificateMissing);
        }
        let records = self
            .gateway
            .recover_with_watermark(stream, after_lsn, committed_lsn, certificate)
            .await?;
        self.decrypt_recovery(stream, after_lsn, Some(committed_lsn), records)
    }

    /// Validates, authenticates, and decrypts a gateway recovery response.
    fn decrypt_recovery(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: Option<u64>,
        mut records: Vec<EncryptedRecord>,
    ) -> Result<Vec<AppendRecord>, ReplicaError> {
        if let Some(committed_lsn) = committed_lsn
            && committed_lsn < after_lsn
        {
            return Err(ReplicaError::RecoveryIncomplete {
                expected_lsn: after_lsn.saturating_add(1),
                committed_lsn,
            });
        }
        records.sort_by_key(EncryptedRecord::lsn);
        let mut expected_lsn = after_lsn.checked_add(1);
        let mut recovered = Vec::with_capacity(records.len());
        for record in records {
            let Some(expected_lsn_value) = expected_lsn else {
                return Err(ReplicaError::RecoveryGap {
                    expected_lsn: u64::MAX,
                    received_lsn: record.lsn,
                });
            };
            if record.stream != stream {
                return Err(ReplicaError::RecoveryStreamMismatch {
                    expected_stream: stream.to_owned(),
                    received_stream: record.stream,
                });
            }
            if record.lsn != expected_lsn_value {
                return Err(ReplicaError::RecoveryGap {
                    expected_lsn: expected_lsn_value,
                    received_lsn: record.lsn,
                });
            }
            if record.committed_lsn != expected_lsn_value.saturating_sub(1) {
                return Err(ReplicaError::InvalidWatermark {
                    lsn: record.lsn,
                    committed_lsn: record.committed_lsn,
                });
            }
            expected_lsn = expected_lsn_value.checked_add(1);
            recovered.push(self.decrypt(record)?);
        }
        if let Some(committed_lsn) = committed_lsn {
            let complete = if committed_lsn == after_lsn {
                recovered.is_empty()
            } else {
                recovered
                    .last()
                    .is_some_and(|record| record.lsn() == committed_lsn)
            };
            if !complete {
                return Err(ReplicaError::RecoveryIncomplete {
                    expected_lsn: expected_lsn.unwrap_or(u64::MAX),
                    committed_lsn,
                });
            }
        }
        Ok(recovered)
    }

    /// Encrypts one plaintext record deterministically for safe gateway retries.
    fn encrypt(&self, record: &AppendRecord) -> Result<EncryptedRecord, ReplicaError> {
        if record.payload.len() > MAX_ENCRYPTED_RECORD_PLAINTEXT_BYTES {
            return Err(ReplicaError::Protocol(
                "append payload exceeds encrypted record plaintext limit".to_owned(),
            ));
        }
        let mut nonce_hasher = Sha256::new();
        nonce_hasher.update(record.stream.as_bytes());
        nonce_hasher.update(record.writer_epoch.to_be_bytes());
        nonce_hasher.update(record.lsn.to_be_bytes());
        nonce_hasher.update(record.committed_lsn.to_be_bytes());
        nonce_hasher.update(Sha256::digest(&record.payload));
        let nonce_hash = nonce_hasher.finalize();
        let mut nonce = [0_u8; 24];
        nonce.copy_from_slice(&nonce_hash[..24]);
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key));
        let ciphertext = cipher
            .encrypt(XNonce::from_slice(&nonce), record.payload.as_slice())
            .map_err(|_| ReplicaError::Encryption)?;
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&self.key).map_err(|_| ReplicaError::Encryption)?;
        mac.update(record.stream.as_bytes());
        mac.update(&record.writer_epoch.to_be_bytes());
        mac.update(&record.lsn.to_be_bytes());
        mac.update(&record.committed_lsn.to_be_bytes());
        mac.update(&ciphertext);
        let authentication = mac.finalize().into_bytes().into();
        Ok(EncryptedRecord {
            stream: record.stream.clone(),
            writer_epoch: record.writer_epoch,
            lsn: record.lsn,
            committed_lsn: record.committed_lsn,
            nonce,
            ciphertext,
            authentication,
        })
    }

    /// Authenticates and decrypts one gateway-agreed opaque record.
    fn decrypt(&self, record: EncryptedRecord) -> Result<AppendRecord, ReplicaError> {
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&self.key).map_err(|_| ReplicaError::Encryption)?;
        mac.update(record.stream.as_bytes());
        mac.update(&record.writer_epoch.to_be_bytes());
        mac.update(&record.lsn.to_be_bytes());
        mac.update(&record.committed_lsn.to_be_bytes());
        mac.update(&record.ciphertext);
        mac.verify_slice(&record.authentication)
            .map_err(|_| ReplicaError::Encryption)?;
        let cipher = XChaCha20Poly1305::new(Key::from_slice(&self.key));
        let payload = cipher
            .decrypt(
                XNonce::from_slice(&record.nonce),
                record.ciphertext.as_slice(),
            )
            .map_err(|_| ReplicaError::Encryption)?;
        Ok(AppendRecord {
            stream: record.stream,
            writer_epoch: record.writer_epoch,
            lsn: record.lsn,
            committed_lsn: record.committed_lsn,
            payload,
        })
    }
}

#[derive(Default)]
struct MemoryState {
    highest_epoch: BTreeMap<String, u64>,
    records: BTreeMap<(String, u64), EncryptedRecord>,
    released: BTreeMap<String, u64>,
}

/// In-memory gateway fixture with node-like fencing and idempotency rules.
pub struct MemoryReplica {
    available: bool,
    state: Mutex<MemoryState>,
}

impl MemoryReplica {
    /// Creates an available in-memory gateway.
    #[must_use]
    pub fn healthy() -> Self {
        Self {
            available: true,
            state: Mutex::new(MemoryState::default()),
        }
    }

    /// Creates a gateway that refuses every operation.
    #[must_use]
    pub fn unavailable() -> Self {
        Self {
            available: false,
            state: Mutex::new(MemoryState::default()),
        }
    }

    /// Applies fencing and stores one record for the in-memory cloud fixture.
    async fn append_record(&self, record: EncryptedRecord) -> Result<(), ReplicaError> {
        if !self.available {
            return Err(ReplicaError::NodeUnavailable);
        }
        if record.committed_lsn >= record.lsn {
            return Err(ReplicaError::InvalidWatermark {
                lsn: record.lsn,
                committed_lsn: record.committed_lsn,
            });
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        let highest_epoch = state
            .highest_epoch
            .get(&record.stream)
            .copied()
            .unwrap_or_default();
        if record.writer_epoch < highest_epoch {
            return Err(ReplicaError::WriterFenced);
        }
        let key = (record.stream.clone(), record.lsn);
        if let Some(existing) = state.records.get(&key) {
            return if existing == &record {
                Ok(())
            } else {
                Err(ReplicaError::LsnConflict)
            };
        }
        state.highest_epoch.insert(
            record.stream.clone(),
            record.writer_epoch.max(highest_epoch),
        );
        state.records.insert(key, record);
        Ok(())
    }

    /// Applies one validated batch while holding the state lock once. The
    /// in-memory fixture mirrors the node's all-or-nothing validation surface;
    /// durable nodes additionally group their log writes and fsync.
    async fn append_records(&self, records: Vec<EncryptedRecord>) -> Result<(), ReplicaError> {
        if !self.available {
            return Err(ReplicaError::NodeUnavailable);
        }
        validate_append_batch(&records)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        for record in &records {
            let highest_epoch = state
                .highest_epoch
                .get(record.stream())
                .copied()
                .unwrap_or_default();
            if record.writer_epoch() < highest_epoch {
                return Err(ReplicaError::WriterFenced);
            }
            let key = (record.stream().to_owned(), record.lsn());
            if let Some(existing) = state.records.get(&key)
                && existing != record
            {
                return Err(ReplicaError::LsnConflict);
            }
        }
        for record in records {
            let highest_epoch = state
                .highest_epoch
                .get(record.stream())
                .copied()
                .unwrap_or_default();
            let key = (record.stream().to_owned(), record.lsn());
            state.highest_epoch.insert(
                record.stream().to_owned(),
                record.writer_epoch().max(highest_epoch),
            );
            state.records.entry(key).or_insert(record);
        }
        Ok(())
    }

    /// Returns all records currently held by the in-memory cloud fixture.
    /// Refuses a recovery that starts inside the released prefix.
    fn ensure_retained(&self, stream: &str, after_lsn: u64) -> Result<(), ReplicaError> {
        let released_lsn = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeUnavailable)?
            .released
            .get(stream)
            .copied()
            .unwrap_or(0);
        if after_lsn < released_lsn {
            return Err(ReplicaError::Released {
                after_lsn,
                released_lsn,
            });
        }
        Ok(())
    }

    async fn records_snapshot(&self, stream: &str) -> Vec<EncryptedRecord> {
        self.state
            .lock()
            .map(|state| {
                state
                    .records
                    .values()
                    .filter(|record| record.stream == stream)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl Replica for MemoryReplica {
    /// Applies epoch fencing and stores one idempotent opaque record.
    async fn append(&self, record: EncryptedRecord) -> Result<(), ReplicaError> {
        self.append_record(record).await
    }

    /// Applies one ordered batch under a single in-memory state transition.
    async fn append_many(&self, records: Vec<EncryptedRecord>) -> Result<(), ReplicaError> {
        self.append_records(records).await
    }

    /// Returns all opaque records currently held by the node fixture.
    async fn records(&self, stream: &str) -> Vec<EncryptedRecord> {
        self.records_snapshot(stream).await
    }
}

#[async_trait]
impl ReplicaGateway for MemoryReplica {
    /// Acknowledges only after the in-memory gateway has accepted the record.
    async fn append(&self, record: EncryptedRecord) -> Result<Option<String>, ReplicaError> {
        self.append_record(record).await.map(|()| None)
    }

    /// Acknowledges one ordered batch after its atomic in-memory transition.
    async fn append_many(
        &self,
        records: Vec<EncryptedRecord>,
    ) -> Result<Option<String>, ReplicaError> {
        self.append_records(records).await.map(|()| None)
    }

    /// Returns the in-memory gateway's records after the requested watermark.
    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.ensure_retained(stream, after_lsn)?;
        Ok(self
            .records_snapshot(stream)
            .await
            .into_iter()
            .filter(|record| record.lsn > after_lsn)
            .collect())
    }

    async fn extent(&self, stream: &str) -> Result<StreamExtent, ReplicaError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        let released_lsn = state.released.get(stream).copied().unwrap_or(0);
        let committed_lsn = state
            .records
            .range((stream.to_owned(), 0)..=(stream.to_owned(), u64::MAX))
            .next_back()
            .map_or(released_lsn, |(_, record)| record.lsn.max(released_lsn));
        Ok(StreamExtent {
            released_lsn,
            committed_lsn,
        })
    }

    async fn release(&self, stream: &str, through_lsn: u64) -> Result<(), ReplicaError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        let committed = state
            .records
            .range((stream.to_owned(), 0)..=(stream.to_owned(), u64::MAX))
            .next_back()
            .map_or(0, |(_, record)| record.lsn);
        let released = state.released.entry(stream.to_owned()).or_default();
        *released = (*released).max(through_lsn.min(committed));
        let released = *released;
        state
            .records
            .retain(|(candidate, lsn), _| candidate != stream || *lsn > released);
        Ok(())
    }

    /// Returns the in-memory gateway's records within the authenticated bound.
    async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        if certificate.is_empty() {
            return Err(ReplicaError::CommitCertificateMissing);
        }
        self.ensure_retained(stream, after_lsn)?;
        Ok(self
            .records_snapshot(stream)
            .await
            .into_iter()
            .filter(|record| record.lsn > after_lsn && record.lsn <= committed_lsn)
            .collect())
    }
}

/// Network client for one load-balanced cloud quorum gateway.
pub struct HttpReplica {
    base_url: String,
    bearer: String,
    activation: Option<ReplicaActivation>,
    client: reqwest::Client,
}

struct ReplicaActivation {
    endpoint: String,
    bearer: String,
    last_success: tokio::sync::Mutex<Option<Instant>>,
}

const ACTIVATION_RENEW_AFTER: Duration = Duration::from_secs(1);

/// Time allowed to establish a TCP/TLS connection to the gateway or operator.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Time allowed for one complete request, including the response body. The
/// Lance host blocks its isolate thread on every gateway call, so a stalled
/// gateway must surface as `GatewayUnavailable` rather than hang the cell.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
struct CapacityExceededBody {
    code: String,
    requested_bytes: u64,
    max_append_bytes: u64,
    cohort_id: u64,
}

#[derive(Deserialize)]
struct AppendErrorBody {
    code: String,
    #[serde(default)]
    error: String,
}

impl HttpReplica {
    /// Creates a client for one gateway endpoint and its tenant-scoped
    /// credential, bounded by the default connect and request timeouts.
    #[must_use]
    pub fn new(base_url: impl Into<String>, bearer: impl Into<String>) -> Self {
        Self::with_timeouts(
            base_url,
            bearer,
            DEFAULT_CONNECT_TIMEOUT,
            DEFAULT_REQUEST_TIMEOUT,
        )
    }

    /// Creates a client whose every gateway or operator call must connect
    /// within `connect_timeout` and complete within `request_timeout`; a
    /// call that exceeds either fails with `GatewayUnavailable`.
    #[must_use]
    pub fn with_timeouts(
        base_url: impl Into<String>,
        bearer: impl Into<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .expect("the compiled TLS backend builds an HTTP client");
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            bearer: bearer.into(),
            activation: None,
            client,
        }
    }

    /// Adds the host-only operator admission performed before every direct
    /// gateway call. Worker code never receives this credential.
    #[must_use]
    pub fn with_activation(
        mut self,
        endpoint: impl Into<String>,
        bearer: impl Into<String>,
    ) -> Self {
        self.activation = Some(ReplicaActivation {
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            bearer: bearer.into(),
            last_success: tokio::sync::Mutex::new(None),
        });
        self
    }

    async fn activate(&self) -> Result<(), ReplicaError> {
        let Some(activation) = &self.activation else {
            return Ok(());
        };
        // One private lease covers concurrent and back-to-back data-plane
        // calls briefly. Holding the async gate through acquisition prevents
        // cold traffic from reaching Bitr before quorum has been restored.
        let mut last_success = activation.last_success.lock().await;
        if last_success.is_some_and(|instant| instant.elapsed() < ACTIVATION_RENEW_AFTER) {
            return Ok(());
        }
        let response = self
            .client
            .post(format!("{}/internal/v1/bitr/activate", activation.endpoint))
            .bearer_auth(&activation.bearer)
            .send()
            .await
            .map_err(|error| {
                tracing::warn!(event = "replica.activation.failed", error = %error);
                ReplicaError::GatewayUnavailable
            })?;
        if response.status().is_success() {
            *last_success = Some(Instant::now());
            Ok(())
        } else if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            Err(ReplicaError::GatewayUnauthorized)
        } else {
            tracing::warn!(
                event = "replica.activation.rejected",
                status = response.status().as_u16()
            );
            Err(ReplicaError::GatewayUnavailable)
        }
    }

    /// Maps a gateway status into the client error taxonomy.
    fn status_error(status: reqwest::StatusCode) -> ReplicaError {
        match status {
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                ReplicaError::GatewayUnauthorized
            }
            reqwest::StatusCode::CONFLICT => ReplicaError::WriterFenced,
            reqwest::StatusCode::SERVICE_UNAVAILABLE | reqwest::StatusCode::GATEWAY_TIMEOUT => {
                ReplicaError::GatewayUnavailable
            }
            status => ReplicaError::GatewayRejected {
                status: status.as_u16(),
            },
        }
    }

    /// Preserves known protocol diagnostics without exposing arbitrary gateway
    /// bodies or buffering an unbounded error response.
    async fn gateway_error(mut response: reqwest::Response) -> ReplicaError {
        let status = response.status();
        if matches!(
            status,
            reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::CONFLICT
        ) {
            let mut bytes = Vec::new();
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) if bytes.len() + chunk.len() <= 4096 => {
                        bytes.extend_from_slice(&chunk)
                    }
                    Ok(None) => break,
                    _ => return Self::status_error(status),
                }
            }
            if let Ok(body) = serde_json::from_slice::<AppendErrorBody>(&bytes) {
                match body.code.as_str() {
                    "invalid_watermark" | "recovery_failed" => {
                        return ReplicaError::Protocol(body.error.chars().take(1024).collect());
                    }
                    "lsn_conflict" if status == reqwest::StatusCode::CONFLICT => {
                        return ReplicaError::LsnConflict;
                    }
                    _ => {}
                }
            }
        }
        Self::status_error(status)
    }

    /// Fetches and decodes one committed record response from the gateway.
    async fn fetch_records(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: Option<u64>,
        certificate: Option<&str>,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.activate().await?;
        if certificate.is_some_and(str::is_empty) {
            return Err(ReplicaError::CommitCertificateMissing);
        }
        let mut query = vec![
            ("stream".to_owned(), stream.to_owned()),
            ("after_lsn".to_owned(), after_lsn.to_string()),
        ];
        if let Some(committed_lsn) = committed_lsn {
            query.push(("committed_lsn".to_owned(), committed_lsn.to_string()));
        }
        let mut request = self
            .client
            .get(format!("{}/v1/records", self.base_url))
            .bearer_auth(&self.bearer)
            .query(&query);
        if let Some(certificate) = certificate {
            request = request.header(COMMIT_CERTIFICATE_HEADER, certificate);
        }
        let response = request
            .send()
            .await
            .map_err(|_| ReplicaError::GatewayUnavailable)?;
        if !response.status().is_success() {
            return Err(Self::gateway_error(response).await);
        }
        response
            .json::<Vec<EncryptedRecord>>()
            .await
            .map_err(|error| ReplicaError::Protocol(format!("invalid recovery response: {error}")))
    }

    async fn append_response(response: reqwest::Response) -> Result<Option<String>, ReplicaError> {
        let status = response.status();
        if status == reqwest::StatusCode::NO_CONTENT {
            let certificate = response
                .headers()
                .get(COMMIT_CERTIFICATE_HEADER)
                .map(|value| {
                    value.to_str().map(str::to_owned).map_err(|_| {
                        ReplicaError::Protocol(
                            "commit certificate header is not valid UTF-8".to_owned(),
                        )
                    })
                })
                .transpose()?;
            if certificate.as_deref().is_some_and(str::is_empty) {
                return Err(ReplicaError::Protocol(
                    "commit certificate header is empty".to_owned(),
                ));
            }
            return Ok(certificate);
        }
        if status.is_server_error() {
            let detail = response.text().await.unwrap_or_default();
            tracing::warn!(event = "replica.append.rejected", status = status.as_u16(), detail = %detail.chars().take(1024).collect::<String>());
            return Err(Self::status_error(status));
        }
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            let body = response
                .bytes()
                .await
                .map_err(|_| Self::status_error(status))?;
            if let Ok(body) = serde_json::from_slice::<CapacityExceededBody>(&body)
                && body.code == CAPACITY_EXCEEDED_CODE
            {
                return Err(ReplicaError::CapacityExceeded {
                    cohort_id: body.cohort_id,
                    requested_bytes: body.requested_bytes,
                    max_append_bytes: body.max_append_bytes,
                });
            }
            return Err(Self::status_error(status));
        }
        Err(Self::gateway_error(response).await)
    }
}

#[async_trait]
impl ReplicaGateway for HttpReplica {
    /// Sends one opaque record and waits for the gateway's post-quorum response.
    async fn append(&self, record: EncryptedRecord) -> Result<Option<String>, ReplicaError> {
        self.activate().await?;
        let body = record.encode_binary()?;
        let response = self
            .client
            .post(format!("{}/v1/append", self.base_url))
            .bearer_auth(&self.bearer)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(body)
            .send()
            .await
            .map_err(|error| {
                tracing::warn!(event = "replica.append.failed", error = %error);
                ReplicaError::GatewayUnavailable
            })?;
        Self::append_response(response).await
    }

    /// Sends one bounded framed batch and waits for the gateway's quorum response.
    async fn append_many(
        &self,
        records: Vec<EncryptedRecord>,
    ) -> Result<Option<String>, ReplicaError> {
        self.activate().await?;
        validate_append_batch(&records)?;
        let body = EncryptedRecord::encode_binary_batch(&records)?;
        let response = self
            .client
            .post(format!("{}/v1/append-many", self.base_url))
            .bearer_auth(&self.bearer)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(body)
            .send()
            .await
            .map_err(|error| {
                tracing::warn!(event = "replica.append_many.failed", error = %error);
                ReplicaError::GatewayUnavailable
            })?;
        Self::append_response(response).await
    }

    /// Fetches committed opaque records from the gateway for recovery.
    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.fetch_records(stream, after_lsn, None, None).await
    }

    /// Fetches a committed opaque tail with the authenticated recovery bound.
    async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        certificate: &str,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.fetch_records(stream, after_lsn, Some(committed_lsn), Some(certificate))
            .await
    }

    async fn extent(&self, stream: &str) -> Result<StreamExtent, ReplicaError> {
        self.activate().await?;
        let response = self
            .client
            .get(format!("{}/v1/extent", self.base_url))
            .bearer_auth(&self.bearer)
            .query(&[("stream", stream)])
            .send()
            .await
            .map_err(|_| ReplicaError::GatewayUnavailable)?;
        if !response.status().is_success() {
            return Err(Self::gateway_error(response).await);
        }
        response
            .json::<StreamExtent>()
            .await
            .map_err(|error| ReplicaError::Protocol(format!("invalid extent response: {error}")))
    }

    async fn release(&self, stream: &str, through_lsn: u64) -> Result<(), ReplicaError> {
        self.activate().await?;
        let response = self
            .client
            .post(format!("{}/v1/release", self.base_url))
            .bearer_auth(&self.bearer)
            .json(&ReleaseRequest {
                stream: stream.to_owned(),
                through_lsn,
            })
            .send()
            .await
            .map_err(|_| ReplicaError::GatewayUnavailable)?;
        if !response.status().is_success() {
            return Err(Self::gateway_error(response).await);
        }
        Ok(())
    }
}

/// The body of a release request.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReleaseRequest {
    pub stream: String,
    pub through_lsn: u64,
}
