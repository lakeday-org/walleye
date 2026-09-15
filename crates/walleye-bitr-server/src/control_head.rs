//! A single conditional object-store head for replica control metadata.
//!
//! The archive's per-stream heads answer a different question: how far a
//! committed stream prefix has been published.  This module owns the one
//! mutable control head that describes membership, cohorts, and the complete
//! immutable routing manifest.  It intentionally has no retry loop.  An ETag
//! conflict is a serialization result which the caller must re-read and
//! reconcile with its operation id.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::placement::PlacementEpoch;
use crate::{DurableCohort, DurableMember, REPLICATION_FACTOR, ReplicaManifest, StreamSegment};

/// Version of the serialized control-head document.
pub const CONTROL_HEAD_VERSION: u8 = 1;

/// A deliberately finite bound for one metadata object.  The manifest is
/// complete and can grow with immutable stream ranges, so this is generous
/// enough for a large tenant while still preventing an accidental unbounded
/// object-store write from the control path.
pub const MAX_CONTROL_HEAD_BYTES: usize = 32 * 1024 * 1024;

const MAX_OPERATION_ID_BYTES: usize = 512;
const MAX_COMPLETED_OPERATIONS: usize = 100_000;
const MAX_SOURCE_AUTHORITY_BYTES: usize = 256;
const MAX_SOURCE_DIGEST_BYTES: usize = 256;
const MAX_PREFIX_BYTES: usize = 1024;
const MAX_PENDING_HANDOFFS: usize = 4096;
const MAX_STREAM_NAME_BYTES: usize = 1024;
const CONTROL_HEAD_FILE: &str = "control/head.json";

/// The old authority's proof that the metadata copied into a new head was a
/// particular source state.  The certificate is intentionally opaque here:
/// the old replica quorum creates it, while this store only requires callers
/// to carry and compare the exact marker during the authority handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlSourceMarker {
    /// Name of the authority that produced the source state, for example the
    /// legacy replica quorum.
    pub authority: String,
    /// Authority epoch observed at the source.
    pub authority_epoch: u64,
    /// Source metadata revision covered by the marker.
    pub revision: u64,
    /// Digest or certificate for the exact source metadata bytes.
    pub digest: String,
}

impl ControlSourceMarker {
    /// Creates a source marker after the caller has certified the source
    /// metadata.  Certification itself remains outside this object store
    /// primitive.
    pub fn new(
        authority: impl Into<String>,
        authority_epoch: u64,
        revision: u64,
        digest: impl Into<String>,
    ) -> Result<Self, ControlHeadError> {
        let marker = Self {
            authority: authority.into(),
            authority_epoch,
            revision,
            digest: digest.into(),
        };
        marker.validate()?;
        Ok(marker)
    }

    fn validate(&self) -> Result<(), ControlHeadError> {
        if self.authority.trim().is_empty() || self.authority.len() > MAX_SOURCE_AUTHORITY_BYTES {
            return Err(ControlHeadError::Invalid(
                "control source authority is empty or too long".to_owned(),
            ));
        }
        if self.authority_epoch == 0 || self.revision == 0 {
            return Err(ControlHeadError::Invalid(
                "control source authority epoch and revision must be nonzero".to_owned(),
            ));
        }
        if self.digest.trim().is_empty() || self.digest.len() > MAX_SOURCE_DIGEST_BYTES {
            return Err(ControlHeadError::Invalid(
                "control source digest is empty or too long".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Durable phase of a stream-local cohort handoff.
///
/// The descriptor is committed before the first placement fence.  Advancing
/// it is monotonic and never rewrites the source route.  A descriptor at
/// `RoutePublished` still owns the stream until the operation removes it in a
/// final head CAS, which makes a crash between route publication and cleanup
/// resumable without allowing another operation to claim the stream.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingHandoffStage {
    #[default]
    Prepared,
    SourceFenced,
    ArchiveVerified,
    RoutePublished,
}

impl PendingHandoffStage {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Prepared => 0,
            Self::SourceFenced => 1,
            Self::ArchiveVerified => 2,
            Self::RoutePublished => 3,
        }
    }

    #[must_use]
    pub const fn operation_suffix(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::SourceFenced => "source-fenced",
            Self::ArchiveVerified => "archive-verified",
            Self::RoutePublished => "route-published",
        }
    }
}

/// The exact successor identity prepared for a stream handoff.
///
/// The member values are copied into the descriptor rather than represented
/// only by ids.  This binds the claim to the prepared Machine/volume
/// incarnation and prevents an old operation from silently reusing a member
/// id after its backing volume was replaced.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PendingHandoffDescriptor {
    /// Stable logical idempotency key for the whole stream handoff.
    pub operation_id: String,
    /// The stream exclusively owned by this operation.  The enclosing map
    /// key must equal this value.
    pub stream: String,
    /// Immutable route observed before any placement fence.
    pub source: StreamSegment,
    /// Cohort selected as the successor writer.
    pub target_cohort_id: u64,
    /// Complete prepared member incarnations for the successor cohort.
    pub target_members: Vec<DurableMember>,
    /// Exact member-set digest that the successor route will carry.
    pub target_member_hash: String,
    #[serde(default)]
    pub target_tier: String,
    #[serde(default)]
    pub target_max_append_bytes: u64,
    /// Encrypted records retain the source writer epoch.  This explicit copy
    /// prevents a resume path from inventing an authenticated writer epoch.
    #[serde(default)]
    pub target_writer_epoch: u64,
    /// Host-side placement value installed while the source is drained.
    pub transition_placement: PlacementEpoch,
    /// Exact successor placement epoch selected before fencing.  Its route
    /// digest is not known until the archive boundary determines the exact
    /// successor start LSN.
    pub successor_placement_epoch: u64,
    /// Host-side placement value carried by the successor route once the
    /// archive boundary is known.
    #[serde(default)]
    pub successor_placement: Option<PlacementEpoch>,
    #[serde(default)]
    pub stage: PendingHandoffStage,
    /// Certified source archive watermark after the source fence.
    #[serde(default)]
    pub archived_lsn: Option<u64>,
    /// Exact successor route, committed only at `RoutePublished`.
    #[serde(default)]
    pub successor: Option<StreamSegment>,
}

impl PendingHandoffDescriptor {
    /// Validates the descriptor's self-contained identity and phase data.
    /// Binding it to the current head's membership and route is performed by
    /// the claim/advance CAS below, where the complete metadata is available.
    pub fn validate(&self, map_key: Option<&str>) -> Result<(), ControlHeadError> {
        if let Some(map_key) = map_key
            && map_key != self.stream
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff map key does not match stream".to_owned(),
            ));
        }
        if self.operation_id.trim().is_empty() || self.operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(ControlHeadError::Invalid(
                "pending handoff operation id is empty or too long".to_owned(),
            ));
        }
        if self.stream.trim().is_empty() || self.stream.len() > MAX_STREAM_NAME_BYTES {
            return Err(ControlHeadError::Invalid(
                "pending handoff stream is empty or too long".to_owned(),
            ));
        }
        if self.target_members.len() != REPLICATION_FACTOR
            || self.target_member_hash.trim().is_empty()
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff successor cohort is incomplete".to_owned(),
            ));
        }
        if self
            .target_members
            .windows(2)
            .any(|window| window[0].id >= window[1].id)
            || self.target_members.iter().any(|member| {
                member.id.trim().is_empty()
                    || member.url.trim().is_empty()
                    || member.cohort_id != self.target_cohort_id
            })
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff successor members are not deterministic".to_owned(),
            ));
        }
        if self.source.start_lsn == 0
            || self.source.end_lsn.is_some()
            || self.source.member_ids.len() != REPLICATION_FACTOR
            || self.source.member_hash.trim().is_empty()
            || self
                .source
                .member_ids
                .windows(2)
                .any(|window| window[0] >= window[1])
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff source route is not an open complete range".to_owned(),
            ));
        }
        if self.source.cohort_id == self.target_cohort_id
            || self.target_writer_epoch != self.source.writer_epoch
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff source and successor identities conflict".to_owned(),
            ));
        }
        validate_placement(&self.transition_placement, "transition")?;
        if self.successor_placement_epoch == 0 {
            return Err(ControlHeadError::Invalid(
                "pending handoff successor placement epoch is zero".to_owned(),
            ));
        }
        if self.transition_placement.epoch <= self.source.placement_epoch
            || self.successor_placement_epoch <= self.transition_placement.epoch
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff placement epochs are not strictly increasing".to_owned(),
            ));
        }
        if self.stage.rank() < PendingHandoffStage::ArchiveVerified.rank()
            && (self.archived_lsn.is_some() || self.successor_placement.is_some())
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff archive watermark is ahead of its stage".to_owned(),
            ));
        }
        if self.stage.rank() >= PendingHandoffStage::ArchiveVerified.rank()
            && (self.archived_lsn.is_none() || self.successor_placement.is_none())
        {
            return Err(ControlHeadError::Invalid(
                "pending handoff archive stage has no watermark".to_owned(),
            ));
        }
        if self.stage.rank() >= PendingHandoffStage::ArchiveVerified.rank() {
            let Some(successor) = &self.successor else {
                return Err(ControlHeadError::Invalid(
                    "archived handoff has no successor route".to_owned(),
                ));
            };
            let Some(archived_lsn) = self.archived_lsn else {
                return Err(ControlHeadError::Invalid(
                    "archived handoff has no archive watermark".to_owned(),
                ));
            };
            let Some(successor_placement) = &self.successor_placement else {
                return Err(ControlHeadError::Invalid(
                    "archived handoff has no successor placement".to_owned(),
                ));
            };
            if successor.cohort_id != self.target_cohort_id
                || successor.member_ids
                    != self
                        .target_members
                        .iter()
                        .map(|m| m.id.clone())
                        .collect::<Vec<_>>()
                || successor.member_hash != self.target_member_hash
                || successor.writer_epoch != self.target_writer_epoch
                || successor.placement_epoch != successor_placement.epoch
                || successor_placement.epoch != self.successor_placement_epoch
                || successor.start_lsn != archived_lsn.saturating_add(1).max(self.source.start_lsn)
                || successor.end_lsn.is_some()
            {
                return Err(ControlHeadError::Invalid(
                    "archived handoff successor route does not match its claim".to_owned(),
                ));
            }
        } else if self.successor.is_some() {
            return Err(ControlHeadError::Invalid(
                "unpublished handoff carries a successor route".to_owned(),
            ));
        }
        Ok(())
    }

    fn immutable_eq(&self, other: &Self) -> bool {
        self.operation_id == other.operation_id
            && self.stream == other.stream
            && self.source == other.source
            && self.target_cohort_id == other.target_cohort_id
            && self
                .target_members
                .iter()
                .zip(&other.target_members)
                .all(|(left, right)| same_member_incarnation(left, right))
            && self.target_member_hash == other.target_member_hash
            && self.target_tier == other.target_tier
            && self.target_max_append_bytes == other.target_max_append_bytes
            && self.target_writer_epoch == other.target_writer_epoch
            && self.transition_placement == other.transition_placement
            && self.successor_placement_epoch == other.successor_placement_epoch
            && (self.successor_placement.is_none()
                || other.successor_placement == self.successor_placement)
            && (self.successor.is_none() || other.successor == self.successor)
    }
}

fn same_member_incarnation(left: &DurableMember, right: &DurableMember) -> bool {
    left.id == right.id
        && left.url == right.url
        && left.cohort_id == right.cohort_id
        && left.name == right.name
        && left.machine_id == right.machine_id
        && left.volume_id == right.volume_id
        && left.ordinal == right.ordinal
        && left.tier == right.tier
        && left.max_append_bytes == right.max_append_bytes
}

fn validate_placement(placement: &PlacementEpoch, label: &str) -> Result<(), ControlHeadError> {
    if placement.epoch == 0 || placement.route_digest.trim().is_empty() {
        return Err(ControlHeadError::Invalid(format!(
            "pending handoff {label} placement is incomplete"
        )));
    }
    Ok(())
}

/// Computes the same member-set digest carried by immutable route segments.
/// The descriptor also compares every serialized member value, so this hash
/// is only the compact route binding and not the incarnation proof by itself.
fn member_hash(members: &[DurableMember]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/cohort-members/v1\0");
    for member in members {
        digest.update(member.id.as_bytes());
        digest.update([0]);
        digest.update(member.url.as_bytes());
        digest.update([0]);
    }
    hex::encode(digest.finalize())
}

/// Derives a bounded idempotency key for one durable stream-handoff phase.
/// Phase receipts live beside the caller's operation receipt in the same
/// control head, so a replay can adopt the already committed phase without
/// inventing a new placement epoch. The stream is part of the digest because
/// one replacement operation advances many streams in sequence; sharing one
/// phase receipt across those streams would make the second CAS look like a
/// conflicting replay of the first stream.
fn pending_operation_id(
    operation_id: &str,
    stream: &str,
    phase: &str,
) -> Result<String, ControlHeadError> {
    if operation_id.trim().is_empty() || stream.trim().is_empty() || phase.trim().is_empty() {
        return Err(ControlHeadError::Invalid(
            "pending handoff operation id, stream, and phase must not be empty".to_owned(),
        ));
    }
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/pending-handoff/v1\0");
    digest.update(operation_id.as_bytes());
    digest.update([0]);
    digest.update(stream.as_bytes());
    digest.update([0]);
    digest.update(phase.as_bytes());
    // The digest keeps the receipt key bounded even when a caller's
    // operation id or stream name is near its public input limit.
    Ok(format!(
        "handoff:{phase}:{}",
        hex::encode(digest.finalize())
    ))
}

/// The complete metadata payload protected by one control-head CAS.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlMetadata {
    /// Membership epoch from the legacy durable control document.
    pub membership_epoch: u64,
    /// All members, including joining, draining, and tombstoned members.
    #[serde(default)]
    pub members: BTreeMap<String, DurableMember>,
    /// Immutable cohort definitions.
    #[serde(default)]
    pub cohorts: BTreeMap<u64, DurableCohort>,
    /// The complete immutable stream-to-cohort manifest.
    pub manifest: ReplicaManifest,
    /// Stream-local handoff claims.  These live inside the object-head
    /// metadata digest so every normal head CAS carries them forward and a
    /// competing operation cannot fence a stream behind a stale claim.
    #[serde(default)]
    pub pending_handoffs: BTreeMap<String, PendingHandoffDescriptor>,
}

impl ControlMetadata {
    /// Computes a deterministic digest over the complete metadata payload.
    pub fn digest(&self) -> Result<String, ControlHeadError> {
        let encoded = serde_json::to_vec(self)?;
        Ok(hex::encode(Sha256::digest(encoded)))
    }

    fn validate(&self) -> Result<(), ControlHeadError> {
        if self.membership_epoch == 0 {
            return Err(ControlHeadError::Invalid(
                "control membership epoch must be nonzero".to_owned(),
            ));
        }
        self.manifest
            .validate()
            .map_err(|error| ControlHeadError::Invalid(error.to_string()))?;

        for (id, member) in &self.members {
            if id.trim().is_empty() || member.id != *id {
                return Err(ControlHeadError::Invalid(
                    "control member map key does not match member identity".to_owned(),
                ));
            }
        }
        let mut member_cohort_counts = BTreeMap::<String, usize>::new();
        for (id, cohort) in &self.cohorts {
            if *id != cohort.id || cohort.members.is_empty() {
                return Err(ControlHeadError::Invalid(
                    "control cohort map key does not match cohort identity".to_owned(),
                ));
            }
            if cohort
                .members
                .windows(2)
                .any(|window| window[0] >= window[1])
            {
                return Err(ControlHeadError::Invalid(
                    "control cohort members are not in deterministic order".to_owned(),
                ));
            }
            let mut members = cohort.members.clone();
            members.sort();
            members.dedup();
            if members.len() != cohort.members.len() {
                return Err(ControlHeadError::Invalid(
                    "control cohort contains duplicate member identities".to_owned(),
                ));
            }
            for member_id in &cohort.members {
                let Some(member) = self.members.get(member_id) else {
                    return Err(ControlHeadError::Invalid(
                        "control cohort references an unknown member".to_owned(),
                    ));
                };
                if member.cohort_id != *id {
                    return Err(ControlHeadError::Invalid(
                        "control member and cohort identities disagree".to_owned(),
                    ));
                }
                *member_cohort_counts.entry(member_id.clone()).or_default() += 1;
            }
        }

        if self
            .members
            .values()
            .any(|member| member_cohort_counts.get(&member.id) != Some(&1))
        {
            return Err(ControlHeadError::Invalid(
                "control member is not assigned to exactly one cohort".to_owned(),
            ));
        }

        if self.manifest.cohort_id != 0 && !self.cohorts.contains_key(&self.manifest.cohort_id) {
            return Err(ControlHeadError::Invalid(
                "control manifest references an unknown default cohort".to_owned(),
            ));
        }
        for segments in self.manifest.stream_segments.values() {
            for segment in segments {
                let Some(cohort) = self.cohorts.get(&segment.cohort_id) else {
                    return Err(ControlHeadError::Invalid(
                        "control manifest references an unknown segment cohort".to_owned(),
                    ));
                };
                if !segment.member_ids.is_empty() && segment.member_ids != cohort.members {
                    return Err(ControlHeadError::Invalid(
                        "control manifest segment member set differs from its cohort".to_owned(),
                    ));
                }
            }
        }
        if self.pending_handoffs.len() > MAX_PENDING_HANDOFFS {
            return Err(ControlHeadError::Invalid(
                "too many pending stream handoffs".to_owned(),
            ));
        }
        for (stream, descriptor) in &self.pending_handoffs {
            descriptor.validate(Some(stream))?;
        }
        Ok(())
    }
}

/// One complete object-store control head.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlHead {
    pub version: u8,
    /// Monotonic authority generation.  A normal metadata update retains the
    /// generation; an authority migration advances it by exactly one.
    pub authority_epoch: u64,
    /// Monotonic object-head revision.  Every successful update advances it
    /// by exactly one, including an authority migration.
    pub revision: u64,
    /// Idempotency key for the operation that produced this head.
    pub operation_id: String,
    /// Durable idempotency receipts for every successful metadata operation
    /// represented by this head. Keeping receipts in the head lets a delayed
    /// retry be recognized after later CASes and after the old cohort retires.
    #[serde(default)]
    pub completed_operations: BTreeMap<String, ControlOperationReceipt>,
    /// Source quorum marker carried through the authority handoff.  Ordinary
    /// updates must preserve it; an authority migration may replace it while
    /// advancing `authority_epoch`.
    pub source_marker: ControlSourceMarker,
    /// Self-authenticating digest of `metadata`.
    pub metadata_digest: String,
    pub metadata: ControlMetadata,
}

impl ControlHead {
    /// Constructs and validates a complete head.
    pub fn new(
        authority_epoch: u64,
        revision: u64,
        operation_id: impl Into<String>,
        source_marker: ControlSourceMarker,
        metadata: ControlMetadata,
    ) -> Result<Self, ControlHeadError> {
        let operation_id = operation_id.into();
        let operation_receipt_id = operation_id.clone();
        let metadata_digest = metadata.digest()?;
        let head = Self {
            version: CONTROL_HEAD_VERSION,
            authority_epoch,
            revision,
            operation_id,
            completed_operations: BTreeMap::from([(
                operation_receipt_id,
                ControlOperationReceipt {
                    authority_epoch,
                    revision,
                    metadata_digest: metadata_digest.clone(),
                },
            )]),
            source_marker,
            metadata_digest,
            metadata,
        };
        head.validate()?;
        Ok(head)
    }

    /// Merges receipts from the currently published head. A conflicting
    /// receipt is rejected instead of being silently replaced.
    pub fn merge_completed_operations(&mut self, current: &Self) -> Result<(), ControlHeadError> {
        for (operation_id, receipt) in &current.completed_operations {
            if let Some(existing) = self.completed_operations.get(operation_id)
                && existing != receipt
            {
                return Err(ControlHeadError::OperationConflict {
                    operation_id: operation_id.clone(),
                });
            }
            self.completed_operations
                .entry(operation_id.clone())
                .or_insert_with(|| receipt.clone());
        }
        self.completed_operations.insert(
            self.operation_id.clone(),
            ControlOperationReceipt {
                authority_epoch: self.authority_epoch,
                revision: self.revision,
                metadata_digest: self.metadata_digest.clone(),
            },
        );
        self.validate()
    }

    /// Returns the canonical digest of the complete metadata payload.
    pub fn computed_metadata_digest(&self) -> Result<String, ControlHeadError> {
        self.metadata.digest()
    }

    fn validate(&self) -> Result<(), ControlHeadError> {
        if self.version != CONTROL_HEAD_VERSION {
            return Err(ControlHeadError::Invalid(format!(
                "unsupported control-head version {}",
                self.version
            )));
        }
        if self.authority_epoch == 0 || self.revision == 0 {
            return Err(ControlHeadError::Invalid(
                "control-head authority epoch and revision must be nonzero".to_owned(),
            ));
        }
        if self.operation_id.trim().is_empty() || self.operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(ControlHeadError::Invalid(
                "control-head operation id is empty or too long".to_owned(),
            ));
        }
        if self.completed_operations.is_empty()
            || self.completed_operations.len() > MAX_COMPLETED_OPERATIONS
        {
            return Err(ControlHeadError::Invalid(
                "control-head completed operation receipts are empty or too large".to_owned(),
            ));
        }
        let Some(current_receipt) = self.completed_operations.get(&self.operation_id) else {
            return Err(ControlHeadError::Invalid(
                "control-head is missing its current operation receipt".to_owned(),
            ));
        };
        if current_receipt.authority_epoch != self.authority_epoch
            || current_receipt.revision != self.revision
            || current_receipt.metadata_digest != self.metadata_digest
        {
            return Err(ControlHeadError::Invalid(
                "control-head current operation receipt does not match the head".to_owned(),
            ));
        }
        if self.completed_operations.values().any(|receipt| {
            receipt.authority_epoch == 0
                || receipt.revision == 0
                || receipt.authority_epoch > self.authority_epoch
                || receipt.revision > self.revision
                || receipt.metadata_digest.trim().is_empty()
        }) {
            return Err(ControlHeadError::Invalid(
                "control-head contains an invalid completed operation receipt".to_owned(),
            ));
        }
        self.source_marker.validate()?;
        if self.source_marker.authority_epoch > self.authority_epoch
            || self.source_marker.revision > self.revision
        {
            return Err(ControlHeadError::Invalid(
                "control source marker is newer than the control head".to_owned(),
            ));
        }
        self.metadata.validate()?;
        let digest = self.metadata.digest()?;
        if self.metadata_digest != digest {
            return Err(ControlHeadError::Invalid(
                "control-head metadata digest does not match its payload".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A loaded head plus the object-store identity required for its next CAS.
#[derive(Clone, Debug)]
pub struct ControlHeadSnapshot {
    pub head: ControlHead,
    pub version: UpdateVersion,
}

/// Compact durable result identity for one completed control-head operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlOperationReceipt {
    pub authority_epoch: u64,
    pub revision: u64,
    pub metadata_digest: String,
}

impl ControlHeadSnapshot {
    /// Returns the object-store conditional identity.
    #[must_use]
    pub fn version(&self) -> &UpdateVersion {
        &self.version
    }
}

/// Errors from the bounded control-head store.
#[derive(Debug, Error)]
pub enum ControlHeadError {
    #[error("invalid replica control head: {0}")]
    Invalid(String),
    #[error("replica control head object-store operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("replica control head JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("replica control head already exists")]
    AlreadyExists,
    #[error("replica control head conditional update failed")]
    Precondition,
    #[error("replica control head source marker does not match the expected authority")]
    SourceMarkerMismatch,
    #[error("replica control head operation {operation_id} conflicts with an existing value")]
    OperationConflict { operation_id: String },
    #[error("pending handoff for stream {stream} is owned by operation {operation_id}")]
    PendingHandoffConflict {
        stream: String,
        operation_id: String,
    },
    #[error("replica control head object has no ETag for conditional update")]
    MissingEtag,
}

/// One object-store namespace containing the mutable control head.
#[derive(Clone)]
pub struct ControlHeadStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl ControlHeadStore {
    /// Creates a control-head store under a non-empty object prefix.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
    ) -> Result<Self, ControlHeadError> {
        let prefix = prefix.into().trim_matches('/').to_owned();
        if prefix.is_empty() || prefix.len() > MAX_PREFIX_BYTES {
            return Err(ControlHeadError::Invalid(
                "control-head prefix is empty or too long".to_owned(),
            ));
        }
        Ok(Self { store, prefix })
    }

    /// Returns the object path used for the mutable head.
    #[must_use]
    pub fn path(&self) -> Path {
        Path::from(format!("{}/{}", self.prefix, CONTROL_HEAD_FILE))
    }

    /// Reads and validates the current head and its ETag identity.
    pub async fn load(&self) -> Result<Option<ControlHeadSnapshot>, ControlHeadError> {
        let path = self.path();
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if result.meta.size > MAX_CONTROL_HEAD_BYTES as u64 {
            return Err(ControlHeadError::Invalid(format!(
                "control-head object exceeds {} bytes",
                MAX_CONTROL_HEAD_BYTES
            )));
        }
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        if version.e_tag.is_none() {
            return Err(ControlHeadError::MissingEtag);
        }
        let bytes = result.bytes().await?;
        if bytes.len() > MAX_CONTROL_HEAD_BYTES {
            return Err(ControlHeadError::Invalid(format!(
                "control-head object exceeds {} bytes",
                MAX_CONTROL_HEAD_BYTES
            )));
        }
        let head = serde_json::from_slice::<ControlHead>(&bytes)?;
        head.validate()?;
        Ok(Some(ControlHeadSnapshot { head, version }))
    }

    /// Creates the first head with an atomic conditional `Create`.
    ///
    /// If a caller loses a create race but its exact operation is already the
    /// published value, the operation is treated as an idempotent success.
    /// A different operation receives [`ControlHeadError::AlreadyExists`].
    pub async fn create(&self, head: ControlHead) -> Result<ControlHeadSnapshot, ControlHeadError> {
        self.validate_size_and_head(&head)?;
        let bytes = serde_json::to_vec(&head)?;
        let result = self
            .store
            .put_opts(
                &self.path(),
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await;
        match result {
            Ok(result) => Self::snapshot_from_put(head, result),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let current = self.load().await?.ok_or(ControlHeadError::Precondition)?;
                if let Some(receipt) = current.head.completed_operations.get(&head.operation_id) {
                    if Self::receipt_matches_head(receipt, &head) {
                        return Ok(current);
                    }
                    return Err(ControlHeadError::OperationConflict {
                        operation_id: head.operation_id,
                    });
                }
                Err(ControlHeadError::AlreadyExists)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Atomically advances the head from one loaded version to the next.
    ///
    /// There is no retry loop.  The expected snapshot is checked against a
    /// fresh read before the conditional update, which gives callers a
    /// specific source-marker error instead of silently overwriting a head
    /// from another authority.  The ETag remains the final race detector.
    pub async fn update(
        &self,
        expected: &ControlHeadSnapshot,
        next: ControlHead,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        self.update_inner(expected, next, true).await
    }

    async fn update_exact(
        &self,
        expected: &ControlHeadSnapshot,
        next: ControlHead,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        self.update_inner(expected, next, false).await
    }

    async fn update_inner(
        &self,
        expected: &ControlHeadSnapshot,
        next: ControlHead,
        preserve_pending: bool,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        let current = self.load().await?.ok_or(ControlHeadError::Precondition)?;

        let mut next = next;
        if preserve_pending {
            Self::preserve_pending_handoffs(&current.head, &mut next)?;
            // A legacy caller may have built `next` from the membership
            // document rather than cloning the object-head metadata.  Once a
            // pending claim is carried forward, refresh the self-authenticating
            // metadata digest and its current-operation receipt before checking
            // an idempotent replay.  Otherwise a response-lost retry that
            // omitted the claim would be reported as an operation conflict
            // even though the exact operation is already committed.
            next.metadata_digest = next.metadata.digest()?;
            if let Some(receipt) = next.completed_operations.get_mut(&next.operation_id) {
                receipt.metadata_digest = next.metadata_digest.clone();
            }
        }

        if let Some(receipt) = current.head.completed_operations.get(&next.operation_id) {
            if Self::receipt_matches_head(receipt, &next) {
                return Ok(current);
            }
            return Err(ControlHeadError::OperationConflict {
                operation_id: next.operation_id,
            });
        }

        Self::validate_expected(expected, &current)?;

        Self::validate_transition(&current.head, &next)?;
        next.merge_completed_operations(&current.head)?;
        self.validate_size_and_head(&next)?;
        let bytes = serde_json::to_vec(&next)?;
        let result = self
            .store
            .put_opts(
                &self.path(),
                Bytes::from(bytes).into(),
                PutOptions {
                    mode: PutMode::Update(expected.version.clone()),
                    ..PutOptions::default()
                },
            )
            .await;
        match result {
            Ok(result) => Self::snapshot_from_put(next, result),
            Err(object_store::Error::Precondition { .. }) => {
                // A peer may have won the same idempotent CAS between our
                // read and conditional write. Reconcile that one race by
                // loading the committed head and checking the durable receipt
                // for this exact candidate. This is a bounded read, not a
                // retry loop; a different operation remains a precondition
                // failure for the caller to reconcile.
                let latest = self.load().await?.ok_or(ControlHeadError::Precondition)?;
                if latest
                    .head
                    .completed_operations
                    .get(&next.operation_id)
                    .is_some_and(|receipt| Self::receipt_matches_head(receipt, &next))
                {
                    Ok(latest)
                } else {
                    Err(ControlHeadError::Precondition)
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Claims a stream before a placement fence.  The source route and exact
    /// prepared successor are checked against the loaded head while this CAS
    /// is serialized; a different operation receives a typed conflict and no
    /// node-side fence can have been installed by this primitive.
    pub async fn claim_pending_handoff(
        &self,
        expected: &ControlHeadSnapshot,
        descriptor: PendingHandoffDescriptor,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        descriptor.validate(Some(&descriptor.stream))?;
        if descriptor.stage != PendingHandoffStage::Prepared {
            return Err(ControlHeadError::Invalid(
                "pending handoff claims must start in prepared stage".to_owned(),
            ));
        }
        let current = self.load().await?.ok_or(ControlHeadError::Precondition)?;
        let claim_operation =
            pending_operation_id(&descriptor.operation_id, &descriptor.stream, "claim")?;
        if let Some(existing) = current
            .head
            .metadata
            .pending_handoffs
            .get(&descriptor.stream)
        {
            if existing.immutable_eq(&descriptor) {
                return Ok(current);
            }
            return Err(ControlHeadError::PendingHandoffConflict {
                stream: descriptor.stream,
                operation_id: existing.operation_id.clone(),
            });
        }
        if current
            .head
            .completed_operations
            .contains_key(&claim_operation)
        {
            return Err(ControlHeadError::OperationConflict {
                operation_id: descriptor.operation_id,
            });
        }
        Self::validate_expected(expected, &current)?;
        Self::validate_handoff_against_metadata(&current.head.metadata, &descriptor, false)?;
        let mut metadata = current.head.metadata.clone();
        metadata
            .pending_handoffs
            .insert(descriptor.stream.clone(), descriptor);
        let next = Self::head_with_metadata(&current.head, metadata, claim_operation)?;
        self.update_exact(&current, next).await
    }

    /// Advances one stream claim monotonically.  The logical operation id and
    /// all immutable source/target/placement fields must remain identical;
    /// only the durable phase, archive watermark, and published successor may
    /// change.
    pub async fn advance_pending_handoff(
        &self,
        expected: &ControlHeadSnapshot,
        descriptor: PendingHandoffDescriptor,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        descriptor.validate(Some(&descriptor.stream))?;
        if descriptor.stage == PendingHandoffStage::Prepared {
            return Err(ControlHeadError::Invalid(
                "pending handoff advance must move beyond prepared stage".to_owned(),
            ));
        }
        let current = self.load().await?.ok_or(ControlHeadError::Precondition)?;
        let Some(existing) = current
            .head
            .metadata
            .pending_handoffs
            .get(&descriptor.stream)
        else {
            let operation = pending_operation_id(
                &descriptor.operation_id,
                &descriptor.stream,
                descriptor.stage.operation_suffix(),
            )?;
            if current.head.completed_operations.contains_key(&operation) {
                return Ok(current);
            }
            return Err(ControlHeadError::Precondition);
        };
        if existing.operation_id != descriptor.operation_id {
            return Err(ControlHeadError::PendingHandoffConflict {
                stream: descriptor.stream,
                operation_id: existing.operation_id.clone(),
            });
        }
        if !existing.immutable_eq(&descriptor) {
            return Err(ControlHeadError::OperationConflict {
                operation_id: descriptor.operation_id,
            });
        }
        if descriptor.stage.rank() <= existing.stage.rank() {
            let operation = pending_operation_id(
                &descriptor.operation_id,
                &descriptor.stream,
                descriptor.stage.operation_suffix(),
            )?;
            let exact_phase = descriptor.archived_lsn == existing.archived_lsn
                && descriptor.successor == existing.successor
                && (descriptor.successor_placement.is_none()
                    || descriptor.successor_placement == existing.successor_placement);
            if descriptor.stage.rank() < existing.stage.rank() && exact_phase {
                if current.head.completed_operations.contains_key(&operation) {
                    return Ok(current);
                }
                return Err(ControlHeadError::Precondition);
            }
            if descriptor.stage == existing.stage {
                if exact_phase && current.head.completed_operations.contains_key(&operation) {
                    return Ok(current);
                }
                return Err(ControlHeadError::OperationConflict {
                    operation_id: descriptor.operation_id,
                });
            }
            return Err(ControlHeadError::Precondition);
        }
        let observed_claim = existing.clone();
        if let Err(error) = Self::validate_expected(expected, &current) {
            if matches!(error, ControlHeadError::Precondition) {
                return self
                    .rebase_pending_handoff_advance(
                        &descriptor,
                        pending_operation_id(
                            &descriptor.operation_id,
                            &descriptor.stream,
                            descriptor.stage.operation_suffix(),
                        )?,
                        &observed_claim,
                    )
                    .await;
            }
            return Err(error);
        }
        Self::validate_handoff_against_metadata(&current.head.metadata, &descriptor, true)?;
        let operation = pending_operation_id(
            &descriptor.operation_id,
            &descriptor.stream,
            descriptor.stage.operation_suffix(),
        )?;
        let mut metadata = current.head.metadata.clone();
        metadata
            .pending_handoffs
            .insert(descriptor.stream.clone(), descriptor.clone());
        let next = Self::head_with_metadata(&current.head, metadata, operation.clone())?;
        match self.update_exact(&current, next).await {
            Ok(snapshot) => Ok(snapshot),
            Err(ControlHeadError::Precondition) => {
                // A different stream may have advanced its own claim between
                // the read above and this CAS. Rebase this stream-local delta
                // once against that committed head when its claim is byte
                // identical to the one we validated. This is a bounded,
                // resource-aware reconciliation, not a retry loop. A change
                // to this stream (including a competing operation) remains a
                // typed conflict.
                self.rebase_pending_handoff_advance(&descriptor, operation, &observed_claim)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn rebase_pending_handoff_advance(
        &self,
        descriptor: &PendingHandoffDescriptor,
        operation: String,
        observed_claim: &PendingHandoffDescriptor,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        let latest = self.load().await?.ok_or(ControlHeadError::Precondition)?;
        let Some(latest_claim) = latest
            .head
            .metadata
            .pending_handoffs
            .get(&descriptor.stream)
        else {
            return Err(ControlHeadError::Precondition);
        };
        if latest_claim.operation_id != descriptor.operation_id {
            return Err(ControlHeadError::PendingHandoffConflict {
                stream: descriptor.stream.clone(),
                operation_id: latest_claim.operation_id.clone(),
            });
        }
        let phase_receipt = latest.head.completed_operations.contains_key(&operation);
        if latest_claim == descriptor && phase_receipt {
            return Ok(latest);
        }
        if latest_claim != observed_claim {
            return if latest_claim.stage.rank() >= descriptor.stage.rank() {
                Err(ControlHeadError::Precondition)
            } else {
                Err(ControlHeadError::OperationConflict {
                    operation_id: descriptor.operation_id.clone(),
                })
            };
        }

        // Validate the stream-local intent against the latest complete
        // metadata before constructing a rebased head. Other streams may
        // have advanced, but none of their changes are discarded.
        Self::validate_handoff_against_metadata(&latest.head.metadata, descriptor, true)?;
        let mut metadata = latest.head.metadata.clone();
        metadata
            .pending_handoffs
            .insert(descriptor.stream.clone(), descriptor.clone());
        let next = Self::head_with_metadata(&latest.head, metadata, operation)?;
        // One ETag CAS is enough: if this also loses, the caller must observe
        // the new head and reconcile rather than turning this into polling.
        self.update_exact(&latest, next).await
    }

    /// Publishes the exact successor route and its final membership state in
    /// one object-head CAS.  The caller must provide the complete next
    /// metadata value with the descriptor at `RoutePublished`; this prevents
    /// a route from becoming visible while the durable claim still says that
    /// the source is only archived.
    pub async fn publish_pending_handoff(
        &self,
        expected: &ControlHeadSnapshot,
        metadata: ControlMetadata,
        descriptor: PendingHandoffDescriptor,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        descriptor.validate(Some(&descriptor.stream))?;
        if descriptor.stage != PendingHandoffStage::RoutePublished {
            return Err(ControlHeadError::Invalid(
                "pending handoff publication requires route_published stage".to_owned(),
            ));
        }
        if metadata.pending_handoffs.get(&descriptor.stream) != Some(&descriptor) {
            return Err(ControlHeadError::Precondition);
        }
        let current = self.load().await?.ok_or(ControlHeadError::Precondition)?;
        let Some(existing) = current
            .head
            .metadata
            .pending_handoffs
            .get(&descriptor.stream)
        else {
            return Err(ControlHeadError::Precondition);
        };
        if existing.operation_id != descriptor.operation_id {
            return Err(ControlHeadError::PendingHandoffConflict {
                stream: descriptor.stream,
                operation_id: existing.operation_id.clone(),
            });
        }
        let operation = pending_operation_id(
            &descriptor.operation_id,
            &descriptor.stream,
            descriptor.stage.operation_suffix(),
        )?;
        // The route publication itself is idempotent. A second coordinator
        // can load the head after the first CAS has committed, before it has
        // observed the response; return that committed head instead of
        // turning an exact replay into a conflict.
        if existing == &descriptor
            && existing.stage == PendingHandoffStage::RoutePublished
            && current.head.completed_operations.contains_key(&operation)
        {
            return Ok(current);
        }
        if !existing.immutable_eq(&descriptor)
            || descriptor.stage.rank() <= existing.stage.rank()
            || existing.stage.rank() < PendingHandoffStage::ArchiveVerified.rank()
        {
            return Err(ControlHeadError::Precondition);
        }
        Self::validate_expected(expected, &current)?;
        Self::validate_handoff_against_metadata(&metadata, &descriptor, true)?;
        let next = Self::head_with_metadata(&current.head, metadata, operation)?;
        self.update_exact(&current, next).await
    }

    /// Removes a route-published claim.  The route must already be present in
    /// the same head metadata, so a crash before this CAS leaves an owned,
    /// resumable descriptor rather than reopening the stream to a competitor.
    pub async fn complete_pending_handoff(
        &self,
        expected: &ControlHeadSnapshot,
        stream: &str,
        operation_id: &str,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        let current = self.load().await?.ok_or(ControlHeadError::Precondition)?;
        let complete_operation = pending_operation_id(operation_id, stream, "complete")?;
        let Some(existing) = current.head.metadata.pending_handoffs.get(stream) else {
            if current
                .head
                .completed_operations
                .contains_key(&complete_operation)
            {
                return Ok(current);
            }
            return Err(ControlHeadError::Precondition);
        };
        if existing.operation_id != operation_id {
            return Err(ControlHeadError::PendingHandoffConflict {
                stream: stream.to_owned(),
                operation_id: existing.operation_id.clone(),
            });
        }
        if existing.stage != PendingHandoffStage::RoutePublished {
            return Err(ControlHeadError::Precondition);
        }
        Self::validate_expected(expected, &current)?;
        Self::validate_handoff_against_metadata(&current.head.metadata, existing, true)?;
        let mut metadata = current.head.metadata.clone();
        metadata.pending_handoffs.remove(stream);
        let next = Self::head_with_metadata(&current.head, metadata, complete_operation)?;
        self.update_exact(&current, next).await
    }

    fn head_with_metadata(
        current: &ControlHead,
        metadata: ControlMetadata,
        operation_id: String,
    ) -> Result<ControlHead, ControlHeadError> {
        ControlHead::new(
            current.authority_epoch,
            current.revision.checked_add(1).ok_or_else(|| {
                ControlHeadError::Invalid("control-head revision exhausted".to_owned())
            })?,
            operation_id,
            current.source_marker.clone(),
            metadata,
        )
    }

    fn preserve_pending_handoffs(
        current: &ControlHead,
        next: &mut ControlHead,
    ) -> Result<(), ControlHeadError> {
        for (stream, existing) in &current.metadata.pending_handoffs {
            match next.metadata.pending_handoffs.get(stream) {
                Some(candidate) if candidate != existing => {
                    return Err(ControlHeadError::PendingHandoffConflict {
                        stream: stream.clone(),
                        operation_id: existing.operation_id.clone(),
                    });
                }
                Some(_) => {}
                None => {
                    next.metadata
                        .pending_handoffs
                        .insert(stream.clone(), existing.clone());
                }
            }
        }
        Ok(())
    }

    fn validate_handoff_against_metadata(
        metadata: &ControlMetadata,
        descriptor: &PendingHandoffDescriptor,
        allow_published: bool,
    ) -> Result<(), ControlHeadError> {
        let source_cohort = metadata
            .cohorts
            .get(&descriptor.source.cohort_id)
            .ok_or_else(|| {
                ControlHeadError::Invalid("pending handoff source cohort is absent".to_owned())
            })?;
        let source_status_allowed = if descriptor.stage == PendingHandoffStage::RoutePublished {
            // Publishing one successor is intentionally earlier than the
            // cohort retirement CAS. The old cohort remains readable while
            // other streams are still being drained; an Active source is
            // valid for a partial horizontal move, while Retired is also
            // accepted for recovery of the older aggregate publication path.
            matches!(
                source_cohort.status,
                crate::CohortStatus::Active
                    | crate::CohortStatus::Draining
                    | crate::CohortStatus::Retired
            )
        } else {
            matches!(
                source_cohort.status,
                crate::CohortStatus::Active | crate::CohortStatus::Draining
            )
        };
        if source_cohort.members != descriptor.source.member_ids || !source_status_allowed {
            return Err(ControlHeadError::Precondition);
        }
        for member_id in &descriptor.source.member_ids {
            let Some(member) = metadata.members.get(member_id) else {
                return Err(ControlHeadError::Precondition);
            };
            let member_status_allowed = if descriptor.stage == PendingHandoffStage::RoutePublished {
                (source_cohort.status == crate::CohortStatus::Retired
                    && member.status == crate::MemberStatus::Removed)
                    || (source_cohort.status != crate::CohortStatus::Retired
                        && matches!(
                            member.status,
                            crate::MemberStatus::Active | crate::MemberStatus::Draining
                        ))
            } else {
                matches!(
                    member.status,
                    crate::MemberStatus::Active | crate::MemberStatus::Draining
                )
            };
            if !member_status_allowed || member.cohort_id != descriptor.source.cohort_id {
                return Err(ControlHeadError::Precondition);
            }
        }
        let source_members = descriptor
            .source
            .member_ids
            .iter()
            .filter_map(|id| metadata.members.get(id).cloned())
            .collect::<Vec<_>>();
        if source_members.len() != REPLICATION_FACTOR
            || member_hash(&source_members) != descriptor.source.member_hash
        {
            return Err(ControlHeadError::Precondition);
        }
        let target_cohort = metadata
            .cohorts
            .get(&descriptor.target_cohort_id)
            .ok_or_else(|| {
                ControlHeadError::Invalid("pending handoff target cohort is absent".to_owned())
            })?;
        let target_ids = descriptor
            .target_members
            .iter()
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        if target_cohort.members != target_ids
            || !matches!(
                target_cohort.status,
                crate::CohortStatus::Joining | crate::CohortStatus::Active
            )
            || target_cohort.tier != descriptor.target_tier
            || target_cohort.max_append_bytes != descriptor.target_max_append_bytes
            || member_hash(&descriptor.target_members) != descriptor.target_member_hash
        {
            return Err(ControlHeadError::Precondition);
        }
        for target in &descriptor.target_members {
            if metadata
                .members
                .get(&target.id)
                .is_none_or(|member| !same_member_incarnation(member, target))
                || !matches!(
                    metadata.members[&target.id].status,
                    crate::MemberStatus::Joining | crate::MemberStatus::Active
                )
            {
                return Err(ControlHeadError::Precondition);
            }
        }
        let ranges = metadata
            .manifest
            .stream_segments
            .get(&descriptor.stream)
            .ok_or_else(|| ControlHeadError::Precondition)?;
        if descriptor.stage != PendingHandoffStage::RoutePublished {
            if ranges.last() != Some(&descriptor.source) {
                return Err(ControlHeadError::Precondition);
            }
            return Ok(());
        }
        if !allow_published {
            return Err(ControlHeadError::Precondition);
        }
        let successor = descriptor
            .successor
            .as_ref()
            .ok_or_else(|| ControlHeadError::Precondition)?;
        let successor_index = ranges
            .iter()
            .position(|candidate| candidate == successor)
            .ok_or_else(|| ControlHeadError::Precondition)?;
        if descriptor
            .archived_lsn
            .is_some_and(|archived| archived >= descriptor.source.start_lsn)
        {
            let Some(source_index) = ranges.iter().position(|candidate| {
                candidate.start_lsn == descriptor.source.start_lsn
                    && candidate.cohort_id == descriptor.source.cohort_id
                    && candidate.member_ids == descriptor.source.member_ids
                    && candidate.member_hash == descriptor.source.member_hash
                    && candidate.writer_epoch == descriptor.source.writer_epoch
                    && candidate.placement_epoch == descriptor.source.placement_epoch
                    && candidate.end_lsn == descriptor.archived_lsn
            }) else {
                return Err(ControlHeadError::Precondition);
            };
            if source_index.saturating_add(1) != successor_index {
                return Err(ControlHeadError::Precondition);
            }
        } else if successor.start_lsn != descriptor.source.start_lsn {
            return Err(ControlHeadError::Precondition);
        }
        Ok(())
    }

    fn validate_size_and_head(&self, head: &ControlHead) -> Result<(), ControlHeadError> {
        head.validate()?;
        let bytes = serde_json::to_vec(head)?;
        if bytes.len() > MAX_CONTROL_HEAD_BYTES {
            return Err(ControlHeadError::Invalid(format!(
                "control-head payload exceeds {} bytes",
                MAX_CONTROL_HEAD_BYTES
            )));
        }
        Ok(())
    }

    fn validate_expected(
        expected: &ControlHeadSnapshot,
        current: &ControlHeadSnapshot,
    ) -> Result<(), ControlHeadError> {
        if expected.head.source_marker != current.head.source_marker {
            return Err(ControlHeadError::SourceMarkerMismatch);
        }
        if expected.head != current.head {
            return Err(ControlHeadError::Precondition);
        }
        Ok(())
    }

    fn validate_transition(
        current: &ControlHead,
        next: &ControlHead,
    ) -> Result<(), ControlHeadError> {
        if next.revision != current.revision.saturating_add(1) {
            return Err(ControlHeadError::Precondition);
        }
        match next.authority_epoch.cmp(&current.authority_epoch) {
            std::cmp::Ordering::Less => return Err(ControlHeadError::Precondition),
            std::cmp::Ordering::Greater
                if next.authority_epoch != current.authority_epoch.saturating_add(1) =>
            {
                return Err(ControlHeadError::Precondition);
            }
            _ => {}
        }
        if next.authority_epoch == current.authority_epoch
            && next.source_marker != current.source_marker
        {
            return Err(ControlHeadError::SourceMarkerMismatch);
        }
        if next.authority_epoch == current.authority_epoch.saturating_add(1)
            && (next.source_marker.authority_epoch != current.authority_epoch
                || next.source_marker.revision != current.revision
                || next.source_marker.digest != current.metadata_digest)
        {
            return Err(ControlHeadError::SourceMarkerMismatch);
        }
        Ok(())
    }

    fn receipt_matches_head(receipt: &ControlOperationReceipt, head: &ControlHead) -> bool {
        receipt.authority_epoch == head.authority_epoch
            && receipt.revision == head.revision
            && receipt.metadata_digest == head.metadata_digest
    }

    fn snapshot_from_put(
        head: ControlHead,
        result: object_store::PutResult,
    ) -> Result<ControlHeadSnapshot, ControlHeadError> {
        let version = UpdateVersion {
            e_tag: result.e_tag,
            version: result.version,
        };
        if version.e_tag.is_none() {
            return Err(ControlHeadError::MissingEtag);
        }
        Ok(ControlHeadSnapshot { head, version })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
    use object_store::memory::InMemory;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn manifest() -> ReplicaManifest {
        serde_json::from_value(json!({
            "version": 1,
            "revision": 0,
            "digest": "",
            "writer_epoch": 0,
            "cohort_id": 0,
            "member_set_hash": "",
            "write_cohort_id": 0,
            "stream_segments": {},
            "operation_id": "",
            "tier": "",
            "max_append_bytes": 0
        }))
        .expect("empty manifest")
    }

    fn marker(digest: &str) -> ControlSourceMarker {
        ControlSourceMarker::new("legacy-replica-quorum", 1, 1, digest).expect("marker")
    }

    fn metadata() -> ControlMetadata {
        ControlMetadata {
            membership_epoch: 1,
            members: BTreeMap::new(),
            cohorts: BTreeMap::new(),
            manifest: manifest(),
            pending_handoffs: BTreeMap::new(),
        }
    }

    fn head(operation_id: &str, revision: u64, source: &str) -> ControlHead {
        ControlHead::new(1, revision, operation_id, marker(source), metadata()).expect("head")
    }

    fn pending_fixture() -> (ControlHead, PendingHandoffDescriptor) {
        let source_members = (0..REPLICATION_FACTOR)
            .map(|index| DurableMember {
                id: format!("source-{index}"),
                url: format!("http://source-{index}"),
                status: crate::MemberStatus::Active,
                cohort_id: 0,
                ..DurableMember::default()
            })
            .collect::<Vec<_>>();
        let target_members = (0..REPLICATION_FACTOR)
            .map(|index| DurableMember {
                id: format!("target-{index}"),
                url: format!("http://target-{index}"),
                status: crate::MemberStatus::Joining,
                cohort_id: 1,
                ..DurableMember::default()
            })
            .collect::<Vec<_>>();
        let source = StreamSegment {
            start_lsn: 1,
            end_lsn: None,
            cohort_id: 0,
            member_ids: source_members
                .iter()
                .map(|member| member.id.clone())
                .collect(),
            member_hash: member_hash(&source_members),
            writer_epoch: 1,
            manifest_revision: 1,
            placement_epoch: 1,
            operation_id: "source-route".to_owned(),
            tier: "hot".to_owned(),
            max_append_bytes: 0,
        };
        let segments = BTreeMap::from([("tenant/a".to_owned(), vec![source.clone()])]);
        let manifest = ReplicaManifest {
            version: 1,
            revision: 1,
            digest: crate::manifest_digest(&segments),
            writer_epoch: 1,
            cohort_id: 0,
            member_set_hash: source.member_hash.clone(),
            write_cohort_id: 0,
            stream_segments: segments,
            operation_id: "source-route".to_owned(),
            tier: "hot".to_owned(),
            max_append_bytes: 0,
        };
        let members = source_members
            .iter()
            .chain(&target_members)
            .cloned()
            .map(|member| (member.id.clone(), member))
            .collect();
        let cohorts = BTreeMap::from([
            (
                0,
                DurableCohort {
                    id: 0,
                    members: source.member_ids.clone(),
                    status: crate::CohortStatus::Active,
                    tier: "hot".to_owned(),
                    max_append_bytes: 0,
                },
            ),
            (
                1,
                DurableCohort {
                    id: 1,
                    members: target_members
                        .iter()
                        .map(|member| member.id.clone())
                        .collect(),
                    status: crate::CohortStatus::Joining,
                    tier: "hot".to_owned(),
                    max_append_bytes: 0,
                },
            ),
        ]);
        let metadata = ControlMetadata {
            membership_epoch: 1,
            members,
            cohorts,
            manifest,
            pending_handoffs: BTreeMap::new(),
        };
        let target_member_hash = member_hash(&target_members);
        let descriptor = PendingHandoffDescriptor {
            operation_id: "handoff-1".to_owned(),
            stream: "tenant/a".to_owned(),
            source,
            target_cohort_id: 1,
            target_members,
            target_member_hash,
            target_tier: "hot".to_owned(),
            target_max_append_bytes: 0,
            target_writer_epoch: 1,
            transition_placement: PlacementEpoch::new(2, "transition"),
            successor_placement_epoch: 3,
            successor_placement: None,
            stage: PendingHandoffStage::Prepared,
            archived_lsn: None,
            successor: None,
        };
        (
            ControlHead::new(1, 1, "base", marker("source"), metadata).expect("fixture head"),
            descriptor,
        )
    }

    fn successor_fixture(
        descriptor: &PendingHandoffDescriptor,
        archived_lsn: u64,
        manifest_revision: u64,
        operation_id: &str,
    ) -> StreamSegment {
        StreamSegment {
            start_lsn: archived_lsn
                .saturating_add(1)
                .max(descriptor.source.start_lsn),
            end_lsn: None,
            cohort_id: descriptor.target_cohort_id,
            member_ids: descriptor
                .target_members
                .iter()
                .map(|member| member.id.clone())
                .collect(),
            member_hash: descriptor.target_member_hash.clone(),
            writer_epoch: descriptor.target_writer_epoch,
            manifest_revision,
            placement_epoch: descriptor.successor_placement_epoch,
            operation_id: operation_id.to_owned(),
            tier: descriptor.target_tier.clone(),
            max_append_bytes: descriptor.target_max_append_bytes,
        }
    }

    #[tokio::test]
    async fn concurrent_create_has_one_winner() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let left = ControlHeadStore::new(Arc::clone(&store), "replica")?;
        let right = ControlHeadStore::new(store, "replica")?;
        let (left, right) = tokio::join!(
            left.create(head("create-left", 1, "left")),
            right.create(head("create-right", 1, "right")),
        );
        assert!(left.is_ok() ^ right.is_ok(), "exactly one create wins");
        assert!(matches!(
            (left, right),
            (Err(ControlHeadError::AlreadyExists), Ok(_))
                | (Ok(_), Err(ControlHeadError::AlreadyExists))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_update_has_one_etag_winner() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = ControlHeadStore::new(Arc::clone(&store), "replica")?;
        let second = ControlHeadStore::new(Arc::clone(&store), "replica")?;
        first.create(head("base", 1, "source")).await?;
        let expected = first.load().await?.expect("base head");
        let left_next = head("update-left", 2, "source");
        let right_next = head("update-right", 2, "source");
        let (left, right) = tokio::join!(
            first.update(&expected, left_next),
            second.update(&expected, right_next),
        );
        assert!(left.is_ok() ^ right.is_ok(), "exactly one update wins");
        assert!(matches!(
            (left, right),
            (Err(ControlHeadError::Precondition), Ok(_))
                | (Ok(_), Err(ControlHeadError::Precondition))
        ));
        assert_eq!(first.load().await?.expect("updated head").head.revision, 2);
        Ok(())
    }

    #[tokio::test]
    async fn source_marker_mismatch_is_rejected_before_put()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        control.create(head("base", 1, "source")).await?;
        let mut expected = control.load().await?.expect("base head");
        expected.head.source_marker = marker("tampered-source");
        let error = control
            .update(&expected, head("update", 2, "source"))
            .await
            .expect_err("tampered source marker");
        assert!(matches!(error, ControlHeadError::SourceMarkerMismatch));
        assert_eq!(control.load().await?.expect("head").head.revision, 1);
        Ok(())
    }

    #[tokio::test]
    async fn exact_operation_replay_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        control.create(head("base", 1, "source")).await?;
        let expected = control.load().await?.expect("base head");
        let next = head("update", 2, "source");
        control.update(&expected, next.clone()).await?;
        // The response to the first write may have been lost. Replaying with
        // the original expected snapshot must return the already committed
        // value by operation id without issuing another write.
        let replay = control.update(&expected, next).await?;
        assert_eq!(replay.head.revision, 2);
        Ok(())
    }

    #[tokio::test]
    async fn pending_handoff_claim_is_exclusive_and_survives_normal_cas()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        let (base, descriptor) = pending_fixture();
        control.create(base).await?;
        let expected = control.load().await?.expect("base head");
        let claimed = control
            .claim_pending_handoff(&expected, descriptor.clone())
            .await?;
        assert_eq!(
            claimed.head.metadata.pending_handoffs["tenant/a"],
            descriptor
        );

        // A competing operation is rejected before it can advance any
        // placement sidecar. The original claim also replays from a stale
        // expected snapshot by its durable phase receipt.
        let mut competitor = descriptor.clone();
        competitor.operation_id = "handoff-2".to_owned();
        assert!(matches!(
            control.claim_pending_handoff(&expected, competitor).await,
            Err(ControlHeadError::PendingHandoffConflict { .. })
        ));
        assert_eq!(
            control
                .claim_pending_handoff(&expected, descriptor.clone())
                .await?
                .head
                .revision,
            claimed.head.revision
        );

        // A regular head CAS which forgot the map cannot drop the claim.
        let current = control.load().await?.expect("claimed head");
        let mut ordinary_metadata = current.head.metadata.clone();
        ordinary_metadata.pending_handoffs.clear();
        let ordinary = ControlHead::new(
            current.head.authority_epoch,
            current.head.revision + 1,
            "ordinary",
            current.head.source_marker.clone(),
            ordinary_metadata,
        )?;
        let preserved = control.update(&current, ordinary).await?;
        assert_eq!(
            preserved.head.metadata.pending_handoffs["tenant/a"],
            descriptor
        );
        Ok(())
    }

    #[tokio::test]
    async fn pending_handoff_phases_bind_archive_route_and_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        let (base, descriptor) = pending_fixture();
        control.create(base).await?;
        let expected = control.load().await?.expect("base head");
        let claimed = control
            .claim_pending_handoff(&expected, descriptor.clone())
            .await?;

        let mut fenced_descriptor = descriptor.clone();
        fenced_descriptor.stage = PendingHandoffStage::SourceFenced;
        let fenced = control
            .advance_pending_handoff(&claimed, fenced_descriptor.clone())
            .await?;

        let mut archived_descriptor = fenced_descriptor;
        archived_descriptor.stage = PendingHandoffStage::ArchiveVerified;
        archived_descriptor.archived_lsn = Some(3);
        archived_descriptor.successor_placement = Some(PlacementEpoch::new(3, "successor"));
        archived_descriptor.successor = Some(StreamSegment {
            start_lsn: 4,
            end_lsn: None,
            cohort_id: 1,
            member_ids: archived_descriptor
                .target_members
                .iter()
                .map(|member| member.id.clone())
                .collect(),
            member_hash: archived_descriptor.target_member_hash.clone(),
            writer_epoch: 1,
            manifest_revision: 2,
            placement_epoch: 3,
            operation_id: "successor-route".to_owned(),
            tier: "hot".to_owned(),
            max_append_bytes: 0,
        });
        let archived = control
            .advance_pending_handoff(&fenced, archived_descriptor.clone())
            .await?;

        // The archive proof commits the complete successor before target
        // placement is installed. A resume cannot rebuild the route with a
        // later manifest revision or any other changed identity.
        let mut altered_successor = archived_descriptor.clone();
        altered_successor.stage = PendingHandoffStage::RoutePublished;
        altered_successor
            .successor
            .as_mut()
            .expect("archived successor")
            .manifest_revision += 1;
        assert!(matches!(
            control
                .advance_pending_handoff(&archived, altered_successor.clone())
                .await,
            Err(ControlHeadError::OperationConflict { .. })
        ));
        let mut altered_metadata = archived.head.metadata.clone();
        altered_metadata
            .pending_handoffs
            .insert("tenant/a".to_owned(), altered_successor.clone());
        assert!(matches!(
            control
                .publish_pending_handoff(&archived, altered_metadata, altered_successor)
                .await,
            Err(ControlHeadError::Precondition)
        ));

        let mut published_descriptor = archived_descriptor;
        published_descriptor.stage = PendingHandoffStage::RoutePublished;
        let mut metadata = archived.head.metadata.clone();
        // A partial horizontal move may publish one stream while its source
        // cohort remains active for unrelated streams and new assignments.
        // Full replacement uses the same descriptor while Draining, followed
        // by a later cohort-finish CAS to Retired/Removed.
        for id in &descriptor.source.member_ids {
            metadata.members.get_mut(id).expect("source member").status =
                crate::MemberStatus::Active;
        }
        for id in &descriptor
            .target_members
            .iter()
            .map(|member| member.id.clone())
            .collect::<Vec<_>>()
        {
            metadata.members.get_mut(id).expect("target member").status =
                crate::MemberStatus::Active;
        }
        metadata.cohorts.get_mut(&0).expect("source cohort").status = crate::CohortStatus::Active;
        metadata.cohorts.get_mut(&1).expect("target cohort").status = crate::CohortStatus::Active;
        let source = metadata.manifest.stream_segments["tenant/a"][0].clone();
        let successor = published_descriptor.successor.clone().expect("successor");
        let segments = BTreeMap::from([(
            "tenant/a".to_owned(),
            vec![
                StreamSegment {
                    end_lsn: Some(3),
                    ..source
                },
                successor,
            ],
        )]);
        metadata.manifest.revision = 2;
        metadata.manifest.operation_id = "successor-route".to_owned();
        metadata.manifest.stream_segments = segments;
        metadata.manifest.digest = crate::manifest_digest(&metadata.manifest.stream_segments);
        metadata
            .pending_handoffs
            .insert("tenant/a".to_owned(), published_descriptor.clone());
        let published = control
            .publish_pending_handoff(&archived, metadata, published_descriptor)
            .await?;
        let replay = control
            .publish_pending_handoff(
                &archived,
                published.head.metadata.clone(),
                published.head.metadata.pending_handoffs["tenant/a"].clone(),
            )
            .await?;
        assert_eq!(replay.head.revision, published.head.revision);
        let completed = control
            .complete_pending_handoff(&published, "tenant/a", "handoff-1")
            .await?;
        assert!(completed.head.metadata.pending_handoffs.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn pending_handoff_phase_receipts_are_scoped_by_stream()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        let (base, descriptor) = pending_fixture();

        // A replacement operation advances all open streams under one
        // caller-level operation id. Add a second source route so both
        // streams exercise the same phase receipt keys.
        let mut metadata = base.metadata.clone();
        let mut second_source = descriptor.source.clone();
        second_source.operation_id = "source-route-b".to_owned();
        metadata
            .manifest
            .stream_segments
            .insert("tenant/b".to_owned(), vec![second_source.clone()]);
        metadata.manifest.digest = crate::manifest_digest(&metadata.manifest.stream_segments);
        let base = ControlHead::new(
            base.authority_epoch,
            base.revision,
            base.operation_id,
            base.source_marker,
            metadata,
        )?;
        control.create(base).await?;

        let mut second = descriptor.clone();
        second.stream = "tenant/b".to_owned();
        second.source = second_source;
        let first_expected = control.load().await?.expect("base head");
        let first_claim = control
            .claim_pending_handoff(&first_expected, descriptor.clone())
            .await?;
        let second_claim = control
            .claim_pending_handoff(&first_claim, second.clone())
            .await?;

        let mut first_fenced = descriptor;
        first_fenced.stage = PendingHandoffStage::SourceFenced;
        let first_fenced = control
            .advance_pending_handoff(&second_claim, first_fenced)
            .await?;
        let mut first_archived = first_fenced.head.metadata.pending_handoffs["tenant/a"].clone();
        first_archived.stage = PendingHandoffStage::ArchiveVerified;
        first_archived.archived_lsn = Some(3);
        first_archived.successor_placement = Some(PlacementEpoch::new(3, "successor-a"));
        first_archived.successor = Some(successor_fixture(&first_archived, 3, 2, "successor-a"));
        first_archived.successor = Some(StreamSegment {
            start_lsn: 4,
            end_lsn: None,
            cohort_id: 1,
            member_ids: first_archived
                .target_members
                .iter()
                .map(|member| member.id.clone())
                .collect(),
            member_hash: first_archived.target_member_hash.clone(),
            writer_epoch: 1,
            manifest_revision: 2,
            placement_epoch: 3,
            operation_id: "successor-a".to_owned(),
            tier: "hot".to_owned(),
            max_append_bytes: 0,
        });
        let after_first = control
            .advance_pending_handoff(&first_fenced, first_archived)
            .await?;

        let mut second_fenced = second;
        second_fenced.stage = PendingHandoffStage::SourceFenced;
        let second_fenced_snapshot = control
            .advance_pending_handoff(&after_first, second_fenced)
            .await?;
        let mut second_archived =
            second_fenced_snapshot.head.metadata.pending_handoffs["tenant/b"].clone();
        second_archived.stage = PendingHandoffStage::ArchiveVerified;
        second_archived.archived_lsn = Some(3);
        second_archived.successor_placement = Some(PlacementEpoch::new(3, "successor-b"));
        second_archived.successor = Some(successor_fixture(&second_archived, 3, 2, "successor-b"));
        second_archived.successor = Some(StreamSegment {
            start_lsn: 4,
            end_lsn: None,
            cohort_id: 1,
            member_ids: second_archived
                .target_members
                .iter()
                .map(|member| member.id.clone())
                .collect(),
            member_hash: second_archived.target_member_hash.clone(),
            writer_epoch: 1,
            manifest_revision: 2,
            placement_epoch: 3,
            operation_id: "successor-b".to_owned(),
            tier: "hot".to_owned(),
            max_append_bytes: 0,
        });
        let final_snapshot = control
            .advance_pending_handoff(&second_fenced_snapshot, second_archived)
            .await?;

        assert_eq!(
            final_snapshot.head.metadata.pending_handoffs.len(),
            2,
            "both streams retain their independent durable claims"
        );
        assert_ne!(
            pending_operation_id("replacement", "tenant/a", "archive-verified")?,
            pending_operation_id("replacement", "tenant/b", "archive-verified")?
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_same_phase_advance_reconciles_the_committed_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        let (base, descriptor) = pending_fixture();
        control.create(base).await?;
        let expected = control.load().await?.expect("base head");
        let claimed = control
            .claim_pending_handoff(&expected, descriptor.clone())
            .await?;
        let mut fenced = descriptor;
        fenced.stage = PendingHandoffStage::SourceFenced;

        // Two coordinators can resume the same durable claim after a lost
        // response. Both callers must observe the one committed receipt; the
        // loser must not surface a transient 409/503 for an identical phase.
        let left = control.clone();
        let right = control;
        let (left, right) = tokio::join!(
            left.advance_pending_handoff(&claimed, fenced.clone()),
            right.advance_pending_handoff(&claimed, fenced),
        );
        let left = left?;
        let right = right?;
        assert_eq!(left.head.revision, right.head.revision);
        assert_eq!(
            left.head.metadata.pending_handoffs["tenant/a"].stage,
            PendingHandoffStage::SourceFenced
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_disjoint_phase_advances_rebase_without_dropping_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(Arc::clone(&store), "replica")?;
        let (base, descriptor) = pending_fixture();
        let mut metadata = base.metadata.clone();
        let mut second_source = descriptor.source.clone();
        second_source.operation_id = "source-route-b".to_owned();
        metadata
            .manifest
            .stream_segments
            .insert("tenant/b".to_owned(), vec![second_source.clone()]);
        metadata.manifest.digest = crate::manifest_digest(&metadata.manifest.stream_segments);
        let base = ControlHead::new(
            base.authority_epoch,
            base.revision,
            base.operation_id,
            base.source_marker,
            metadata,
        )?;
        control.create(base).await?;

        let mut second = descriptor.clone();
        second.stream = "tenant/b".to_owned();
        second.source = second_source;
        let expected = control.load().await?.expect("base head");
        let first_claim = control
            .claim_pending_handoff(&expected, descriptor.clone())
            .await?;
        let both_claims = control
            .claim_pending_handoff(&first_claim, second.clone())
            .await?;

        let mut first_fenced = descriptor;
        first_fenced.stage = PendingHandoffStage::SourceFenced;
        let mut second_fenced = second;
        second_fenced.stage = PendingHandoffStage::SourceFenced;

        // Both coordinators read the same head. One CAS wins; the other must
        // rebase its stream-local phase delta onto that committed head so the
        // two claims are advanced together without a retry loop.
        let left = control.clone();
        let right = control;
        let (left, right) = tokio::join!(
            left.advance_pending_handoff(&both_claims, first_fenced),
            right.advance_pending_handoff(&both_claims, second_fenced),
        );
        let left = left?;
        let right = right?;
        assert!(
            left.head
                .metadata
                .pending_handoffs
                .get("tenant/a")
                .and_then(|claim| {
                    (claim.stage == PendingHandoffStage::SourceFenced).then_some(())
                })
                .is_some()
        );
        assert!(
            right
                .head
                .metadata
                .pending_handoffs
                .get("tenant/b")
                .is_some_and(|claim| claim.stage == PendingHandoffStage::SourceFenced)
        );
        let check = ControlHeadStore::new(store, "replica")?;
        let persisted = check.load().await?.expect("persisted rebased head");
        assert!(
            persisted
                .head
                .metadata
                .pending_handoffs
                .values()
                .all(|claim| claim.stage == PendingHandoffStage::SourceFenced)
        );
        Ok(())
    }

    #[tokio::test]
    async fn delayed_replay_survives_later_head_cas() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        control.create(head("base", 1, "source")).await?;
        let base = control.load().await?.expect("base head");
        let first = head("update-one", 2, "source");
        control.update(&base, first).await?;
        let after_first = control.load().await?.expect("first update");
        control
            .update(&after_first, head("update-two", 3, "source"))
            .await?;

        // The response for update-one can arrive after update-two has won.
        // Its original expected snapshot is stale, but the durable receipt in
        // the current head makes the replay an idempotent read of that head.
        let replay = control
            .update(&base, head("update-one", 2, "source"))
            .await?;
        assert_eq!(replay.head.revision, 3);
        assert!(replay.head.completed_operations.contains_key("update-one"));
        Ok(())
    }

    #[tokio::test]
    async fn authority_transfer_requires_adjacent_epoch() -> Result<(), Box<dyn std::error::Error>>
    {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        control.create(head("base", 1, "source")).await?;
        let expected = control.load().await?.expect("base head");
        let next = ControlHead::new(3, 2, "transfer", marker("new-source"), metadata())?;
        assert!(matches!(
            control.update(&expected, next).await,
            Err(ControlHeadError::Precondition)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn authority_transfer_source_marker_binds_current_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let control = ControlHeadStore::new(store, "replica")?;
        control.create(head("base", 1, "source")).await?;
        let expected = control.load().await?.expect("base head");
        let wrong = ControlHead::new(
            2,
            2,
            "transfer-wrong",
            marker("wrong-source-digest"),
            metadata(),
        )?;
        assert!(matches!(
            control.update(&expected, wrong).await,
            Err(ControlHeadError::SourceMarkerMismatch)
        ));

        let source = ControlSourceMarker::new(
            "new-authority",
            expected.head.authority_epoch,
            expected.head.revision,
            expected.head.metadata_digest.clone(),
        )?;
        let valid = ControlHead::new(2, 2, "transfer-valid", source, metadata())?;
        control.update(&expected, valid).await?;
        Ok(())
    }

    /// Runs the same race against the configured provider when explicitly
    /// enabled inside the staging image.  Local unit tests use InMemory; this
    /// qualification is the check that the real bucket honors ETag
    /// conditional writes rather than silently overwriting them.
    #[tokio::test]
    #[ignore = "requires explicitly enabled staging object-store credentials"]
    async fn configured_provider_honors_control_head_cas() -> Result<(), Box<dyn std::error::Error>>
    {
        if std::env::var("LAKEDAY_CONTROL_HEAD_REAL_TEST").as_deref() != Ok("1") {
            return Ok(());
        }
        let bucket = std::env::var("LAKEDAY_REPLICA_ARCHIVE_BUCKET")?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let prefix = format!(
            "{}/control-head-cas-test-{}-{suffix}",
            std::env::var("LAKEDAY_REPLICA_ARCHIVE_PREFIX")
                .unwrap_or_else(|_| "replica".to_owned()),
            std::process::id()
        );
        let provider: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .build()?,
        );
        let control = ControlHeadStore::new(Arc::clone(&provider), &prefix)?;
        let left = control.clone();
        let right = control.clone();
        let (left, right) = tokio::join!(
            left.create(head("real-create-left", 1, "left")),
            right.create(head("real-create-right", 1, "right")),
        );
        assert!(left.is_ok() ^ right.is_ok(), "exactly one real create wins");
        assert!(matches!(
            (left, right),
            (Err(ControlHeadError::AlreadyExists), Ok(_))
                | (Ok(_), Err(ControlHeadError::AlreadyExists))
        ));

        let expected = control.load().await?.expect("real head");
        let left_next = ControlHead::new(
            expected.head.authority_epoch,
            expected.head.revision + 1,
            "real-update-left",
            expected.head.source_marker.clone(),
            metadata(),
        )?;
        let right_next = ControlHead::new(
            expected.head.authority_epoch,
            expected.head.revision + 1,
            "real-update-right",
            expected.head.source_marker.clone(),
            metadata(),
        )?;
        let (left, right) = tokio::join!(
            control.update(&expected, left_next),
            control.update(&expected, right_next),
        );
        assert!(left.is_ok() ^ right.is_ok(), "exactly one real update wins");
        assert!(matches!(
            (left, right),
            (Err(ControlHeadError::Precondition), Ok(_))
                | (Ok(_), Err(ControlHeadError::Precondition))
        ));
        provider.delete(&control.path()).await?;
        Ok(())
    }
}
