//! Cloud-owned encrypted replica cell.
//!
//! The process can run as either a disk-backed node or as the client-facing
//! gateway for a set of nodes. Nodes never decrypt records. The gateway
//! authenticates tenant tokens, writes each opaque record to its three
//! deterministic storage members, and acknowledges after two members durably
//! append and commit it. The remaining write continues independently.

mod archive;
mod authority;
mod control_head;
mod metadata_authority;
mod online_retirement;
mod placement;
mod route_cache;
#[cfg(test)]
mod writer_state_tests;

pub use archive::{ArchiveError, OpaqueArchive};
pub use authority::{
    AuthorityError, FrozenSourceQuorum, LEGACY_CONTROL_AUTHORITY, MetadataFreeze,
    MetadataFreezeAck, SourceQuorumCertificate, SourceReplicaSnapshot, certify_frozen_source,
    certify_source_quorum,
};
pub use control_head::{
    ControlHead, ControlHeadError, ControlHeadSnapshot, ControlHeadStore, ControlMetadata,
    ControlSourceMarker, PendingHandoffDescriptor, PendingHandoffStage,
};
pub use metadata_authority::{
    AuthoritativeHeadError, MetadataAuthorityState, MetadataHeadUpdate, MetadataHeadUpdateError,
};

pub use online_retirement::OnlineCohortReplacementReceipt;
pub use placement::{
    PlacementEpoch, PlacementError, PlacementLease, PlacementStore, placement_state_path,
};
use route_cache::{RouteRepairCache, RouteRepairContext, RouteRepairProof};

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path as RoutePath, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use futures::future::join_all;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use walleye_bitr::{
    CAPACITY_EXCEEDED_CODE, ENCRYPTED_RECORD_CONTENT_TYPE, EncryptedRecord,
    MAX_ENCRYPTED_RECORD_BATCH_BYTES, Replica, ReplicaError, validate_append_batch,
};

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_STORAGE_DATA_DIR: &str = "/data";
const DEFAULT_STORAGE_TIER: &str = "storage";
const DEFAULT_METRICS_MAX_AGE: Duration = Duration::from_secs(30);
const METRICS_CLOCK_SKEW: Duration = Duration::from_secs(5);
// Recovery snapshots can contain the entire encrypted hot tail before the
// first archival compaction pass. Keep the low-latency client default for
// append/health calls, but allow this bounded bulk transfer to finish.
const SNAPSHOT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Returns the current wall-clock time as Unix milliseconds.
///
/// Metrics use a wall-clock timestamp so a gateway can reject a sample that
/// was produced by a node that has stopped refreshing its status. The
/// monotonic append latency counter itself uses [`Instant`].
fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u128::from(u64::MAX)) as u64
        })
}

/// Creates a process-local boot identity without relying on an external UUID
/// service. It is intentionally different for every process construction,
/// including two instances started in one test.
fn new_boot_id() -> String {
    static NEXT_BOOT_ID: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_BOOT_ID.fetch_add(1, Ordering::Relaxed);
    let mut digest = Sha256::new();
    digest.update(unix_time_ms().to_le_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(sequence.to_le_bytes());
    hex::encode(digest.finalize())
}

/// Header used to identify a direct-mode control request. The existing
/// internal token remains the authorization credential; this name is useful to
/// operators and tests that inspect the control surface.
pub const CONTROL_STATE_HEADER: &str = "x-lakeday-replica-control";
/// Host-owned placement admission metadata. These headers never alter the
/// authenticated EncryptedRecord writer epoch.
pub const PLACEMENT_EPOCH_HEADER: &str = "x-lakeday-replica-placement-epoch";
pub const PLACEMENT_DIGEST_HEADER: &str = "x-lakeday-replica-placement-digest";
const PLACEMENT_FENCED_HEADER: &str = "x-lakeday-replica-placement-fenced";

const CONTROL_STATE_VERSION: u8 = 1;
const CONTROL_STATE_FILENAME: &str = "replica-control.json";
const MANIFEST_VERSION: u8 = 1;
/// Every scale-out unit is an immutable three-member cohort.
pub const REPLICATION_FACTOR: usize = 3;
/// A record is acknowledged after two of the three cohort members durably
/// append it and fsync its commit marker.
pub const ACK_QUORUM: usize = 2;
/// A lost node response is safe to retry because append and commit are both
/// idempotent for the exact same record. Keep the retry bounded so a dead node
/// cannot hold the quorum path indefinitely.
const NODE_OPERATION_ATTEMPTS: usize = 2;
/// The original cohort is also the durable control-manifest quorum.
const CONTROL_COHORT_SIZE: usize = REPLICATION_FACTOR;

/// A direct member's durable lifecycle state.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberStatus {
    /// Joined into the membership document but not yet serving writes.
    Joining,
    /// Eligible for quorum writes.
    #[default]
    Active,
    /// Kept in the document while it drains and is excluded from new writes.
    Draining,
    /// Retained as a tombstone so an old operation cannot resurrect it.
    Removed,
}

/// One direct member and its durable lifecycle state.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableMember {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub status: MemberStatus,
    /// Cohort identity is durable and never inferred from the current member
    /// list.  Zero is the bootstrap cohort.
    #[serde(default)]
    pub cohort_id: u64,
    /// Optional Fly identity metadata retained with the membership record so
    /// the autoscaler can verify the exact resource it prepared before the
    /// cohort activation CAS.
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub machine_id: String,
    #[serde(default)]
    pub volume_id: String,
    #[serde(default)]
    pub ordinal: u64,
    /// Storage tier advertised by the provider (for example `hot` or
    /// `archive`). It is part of the durable member identity used by the
    /// placement manifest.
    #[serde(default)]
    pub tier: String,
    /// Maximum encoded append payload accepted by this member's cohort. Zero
    /// means no explicit limit for legacy members.
    #[serde(default)]
    pub max_append_bytes: u64,
}

/// Optional provider identity carried with a joining request. The direct
/// gateway does not need these fields to route an append, but retaining them
/// in the durable membership document lets an autoscaler verify that the
/// exact Fly Machine and volume it prepared are the ones being activated.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableMemberIdentity {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub machine_id: String,
    #[serde(default)]
    pub volume_id: String,
    #[serde(default)]
    pub ordinal: u64,
    /// Storage tier advertised by the joining volume.
    #[serde(default)]
    pub tier: String,
    /// Maximum encoded append payload advertised by the joining volume.
    /// Zero means that the member does not impose an explicit limit.
    #[serde(default)]
    pub max_append_bytes: u64,
}

impl DurableMember {
    fn node(&self) -> ReplicaNode {
        ReplicaNode::new(self.id.clone(), self.url.clone())
    }
}

/// Lifecycle of one immutable replica cohort.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CohortStatus {
    /// Fewer than three members are ready; this cohort is never a write
    /// target.
    Joining,
    /// Exactly three members are active and receive new segments.
    #[default]
    Active,
    /// No new segments are assigned, but members remain available for
    /// historical recovery and in-cohort repair.
    Draining,
    /// Retained as historical metadata after all live members are retired.
    Retired,
}

/// Durable membership for one immutable three-member write cohort.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableCohort {
    pub id: u64,
    /// Stable member ids, in deterministic order.  A cohort is complete only
    /// when this list contains exactly three non-removed members.
    pub members: Vec<String>,
    #[serde(default)]
    pub status: CohortStatus,
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub max_append_bytes: u64,
}

/// Immutable stream-to-cohort routing boundary.
///
/// Ranges are inclusive.  `end_lsn` is optional only for forward-compatible
/// readers; the gateway currently persists finite singleton ranges so a later
/// activation can add a boundary without mutating an earlier range.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamSegment {
    pub start_lsn: u64,
    #[serde(default)]
    pub end_lsn: Option<u64>,
    pub cohort_id: u64,
    /// Exact immutable member ids selected for this segment.  Keeping the set
    /// in the manifest prevents a coordinator from silently changing a
    /// historical segment when membership is repaired.
    #[serde(default)]
    pub member_ids: Vec<String>,
    /// Digest of the exact member identities and URLs in `member_ids`.
    #[serde(default)]
    pub member_hash: String,
    /// Writer epoch observed at the segment boundary.
    #[serde(default)]
    pub writer_epoch: u64,
    /// Manifest revision that published this segment.
    #[serde(default)]
    pub manifest_revision: u64,
    /// Host placement epoch that must be presented to every member of this
    /// immutable range. It is separate from `manifest_revision` because a
    /// transition fence consumes an epoch before the successor route is
    /// published.
    #[serde(default)]
    pub placement_epoch: u64,
    /// Deterministic idempotency key for the manifest operation that created
    /// or cut over this segment.
    #[serde(default)]
    pub operation_id: String,
    /// Capacity policy copied from the selected immutable cohort at the
    /// boundary. It remains stable if later members advertise another tier.
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub max_append_bytes: u64,
}

/// Durable per-volume control state. Every successful mutation is written to
/// a temporary file, synced, atomically renamed, and followed by a directory
/// sync. The membership epoch thus crosses a process restart as one atomic
/// state transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableControlState {
    pub version: u8,
    pub membership_epoch: u64,
    /// Revision of the replicated routing manifest.  Membership epochs and
    /// manifest revisions advance independently so a route CAS cannot be
    /// mistaken for a membership transition.
    #[serde(default)]
    pub manifest_revision: u64,
    /// Digest of the complete routing manifest at `manifest_revision`.
    #[serde(default)]
    pub manifest_digest: String,
    /// Idempotency key that produced `manifest_revision`.
    #[serde(default)]
    pub manifest_operation_id: String,
    /// Writer epoch and cohort binding carried by the current manifest.
    #[serde(default)]
    pub manifest_writer_epoch: u64,
    #[serde(default)]
    pub manifest_cohort_id: u64,
    #[serde(default)]
    pub manifest_member_hash: String,
    /// Explicit cohort that receives the next immutable route. Zero keeps
    /// the legacy rendezvous selector until an online activation publishes a
    /// target. Historical stream segments never consult this field.
    #[serde(default)]
    pub write_cohort_id: u64,
    /// Authority generation for the object-store control head. Legacy local
    /// control files start at generation one and are migrated on first open.
    #[serde(default)]
    pub control_authority_epoch: u64,
    /// Object-store control-head revision/digest last adopted by this cache.
    #[serde(default)]
    pub control_head_revision: u64,
    #[serde(default)]
    pub control_head_digest: String,
    /// Cohort currently allowed to answer metadata CAS requests. Zero keeps
    /// the bootstrap control cohort until an authority handoff commits.
    #[serde(default)]
    pub control_authority_cohort_id: u64,
    /// Once a quorum has durably frozen metadata, legacy membership and
    /// manifest CAS requests remain rejected across restart. Record appends
    /// are independent and continue through the placement protocol.
    #[serde(default)]
    pub metadata_freeze: Option<authority::MetadataFreeze>,
    /// Set only after the placement sidecar has been durably initialized from
    /// legacy routes (or an empty legacy control state). This marker lives in
    /// the control document so deleting the sidecar after migration cannot
    /// silently recreate epoch-zero admission gates from stale routes.
    #[serde(default)]
    pub placement_initialized: bool,
    /// All members, including joining/draining/tombstoned members.
    #[serde(default)]
    pub members: BTreeMap<String, DurableMember>,
    /// Idempotency records for membership operations. Values are serialized
    /// `MembershipSnapshot`s so retries return the same epoch and member set.
    #[serde(default)]
    pub operations: BTreeMap<String, String>,
    /// Idempotency records for replicated manifest CAS operations.
    #[serde(default)]
    pub manifest_operations: BTreeMap<String, String>,
    /// Immutable cohort definitions.  Older control documents omit this
    /// field and are migrated to cohort zero on open.
    #[serde(default)]
    pub cohorts: BTreeMap<u64, DurableCohort>,
    /// Ordered, immutable stream ranges.  A range is never rewritten when a
    /// new cohort is activated; a new range is appended at the cutover LSN.
    #[serde(default)]
    pub stream_segments: BTreeMap<String, Vec<StreamSegment>>,
}

impl Default for DurableControlState {
    fn default() -> Self {
        Self {
            version: CONTROL_STATE_VERSION,
            membership_epoch: 0,
            manifest_revision: 0,
            manifest_digest: String::new(),
            manifest_operation_id: String::new(),
            manifest_writer_epoch: 0,
            manifest_cohort_id: 0,
            manifest_member_hash: String::new(),
            write_cohort_id: 0,
            control_authority_epoch: 1,
            control_head_revision: 0,
            control_head_digest: String::new(),
            control_authority_cohort_id: 0,
            metadata_freeze: None,
            placement_initialized: false,
            members: BTreeMap::new(),
            operations: BTreeMap::new(),
            manifest_operations: BTreeMap::new(),
            cohorts: BTreeMap::new(),
            stream_segments: BTreeMap::new(),
        }
    }
}

/// Public membership response used by the direct control API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MembershipSnapshot {
    pub membership_epoch: u64,
    #[serde(default)]
    pub manifest_revision: u64,
    #[serde(default)]
    pub manifest_digest: String,
    #[serde(default)]
    pub write_cohort_id: u64,
    #[serde(default)]
    pub control_authority_epoch: u64,
    #[serde(default)]
    pub control_head_revision: u64,
    #[serde(default)]
    pub control_head_digest: String,
    #[serde(default)]
    pub control_authority_cohort_id: u64,
    #[serde(default)]
    pub metadata_freeze: Option<authority::MetadataFreeze>,
    #[serde(default)]
    pub manifest: Option<ReplicaManifest>,
    pub members: Vec<DurableMember>,
    #[serde(default)]
    pub cohorts: Vec<DurableCohort>,
    #[serde(default)]
    pub stream_segments: BTreeMap<String, Vec<StreamSegment>>,
}

impl MembershipSnapshot {
    fn from_state(state: &DurableControlState) -> Self {
        Self {
            membership_epoch: state.membership_epoch,
            manifest_revision: state.manifest_revision,
            manifest_digest: state.manifest_digest.clone(),
            write_cohort_id: state.write_cohort_id,
            control_authority_epoch: state.control_authority_epoch,
            control_head_revision: state.control_head_revision,
            control_head_digest: state.control_head_digest.clone(),
            control_authority_cohort_id: state.control_authority_cohort_id,
            metadata_freeze: state.metadata_freeze.clone(),
            manifest: Some(manifest_from_state(state)),
            members: state.members.values().cloned().collect(),
            cohorts: state.cohorts.values().cloned().collect(),
            stream_segments: state.stream_segments.clone(),
        }
    }

    fn active_nodes(&self) -> Vec<ReplicaNode> {
        self.members
            .iter()
            .filter(|member| {
                member.status == MemberStatus::Active
                    && (self.cohorts.is_empty()
                        || self.cohorts.iter().any(|cohort| {
                            cohort.id == member.cohort_id
                                && cohort.status == CohortStatus::Active
                                && cohort.members.len() == REPLICATION_FACTOR
                        }))
            })
            .map(DurableMember::node)
            .collect()
    }

    fn all_nodes(&self) -> Vec<ReplicaNode> {
        self.members
            .iter()
            .filter(|member| member.status != MemberStatus::Removed)
            .map(DurableMember::node)
            .collect()
    }
}

/// Replicated routing manifest returned by a control-volume CAS.  A manifest
/// revision is a quorum-committed value, not a coordinator-local sequence;
/// two stateless coordinators can therefore publish at most one conflicting
/// value for a revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaManifest {
    pub version: u8,
    pub revision: u64,
    pub digest: String,
    /// Highest writer epoch represented by this manifest.
    #[serde(default)]
    pub writer_epoch: u64,
    /// Cohort selected for new routes at this revision.
    #[serde(default)]
    pub cohort_id: u64,
    /// Immutable member-set digest for `cohort_id`.
    #[serde(default)]
    pub member_set_hash: String,
    /// Explicit cohort selected for future route publication. This is
    /// separate from each immutable segment's cohort binding.
    #[serde(default)]
    pub write_cohort_id: u64,
    pub stream_segments: BTreeMap<String, Vec<StreamSegment>>,
    #[serde(default)]
    pub operation_id: String,
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub max_append_bytes: u64,
}

/// Compatibility name for callers that refer to one manifest read as a
/// snapshot.
pub type ManifestSnapshot = ReplicaManifest;

impl ReplicaManifest {
    /// Validates the manifest's self-authenticating content fields.
    pub fn validate(&self) -> Result<(), ReplicaError> {
        if self.version != MANIFEST_VERSION {
            return Err(ReplicaError::Protocol(
                "unsupported replica placement manifest version".to_owned(),
            ));
        }
        if self.revision == 0 {
            // Revision zero is only the empty bootstrap value. It must not
            // carry an operation or routing data that could be replayed.
            if !self.operation_id.is_empty()
                || !self.stream_segments.is_empty()
                || self.writer_epoch != 0
                || self.cohort_id != 0
                || !self.member_set_hash.is_empty()
                || self.write_cohort_id != 0
                || !self.tier.is_empty()
                || self.max_append_bytes != 0
            {
                return Err(ReplicaError::Protocol(
                    "non-empty placement manifest has zero revision".to_owned(),
                ));
            }
            return Ok(());
        }
        if self.operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "placement manifest operation_id must not be empty".to_owned(),
            ));
        }
        if self.member_set_hash.is_empty() {
            return Err(ReplicaError::Protocol(
                "placement manifest is missing its member-set hash".to_owned(),
            ));
        }
        if self.digest != manifest_digest(&self.stream_segments) {
            return Err(ReplicaError::Protocol(
                "placement manifest digest does not match its ranges".to_owned(),
            ));
        }
        validate_stream_segments(&self.stream_segments)
    }

    /// Returns a stable digest of the complete manifest value, excluding its
    /// stored `digest` field. This is useful when comparing independent
    /// control stores and intentionally includes operation and cohort
    /// bindings in addition to the stream ranges.
    pub fn content_digest(&self) -> String {
        let mut value = self.clone();
        value.digest.clear();
        let encoded = serde_json::to_vec(&value).unwrap_or_default();
        hex::encode(Sha256::digest(encoded))
    }
}

/// Returns the canonical control-state path on a mounted volume.
#[must_use]
pub fn control_state_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(CONTROL_STATE_FILENAME)
}

/// Compatibility alias with the longer name used by operators.
#[must_use]
pub fn replica_control_state_path(data_dir: impl AsRef<Path>) -> PathBuf {
    control_state_path(data_dir)
}

/// A small durable CAS store shared by the storage and gateway halves of one
/// combined Machine.
pub struct DurableControl {
    path: PathBuf,
    state: Mutex<DurableControlState>,
    /// The manifest last built and validated from `state`. Reads outnumber
    /// writes by orders of magnitude on the append path and each rebuild
    /// serializes and hashes the whole segment map twice, so a member serves
    /// repeated reads of one revision from this copy. Every write clears it.
    manifest_cache: Mutex<Option<ReplicaManifest>>,
}

impl DurableControl {
    /// Opens or initializes a volume control document. Existing state is
    /// authoritative; `initial_members` is used only for first bootstrap.
    pub fn open(
        path: impl AsRef<Path>,
        initial_members: &[ReplicaNode],
    ) -> Result<Arc<Self>, ReplicaError> {
        let path = path.as_ref().to_owned();
        let existed = path.exists();
        let parent = path.parent().ok_or_else(|| {
            ReplicaError::NodeStorage("control state has no parent directory".to_owned())
        })?;
        std::fs::create_dir_all(parent)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let mut state = match std::fs::read(&path) {
            Ok(bytes) => {
                serde_json::from_slice::<DurableControlState>(&bytes).map_err(|error| {
                    ReplicaError::NodeStorage(format!("invalid replica control state: {error}"))
                })?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let mut state = DurableControlState::default();
                let mut ids = BTreeSet::new();
                let mut urls = BTreeSet::new();
                for (index, node) in initial_members.iter().enumerate() {
                    validate_node(node)?;
                    if !ids.insert(node.id.clone()) || !urls.insert(node.url.clone()) {
                        return Err(ReplicaError::NodeStorage(
                            "direct membership identities and URLs must be unique".to_owned(),
                        ));
                    }
                    state.members.insert(
                        node.id.clone(),
                        DurableMember {
                            id: node.id.clone(),
                            url: node.url.clone(),
                            status: MemberStatus::Active,
                            // Bootstrap callers provide members grouped by
                            // cohort. Preserve that order in the durable
                            // seed so a fresh stateless coordinator can
                            // identify the original control cohort before it
                            // adopts the replicated route manifest.
                            cohort_id: (index / REPLICATION_FACTOR) as u64,
                            ..DurableMember::default()
                        },
                    );
                }
                if !state.members.is_empty() {
                    state.membership_epoch = 1;
                }
                state
            }
            Err(error) => return Err(ReplicaError::NodeStorage(error.to_string())),
        };
        if state.version != CONTROL_STATE_VERSION {
            return Err(ReplicaError::NodeStorage(
                "unsupported replica control state version".to_owned(),
            ));
        }
        let normalized = reconcile_cohorts(&mut state)?;
        validate_control_state(&state)?;
        let store = Arc::new(Self {
            path,
            state: Mutex::new(state),
            manifest_cache: Mutex::new(None),
        });
        // A newly-created file is made durable before its owner can serve.
        if !existed || normalized {
            let state = store
                .state
                .lock()
                .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?
                .clone();
            persist_control_state(&store.path, &state)?;
        }
        Ok(store)
    }

    /// Returns the backing path, useful for volume backup and tests.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns one coherent state snapshot.
    pub fn state(&self) -> Result<DurableControlState, ReplicaError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))
    }

    /// Persists the placement-sidecar migration checkpoint after the sidecar
    /// itself has reached durable storage. This ordering makes a crash either
    /// recover from the exact sidecar or retry the same legacy bootstrap; it
    /// can never advertise initialized placement while the sidecar is absent.
    fn mark_placement_initialized(&self) -> Result<(), ReplicaError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        if state.placement_initialized {
            return Ok(());
        }
        let mut next = state.clone();
        next.placement_initialized = true;
        validate_control_state(&next)?;
        persist_control_state(&self.path, &next)?;
        *state = next;
        self.clear_manifest_cache();
        Ok(())
    }

    /// Returns the direct membership snapshot.
    pub fn membership(&self) -> Result<MembershipSnapshot, ReplicaError> {
        self.with_state(MembershipSnapshot::from_state)
    }

    /// Installs a complete object-store control head into this local cache.
    /// The head is accepted only when it is newer in the authority/revision
    /// order; equal generations must name the same metadata digest. This is
    /// the restart path after the former control cohort has been retired.
    pub fn adopt_control_head(&self, head: &ControlHead) -> Result<(), ReplicaError> {
        let metadata = &head.metadata;
        let manifest = &metadata.manifest;
        manifest.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        if head.authority_epoch < state.control_authority_epoch
            || (head.authority_epoch == state.control_authority_epoch
                && head.revision < state.control_head_revision)
        {
            return Err(ReplicaError::LsnConflict);
        }
        if head.authority_epoch == state.control_authority_epoch
            && head.revision == state.control_head_revision
            && !state.control_head_digest.is_empty()
            && state.control_head_digest != head.metadata_digest
        {
            return Err(ReplicaError::LsnConflict);
        }
        let mut next = state.clone();
        next.membership_epoch = metadata.membership_epoch;
        next.members = metadata.members.clone();
        next.cohorts = metadata.cohorts.clone();
        next.write_cohort_id = manifest.write_cohort_id;
        next.manifest_revision = manifest.revision;
        next.manifest_digest = manifest.digest.clone();
        next.manifest_operation_id = manifest.operation_id.clone();
        next.manifest_writer_epoch = manifest.writer_epoch;
        next.manifest_cohort_id = manifest.cohort_id;
        next.manifest_member_hash = manifest.member_set_hash.clone();
        next.stream_segments = manifest.stream_segments.clone();
        next.control_authority_epoch = head.authority_epoch;
        next.control_head_revision = head.revision;
        next.control_head_digest = head.metadata_digest.clone();
        validate_control_state(&next)?;
        persist_control_state(&self.path, &next)?;
        *state = next;
        self.clear_manifest_cache();
        Ok(())
    }

    /// Carries the object-head authority receipt into a peer's local cache
    /// after it has adopted the exact membership/manifest snapshot. The
    /// object store remains the authority; these fields only make stale local
    /// control files reject a later metadata CAS.
    pub fn adopt_authority_fields(
        &self,
        authority_epoch: u64,
        head_revision: u64,
        head_digest: &str,
        authority_cohort_id: u64,
    ) -> Result<(), ReplicaError> {
        if authority_epoch == 0 || head_revision == 0 || head_digest.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "invalid control-head authority receipt".to_owned(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        if authority_epoch < state.control_authority_epoch
            || (authority_epoch == state.control_authority_epoch
                && head_revision < state.control_head_revision)
        {
            return Err(ReplicaError::LsnConflict);
        }
        if authority_epoch == state.control_authority_epoch
            && head_revision == state.control_head_revision
            && !state.control_head_digest.is_empty()
            && state.control_head_digest != head_digest
        {
            return Err(ReplicaError::LsnConflict);
        }
        let mut next = state.clone();
        next.control_authority_epoch = authority_epoch;
        next.control_head_revision = head_revision;
        next.control_head_digest = head_digest.to_owned();
        next.control_authority_cohort_id = authority_cohort_id;
        persist_control_state(&self.path, &next)?;
        *state = next;
        Ok(())
    }

    /// Reads the control document under its lock without cloning it. The
    /// closure must not block or re-enter the control store; every read on
    /// the append path goes through here so a large document costs one
    /// traversal rather than one full copy per read.
    pub fn with_state<T>(
        &self,
        read: impl FnOnce(&DurableControlState) -> T,
    ) -> Result<T, ReplicaError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        Ok(read(&state))
    }

    /// Returns a previously recorded idempotent membership operation, if any.
    pub fn operation_snapshot(
        &self,
        operation_id: &str,
    ) -> Result<Option<MembershipSnapshot>, ReplicaError> {
        self.with_state(|state| {
            state
                .operations
                .get(operation_id)
                .map(|encoded| {
                    serde_json::from_str(encoded)
                        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
                })
                .transpose()
        })?
    }

    /// Returns the currently active members.
    pub fn active_members(&self) -> Result<Vec<ReplicaNode>, ReplicaError> {
        Ok(self.membership()?.active_nodes())
    }

    /// Returns all non-removed members, including joining and draining ones.
    pub fn all_members(&self) -> Result<Vec<ReplicaNode>, ReplicaError> {
        Ok(self.membership()?.all_nodes())
    }

    /// Atomically applies a membership CAS and records an operation result.
    pub fn cas_membership(
        &self,
        expected_epoch: u64,
        members: Vec<DurableMember>,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.cas_membership_document(expected_epoch, members, None, None, operation_id)
    }

    /// Applies a membership CAS together with the durable cohort and stream
    /// routing document carried by a peer coordinator.  Membership and route
    /// metadata cross the same atomic rename, so a restarted coordinator
    /// cannot observe a new cohort without its routing history.
    pub fn cas_membership_document(
        &self,
        expected_epoch: u64,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.cas_membership_document_with_manifest(
            expected_epoch,
            None,
            false,
            members,
            cohorts,
            stream_segments,
            None,
            None,
            operation_id,
        )
    }

    /// Membership CAS variant carrying the latest replicated manifest. A
    /// membership update must never regress a route that won a newer manifest
    /// CAS while this operation was in flight.
    #[allow(clippy::too_many_arguments)]
    pub fn cas_membership_document_with_manifest(
        &self,
        expected_epoch: u64,
        target_epoch: Option<u64>,
        repair: bool,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        manifest_revision: Option<u64>,
        manifest_digest_value: Option<String>,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.cas_membership_document_with_options(
            expected_epoch,
            target_epoch,
            repair,
            members,
            cohorts,
            stream_segments,
            manifest_revision,
            manifest_digest_value,
            None,
            false,
            false,
            operation_id,
        )
    }

    /// Applies an additive membership transition without acquiring the
    /// cell-wide maintenance fence. `write_cohort_id` is committed in the
    /// same durable document as the member/cohort map, so a new writer target
    /// cannot become visible independently of the prepared cohort.
    #[allow(clippy::too_many_arguments)]
    pub fn cas_membership_document_online(
        &self,
        expected_epoch: u64,
        target_epoch: Option<u64>,
        repair: bool,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        manifest_revision: Option<u64>,
        manifest_digest_value: Option<String>,
        write_cohort_id: Option<u64>,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.cas_membership_document_with_options(
            expected_epoch,
            target_epoch,
            repair,
            members,
            cohorts,
            stream_segments,
            manifest_revision,
            manifest_digest_value,
            write_cohort_id,
            true,
            false,
            operation_id,
        )
    }

    /// Applies a whole-cohort replacement transition. The route map must
    /// already contain a successor range for every open source range; this
    /// mode is separate from additive online activation so an ordinary join
    /// can never retire an existing authority by accident.
    #[allow(clippy::too_many_arguments)]
    pub fn cas_membership_document_online_replacement(
        &self,
        expected_epoch: u64,
        target_epoch: Option<u64>,
        repair: bool,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        manifest_revision: Option<u64>,
        manifest_digest_value: Option<String>,
        write_cohort_id: Option<u64>,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.cas_membership_document_with_options(
            expected_epoch,
            target_epoch,
            repair,
            members,
            cohorts,
            stream_segments,
            manifest_revision,
            manifest_digest_value,
            write_cohort_id,
            true,
            true,
            operation_id,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn cas_membership_document_with_options(
        &self,
        expected_epoch: u64,
        target_epoch: Option<u64>,
        repair: bool,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        manifest_revision: Option<u64>,
        manifest_digest_value: Option<String>,
        write_cohort_id: Option<u64>,
        online: bool,
        replacement: bool,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "membership operation_id must not be empty".to_owned(),
            ));
        }
        let mut next_members = BTreeMap::new();
        for member in members {
            let node = member.node();
            validate_node(&node)?;
            if next_members.insert(member.id.clone(), member).is_some() {
                return Err(ReplicaError::LsnConflict);
            }
        }
        validate_members_state(&next_members)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        // A normal retry returns the durable idempotency result before
        // comparing epochs. Repair broadcasts use the exact-state check below
        // instead: an old operation entry must not make a peer that has since
        // advanced appear to have adopted the current snapshot.
        if !repair && let Some(previous) = state.operations.get(operation_id) {
            return serde_json::from_str(previous)
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()));
        }
        if repair
            && target_epoch == Some(state.membership_epoch)
            && state.members == next_members
            && cohorts
                .as_ref()
                .is_none_or(|value| state.cohorts.values().cloned().collect::<Vec<_>>() == *value)
            && stream_segments
                .as_ref()
                .is_none_or(|value| state.stream_segments == *value)
            && write_cohort_id.is_none_or(|value| state.write_cohort_id == value)
        {
            return Ok(MembershipSnapshot::from_state(&state));
        }
        // Once a quorum has persisted the metadata-only authority freeze, the
        // legacy volume CAS is fenced across restarts. Idempotent replays and
        // exact repair adoption above remain safe and return their durable
        // result before this gate.
        if state.metadata_freeze.is_some() || state.control_head_revision != 0 {
            return Err(ReplicaError::WriterFenced);
        }
        if state.membership_epoch != expected_epoch {
            return Err(ReplicaError::LsnConflict);
        }
        // Older callers only send `id`, `url`, and `status`. Preserve the
        // durable cohort assignment for known members and place a group of
        // new default-zero members into one fresh (or already joining)
        // cohort. This keeps the wire-compatible CAS endpoint from silently
        // moving an existing stream back into cohort zero.
        if cohorts.is_none() {
            normalize_members_for_cas(&mut next_members, &state)?;
        }
        let may_adopt_authoritative_bootstrap = may_adopt_authoritative_bootstrap(&state, repair);
        for (id, member) in &next_members {
            if let Some(previous) = state.members.get(id) {
                if previous.status == MemberStatus::Removed
                    && member.status != MemberStatus::Removed
                {
                    return Err(ReplicaError::LsnConflict);
                }
                if previous.cohort_id != member.cohort_id && !may_adopt_authoritative_bootstrap {
                    return Err(ReplicaError::LsnConflict);
                }
            }
        }
        validate_members_state(&next_members)?;
        let adjacent_epoch = expected_epoch
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Protocol("membership epoch exhausted".to_owned()))?;
        let next_epoch = target_epoch.unwrap_or(adjacent_epoch);
        if next_epoch < adjacent_epoch || (!repair && next_epoch != adjacent_epoch) {
            return Err(ReplicaError::LsnConflict);
        }
        let mut next = state.clone();
        next.membership_epoch = next_epoch;
        next.members = next_members;
        if let Some(cohorts) = cohorts {
            next.cohorts = cohorts
                .into_iter()
                .map(|cohort| (cohort.id, cohort))
                .collect();
        }
        if let Some(write_cohort_id) = write_cohort_id {
            if write_cohort_id == 0 {
                return Err(ReplicaError::LsnConflict);
            }
            next.write_cohort_id = write_cohort_id;
        }
        let current_digest = if state.manifest_digest.is_empty() {
            manifest_digest(&state.stream_segments)
        } else {
            state.manifest_digest.clone()
        };
        let (next_revision, next_segments) = match stream_segments {
            Some(incoming_segments) => {
                let incoming_revision = manifest_revision.unwrap_or(state.manifest_revision);
                let incoming_digest = manifest_digest_value
                    .filter(|digest| !digest.is_empty())
                    .unwrap_or_else(|| manifest_digest(&incoming_segments));
                // Revision zero is the pre-manifest bootstrap format.  Older
                // coordinators serialized its empty digest as `""`, while
                // current code derives the canonical digest of the empty
                // range map.  Treat those two encodings as the same only at
                // that exact bootstrap boundary; a nonzero revision still
                // requires its authenticated digest to match byte-for-byte.
                let incoming_digest = if incoming_revision == 0
                    && incoming_segments.is_empty()
                    && state.stream_segments.is_empty()
                    && state.manifest_digest.is_empty()
                {
                    current_digest.clone()
                } else {
                    incoming_digest
                };
                if incoming_revision == state.manifest_revision {
                    if incoming_digest != current_digest
                        || incoming_segments != state.stream_segments
                    {
                        return Err(ReplicaError::LsnConflict);
                    }
                    (state.manifest_revision, state.stream_segments.clone())
                } else if incoming_revision > state.manifest_revision {
                    if incoming_digest != manifest_digest(&incoming_segments) {
                        return Err(ReplicaError::LsnConflict);
                    }
                    validate_manifest_segments(&next, &incoming_segments, incoming_revision)?;
                    if !repair {
                        validate_manifest_transition(&state.stream_segments, &incoming_segments)?;
                    }
                    (incoming_revision, incoming_segments)
                } else {
                    // A route CAS won while this membership update was in
                    // flight. Keep the newer route and commit membership
                    // without regressing the manifest.
                    (state.manifest_revision, state.stream_segments.clone())
                }
            }
            None => (
                state.manifest_revision.checked_add(1).ok_or_else(|| {
                    ReplicaError::Protocol("manifest revision exhausted".to_owned())
                })?,
                state.stream_segments.clone(),
            ),
        };
        // Make the candidate route visible while deriving cohort lifecycle.
        // Activation may happen while the old tail is still open; in that
        // case it remains retained as an active source until the first append
        // durably seals the tail and creates the cutover range. A route CAS
        // carries the sealed predecessor, allowing reconciliation to drain
        // the old cohort atomically with the new manifest.
        next.stream_segments = next_segments.clone();
        if online && !may_adopt_authoritative_bootstrap {
            if replacement {
                validate_online_replacement_transition(&state, &next)?;
            } else {
                validate_online_membership_transition(&state, &next)?;
            }
        }
        // `reconcile_cohorts` validates an existing manifest digest against
        // the candidate ranges.  When this CAS carries a newer route, the
        // cloned state still contains the predecessor digest; stage the
        // already-validated candidate metadata before reconciliation so a
        // lagging coordinator can accept the same immutable manifest rather
        // than rejecting it as internally inconsistent.
        if next_revision > state.manifest_revision {
            next.manifest_revision = next_revision;
            next.manifest_digest = manifest_digest(&next_segments);
        }
        reconcile_cohorts(&mut next)?;
        let (writer_epoch, cohort_id, member_set_hash) =
            manifest_metadata_for_state(&next, &next_segments);
        let (manifest_tier, manifest_max_append_bytes) =
            manifest_policy_for_state(&next, &next_segments, cohort_id);
        let next_manifest = ReplicaManifest {
            version: MANIFEST_VERSION,
            revision: next_revision,
            digest: manifest_digest(&next_segments),
            writer_epoch,
            cohort_id,
            member_set_hash: member_set_hash.clone(),
            write_cohort_id: next.write_cohort_id,
            stream_segments: next_segments.clone(),
            operation_id: operation_id.to_owned(),
            tier: manifest_tier,
            max_append_bytes: manifest_max_append_bytes,
        };
        if next_revision > state.manifest_revision {
            next.manifest_revision = next_revision;
            next.manifest_digest = next_manifest.digest.clone();
            next.manifest_operation_id = operation_id.to_owned();
            next.manifest_writer_epoch = writer_epoch;
            next.manifest_cohort_id = cohort_id;
            next.manifest_member_hash = member_set_hash;
            next.stream_segments = next_segments;
            next.manifest_operations.insert(
                operation_id.to_owned(),
                serde_json::to_string(&next_manifest)
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
            );
        } else if write_cohort_id.is_some() {
            // The route digest and revision remain unchanged when activation
            // only changes the future writer target. Persist the target
            // binding and a new idempotency record nonetheless so a cached
            // manifest cannot steer a writer back to the retired cohort.
            next.manifest_operation_id = operation_id.to_owned();
            next.manifest_writer_epoch = writer_epoch;
            next.manifest_cohort_id = cohort_id;
            next.manifest_member_hash = member_set_hash;
            next.manifest_operations.insert(
                operation_id.to_owned(),
                serde_json::to_string(&next_manifest)
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
            );
        }
        validate_control_state(&next)?;
        if next
            .cohorts
            .values()
            .filter(|cohort| cohort.status == CohortStatus::Active)
            .count()
            == 0
            || next
                .cohorts
                .values()
                .filter(|cohort| cohort.status == CohortStatus::Active)
                .any(|cohort| cohort.members.len() != REPLICATION_FACTOR)
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let snapshot = MembershipSnapshot::from_state(&next);
        next.operations.insert(
            operation_id.to_owned(),
            serde_json::to_string(&snapshot)
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
        );
        prune_operation_records(&mut next);
        persist_control_state(&self.path, &next)?;
        *state = next;
        self.clear_manifest_cache();
        Ok(snapshot)
    }

    /// Returns the newest complete active cohort.  A joining or partial
    /// cohort is deliberately invisible to the write path.
    pub fn newest_active_cohort(&self) -> Result<Option<DurableCohort>, ReplicaError> {
        self.with_state(|state| {
            state
                .cohorts
                .values()
                .filter(|cohort| {
                    cohort.status == CohortStatus::Active
                        && cohort.members.len() == REPLICATION_FACTOR
                })
                .max_by_key(|cohort| cohort.id)
                .cloned()
        })
    }

    /// Returns the durable route for one stream LSN, if a cutover segment has
    /// already been published.
    pub fn stream_segment(
        &self,
        stream: &str,
        lsn: u64,
    ) -> Result<Option<StreamSegment>, ReplicaError> {
        self.with_state(|state| {
            state.stream_segments.get(stream).and_then(|segments| {
                segments
                    .iter()
                    .find(|segment| {
                        segment.start_lsn <= lsn && segment.end_lsn.is_none_or(|end| lsn <= end)
                    })
                    .cloned()
            })
        })
    }

    /// Returns the complete locally persisted routing manifest. The caller
    /// must establish a quorum when the document is used for admission; this
    /// method is intentionally just a volume read for node replication.
    pub fn manifest(&self) -> Result<ReplicaManifest, ReplicaError> {
        self.with_state(|state| {
            if let Ok(cache) = self.manifest_cache.lock()
                && let Some(cached) = cache.as_ref()
                && cached.revision == state.manifest_revision
                && cached.digest == state.manifest_digest
                && cached.operation_id == state.manifest_operation_id
            {
                return Ok(cached.clone());
            }
            let manifest = manifest_from_state(state);
            validate_manifest_against_state(state, &manifest)?;
            if let Ok(mut cache) = self.manifest_cache.lock() {
                *cache = Some(manifest.clone());
            }
            Ok(manifest)
        })?
    }

    /// Drops the cached manifest. Called under the state lock by every
    /// write, including ones that leave the revision alone but change the
    /// cohort policy the manifest reports.
    fn clear_manifest_cache(&self) {
        if let Ok(mut cache) = self.manifest_cache.lock() {
            *cache = None;
        }
    }

    /// Applies one full-manifest CAS. Every control cohort member evaluates
    /// the same expected revision and digest under its volume lock; a gateway
    /// may use the resulting two-of-three quorum as the only route admission
    /// evidence. `repair` is reserved for the losing third write after a
    /// quorum has already been observed.
    pub fn cas_manifest(
        &self,
        expected_revision: u64,
        expected_digest: &str,
        stream_segments: BTreeMap<String, Vec<StreamSegment>>,
        operation_id: &str,
        repair: bool,
    ) -> Result<ReplicaManifest, ReplicaError> {
        self.cas_manifest_with_cutover(
            expected_revision,
            expected_digest,
            stream_segments,
            operation_id,
            repair,
            None,
        )
    }

    /// Applies a manifest CAS with the durable LSN at which an open tail was
    /// sealed. A caller that attempts to close an open range without this
    /// explicit boundary is rejected; this prevents a coordinator from
    /// inventing a historical cutover that no append has reached.
    pub fn cas_manifest_with_cutover(
        &self,
        expected_revision: u64,
        expected_digest: &str,
        stream_segments: BTreeMap<String, Vec<StreamSegment>>,
        operation_id: &str,
        repair: bool,
        cutover_lsn: Option<u64>,
    ) -> Result<ReplicaManifest, ReplicaError> {
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "manifest operation_id must not be empty".to_owned(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        if let Some(previous) = state.manifest_operations.get(operation_id) {
            return serde_json::from_str(previous)
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()));
        }
        if state.metadata_freeze.is_some() || state.control_head_revision != 0 {
            return Err(ReplicaError::WriterFenced);
        }
        if repair {
            // A repair request carries the already quorum-committed complete
            // target revision/digest. A data member can legitimately be
            // several revisions behind: membership propagation may have
            // completed before unrelated streams published later ranges.
            // Installing the complete, digest-bound manifest is safe and
            // avoids making the first append on that member fail merely
            // because it missed intermediate route revisions. It can never
            // roll a newer value backwards.
            let target_revision = expected_revision;
            let target_digest = manifest_digest(&stream_segments);
            if target_revision == 0
                || expected_digest != target_digest
                || state.manifest_revision > target_revision
            {
                return Err(ReplicaError::LsnConflict);
            }
            let current_digest = if state.manifest_digest.is_empty() {
                manifest_digest(&state.stream_segments)
            } else {
                state.manifest_digest.clone()
            };
            if state.manifest_revision == target_revision && current_digest == target_digest {
                return Ok(manifest_from_state(&state));
            }
            validate_manifest_segments(&state, &stream_segments, target_revision)?;
            let (writer_epoch, cohort_id, member_set_hash) =
                manifest_metadata_for_state(&state, &stream_segments);
            let (manifest_tier, manifest_max_append_bytes) =
                manifest_policy_for_state(&state, &stream_segments, cohort_id);
            let manifest = ReplicaManifest {
                version: MANIFEST_VERSION,
                revision: target_revision,
                digest: target_digest.clone(),
                writer_epoch,
                cohort_id,
                member_set_hash: member_set_hash.clone(),
                write_cohort_id: state.write_cohort_id,
                stream_segments: stream_segments.clone(),
                operation_id: operation_id.to_owned(),
                tier: manifest_tier,
                max_append_bytes: manifest_max_append_bytes,
            };
            let mut next = state.clone();
            next.manifest_revision = target_revision;
            next.manifest_digest = target_digest;
            next.manifest_operation_id = operation_id.to_owned();
            next.manifest_writer_epoch = writer_epoch;
            next.manifest_cohort_id = cohort_id;
            next.manifest_member_hash = member_set_hash;
            next.stream_segments = stream_segments;
            next.manifest_operations.insert(
                operation_id.to_owned(),
                serde_json::to_string(&manifest)
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
            );
            prune_operation_records(&mut next);
            validate_control_state(&next)?;
            persist_control_state(&self.path, &next)?;
            *state = next;
            self.clear_manifest_cache();
            return Ok(manifest);
        }
        let current_digest = if state.manifest_digest.is_empty() {
            manifest_digest(&state.stream_segments)
        } else {
            state.manifest_digest.clone()
        };
        if state.manifest_revision != expected_revision
            || (!expected_digest.is_empty() && current_digest != expected_digest)
        {
            return Err(ReplicaError::LsnConflict);
        }
        let next_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
        validate_manifest_segments(&state, &stream_segments, next_revision)?;
        validate_manifest_transition(&state.stream_segments, &stream_segments)?;
        validate_manifest_cutover(&state.stream_segments, &stream_segments, cutover_lsn)?;
        let digest = manifest_digest(&stream_segments);
        let (writer_epoch, cohort_id, member_set_hash) =
            manifest_metadata_for_state(&state, &stream_segments);
        let (manifest_tier, manifest_max_append_bytes) =
            manifest_policy_for_state(&state, &stream_segments, cohort_id);
        let manifest = ReplicaManifest {
            version: MANIFEST_VERSION,
            revision: next_revision,
            digest: digest.clone(),
            writer_epoch,
            cohort_id,
            member_set_hash: member_set_hash.clone(),
            write_cohort_id: state.write_cohort_id,
            stream_segments: stream_segments.clone(),
            operation_id: operation_id.to_owned(),
            tier: manifest_tier,
            max_append_bytes: manifest_max_append_bytes,
        };
        let mut next = state.clone();
        next.manifest_revision = next_revision;
        next.manifest_digest = digest;
        next.manifest_operation_id = operation_id.to_owned();
        next.manifest_writer_epoch = writer_epoch;
        next.manifest_cohort_id = cohort_id;
        next.manifest_member_hash = member_set_hash;
        next.stream_segments = stream_segments;
        next.manifest_operations.insert(
            operation_id.to_owned(),
            serde_json::to_string(&manifest)
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
        );
        prune_operation_records(&mut next);
        validate_control_state(&next)?;
        persist_control_state(&self.path, &next)?;
        *state = next;
        self.clear_manifest_cache();
        Ok(manifest)
    }

    /// Installs a manifest already proven by a remote two-of-three CAS. This
    /// is used by a stateless coordinator whose local cache volume is not one
    /// of the three control members; it never permits a lower revision.
    pub fn adopt_manifest(
        &self,
        manifest: &ReplicaManifest,
        force_same_revision: bool,
    ) -> Result<(), ReplicaError> {
        manifest.validate()?;
        if manifest.digest != manifest_digest(&manifest.stream_segments) {
            return Err(ReplicaError::LsnConflict);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        if manifest.revision < state.manifest_revision {
            return Err(ReplicaError::LsnConflict);
        }
        let current_digest = if state.manifest_digest.is_empty() {
            manifest_digest(&state.stream_segments)
        } else {
            state.manifest_digest.clone()
        };
        let divergent = current_digest != manifest.digest;
        if manifest.revision == state.manifest_revision && divergent && !force_same_revision {
            return Err(ReplicaError::LsnConflict);
        }
        validate_manifest_segments(&state, &manifest.stream_segments, manifest.revision)?;
        // A forced adoption is only used after a two-member control quorum
        // has proven the target value. It is allowed to replace a corrupt
        // same- or lower-revision local cache; a normal caller must still
        // satisfy the append-only transition rule.
        if !(force_same_revision && divergent) {
            validate_manifest_transition(&state.stream_segments, &manifest.stream_segments)?;
        }
        validate_write_cohort(&state, manifest.write_cohort_id)?;
        let (writer_epoch, cohort_id, member_set_hash) =
            manifest_metadata_for_state(&state, &manifest.stream_segments);
        let mut next = state.clone();
        next.write_cohort_id = manifest.write_cohort_id;
        next.manifest_revision = manifest.revision;
        next.manifest_digest = manifest.digest.clone();
        next.manifest_operation_id = manifest.operation_id.clone();
        next.manifest_writer_epoch = writer_epoch;
        next.manifest_cohort_id = cohort_id;
        next.manifest_member_hash = if manifest.member_set_hash.is_empty() {
            member_set_hash
        } else {
            manifest.member_set_hash.clone()
        };
        next.stream_segments = manifest.stream_segments.clone();
        next.manifest_operations.insert(
            manifest.operation_id.clone(),
            serde_json::to_string(manifest)
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
        );
        prune_operation_records(&mut next);
        validate_control_state(&next)?;
        persist_control_state(&self.path, &next)?;
        *state = next;
        self.clear_manifest_cache();
        Ok(())
    }
}

fn may_adopt_authoritative_bootstrap(state: &DurableControlState, repair: bool) -> bool {
    repair
        && state.membership_epoch <= 1
        && state.manifest_revision == 0
        && state.stream_segments.is_empty()
        && state.operations.is_empty()
        && state.manifest_operations.is_empty()
}

/// A manifest proven by the fixed control cohort together with the members
/// that returned exactly this value, so a subsequent route repair can skip
/// the members already known to hold it durably.
#[derive(Clone, Debug)]
struct QuorumManifest {
    /// The quorum-certified manifest.
    manifest: ReplicaManifest,
    /// Control members whose durable document equals `manifest`.
    agreed: BTreeSet<String>,
}

/// Least time the stragglers of a fan-out get once the answers it needs have
/// arrived. A healthy in-region member answers well inside this window; a
/// suspended or CPU-starved one is reported absent for the phase instead of
/// stalling the caller until the client timeout.
const FAN_OUT_STRAGGLER_GRACE: Duration = Duration::from_millis(100);

/// Time a fan-out keeps waiting for stragglers after its quorum answered: as
/// long again as the quorum took, and never less than
/// [`FAN_OUT_STRAGGLER_GRACE`]. Phases whose result improves with every
/// member (status observation, snapshot evidence) use this so a slightly
/// slower healthy member still contributes.
fn straggler_grace(quorum_elapsed: Duration) -> Duration {
    quorum_elapsed.max(FAN_OUT_STRAGGLER_GRACE)
}

/// Runs one request per node concurrently and returns the answers in node
/// order, `None` for a member that had not answered when the phase ended.
///
/// The phase ends when every member answered, or once `satisfied` holds for
/// the answers so far and, when `straggler_grace` is given, that grace has
/// elapsed. Straggler requests stay detached and complete on their own, so
/// a slow member still receives a repair or records the observation; the
/// caller simply no longer waits on it. Answers that arrived before the
/// phase ended are always included.
async fn fan_out<T, Fut>(
    nodes: &[ReplicaNode],
    request: impl Fn(ReplicaNode) -> Fut,
    satisfied: impl Fn(&[Option<T>]) -> bool,
    straggler_grace: Option<fn(Duration) -> Duration>,
) -> Vec<Option<T>>
where
    T: Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    for (index, node) in nodes.iter().cloned().enumerate() {
        let sender = sender.clone();
        let pending = request(node);
        tokio::spawn(async move {
            let _ = sender.send((index, pending.await));
        });
    }
    drop(sender);
    let started = Instant::now();
    let mut answers = std::iter::repeat_with(|| None)
        .take(nodes.len())
        .collect::<Vec<Option<T>>>();
    let mut deadline = None;
    let mut satisfied_at = None;
    loop {
        // Check before waiting: a phase whose quorum is already covered
        // (nothing pending, or enough members answered) must not block on
        // a member that never answers.
        if satisfied_at.is_none() && satisfied(&answers) {
            satisfied_at = Some(started.elapsed());
            match straggler_grace {
                Some(grace) => {
                    deadline = Some(tokio::time::Instant::now() + grace(started.elapsed()));
                }
                None => break,
            }
        }
        let next = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(next) => next,
                Err(_) => break,
            },
            None => receiver.recv().await,
        };
        let Some((index, answer)) = next else {
            break;
        };
        answers[index] = Some(answer);
    }
    while let Ok((index, answer)) = receiver.try_recv() {
        answers[index] = Some(answer);
    }
    answers
}

/// Finds the immutable range that owns one record in a manifest.
fn manifest_route(manifest: &ReplicaManifest, stream: &str, lsn: u64) -> Option<StreamSegment> {
    manifest
        .stream_segments
        .get(stream)
        .and_then(|segments| {
            segments.iter().find(|segment| {
                segment.start_lsn <= lsn && segment.end_lsn.is_none_or(|end| lsn <= end)
            })
        })
        .cloned()
}

/// A direct membership view backed by the same per-volume control document as
/// the local storage listener.
#[derive(Clone)]
struct DirectMembership {
    control: Arc<DurableControl>,
}

enum Membership {
    /// Immutable membership supplied by one deployment configuration. This
    /// remains useful for local tests and one-gateway installations.
    Static(Arc<RwLock<BTreeMap<String, ReplicaNode>>>),
    /// Direct Fly membership persisted in a per-volume control document.
    Direct(DirectMembership),
}

fn validate_members_state(members: &BTreeMap<String, DurableMember>) -> Result<(), ReplicaError> {
    let mut urls = BTreeSet::new();
    for (id, member) in members {
        if id != &member.id || member.id.trim().is_empty() {
            return Err(ReplicaError::NodeStorage(
                "direct membership id is invalid".to_owned(),
            ));
        }
        let node = member.node();
        validate_node(&node)?;
        if !urls.insert(member.url.clone()) {
            return Err(ReplicaError::NodeStorage(
                "direct membership URLs must be unique".to_owned(),
            ));
        }
    }
    Ok(())
}

fn normalize_members_for_cas(
    members: &mut BTreeMap<String, DurableMember>,
    current: &DurableControlState,
) -> Result<(), ReplicaError> {
    for (id, member) in members.iter_mut() {
        if let Some(previous) = current.members.get(id) {
            // A cohort is immutable.  The caller's omitted/default field must
            // not make a known member appear to move into cohort zero.
            member.cohort_id = previous.cohort_id;
        }
    }

    let pending = members
        .iter()
        .filter(|(id, member)| !current.members.contains_key(*id) && member.cohort_id == 0)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(());
    }

    let joining = current
        .cohorts
        .values()
        .filter(|cohort| {
            cohort.status == CohortStatus::Joining
                && cohort.members.len().saturating_add(pending.len()) <= REPLICATION_FACTOR
        })
        .map(|cohort| cohort.id)
        .max();
    let cohort_id = joining.unwrap_or_else(|| {
        current
            .cohorts
            .keys()
            .next_back()
            .map_or(0, |id| id.saturating_add(1))
    });
    for id in pending {
        if let Some(member) = members.get_mut(&id) {
            member.cohort_id = cohort_id;
        }
    }
    Ok(())
}

/// Rebuilds the derived cohort member lists while preserving cohort lifecycle
/// state.  This is also the on-disk migration for pre-cohort control files:
/// the first three stable members become cohort zero and any later complete
/// groups receive monotonically increasing ids.
fn reconcile_cohorts(state: &mut DurableControlState) -> Result<bool, ReplicaError> {
    validate_members_state(&state.members)?;
    let before = state.clone();

    // A v1 control file had no cohort information. Its membership order is
    // the BTreeMap order, which is stable across all coordinators during this
    // one-time migration. Production bootstrap has exactly three members;
    // grouping additional legacy members keeps old static fixtures readable
    // without allowing a partial group to become active. Once a cohort map is
    // present, member cohort ids are authoritative: silently regrouping them
    // would move old LSNs to a different immutable cohort.
    if state.cohorts.is_empty()
        && !state.members.is_empty()
        && state.members.values().all(|member| member.cohort_id == 0)
    {
        let ids = state.members.keys().cloned().collect::<Vec<_>>();
        for (index, id) in ids.into_iter().enumerate() {
            if let Some(member) = state.members.get_mut(&id) {
                member.cohort_id = (index / REPLICATION_FACTOR) as u64;
            }
        }
    }

    let mut groups = BTreeMap::<u64, Vec<String>>::new();
    for member in state.members.values() {
        groups
            .entry(member.cohort_id)
            .or_default()
            .push(member.id.clone());
    }
    for ids in groups.values_mut() {
        ids.sort();
    }
    let previous = std::mem::take(&mut state.cohorts);
    let mut cohorts = BTreeMap::new();
    for (id, members) in groups {
        let old = previous.get(&id);
        let all_active = members.iter().all(|member_id| {
            state
                .members
                .get(member_id)
                .is_some_and(|member| member.status == MemberStatus::Active)
        });
        let active_count = members
            .iter()
            .filter(|member_id| {
                state
                    .members
                    .get(*member_id)
                    .is_some_and(|member| member.status == MemberStatus::Active)
            })
            .count();
        let removed_count = members
            .iter()
            .filter(|member_id| {
                state
                    .members
                    .get(*member_id)
                    .is_some_and(|member| member.status == MemberStatus::Removed)
            })
            .count();
        let draining_count = members
            .iter()
            .filter(|member_id| {
                state
                    .members
                    .get(*member_id)
                    .is_some_and(|member| member.status == MemberStatus::Draining)
            })
            .count();
        let status = match old.map(|cohort| cohort.status) {
            Some(CohortStatus::Retired) => CohortStatus::Retired,
            Some(CohortStatus::Draining) if removed_count == members.len() || active_count == 0 => {
                if removed_count == members.len() {
                    CohortStatus::Retired
                } else {
                    CohortStatus::Draining
                }
            }
            Some(CohortStatus::Draining) => CohortStatus::Draining,
            _ if removed_count == members.len() => CohortStatus::Retired,
            _ if draining_count > 0 => CohortStatus::Draining,
            _ if members.len() == REPLICATION_FACTOR
                && active_count == REPLICATION_FACTOR
                && all_active =>
            {
                CohortStatus::Active
            }
            _ => CohortStatus::Joining,
        };
        let tier = old.filter(|cohort| !cohort.tier.is_empty()).map_or_else(
            || {
                members
                    .iter()
                    .filter_map(|member_id| state.members.get(member_id))
                    .map(|member| member.tier.clone())
                    .find(|tier| !tier.is_empty())
                    .unwrap_or_default()
            },
            |cohort| cohort.tier.clone(),
        );
        let max_append_bytes = old
            .filter(|cohort| cohort.max_append_bytes != 0)
            .map_or_else(
                || {
                    members
                        .iter()
                        .filter_map(|member_id| state.members.get(member_id))
                        .map(|member| member.max_append_bytes)
                        .find(|limit| *limit != 0)
                        .unwrap_or(0)
                },
                |cohort| cohort.max_append_bytes,
            );
        cohorts.insert(
            id,
            DurableCohort {
                id,
                members,
                status,
                tier,
                max_append_bytes,
            },
        );
    }
    state.cohorts = cohorts;

    // A legacy member may not have advertised tier/limit metadata. Once a
    // cohort has an established policy, fill only those absent values from
    // the cohort; conflicting non-empty member policies remain visible and
    // are rejected by `validate_control_state` below.
    let cohort_policies = state
        .cohorts
        .values()
        .map(|cohort| {
            (
                cohort.id,
                cohort.tier.clone(),
                cohort.max_append_bytes,
                cohort.members.clone(),
            )
        })
        .collect::<Vec<_>>();
    for (cohort_id, tier, max_append_bytes, member_ids) in cohort_policies {
        for member_id in member_ids {
            if let Some(member) = state.members.get_mut(&member_id) {
                if member.tier.is_empty() {
                    member.tier = tier.clone();
                }
                if member.max_append_bytes == 0 {
                    member.max_append_bytes = max_append_bytes;
                }
                debug_assert_eq!(member.cohort_id, cohort_id);
            }
        }
    }

    // Complete legacy segment records with the immutable cohort binding that
    // was not present in the first control-file format.  New records always
    // carry these fields; they are what lets a recovery reader reject a
    // segment that has been moved to a different member set.
    let cohort_hashes = state
        .cohorts
        .keys()
        .copied()
        .filter_map(|cohort_id| cohort_member_hash(state, cohort_id).map(|hash| (cohort_id, hash)))
        .collect::<BTreeMap<_, _>>();
    for segments in state.stream_segments.values_mut() {
        for segment in segments {
            let cohort = state.cohorts.get(&segment.cohort_id).ok_or_else(|| {
                ReplicaError::NodeStorage("stream segment references an unknown cohort".to_owned())
            })?;
            if segment.member_ids.is_empty() {
                segment.member_ids = cohort.members.clone();
            }
            if segment.member_hash.is_empty() {
                segment.member_hash =
                    cohort_hashes
                        .get(&segment.cohort_id)
                        .cloned()
                        .ok_or_else(|| {
                            ReplicaError::NodeStorage(
                                "stream segment cannot resolve its cohort member hash".to_owned(),
                            )
                        })?;
            }
            if segment.manifest_revision == 0 && state.manifest_revision != 0 {
                segment.manifest_revision = state.manifest_revision;
            }
            if segment.operation_id.is_empty() && !state.manifest_operation_id.is_empty() {
                segment.operation_id = state.manifest_operation_id.clone();
            }
            if segment.tier.is_empty() {
                segment.tier = cohort.tier.clone();
            }
            if segment.max_append_bytes == 0 {
                segment.max_append_bytes = cohort.max_append_bytes;
            }
        }
    }

    // Control files written before the replicated manifest fields existed are
    // migrated to one durable revision. A populated digest in a current file
    // is authoritative: silently replacing a mismatch would turn tampering
    // into a successful restart.
    let canonical_digest = manifest_digest(&state.stream_segments);
    if state.manifest_revision == 0 && !state.stream_segments.is_empty() {
        state.manifest_revision = 1;
        state.manifest_operation_id = "manifest-migration".to_owned();
        state.manifest_cohort_id = state
            .cohorts
            .values()
            .filter(|cohort| cohort.status == CohortStatus::Active)
            .max_by_key(|cohort| cohort.id)
            .map_or(0, |cohort| cohort.id);
        state.manifest_writer_epoch = state
            .stream_segments
            .values()
            .flat_map(|segments| segments.iter().map(|segment| segment.writer_epoch))
            .max()
            .unwrap_or(0);
        state.manifest_member_hash =
            cohort_member_hash(state, state.manifest_cohort_id).unwrap_or_default();
    }
    if state.manifest_revision != 0 {
        if !state.manifest_digest.is_empty() && state.manifest_digest != canonical_digest {
            return Err(ReplicaError::NodeStorage(
                "replica placement manifest digest does not match its ranges".to_owned(),
            ));
        }
        state.manifest_digest = canonical_digest;
        if state.manifest_operation_id.is_empty() {
            state.manifest_operation_id = "manifest-migration".to_owned();
        }
        if state.manifest_member_hash.is_empty() {
            state.manifest_member_hash =
                cohort_member_hash(state, state.manifest_cohort_id).unwrap_or_default();
        }
    }

    // Complete cohorts remain active until an explicit, archive-proven
    // lifecycle operation drains them. This allows the stable cohort ring to
    // use more than one active cohort; historical ranges retain their
    // original placement regardless of later ring changes.
    Ok(*state != before)
}

fn validate_control_state(state: &DurableControlState) -> Result<(), ReplicaError> {
    validate_members_state(&state.members)?;
    let mut memberships = BTreeMap::<String, usize>::new();
    for (id, cohort) in &state.cohorts {
        if cohort.id != *id
            || cohort.members.is_empty()
            || cohort.members.len() > REPLICATION_FACTOR
            || cohort
                .members
                .windows(2)
                .any(|window| window[0] >= window[1])
        {
            return Err(ReplicaError::NodeStorage(
                "invalid direct replica cohort definition".to_owned(),
            ));
        }
        let mut active = 0;
        for member_id in &cohort.members {
            let Some(member) = state.members.get(member_id) else {
                return Err(ReplicaError::NodeStorage(
                    "cohort references an unknown member".to_owned(),
                ));
            };
            if member.cohort_id != *id {
                return Err(ReplicaError::NodeStorage(
                    "member and cohort identities disagree".to_owned(),
                ));
            }
            *memberships.entry(member_id.clone()).or_default() += 1;
            if member.status == MemberStatus::Active {
                active += 1;
            }
        }
        if cohort.status == CohortStatus::Active
            && (cohort.members.len() != REPLICATION_FACTOR || active != REPLICATION_FACTOR)
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        // A vertical resize replaces the three members one at a time under a
        // maintenance fence. Mixed member tiers are therefore a valid,
        // temporary durable state. The cohort policy remains at the previous
        // safe capacity until all three members advertise the same new tier;
        // the replacement operation updates it atomically at that point.
        // A draining cohort remains readable and writable for the immutable
        // routes it already owns until each route has crossed its durable
        // placement fence.  Only the final Retired state requires that all
        // open tails have been published to a successor.  Treating
        // Draining as already retired here would force a cell-wide outage
        // between the lifecycle CAS and the per-stream handoffs.
        if cohort.status == CohortStatus::Retired
            && state
                .stream_segments
                .values()
                .flatten()
                .any(|segment| segment.cohort_id == *id && segment.end_lsn.is_none())
        {
            return Err(ReplicaError::Protocol(
                "cohort cannot drain or retire while an owned stream range is open".to_owned(),
            ));
        }
    }
    for member in state.members.values() {
        if memberships.get(&member.id) != Some(&1) {
            return Err(ReplicaError::NodeStorage(
                "direct member is not assigned to exactly one cohort".to_owned(),
            ));
        }
    }
    validate_manifest_segments(state, &state.stream_segments, state.manifest_revision)?;
    if state.manifest_revision == 0 && !state.manifest_digest.is_empty() {
        return Err(ReplicaError::NodeStorage(
            "manifest digest exists without a manifest revision".to_owned(),
        ));
    }
    if state.manifest_revision > 0
        && state.manifest_digest != manifest_digest(&state.stream_segments)
    {
        return Err(ReplicaError::NodeStorage(
            "manifest digest does not match its segments".to_owned(),
        ));
    }
    validate_write_cohort(state, state.write_cohort_id)?;
    Ok(())
}

/// Checks the explicit future-write target without changing the immutable
/// placement of any existing range. A zero target preserves the legacy ring
/// selector; a nonzero target must already be a complete active cohort.
fn validate_write_cohort(
    state: &DurableControlState,
    write_cohort_id: u64,
) -> Result<(), ReplicaError> {
    if write_cohort_id == 0 {
        return Ok(());
    }
    let cohort = state
        .cohorts
        .get(&write_cohort_id)
        .ok_or(ReplicaError::LsnConflict)?;
    if cohort.status != CohortStatus::Active
        || cohort.members.len() != REPLICATION_FACTOR
        || cohort.members.iter().any(|id| {
            state
                .members
                .get(id)
                .is_none_or(|member| member.status != MemberStatus::Active)
        })
    {
        return Err(ReplicaError::QuorumUnavailable);
    }
    Ok(())
}

fn validate_manifest_segments(
    state: &DurableControlState,
    stream_segments: &BTreeMap<String, Vec<StreamSegment>>,
    manifest_revision: u64,
) -> Result<(), ReplicaError> {
    for (stream, segments) in stream_segments {
        if stream.trim().is_empty() {
            return Err(ReplicaError::NodeStorage(
                "stream segment map contains an empty stream".to_owned(),
            ));
        }
        let mut previous_end = 0_u64;
        for (index, segment) in segments.iter().enumerate() {
            if segment.start_lsn == 0
                || segment.end_lsn.is_some_and(|end| end < segment.start_lsn)
                || segment.start_lsn <= previous_end
                || (index == 0 && segment.start_lsn != 1)
                || (segment.end_lsn.is_none() && index + 1 != segments.len())
                || !state.cohorts.contains_key(&segment.cohort_id)
            {
                return Err(ReplicaError::NodeStorage(
                    "direct stream segment map is unordered or references an unknown cohort"
                        .to_owned(),
                ));
            }
            if segment.manifest_revision > manifest_revision {
                return Err(ReplicaError::LsnConflict);
            }
            if !segment.member_ids.is_empty() {
                if segment
                    .member_ids
                    .windows(2)
                    .any(|window| window[0] >= window[1])
                    || segment.member_ids.len() != REPLICATION_FACTOR
                    || state
                        .cohorts
                        .get(&segment.cohort_id)
                        .is_none_or(|cohort| cohort.members != segment.member_ids)
                    || segment.member_hash.is_empty()
                    || (manifest_revision > 0
                        && (segment.manifest_revision == 0
                            || segment.operation_id.trim().is_empty()))
                {
                    return Err(ReplicaError::LsnConflict);
                }
                if !segment.member_hash.is_empty()
                    && cohort_member_hash(state, segment.cohort_id).as_deref()
                        != Some(segment.member_hash.as_str())
                {
                    return Err(ReplicaError::LsnConflict);
                }
                let cohort = state
                    .cohorts
                    .get(&segment.cohort_id)
                    .ok_or(ReplicaError::LsnConflict)?;
                if (!segment.tier.is_empty()
                    && !cohort.tier.is_empty()
                    && segment.tier != cohort.tier)
                    || (segment.max_append_bytes != 0
                        && cohort.max_append_bytes != 0
                        && segment.max_append_bytes != cohort.max_append_bytes)
                {
                    return Err(ReplicaError::LsnConflict);
                }
            } else if manifest_revision > 0 {
                return Err(ReplicaError::LsnConflict);
            }
            previous_end = segment.end_lsn.unwrap_or(u64::MAX);
        }
    }
    Ok(())
}

/// Ensures a new manifest only appends history or closes the currently open
/// tail at a durable cutover boundary. Historical finite ranges are
/// immutable. The only permitted mutation of an open range is changing its
/// end from `None` to the LSN immediately preceding the newly appended range.
fn validate_manifest_transition(
    previous: &BTreeMap<String, Vec<StreamSegment>>,
    next: &BTreeMap<String, Vec<StreamSegment>>,
) -> Result<(), ReplicaError> {
    for (stream, old_segments) in previous {
        // A trailing open range may be dropped outright: it is the one
        // shape a fenced cutover cannot seal, because it never held a
        // durable record and its end would precede its start. The cutover
        // proof binds that drop to the certified watermark below the range.
        let trailing_open = old_segments
            .last()
            .is_some_and(|segment| segment.end_lsn.is_none());
        let Some(new_segments) = next.get(stream) else {
            if trailing_open && old_segments.len() == 1 {
                continue;
            }
            return Err(ReplicaError::LsnConflict);
        };
        let retained = if trailing_open && new_segments.len() + 1 == old_segments.len() {
            &old_segments[..old_segments.len() - 1]
        } else {
            &old_segments[..]
        };
        if new_segments.len() < retained.len() {
            return Err(ReplicaError::LsnConflict);
        }
        for (index, old) in retained.iter().enumerate() {
            let Some(new) = new_segments.get(index) else {
                return Err(ReplicaError::LsnConflict);
            };
            let same_identity = old.start_lsn == new.start_lsn
                && old.cohort_id == new.cohort_id
                && (old.member_ids.is_empty() || old.member_ids == new.member_ids)
                && (old.member_hash.is_empty() || old.member_hash == new.member_hash)
                && (old.writer_epoch == 0 || old.writer_epoch == new.writer_epoch)
                && (old.operation_id.is_empty() || old.operation_id == new.operation_id)
                && (old.tier.is_empty() || old.tier == new.tier)
                && (old.max_append_bytes == 0 || old.max_append_bytes == new.max_append_bytes);
            if !same_identity {
                return Err(ReplicaError::LsnConflict);
            }
            match old.end_lsn {
                Some(end) => {
                    if new.end_lsn != Some(end) {
                        return Err(ReplicaError::LsnConflict);
                    }
                }
                None => {
                    // An open range may remain open unchanged. If it closes,
                    // the following range must begin at the exact next LSN;
                    // `validate_manifest_segments` checks that adjacency.
                    if new.end_lsn.is_some() && index + 1 >= new_segments.len() {
                        return Err(ReplicaError::LsnConflict);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Verifies the additional cutover proof carried by a normal manifest CAS.
/// Range adjacency alone cannot prove that an open tail was closed at the
/// durable writer watermark; requiring the explicit LSN prevents an operator
/// or stale coordinator from sealing an arbitrary historical prefix.
fn validate_manifest_cutover(
    previous: &BTreeMap<String, Vec<StreamSegment>>,
    next: &BTreeMap<String, Vec<StreamSegment>>,
    cutover_lsn: Option<u64>,
) -> Result<(), ReplicaError> {
    let mut closed_tail = None;
    let absent = Vec::new();
    for (stream, old_segments) in previous {
        // `validate_manifest_transition` has already bounded which streams
        // and segments may be missing here: only a trailing open range.
        let new_segments = next.get(stream).unwrap_or(&absent);
        for (index, old) in old_segments.iter().enumerate() {
            if old.end_lsn.is_some() {
                continue;
            }
            let closed = match new_segments.get(index) {
                Some(new) => new.end_lsn,
                // A dropped reservation is a cutover at the watermark just
                // below it, which the caller must prove the same way it
                // proves a seal.
                None => Some(old.start_lsn.saturating_sub(1)),
            };
            if closed.is_some_and(|end| closed_tail.replace(end).is_some()) {
                // One append operation has one durable watermark. A manifest
                // that seals multiple streams is not a valid cutover
                // generated by the append path.
                return Err(ReplicaError::LsnConflict);
            }
        }
    }
    if closed_tail != cutover_lsn {
        return Err(ReplicaError::LsnConflict);
    }
    Ok(())
}

/// Validates a membership transition that is allowed to happen while the
/// data plane remains live.  Online transitions are deliberately additive:
/// they may register a fresh joining cohort, or activate one complete
/// joining cohort, but they may not remove, drain, replace, or rewrite an
/// existing range.  Historical placement remains immutable and the append
/// path publishes any future range cutover separately after it has certified
/// the current tail.
fn validate_online_membership_transition(
    current: &DurableControlState,
    next: &DurableControlState,
) -> Result<(), ReplicaError> {
    for (id, member) in &current.members {
        let Some(candidate) = next.members.get(id) else {
            return Err(ReplicaError::LsnConflict);
        };
        if candidate.id != member.id
            || candidate.url != member.url
            || candidate.cohort_id != member.cohort_id
            || candidate.name != member.name
            || candidate.machine_id != member.machine_id
            || candidate.volume_id != member.volume_id
            || candidate.ordinal != member.ordinal
            || candidate.tier != member.tier
            || candidate.max_append_bytes != member.max_append_bytes
        {
            return Err(ReplicaError::LsnConflict);
        }
        match (member.status, candidate.status) {
            (MemberStatus::Joining, MemberStatus::Joining | MemberStatus::Active)
            | (MemberStatus::Active, MemberStatus::Active)
            | (MemberStatus::Draining, MemberStatus::Draining)
            | (MemberStatus::Removed, MemberStatus::Removed) => {}
            _ => {
                return Err(ReplicaError::LsnConflict);
            }
        }
    }

    for (id, member) in &next.members {
        if current.members.contains_key(id) {
            continue;
        }
        if member.cohort_id == 0
            || !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
        {
            return Err(ReplicaError::LsnConflict);
        }
    }

    for (id, cohort) in &current.cohorts {
        let Some(candidate) = next.cohorts.get(id) else {
            return Err(ReplicaError::LsnConflict);
        };
        if candidate.id != cohort.id
            || candidate.tier != cohort.tier
            || candidate.max_append_bytes != cohort.max_append_bytes
            || candidate.members.len() < cohort.members.len()
            || cohort
                .members
                .iter()
                .any(|member_id| !candidate.members.contains(member_id))
            || candidate.members.len() > REPLICATION_FACTOR
        {
            return Err(ReplicaError::LsnConflict);
        }
        match (cohort.status, candidate.status) {
            (CohortStatus::Joining, CohortStatus::Joining) => {}
            (CohortStatus::Joining, CohortStatus::Active) => {
                if cohort.members.len() != REPLICATION_FACTOR
                    || candidate.members.iter().any(|member_id| {
                        next.members
                            .get(member_id)
                            .is_none_or(|member| member.status != MemberStatus::Active)
                    })
                {
                    return Err(ReplicaError::QuorumUnavailable);
                }
            }
            (CohortStatus::Active, CohortStatus::Active)
            | (CohortStatus::Draining, CohortStatus::Draining)
            | (CohortStatus::Retired, CohortStatus::Retired) => {}
            _ => {
                return Err(ReplicaError::LsnConflict);
            }
        }
    }

    for (id, cohort) in &next.cohorts {
        if current.cohorts.contains_key(id) {
            continue;
        }
        if id == &0
            || cohort.members.is_empty()
            || cohort.members.len() > REPLICATION_FACTOR
            || !matches!(cohort.status, CohortStatus::Joining | CohortStatus::Active)
        {
            return Err(ReplicaError::LsnConflict);
        }
        if cohort.status == CohortStatus::Active
            && (cohort.members.len() != REPLICATION_FACTOR
                || cohort.members.iter().any(|member_id| {
                    next.members
                        .get(member_id)
                        .is_none_or(|member| member.status != MemberStatus::Active)
                }))
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
    }

    // A live membership operation never seals an old range or changes its
    // member set. A joining node may have an empty local manifest and adopt
    // the authoritative map, so compare only ranges that already exist on the
    // receiving volume. Any new range is still subject to the normal manifest
    // CAS quorum and cutover proof on the append path.
    for (stream, current_segments) in &current.stream_segments {
        let Some(next_segments) = next.stream_segments.get(stream) else {
            return Err(ReplicaError::LsnConflict);
        };
        if next_segments.len() < current_segments.len()
            || current_segments
                .iter()
                .zip(next_segments)
                .any(|(old, new)| old != new)
        {
            return Err(ReplicaError::LsnConflict);
        }
    }
    Ok(())
}

/// Validates the destructive half of a live whole-cohort handoff. Existing
/// identities stay tombstoned in place, the source cohort becomes retired only
/// after every open source range has an adjacent successor range, and other
/// active cohorts remain untouched. This is deliberately a separate contract
/// from additive activation.
fn validate_online_replacement_transition(
    current: &DurableControlState,
    next: &DurableControlState,
) -> Result<(), ReplicaError> {
    let mut retired_cohorts = BTreeSet::new();
    for (id, member) in &current.members {
        let Some(candidate) = next.members.get(id) else {
            return Err(ReplicaError::LsnConflict);
        };
        if candidate.id != member.id
            || candidate.url != member.url
            || candidate.cohort_id != member.cohort_id
            || candidate.name != member.name
            || candidate.machine_id != member.machine_id
            || candidate.volume_id != member.volume_id
            || candidate.ordinal != member.ordinal
            || candidate.tier != member.tier
            || candidate.max_append_bytes != member.max_append_bytes
        {
            return Err(ReplicaError::LsnConflict);
        }
        match (member.status, candidate.status) {
            (MemberStatus::Active, MemberStatus::Removed)
            | (MemberStatus::Draining, MemberStatus::Removed)
            | (MemberStatus::Joining, MemberStatus::Joining | MemberStatus::Active)
            | (MemberStatus::Active, MemberStatus::Active)
            | (MemberStatus::Draining, MemberStatus::Draining)
            | (MemberStatus::Removed, MemberStatus::Removed) => {}
            _ => {
                return Err(ReplicaError::LsnConflict);
            }
        }
        if member.status == MemberStatus::Active && candidate.status == MemberStatus::Removed {
            retired_cohorts.insert(member.cohort_id);
        }
    }

    for (id, member) in &next.members {
        if current.members.contains_key(id) {
            continue;
        }
        if member.cohort_id == 0
            || !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
        {
            return Err(ReplicaError::LsnConflict);
        }
    }

    for (id, cohort) in &current.cohorts {
        let Some(candidate) = next.cohorts.get(id) else {
            return Err(ReplicaError::LsnConflict);
        };
        if candidate.id != cohort.id
            || candidate.tier != cohort.tier
            || candidate.max_append_bytes != cohort.max_append_bytes
            || candidate.members != cohort.members
        {
            return Err(ReplicaError::LsnConflict);
        }
        match (cohort.status, candidate.status) {
            (CohortStatus::Active, CohortStatus::Retired)
            | (CohortStatus::Active, CohortStatus::Draining)
            | (CohortStatus::Draining, CohortStatus::Retired)
            | (CohortStatus::Joining, CohortStatus::Joining | CohortStatus::Active)
            | (CohortStatus::Active, CohortStatus::Active)
            | (CohortStatus::Draining, CohortStatus::Draining)
            | (CohortStatus::Retired, CohortStatus::Retired) => {}
            _ => {
                return Err(ReplicaError::LsnConflict);
            }
        }
    }

    for (id, cohort) in &next.cohorts {
        if current.cohorts.contains_key(id) {
            continue;
        }
        if *id == 0
            || cohort.members.len() != REPLICATION_FACTOR
            || cohort.status != CohortStatus::Active
            || cohort.members.iter().any(|member_id| {
                next.members
                    .get(member_id)
                    .is_none_or(|member| member.status != MemberStatus::Active)
            })
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
    }

    // A source cohort may only be retired after all of its open ranges have
    // been closed and followed by a successor range. Historical finite ranges
    // remain byte-for-byte unchanged.
    for (stream, current_segments) in &current.stream_segments {
        let Some(next_segments) = next.stream_segments.get(stream) else {
            return Err(ReplicaError::LsnConflict);
        };
        if next_segments.len() < current_segments.len() {
            return Err(ReplicaError::LsnConflict);
        }
        for (index, old) in current_segments.iter().enumerate() {
            let Some(new) = next_segments.get(index) else {
                return Err(ReplicaError::LsnConflict);
            };
            if old.end_lsn.is_some() {
                if old != new {
                    return Err(ReplicaError::LsnConflict);
                }
            } else if retired_cohorts.contains(&old.cohort_id) {
                if old.start_lsn != new.start_lsn
                    || old.cohort_id != new.cohort_id
                    || old.member_ids != new.member_ids
                    || old.member_hash != new.member_hash
                    || old.writer_epoch != new.writer_epoch
                    || new.end_lsn.is_none()
                {
                    return Err(ReplicaError::LsnConflict);
                }
            } else if old != new {
                return Err(ReplicaError::LsnConflict);
            }
        }
        if let Some(last) = current_segments.last()
            && last.end_lsn.is_none()
            && retired_cohorts.contains(&last.cohort_id)
            && next_segments.len() == current_segments.len()
        {
            return Err(ReplicaError::LsnConflict);
        }
    }

    // The first lifecycle CAS in a live handoff only marks the source
    // cohort Draining (and may activate a prepared successor cohort).  It
    // deliberately leaves every existing route unchanged, so there is no
    // retired cohort yet.  Accept that preparation state; the final CAS is
    // the one that must satisfy the archive and successor checks below.
    if retired_cohorts.is_empty() {
        return Ok(());
    }
    for cohort_id in retired_cohorts {
        if next
            .stream_segments
            .values()
            .flatten()
            .any(|segment| segment.cohort_id == cohort_id && segment.end_lsn.is_none())
        {
            return Err(ReplicaError::LsnConflict);
        }
    }
    Ok(())
}

/// Idempotency records retained per operation map. A retry of a committed
/// membership or manifest operation arrives within a few revisions of the
/// original. Retaining the whole history made the control document grow by
/// one full serialized manifest per published route, so a cell at manifest
/// revision 167 carried megabytes of dead records through every control read.
const OPERATION_RECORD_RETENTION: usize = 8;

/// Drops the oldest idempotency records beyond [`OPERATION_RECORD_RETENTION`].
/// Manifest records rank by the revision they produced and membership records
/// by their epoch; the record for the current manifest operation is always
/// kept. A retry older than the retained window no longer replays: it fails
/// closed at the CAS on the expected revision or epoch, which cannot fork.
fn prune_operation_records(state: &mut DurableControlState) {
    #[derive(Deserialize)]
    struct RevisionOnly {
        #[serde(default)]
        revision: u64,
    }
    #[derive(Deserialize)]
    struct EpochOnly {
        #[serde(default)]
        membership_epoch: u64,
    }
    fn prune(map: &mut BTreeMap<String, String>, keep: &str, rank: impl Fn(&str) -> u64) {
        if map.len() <= OPERATION_RECORD_RETENTION {
            return;
        }
        let mut ranked = map
            .iter()
            .map(|(id, value)| (rank(value), id.clone()))
            .collect::<Vec<_>>();
        ranked.sort();
        let excess = map.len() - OPERATION_RECORD_RETENTION;
        for (_, id) in ranked.into_iter().take(excess) {
            if id != keep {
                map.remove(&id);
            }
        }
    }
    let keep = state.manifest_operation_id.clone();
    prune(&mut state.manifest_operations, &keep, |value| {
        serde_json::from_str::<RevisionOnly>(value).map_or(0, |value| value.revision)
    });
    prune(&mut state.operations, "", |value| {
        serde_json::from_str::<EpochOnly>(value).map_or(0, |value| value.membership_epoch)
    });
}

fn persist_control_state(path: &Path, state: &DurableControlState) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::NodeStorage("control state has no parent directory".to_owned())
    })?;
    let encoded =
        serde_json::to_vec(state).map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
    static NEXT_CONTROL_TEMP: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_CONTROL_TEMP.fetch_add(1, Ordering::Relaxed);
    let mut temp_os = path.as_os_str().to_owned();
    temp_os.push(format!(".tmp-{}-{sequence}", std::process::id()));
    let temp = PathBuf::from(temp_os);
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        std::fs::rename(&temp, path)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let directory =
            File::open(parent).map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        directory
            .sync_all()
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    write_result
}

const DERIVATION_VERSION: &str = "lakeday-cloud/deployment-identity/v1";
/// Header used on traffic from the gateway to a node.
pub const INTERNAL_AUTH_HEADER: &str = "x-lakeday-replica-internal-token";
/// Header used by the private operator/admin surface.
pub const ADMIN_AUTH_HEADER: &str = "x-lakeday-replica-admin";
/// Header carrying the gateway maintenance fence returned by the private
/// maintenance endpoint.
pub const MAINTENANCE_AUTH_HEADER: &str = "x-lakeday-replica-maintenance-token";
/// Header carrying the gateway's HMAC-bound commit receipt. It is required
/// for watermark recovery of a record that has neither quorum evidence nor a
/// durable node commit marker.
pub const COMMIT_CERTIFICATE_HEADER: &str = "x-lakeday-replica-commit-certificate";

const COMMIT_CERTIFICATE_VERSION: u8 = 1;
const COMMIT_CERTIFICATE_DOMAIN: &str = "lakeday-cloud/replica-commit-certificate/v1";
const MAINTENANCE_TOKEN_VERSION: u8 = 1;
const MAINTENANCE_TOKEN_DOMAIN: &str = "lakeday-cloud/replica-maintenance-fence/v1";
const MAINTENANCE_MARKER_VERSION: u8 = 1;
const MAINTENANCE_MARKER_SUFFIX: &str = ".maintenance";

/// A member of the replica cell.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaNode {
    /// Stable membership identity. It must not be reused for another disk.
    pub id: String,
    /// Private HTTP URL reachable by the gateway.
    pub url: String,
}

impl ReplicaNode {
    /// Creates one member and normalizes a trailing slash from its URL.
    #[must_use]
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            url: url.into().trim_end_matches('/').to_owned(),
        }
    }
}

/// The data and local commit markers returned by the internal node API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeSnapshot {
    /// All opaque records currently present on the node.
    pub records: Vec<EncryptedRecord>,
    /// Records for which this node has a local durable commit marker.
    pub committed: Vec<EncryptedRecord>,
    /// Archived prefixes removed from this node's hot append log.
    pub trimmed: Vec<TrimmedPrefix>,
}

impl NodeSnapshot {
    fn empty() -> Self {
        Self {
            records: Vec::new(),
            committed: Vec::new(),
            trimmed: Vec::new(),
        }
    }
}

/// Durable node checkpoint retained after an archived prefix is compacted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TrimmedPrefix {
    /// Tenant-qualified stream identity.
    pub stream: String,
    /// Highest contiguous LSN durably published in the shared archive.
    pub archived_lsn: u64,
    /// Highest writer epoch observed before this prefix was removed.
    pub writer_epoch: u64,
}

/// Filesystem capacity reported by a storage node's mounted data volume.
///
/// The values are bytes, not filesystem blocks. `free_bytes` is the
/// filesystem's available-to-the-process capacity (`statvfs.f_bavail`) and
/// `used_bytes` is derived as `total_bytes - free_bytes`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilesystemStatus {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub used_bytes: u64,
}

/// Compatibility alias for callers that use the more explicit name.
pub type FilesystemStats = FilesystemStatus;

/// Returns the durable maintenance marker path associated with a node log.
///
/// The marker is kept beside the append log on the same mounted volume so an
/// atomic rename and directory fsync cover the same durability boundary as
/// replica data. The path is public for operators and tests that need to
/// inspect or back up the complete node state.
#[must_use]
pub fn maintenance_marker_path(path: impl AsRef<Path>) -> PathBuf {
    let mut marker = path.as_ref().as_os_str().to_owned();
    marker.push(MAINTENANCE_MARKER_SUFFIX);
    PathBuf::from(marker)
}

/// Authenticated status returned by one storage node.
///
/// `boot_id` and `timestamp_ms` let a gateway distinguish a fresh sample from
/// a replayed/stale sample after a node restart. The node name and tier are
/// captured when [`DiskReplica`] opens its log and never read from the
/// environment during a request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageNodeStatus {
    /// Process incarnation. It changes on every process start.
    pub boot_id: String,
    /// Online placement protocol supported by this node. Older nodes omit
    /// this field and therefore report version zero; direct-placement nodes
    /// emitted by the current binary report version one.
    #[serde(default)]
    pub online_protocol_version: u32,
    /// Stable node identity (normally the StatefulSet ordinal or hostname).
    #[serde(alias = "name", alias = "node_id")]
    pub node_name: String,
    /// Capacity tier selected for this node (for example `hot` or `archive`).
    #[serde(alias = "storage_tier")]
    pub tier: String,
    /// Bytes occupied by the append-only node log.
    #[serde(alias = "log_size_bytes", alias = "log_bytes_total")]
    pub log_bytes: u64,
    /// Capacity of the mounted data volume containing the node log.
    #[serde(default, alias = "filesystem_status")]
    pub filesystem: FilesystemStatus,
    /// Flat statvfs aliases retained for lightweight metrics consumers.
    #[serde(default, alias = "filesystem_total_bytes", alias = "disk_total_bytes")]
    pub fs_total_bytes: u64,
    #[serde(default, alias = "filesystem_free_bytes", alias = "disk_free_bytes")]
    pub fs_free_bytes: u64,
    #[serde(default, alias = "filesystem_used_bytes", alias = "disk_used_bytes")]
    pub fs_used_bytes: u64,
    /// Unix timestamp in milliseconds at which this sample was collected.
    #[serde(default)]
    pub timestamp_ms: u64,
    /// Compatibility collection timestamp name used by older scrapers.
    #[serde(
        default,
        alias = "timestamp",
        alias = "collected_at_ms",
        alias = "updated_at_ms"
    )]
    pub observed_at_ms: u64,
    /// Whether this node has a durable maintenance fence marker.
    #[serde(default)]
    pub maintenance: bool,
    /// Owner of the active maintenance fence, when one is present.
    #[serde(default)]
    pub maintenance_owner: Option<String>,
    /// Highest maintenance generation ever persisted by this node. This is
    /// retained after release so an old owner token cannot be replayed.
    #[serde(default)]
    pub maintenance_generation: u64,
}

impl StorageNodeStatus {
    fn with_flat_filesystem(mut self) -> Self {
        self.fs_total_bytes = self.filesystem.total_bytes;
        self.fs_free_bytes = self.filesystem.free_bytes;
        self.fs_used_bytes = self.filesystem.used_bytes;
        self
    }

    fn filesystem_capacity(&self) -> FilesystemStatus {
        if self.filesystem.total_bytes > 0 {
            self.filesystem.clone()
        } else {
            FilesystemStatus {
                total_bytes: self.fs_total_bytes,
                free_bytes: self.fs_free_bytes,
                used_bytes: self.fs_used_bytes,
            }
        }
    }

    fn sample_timestamp_ms(&self) -> u64 {
        if self.timestamp_ms == 0 {
            self.observed_at_ms
        } else {
            self.timestamp_ms
        }
    }
}

/// The authenticated owner and generation of a durable maintenance fence.
///
/// The token itself is intentionally not returned in node telemetry. A
/// gateway can reconstruct the canonical token from these fields and its
/// deployment root key, while an operator cannot use a status scrape as a
/// write credential.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MaintenanceFenceStatus {
    /// Whether a marker is currently active on the node.
    pub active: bool,
    /// HMAC-bound owner identity, present when `active` is true.
    pub owner: Option<String>,
    /// Monotonic generation retained by this node.
    pub generation: u64,
}

/// Monotonic gateway counters returned as one coherent sample.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GatewayCounters {
    /// Number of authenticated append operations admitted for processing.
    pub append_attempts: u64,
    /// Number of append operations that reached the configured quorum.
    pub append_acks: u64,
    /// Number of append operations that failed before a quorum was acknowledged.
    pub append_failures: u64,
    /// Ciphertext bytes in append operations acknowledged by the gateway.
    pub acked_bytes: u64,
    /// Total append latency in nanoseconds for acknowledged and failed attempts.
    pub append_latency_nanos: u64,
    /// Conventional `_total` aliases for metrics collectors.
    pub append_attempts_total: u64,
    pub append_acks_total: u64,
    pub append_failures_total: u64,
    pub acked_bytes_total: u64,
    pub append_latency_nanos_total: u64,
}

/// Authenticated gateway metrics and the current storage membership sample.
///
/// Counter values are monotonic for one `boot_id`; consumers must reset their
/// deltas when that identity changes. `timestamp_ms` is the collection time,
/// while `started_at_ms` identifies when this gateway process began reporting.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GatewayMetrics {
    /// Metrics contract schema consumed by the Rust autoscaler.
    pub version: u32,
    /// Process incarnation for all monotonic counters in this response.
    pub boot_id: String,
    /// Unix timestamp in milliseconds when this process was constructed.
    pub started_at_ms: u64,
    /// Unix timestamp in milliseconds when this response was collected.
    pub timestamp_ms: u64,
    /// Compatibility collection timestamp name.
    pub observed_at_ms: u64,
    /// Last time a counter changed, in Unix milliseconds.
    pub updated_at_ms: u64,
    /// Flat counter fields keep the HTTP contract easy for autoscalers to read.
    pub append_attempts: u64,
    pub append_acks: u64,
    pub append_failures: u64,
    pub acked_bytes: u64,
    pub append_latency_nanos: u64,
    pub append_attempts_total: u64,
    pub append_acks_total: u64,
    pub append_failures_total: u64,
    pub acked_bytes_total: u64,
    pub append_latency_nanos_total: u64,
    /// Rate/percentile fields retained for the autoscaler contract. Rates are
    /// calculated from counter deltas between gateway samples.
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub disk_free_bytes: u64,
    pub disk_total_bytes: u64,
    pub throughput_bytes_per_second: u64,
    pub acked_bytes_per_second: u64,
    pub latency_p95_ms: f64,
    pub latency_p50_ms: f64,
    pub window_ms: u64,
    /// The same counters grouped for clients that prefer a namespaced object.
    pub counters: GatewayCounters,
    /// Number of members in the current membership snapshot.
    pub storage_nodes: usize,
    /// Number of members with a fresh, authenticated status sample.
    pub healthy_storage: usize,
    /// Append requests currently admitted and waiting for a quorum decision.
    pub active_requests: usize,
    /// Appends are fenced while maintenance/rebalance is in progress.
    pub maintenance: bool,
    /// True only when this direct gateway has verified provider CAS support
    /// and every active storage member speaks the online placement protocol.
    #[serde(default)]
    pub online_reconfiguration: bool,
    /// Number of storage nodes currently reporting the active durable fence.
    #[serde(default)]
    pub fenced_storage: usize,
    /// Active durable fence generation, when known.
    #[serde(default)]
    pub maintenance_generation: u64,
    /// Active durable fence owner, when known.
    #[serde(default)]
    pub maintenance_owner: Option<String>,
    /// Fresh status for every current storage member.
    pub storage: Vec<StorageNodeStatus>,
}

/// Compatibility name for code that calls the endpoint's result a snapshot.
pub type GatewayMetricsSnapshot = GatewayMetrics;

fn online_reconfiguration_ready(
    direct_mode: bool,
    provider_cas_verified: bool,
    active_nodes: usize,
    statuses: &[StorageNodeStatus],
) -> bool {
    direct_mode
        && provider_cas_verified
        && active_nodes >= REPLICATION_FACTOR
        && active_nodes.is_multiple_of(REPLICATION_FACTOR)
        && statuses.len() == active_nodes
        && statuses
            .iter()
            .all(|status| status.online_protocol_version == 1)
}

struct DiskState {
    file: File,
    highest_epoch: BTreeMap<String, u64>,
    records: BTreeMap<(String, u64), EncryptedRecord>,
    committed: BTreeMap<(String, u64), EncryptedRecord>,
    trimmed: BTreeMap<String, TrimmedPrefix>,
    maintenance: DurableMaintenanceState,
}

/// The durable node-side maintenance state. `generation` is a tombstone: it
/// remains monotonic even after `active` is cleared, preventing replay of an
/// old signed acquisition token after a restart or a release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableMaintenanceState {
    version: u8,
    generation: u64,
    active: Option<DurableMaintenanceFence>,
    last_released: Option<DurableMaintenanceFence>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DurableMaintenanceFence {
    owner: String,
    generation: u64,
    signature: String,
}

impl Default for DurableMaintenanceState {
    fn default() -> Self {
        Self {
            version: MAINTENANCE_MARKER_VERSION,
            generation: 0,
            active: None,
            last_released: None,
        }
    }
}

impl DurableMaintenanceState {
    fn status(&self) -> MaintenanceFenceStatus {
        MaintenanceFenceStatus {
            active: self.active.is_some(),
            owner: self.active.as_ref().map(|fence| fence.owner.clone()),
            generation: self.generation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedMaintenanceToken {
    owner: String,
    generation: u64,
    signature: String,
}

impl VerifiedMaintenanceToken {
    fn marker(&self) -> DurableMaintenanceFence {
        DurableMaintenanceFence {
            owner: self.owner.clone(),
            generation: self.generation,
            signature: self.signature.clone(),
        }
    }
}

/// Append-only node log whose successful append has reached sync_data.
pub struct DiskReplica {
    state: Mutex<DiskState>,
    log_path: PathBuf,
    /// Upper bound on the node log file, from `LAKEDAY_REPLICA_LOG_MAX_BYTES`.
    /// An append that would cross it is refused so the volume never fills;
    /// the archive loop drains the log and frees room again.
    log_max_bytes: Option<u64>,
    data_dir: PathBuf,
    maintenance_path: PathBuf,
    node_name: String,
    tier: String,
    boot_id: String,
    /// Present only for direct/combined mode. Legacy node constructors keep
    /// this unset so their private append surface remains compatible with
    /// existing isolated tests and older deployments.
    control: Option<Arc<DurableControl>>,
    /// Stream-local host placement fences are enabled for direct nodes. The
    /// sidecar survives control-process restart and is independent of the
    /// authenticated record writer epoch.
    placement: Option<PlacementStore>,
}

#[derive(Deserialize, Serialize)]
struct NodeCommitEntry {
    kind: String,
    record: EncryptedRecord,
}

/// Records of a stream from `from_lsn` on, written by writers older than
/// `writer_epoch`, withdrawn from this member because the committed log holds
/// different records there. See [`DiskReplica::supersede`].
#[derive(Deserialize, Serialize)]
struct NodeSupersedeEntry {
    kind: String,
    stream: String,
    from_lsn: u64,
    writer_epoch: u64,
}

/// Remove a stream's records at and after `from_lsn`, and their commit
/// markers, and recompute the highest writer epoch the member has seen for it.
fn withdraw_suffix(
    stream: &str,
    from_lsn: u64,
    highest_epoch: &mut BTreeMap<String, u64>,
    records: &mut BTreeMap<(String, u64), EncryptedRecord>,
    committed: &mut BTreeMap<(String, u64), EncryptedRecord>,
    trimmed: &BTreeMap<String, TrimmedPrefix>,
) {
    records.retain(|(s, lsn), _| s != stream || *lsn < from_lsn);
    committed.retain(|(s, lsn), _| s != stream || *lsn < from_lsn);
    let highest = records
        .range((stream.to_owned(), 0)..=(stream.to_owned(), u64::MAX))
        .map(|(_, record)| record.writer_epoch())
        .chain(trimmed.get(stream).map(|prefix| prefix.writer_epoch))
        .max();
    match highest {
        Some(epoch) => {
            highest_epoch.insert(stream.to_owned(), epoch);
        }
        None => {
            highest_epoch.remove(stream);
        }
    }
}

#[derive(Deserialize, Serialize)]
struct NodeTrimEntry {
    kind: String,
    stream: String,
    archived_lsn: u64,
    writer_epoch: u64,
}

fn write_json_line<T: Serialize>(file: &mut File, value: &T) -> Result<(), ReplicaError> {
    let encoded =
        serde_json::to_vec(value).map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
    file.write_all(&encoded)
        .and_then(|()| file.write_all(b"\n"))
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
}

fn read_maintenance_state(path: &Path) -> Result<DurableMaintenanceState, ReplicaError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DurableMaintenanceState::default());
        }
        Err(error) => return Err(ReplicaError::NodeStorage(error.to_string())),
    };
    let state = serde_json::from_slice::<DurableMaintenanceState>(&bytes).map_err(|error| {
        ReplicaError::NodeStorage(format!("invalid maintenance marker: {error}"))
    })?;
    if state.version != MAINTENANCE_MARKER_VERSION
        || state
            .active
            .as_ref()
            .is_some_and(|fence| fence.generation != state.generation)
        || state
            .last_released
            .as_ref()
            .is_some_and(|fence| fence.generation != state.generation)
        || state
            .active
            .as_ref()
            .is_some_and(|fence| fence.owner.trim().is_empty() || fence.signature.trim().is_empty())
        || state
            .last_released
            .as_ref()
            .is_some_and(|fence| fence.owner.trim().is_empty() || fence.signature.trim().is_empty())
    {
        return Err(ReplicaError::NodeStorage(
            "invalid maintenance marker generation or version".to_owned(),
        ));
    }
    Ok(state)
}

fn persist_maintenance_state(
    path: &Path,
    state: &DurableMaintenanceState,
) -> Result<(), ReplicaError> {
    let parent = path.parent().ok_or_else(|| {
        ReplicaError::NodeStorage("maintenance marker has no parent directory".to_owned())
    })?;
    let encoded =
        serde_json::to_vec(state).map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
    static NEXT_MARKER_TEMP: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_MARKER_TEMP.fetch_add(1, Ordering::Relaxed);
    let mut temp_os = path.as_os_str().to_owned();
    temp_os.push(format!(".tmp-{}-{sequence}", std::process::id()));
    let temp = PathBuf::from(temp_os);
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        std::fs::rename(&temp, path)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let directory =
            File::open(parent).map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        directory
            .sync_all()
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    write_result
}

impl DiskReplica {
    /// Opens a node log and reconstructs its fencing, idempotency, and commit
    /// indexes. Older logs containing raw EncryptedRecord lines are accepted.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReplicaError> {
        Self::open_with_config(
            path,
            default_node_name(),
            default_storage_tier(),
            default_data_dir(),
        )
    }

    /// Opens a node log with an explicit stable identity and data volume.
    ///
    /// Production callers should pass the mounted `/data` volume explicitly;
    /// tests can use a temporary directory without changing process-global
    /// environment variables. The identity is captured once and is therefore
    /// stable for every status sample emitted by this process.
    pub fn open_with_config(
        path: impl AsRef<Path>,
        node_name: impl Into<String>,
        tier: impl Into<String>,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self, ReplicaError> {
        Self::open_internal(path, node_name, tier, data_dir, None)
    }

    fn open_internal(
        path: impl AsRef<Path>,
        node_name: impl Into<String>,
        tier: impl Into<String>,
        data_dir: impl AsRef<Path>,
        control: Option<Arc<DurableControl>>,
    ) -> Result<Self, ReplicaError> {
        let path = path.as_ref();
        let node_name = node_name.into();
        let tier = tier.into();
        if node_name.trim().is_empty() {
            return Err(ReplicaError::NodeStorage(
                "replica node name must not be empty".to_owned(),
            ));
        }
        if tier.trim().is_empty() {
            return Err(ReplicaError::NodeStorage(
                "replica storage tier must not be empty".to_owned(),
            ));
        }
        let data_dir = data_dir.as_ref().to_owned();
        std::fs::create_dir_all(&data_dir)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let maintenance_path = maintenance_marker_path(path);
        let maintenance = read_maintenance_state(&maintenance_path)?;
        let mut reader_file = file
            .try_clone()
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let mut bytes = Vec::new();
        reader_file
            .read_to_end(&mut bytes)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let mut highest_epoch = BTreeMap::new();
        let mut records = BTreeMap::new();
        let mut committed = BTreeMap::new();
        let mut trimmed = BTreeMap::new();
        let has_trailing_newline = bytes.last().is_some_and(|byte| *byte == b'\n');
        let line_count = bytes.split(|byte| *byte == b'\n').count();
        let mut line_start = 0_usize;
        for (line_index, raw_line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            let is_final_partial = line_index + 1 == line_count && !has_trailing_newline;
            let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
            if line.is_empty() {
                line_start = line_start.saturating_add(raw_line.len() + 1);
                continue;
            }
            let syntax = serde_json::from_slice::<serde_json::Value>(line);
            if let Err(error) = &syntax {
                if is_final_partial && error.is_eof() {
                    file.set_len(line_start as u64)
                        .and_then(|()| file.sync_data())
                        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
                    break;
                }
                return Err(ReplicaError::NodeStorage(format!(
                    "invalid node log entry: {error}"
                )));
            }
            if let Ok(record) = serde_json::from_slice::<EncryptedRecord>(line) {
                if trimmed
                    .get(record.stream())
                    .is_some_and(|prefix: &TrimmedPrefix| record.lsn() <= prefix.archived_lsn)
                {
                    return Err(ReplicaError::NodeStorage(
                        "node log contains a record below its compacted prefix".to_owned(),
                    ));
                }
                Self::index_record(&mut highest_epoch, &mut records, record)?;
                line_start = line_start.saturating_add(raw_line.len() + 1);
                continue;
            }
            let kind = syntax
                .as_ref()
                .ok()
                .and_then(|value| value.get("kind"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match kind {
                "commit" => {
                    let entry: NodeCommitEntry = serde_json::from_slice(line).map_err(|error| {
                        ReplicaError::NodeStorage(format!("invalid node log entry: {error}"))
                    })?;
                    let key = record_key(&entry.record);
                    // A marker without its exact data record is not evidence.
                    if records.get(&key) == Some(&entry.record) {
                        committed.insert(key, entry.record);
                    }
                }
                "supersede" => {
                    let entry: NodeSupersedeEntry =
                        serde_json::from_slice(line).map_err(|error| {
                            ReplicaError::NodeStorage(format!(
                                "invalid node supersede entry: {error}"
                            ))
                        })?;
                    withdraw_suffix(
                        &entry.stream,
                        entry.from_lsn,
                        &mut highest_epoch,
                        &mut records,
                        &mut committed,
                        &trimmed,
                    );
                }
                "trim" => {
                    let entry: NodeTrimEntry = serde_json::from_slice(line).map_err(|error| {
                        ReplicaError::NodeStorage(format!("invalid node trim entry: {error}"))
                    })?;
                    if entry.stream.is_empty()
                        || entry.archived_lsn == 0
                        || trimmed.contains_key(&entry.stream)
                    {
                        return Err(ReplicaError::NodeStorage(
                            "invalid or duplicate node trim entry".to_owned(),
                        ));
                    }
                    highest_epoch.insert(entry.stream.clone(), entry.writer_epoch);
                    trimmed.insert(
                        entry.stream.clone(),
                        TrimmedPrefix {
                            stream: entry.stream,
                            archived_lsn: entry.archived_lsn,
                            writer_epoch: entry.writer_epoch,
                        },
                    );
                }
                _ => {
                    return Err(ReplicaError::NodeStorage(
                        "unknown node log entry kind".to_owned(),
                    ));
                }
            }
            line_start = line_start.saturating_add(raw_line.len() + 1);
        }
        let placement = if let Some(control) = control.as_ref() {
            let state = control.state()?;
            let placement_path = placement::placement_state_path(&data_dir);
            let store: Result<PlacementStore, ReplicaError> = if state.placement_initialized {
                // The control marker is durable evidence that this volume
                // already completed migration. A missing or malformed
                // sidecar must stop startup rather than silently reopening
                // epoch-zero gates.
                PlacementStore::open_existing(&placement_path)
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
            } else {
                // Only the pre-head legacy format may be seeded from the
                // local route map. A state that already adopted a control
                // head, or that contains an explicit route placement epoch,
                // must not bootstrap from potentially stale local metadata.
                if state.control_head_revision != 0
                    || !state.control_head_digest.is_empty()
                    || state
                        .stream_segments
                        .values()
                        .flatten()
                        .any(|segment| segment.placement_epoch != 0)
                {
                    return Err(ReplicaError::NodeStorage(
                        "placement migration marker is missing for a non-legacy control state"
                            .to_owned(),
                    ));
                }
                let legacy_routes = state
                    .stream_segments
                    .iter()
                    .filter_map(|(stream, segments)| {
                        segments
                            .last()
                            .map(|route| (stream.clone(), placement_for_route(stream, route)))
                    })
                    .collect();
                let store = PlacementStore::bootstrap_legacy(&placement_path, legacy_routes)
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
                // The sidecar is synced before this control-state marker. If
                // marker persistence fails, open_internal returns an error;
                // the next process can compare and reuse the exact sidecar.
                control.mark_placement_initialized()?;
                Ok(store)
            };
            Some(store?)
        } else {
            None
        };
        Ok(Self {
            state: Mutex::new(DiskState {
                file,
                highest_epoch,
                records,
                committed,
                trimmed,
                maintenance,
            }),
            log_path: path.to_owned(),
            log_max_bytes: std::env::var("LAKEDAY_REPLICA_LOG_MAX_BYTES")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|v| *v > 0),
            data_dir,
            maintenance_path,
            node_name,
            tier,
            boot_id: new_boot_id(),
            control,
            placement,
        })
    }

    /// Opens a direct Fly storage volume with durable membership state.
    /// `initial_members` is used only when the control document does not yet
    /// exist; subsequent starts always use the persisted membership epoch.
    pub fn open_direct(
        path: impl AsRef<Path>,
        node_name: impl Into<String>,
        tier: impl Into<String>,
        data_dir: impl AsRef<Path>,
        initial_members: &[ReplicaNode],
    ) -> Result<Self, ReplicaError> {
        let data_dir = data_dir.as_ref().to_owned();
        std::fs::create_dir_all(&data_dir)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let control = DurableControl::open(control_state_path(&data_dir), initial_members)?;
        Self::open_internal(path, node_name, tier, data_dir, Some(control))
    }

    /// Direct-mode variant with an operator-selected control file path.
    pub fn open_with_control(
        path: impl AsRef<Path>,
        node_name: impl Into<String>,
        tier: impl Into<String>,
        data_dir: impl AsRef<Path>,
        control_path: impl AsRef<Path>,
        initial_members: &[ReplicaNode],
    ) -> Result<Self, ReplicaError> {
        let control = DurableControl::open(control_path, initial_members)?;
        Self::open_internal(path, node_name, tier, data_dir, Some(control))
    }

    /// Descriptive alias for [`Self::open_with_control`] used by combined
    /// runtime integrations.
    pub fn open_with_direct_config(
        path: impl AsRef<Path>,
        node_name: impl Into<String>,
        tier: impl Into<String>,
        data_dir: impl AsRef<Path>,
        control_path: impl AsRef<Path>,
        initial_members: &[ReplicaNode],
    ) -> Result<Self, ReplicaError> {
        Self::open_with_control(
            path,
            node_name,
            tier,
            data_dir,
            control_path,
            initial_members,
        )
    }

    /// Opens a node with an explicit identity while using the default `/data`
    /// mount for filesystem capacity reporting.
    pub fn open_with_identity(
        path: impl AsRef<Path>,
        node_name: impl Into<String>,
        tier: impl Into<String>,
    ) -> Result<Self, ReplicaError> {
        Self::open_with_config(path, node_name, tier, DEFAULT_STORAGE_DATA_DIR)
    }

    /// Opens a node with an explicit data volume while deriving its identity
    /// from the process environment.
    pub fn open_with_data_dir(
        path: impl AsRef<Path>,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self, ReplicaError> {
        Self::open_with_config(path, default_node_name(), default_storage_tier(), data_dir)
    }

    /// Returns this process's boot identity.
    #[must_use]
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    /// Returns the durable direct-mode control store, when enabled.
    #[must_use]
    pub fn control(&self) -> Option<Arc<DurableControl>> {
        self.control.clone()
    }

    fn index_record(
        highest_epoch: &mut BTreeMap<String, u64>,
        records: &mut BTreeMap<(String, u64), EncryptedRecord>,
        record: EncryptedRecord,
    ) -> Result<(), ReplicaError> {
        highest_epoch
            .entry(record.stream().to_owned())
            .and_modify(|epoch: &mut u64| *epoch = (*epoch).max(record.writer_epoch()))
            .or_insert(record.writer_epoch());
        let key = record_key(&record);
        if let Some(existing) = records.insert(key, record.clone())
            && existing != record
        {
            return Err(ReplicaError::NodeStorage(
                "replica log contains conflicting records for one LSN".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns the durable maintenance state reconstructed from the marker
    /// beside this node's append log.
    pub fn maintenance_status(&self) -> Result<MaintenanceFenceStatus, ReplicaError> {
        self.state
            .lock()
            .map(|state| state.maintenance.status())
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))
    }

    fn fence(&self, token: &VerifiedMaintenanceToken) -> Result<(), ReplicaError> {
        let marker = token.marker();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        if let Some(active) = &state.maintenance.active {
            return if active == &marker {
                Ok(())
            } else {
                Err(ReplicaError::WriterFenced)
            };
        }
        if token.generation <= state.maintenance.generation {
            return Err(ReplicaError::WriterFenced);
        }
        let next = DurableMaintenanceState {
            version: MAINTENANCE_MARKER_VERSION,
            generation: token.generation,
            active: Some(marker),
            // A new generation supersedes the previous release tombstone;
            // stale tokens still fail because their generation is lower.
            last_released: None,
        };
        persist_maintenance_state(&self.maintenance_path, &next)?;
        state.maintenance = next;
        Ok(())
    }

    fn release_fence(&self, token: &VerifiedMaintenanceToken) -> Result<(), ReplicaError> {
        let marker = token.marker();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        match &state.maintenance.active {
            Some(active) if active == &marker => {
                let next = DurableMaintenanceState {
                    version: MAINTENANCE_MARKER_VERSION,
                    generation: state.maintenance.generation,
                    active: None,
                    last_released: Some(marker),
                };
                persist_maintenance_state(&self.maintenance_path, &next)?;
                state.maintenance = next;
                Ok(())
            }
            None if state.maintenance.last_released.as_ref() == Some(&marker) => Ok(()),
            None if token.generation > state.maintenance.generation => {
                // A partially acquired fence can leave a node that never
                // persisted the active marker. Record the release tombstone
                // so a retry is idempotent and an older token cannot be
                // replayed on this node.
                let next = DurableMaintenanceState {
                    version: MAINTENANCE_MARKER_VERSION,
                    generation: token.generation,
                    active: None,
                    last_released: Some(marker),
                };
                persist_maintenance_state(&self.maintenance_path, &next)?;
                state.maintenance = next;
                Ok(())
            }
            _ => Err(ReplicaError::GatewayUnauthorized),
        }
    }

    fn verify_fence(&self, token: &VerifiedMaintenanceToken) -> Result<(), ReplicaError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        if state.maintenance.active.as_ref() == Some(&token.marker()) {
            Ok(())
        } else {
            Err(ReplicaError::WriterFenced)
        }
    }

    /// Adds a local commit marker after the exact opaque record is present.
    pub async fn commit(&self, record: EncryptedRecord) -> Result<(), ReplicaError> {
        self.commit_inner(record, None)
    }

    async fn commit_with_maintenance(
        &self,
        record: EncryptedRecord,
        token: &VerifiedMaintenanceToken,
    ) -> Result<(), ReplicaError> {
        self.commit_inner(record, Some(token))
    }

    fn commit_inner(
        &self,
        record: EncryptedRecord,
        maintenance_token: Option<&VerifiedMaintenanceToken>,
    ) -> Result<(), ReplicaError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        let maintenance_allowed = match (&state.maintenance.active, maintenance_token) {
            (None, None) => true,
            (Some(active), Some(token)) => active == &token.marker(),
            (None, Some(_)) | (Some(_), None) => false,
        };
        if !maintenance_allowed {
            return Err(ReplicaError::WriterFenced);
        }
        if record.committed_lsn() >= record.lsn() {
            return Err(ReplicaError::InvalidWatermark {
                lsn: record.lsn(),
                committed_lsn: record.committed_lsn(),
            });
        }
        let key = record_key(&record);
        if state
            .trimmed
            .get(record.stream())
            .is_some_and(|prefix| record.lsn() <= prefix.archived_lsn)
        {
            return Ok(());
        }
        if state.records.get(&key) != Some(&record) {
            return Err(ReplicaError::LsnConflict);
        }
        if state.committed.get(&key) == Some(&record) {
            return Ok(());
        }
        if state.committed.contains_key(&key) {
            return Err(ReplicaError::LsnConflict);
        }
        let encoded = serde_json::to_vec(&NodeCommitEntry {
            kind: "commit".to_owned(),
            record: record.clone(),
        })
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        state
            .file
            .write_all(&encoded)
            .and_then(|()| state.file.write_all(b"\n"))
            .and_then(|()| state.file.sync_data())
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        state.committed.insert(key, record);
        Ok(())
    }

    /// Withdraw this member's records of `stream` from `from_lsn` on, which
    /// writers older than `writer_epoch` left behind and the committed log
    /// replaced.
    ///
    /// A member that was cut off can hold the last record its own writer sent
    /// it before dying: acknowledged by nobody, because it never reached a
    /// quorum, and superseded by the next writer, which recovered the log
    /// without it and wrote a different record at that position. The member
    /// then refuses every append to the stream - it has a record there - and
    /// never counts towards a quorum for it again. Withdrawing the orphan is
    /// safe exactly when every record withdrawn is older than the writer that
    /// replaced it; a record from that writer or a later one is refused, as is
    /// a position the member has already trimmed into the archive.
    pub fn supersede(
        &self,
        stream: &str,
        from_lsn: u64,
        writer_epoch: u64,
    ) -> Result<usize, ReplicaError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        if stream.is_empty() || from_lsn == 0 {
            return Err(ReplicaError::LsnConflict);
        }
        if state
            .trimmed
            .get(stream)
            .is_some_and(|prefix| from_lsn <= prefix.archived_lsn)
        {
            return Err(ReplicaError::LsnConflict);
        }
        let doomed = state
            .records
            .range((stream.to_owned(), from_lsn)..=(stream.to_owned(), u64::MAX))
            .map(|(_, record)| record.writer_epoch())
            .collect::<Vec<_>>();
        if doomed.is_empty() {
            return Ok(0);
        }
        if doomed.iter().any(|epoch| *epoch >= writer_epoch) {
            return Err(ReplicaError::WriterFenced);
        }
        write_json_line(
            &mut state.file,
            &NodeSupersedeEntry {
                kind: "supersede".to_owned(),
                stream: stream.to_owned(),
                from_lsn,
                writer_epoch,
            },
        )?;
        state
            .file
            .sync_data()
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let DiskState {
            highest_epoch,
            records,
            committed,
            trimmed,
            ..
        } = &mut *state;
        withdraw_suffix(stream, from_lsn, highest_epoch, records, committed, trimmed);
        Ok(doomed.len())
    }

    /// Atomically replaces one archived hot prefix with a durable trim fence.
    ///
    /// The caller must supply a watermark already published by
    /// [`OpaqueArchive`]. The replacement log retains every unarchived record,
    /// commit marker, stream watermark, and highest observed writer epoch.
    pub fn compact_archived(
        &self,
        stream: &str,
        archived_lsn: u64,
        writer_epoch: u64,
    ) -> Result<usize, ReplicaError> {
        self.compact_archived_batch(&[(stream.to_owned(), archived_lsn, writer_epoch)])
    }

    /// Compacts all newly archived streams with one atomic log rewrite.
    pub fn compact_archived_batch(
        &self,
        prefixes: &[(String, u64, u64)],
    ) -> Result<usize, ReplicaError> {
        if prefixes.is_empty() {
            return Ok(0);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        let mut next_trimmed = state.trimmed.clone();
        for (stream, archived_lsn, writer_epoch) in prefixes {
            if stream.is_empty() || *archived_lsn == 0 {
                return Err(ReplicaError::NodeStorage(
                    "compaction requires a stream and nonzero archive watermark".to_owned(),
                ));
            }
            let previous = state.trimmed.get(stream);
            if previous.is_some_and(|prefix| *archived_lsn < prefix.archived_lsn) {
                return Err(ReplicaError::NodeStorage(
                    "archive compaction watermark cannot move backward".to_owned(),
                ));
            }
            let retained_epoch = state
                .highest_epoch
                .get(stream)
                .copied()
                .unwrap_or(0)
                .max(*writer_epoch)
                .max(previous.map_or(0, |prefix| prefix.writer_epoch));
            next_trimmed.insert(
                stream.clone(),
                TrimmedPrefix {
                    stream: stream.clone(),
                    archived_lsn: *archived_lsn,
                    writer_epoch: retained_epoch,
                },
            );
        }
        let removed = state
            .records
            .keys()
            .filter(|(stream, lsn)| {
                next_trimmed
                    .get(stream)
                    .is_some_and(|prefix| *lsn <= prefix.archived_lsn)
            })
            .count();
        if removed == 0 && next_trimmed == state.trimmed {
            return Ok(0);
        }
        let retained = state
            .records
            .iter()
            .filter(|((candidate, lsn), _)| {
                next_trimmed
                    .get(candidate)
                    .is_none_or(|prefix| *lsn > prefix.archived_lsn)
            })
            .map(|(key, record)| {
                (
                    key.clone(),
                    record.clone(),
                    state.committed.get(key) == Some(record),
                )
            })
            .collect::<Vec<_>>();

        let parent = self.log_path.parent().ok_or_else(|| {
            ReplicaError::NodeStorage("replica log has no parent directory".to_owned())
        })?;
        static NEXT_COMPACTION_TEMP: AtomicU64 = AtomicU64::new(1);
        let sequence = NEXT_COMPACTION_TEMP.fetch_add(1, Ordering::Relaxed);
        let mut temp_os = self.log_path.as_os_str().to_owned();
        temp_os.push(format!(".compact-{}-{sequence}", std::process::id()));
        let temp = PathBuf::from(temp_os);
        let mut replacement = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&temp)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let write_result = (|| -> Result<(), ReplicaError> {
            for prefix in next_trimmed.values() {
                write_json_line(
                    &mut replacement,
                    &NodeTrimEntry {
                        kind: "trim".to_owned(),
                        stream: prefix.stream.clone(),
                        archived_lsn: prefix.archived_lsn,
                        writer_epoch: prefix.writer_epoch,
                    },
                )?;
            }
            for (_, record, committed) in &retained {
                write_json_line(&mut replacement, record)?;
                if *committed {
                    write_json_line(
                        &mut replacement,
                        &NodeCommitEntry {
                            kind: "commit".to_owned(),
                            record: record.clone(),
                        },
                    )?;
                }
            }
            replacement
                .sync_all()
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
        })();
        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&temp);
            return Err(error);
        }
        if let Err(error) = std::fs::rename(&temp, &self.log_path) {
            let _ = std::fs::remove_file(&temp);
            return Err(ReplicaError::NodeStorage(error.to_string()));
        }

        state.file = replacement;
        state.trimmed = next_trimmed;
        let durable_prefixes = state.trimmed.values().cloned().collect::<Vec<_>>();
        for prefix in durable_prefixes {
            state
                .highest_epoch
                .entry(prefix.stream)
                .and_modify(|epoch| *epoch = (*epoch).max(prefix.writer_epoch))
                .or_insert(prefix.writer_epoch);
        }
        let durable_trimmed = state.trimmed.clone();
        state.records.retain(|key, _| {
            durable_trimmed
                .get(&key.0)
                .is_none_or(|prefix| key.1 > prefix.archived_lsn)
        });
        state.committed.retain(|key, _| {
            durable_trimmed
                .get(&key.0)
                .is_none_or(|prefix| key.1 > prefix.archived_lsn)
        });
        let directory =
            File::open(parent).map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        directory
            .sync_all()
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        Ok(removed)
    }

    /// Returns a point-in-time snapshot for the internal gateway API.
    pub fn snapshot(&self) -> NodeSnapshot {
        self.state
            .lock()
            .map(|state| NodeSnapshot {
                records: state.records.values().cloned().collect(),
                committed: state.committed.values().cloned().collect(),
                trimmed: state.trimmed.values().cloned().collect(),
            })
            .unwrap_or_else(|_| NodeSnapshot::empty())
    }

    /// Collects authenticated storage telemetry from this node.
    ///
    /// A failure to read either the append log or the mounted data volume is
    /// returned to the HTTP layer instead of being represented as zeroes. A
    /// zero capacity value would make an autoscaler believe the disk is empty
    /// and is therefore unsafe.
    pub fn storage_status(&self) -> Result<StorageNodeStatus, ReplicaError> {
        let log_bytes = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?
            .file
            .metadata()
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            .len();
        let filesystem = filesystem_status(&self.data_dir)?;
        let timestamp_ms = unix_time_ms();
        let maintenance = self.maintenance_status()?;
        Ok(StorageNodeStatus {
            boot_id: self.boot_id.clone(),
            online_protocol_version: u32::from(self.placement.is_some()),
            node_name: self.node_name.clone(),
            tier: self.tier.clone(),
            log_bytes,
            filesystem,
            fs_total_bytes: 0,
            fs_free_bytes: 0,
            fs_used_bytes: 0,
            timestamp_ms,
            observed_at_ms: timestamp_ms,
            maintenance: maintenance.active,
            maintenance_owner: maintenance.owner,
            maintenance_generation: maintenance.generation,
        }
        .with_flat_filesystem())
    }

    /// Compatibility alias for callers that call the response a node status.
    pub fn status(&self) -> Result<StorageNodeStatus, ReplicaError> {
        self.storage_status()
    }

    /// Compatibility alias for callers that use the metrics terminology.
    pub fn metrics(&self) -> Result<StorageNodeStatus, ReplicaError> {
        self.storage_status()
    }
}

#[async_trait]
impl Replica for DiskReplica {
    /// Applies epoch fencing and fsyncs one idempotent opaque record.
    async fn append(&self, record: EncryptedRecord) -> Result<(), ReplicaError> {
        self.append_inner(record, None)
    }

    /// Persists an ordered batch with one node-log sync operation.
    async fn append_many(&self, records: Vec<EncryptedRecord>) -> Result<(), ReplicaError> {
        self.append_many_inner(&records, None, false)
    }

    /// Returns all opaque records currently held by the node.
    async fn records(&self, stream: &str) -> Vec<EncryptedRecord> {
        self.state
            .lock()
            .map(|state| {
                state
                    .records
                    .values()
                    .filter(|record| record.stream() == stream)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl DiskReplica {
    fn placement_lease(
        &self,
        stream: &str,
        placement: Option<&PlacementEpoch>,
    ) -> Result<Option<PlacementLease>, ReplicaError> {
        let (Some(store), Some(placement)) = (&self.placement, placement) else {
            if let (Some(store), None) = (&self.placement, placement) {
                // An explicit sidecar entry always requires its token. Only
                // a stream that has never had a placement route may pass
                // through the pre-route compatibility path; this avoids
                // treating a missing header as a wildcard for a migrated
                // legacy route.
                if store
                    .contains_stream(stream)
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
                {
                    return Err(ReplicaError::WriterFenced);
                }
                // A brand-new stream still has the exact epoch-zero
                // bootstrap route. Admit it through the same gate so a
                // concurrent first fence drains this append before
                // publishing its successor; this is not a wildcard for a
                // stream that already has a durable sidecar entry.
                return store
                    .admit(stream, &PlacementEpoch::bootstrap())
                    .map(Some)
                    .map_err(|error| match error {
                        PlacementError::Storage(reason) => ReplicaError::NodeStorage(reason),
                        PlacementError::InvalidStream => {
                            ReplicaError::Protocol("placement stream must not be empty".to_owned())
                        }
                        PlacementError::InvalidEpoch => {
                            ReplicaError::Protocol("invalid placement epoch".to_owned())
                        }
                        PlacementError::Transitioning
                        | PlacementError::Stale { .. }
                        | PlacementError::Future { .. }
                        | PlacementError::Conflict { .. }
                        | PlacementError::Decreasing { .. }
                        | PlacementError::FenceConflict { .. } => ReplicaError::WriterFenced,
                    });
            }
            return Ok(None);
        };
        store
            .admit(stream, placement)
            .map(Some)
            .map_err(|error| match error {
                PlacementError::Storage(reason) => ReplicaError::NodeStorage(reason),
                PlacementError::InvalidStream => {
                    ReplicaError::Protocol("placement stream must not be empty".to_owned())
                }
                PlacementError::InvalidEpoch => {
                    ReplicaError::Protocol("invalid placement epoch".to_owned())
                }
                PlacementError::Transitioning
                | PlacementError::Stale { .. }
                | PlacementError::Future { .. }
                | PlacementError::Conflict { .. }
                | PlacementError::Decreasing { .. }
                | PlacementError::FenceConflict { .. } => ReplicaError::WriterFenced,
            })
    }

    /// Installs the durable host placement fence for one stream. The caller
    /// must perform this before publishing the successor manifest route.
    pub fn install_placement_fence(
        &self,
        stream: &str,
        placement: PlacementEpoch,
    ) -> Result<(), ReplicaError> {
        let store = self.placement.as_ref().ok_or_else(|| {
            ReplicaError::Protocol("placement fences require direct node mode".to_owned())
        })?;
        store
            .install_fence(stream, placement)
            .map_err(|error| match error {
                PlacementError::Storage(reason) => ReplicaError::NodeStorage(reason),
                PlacementError::InvalidStream => {
                    ReplicaError::Protocol("placement stream must not be empty".to_owned())
                }
                PlacementError::InvalidEpoch => {
                    ReplicaError::Protocol("invalid placement epoch".to_owned())
                }
                PlacementError::Decreasing { .. } | PlacementError::FenceConflict { .. } => {
                    ReplicaError::LsnConflict
                }
                PlacementError::Transitioning
                | PlacementError::Stale { .. }
                | PlacementError::Future { .. }
                | PlacementError::Conflict { .. } => ReplicaError::WriterFenced,
            })
    }

    /// Returns the node-local durable placement for one stream.  This is
    /// intentionally a read of the sidecar, not a control-manifest hint: a
    /// restarted gateway must not mint an epoch that is lower than a fence
    /// already persisted on a member.
    pub fn current_placement(&self, stream: &str) -> Result<PlacementEpoch, ReplicaError> {
        self.placement
            .as_ref()
            .ok_or_else(|| {
                ReplicaError::Protocol("placement state is unavailable on this node".to_owned())
            })?
            .current(stream)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
    }

    async fn append_with_maintenance(
        &self,
        record: EncryptedRecord,
        token: &VerifiedMaintenanceToken,
    ) -> Result<(), ReplicaError> {
        self.append_inner(record, Some(token))
    }

    /// Appends data and commit markers for one batch before issuing one
    /// durability sync. This is the node-side boundary used by the gateway's
    /// normal append-many route; all validation completes before the log is
    /// changed so a rejected batch cannot leave a partial prefix.
    pub async fn append_and_commit_many(
        &self,
        records: &[EncryptedRecord],
    ) -> Result<(), ReplicaError> {
        self.append_many_inner(records, None, true)
    }

    /// Direct gateway append boundary carrying the host placement lease. The
    /// authenticated record bytes are unchanged; the lease only protects the
    /// route admission through the node's fsync and commit marker.
    async fn append_and_commit_many_with_placement(
        &self,
        records: &[EncryptedRecord],
        placement: Option<PlacementEpoch>,
    ) -> Result<(), ReplicaError> {
        self.append_many_inner_with_placement(records, None, true, placement.as_ref())
    }

    /// Whether the durable manifest proves `record` opens an immutable range
    /// on this member: the range must name this member, begin exactly at the
    /// record's LSN, and close the prior range at the record's authenticated
    /// cLSN. This is the one predecessor proof a member accepts without the
    /// preceding record on its own volume, so it is consulted only when no
    /// durable, compacted, or in-batch predecessor exists.
    fn has_manifest_boundary(&self, record: &EncryptedRecord) -> bool {
        self.control.as_ref().is_some_and(|control| {
            control
                .with_state(|control| {
                    control
                        .stream_segments
                        .get(record.stream())
                        .is_some_and(|segments| {
                            segments.iter().enumerate().any(|(index, segment)| {
                                segment.start_lsn == record.lsn()
                                    && segment.member_ids.iter().any(|id| id == &self.node_name)
                                    && record.committed_lsn() == segment.start_lsn.saturating_sub(1)
                                    && (index == 0
                                        || segments[index - 1].end_lsn
                                            == Some(record.committed_lsn()))
                            })
                        })
                })
                .unwrap_or(false)
        })
    }

    fn append_inner(
        &self,
        record: EncryptedRecord,
        maintenance_token: Option<&VerifiedMaintenanceToken>,
    ) -> Result<(), ReplicaError> {
        self.append_inner_with_placement(record, maintenance_token, None)
    }

    fn append_inner_with_placement(
        &self,
        record: EncryptedRecord,
        maintenance_token: Option<&VerifiedMaintenanceToken>,
        placement: Option<&PlacementEpoch>,
    ) -> Result<(), ReplicaError> {
        let _placement_lease = if maintenance_token.is_some() {
            None
        } else {
            self.placement_lease(record.stream(), placement)?
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        let maintenance_allowed = match (&state.maintenance.active, maintenance_token) {
            (None, None) => true,
            (Some(active), Some(token)) => active == &token.marker(),
            (None, Some(_)) | (Some(_), None) => false,
        };
        if !maintenance_allowed {
            return Err(ReplicaError::WriterFenced);
        }
        if record.committed_lsn() >= record.lsn() {
            return Err(ReplicaError::InvalidWatermark {
                lsn: record.lsn(),
                committed_lsn: record.committed_lsn(),
            });
        }
        let highest_epoch = state
            .highest_epoch
            .get(record.stream())
            .copied()
            .unwrap_or(0);
        if record.writer_epoch() < highest_epoch {
            return Err(ReplicaError::WriterFenced);
        }
        let trimmed_predecessor = state.trimmed.get(record.stream()).cloned();
        if trimmed_predecessor
            .as_ref()
            .is_some_and(|prefix| record.lsn() <= prefix.archived_lsn)
        {
            return Err(ReplicaError::LsnConflict);
        }
        let key = record_key(&record);
        if let Some(existing) = state.records.get(&key) {
            return if existing == &record {
                Ok(())
            } else {
                Err(ReplicaError::LsnConflict)
            };
        }
        // A storage member is an ordered log, not an unordered cache.  Never
        // accept a new record with a hole before it: a second coordinator can
        // race the first one, but it cannot manufacture LSN N+1 on a member
        // that has not durably received N.  The gateway already checks the
        // authenticated cLSN; repeating the predecessor check at the node
        // keeps direct/internal callers from bypassing that fence.
        let has_durable_predecessor = state
            .records
            .contains_key(&(record.stream().to_owned(), record.lsn().saturating_sub(1)));
        let has_compacted_predecessor = trimmed_predecessor
            .as_ref()
            .is_some_and(|prefix| prefix.archived_lsn == record.lsn().saturating_sub(1));
        // A newly activated cohort may legitimately receive the first record
        // of its immutable manifest range without owning the historical
        // prefix. The control manifest is the predecessor proof in that one
        // case: it must name this member, begin exactly at this LSN, and close
        // a prior range at the record's authenticated cLSN. Every other
        // append still requires the immediately preceding record on this
        // volume, including a record sent through a direct/internal route.
        let needs_manifest_boundary =
            record.lsn() > 1 && !has_durable_predecessor && !has_compacted_predecessor;
        if needs_manifest_boundary && !self.has_manifest_boundary(&record) {
            return Err(ReplicaError::LsnConflict);
        }
        let encoded = serde_json::to_vec(&record)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if let Some(cap) = self.log_max_bytes {
            let current = state
                .file
                .metadata()
                .map(|meta| meta.len())
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if current.saturating_add(encoded.len() as u64 + 1) > cap {
                return Err(ReplicaError::NodeStorage(format!(
                    "node log budget exhausted: {current} bytes used of {cap}; waiting for the \
                     archive to drain"
                )));
            }
        }
        state
            .file
            .write_all(&encoded)
            .and_then(|()| state.file.write_all(b"\n"))
            .and_then(|()| state.file.sync_data())
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        state.highest_epoch.insert(
            record.stream().to_owned(),
            record.writer_epoch().max(highest_epoch),
        );
        state.records.insert(key, record);
        Ok(())
    }

    fn append_many_inner(
        &self,
        records: &[EncryptedRecord],
        maintenance_token: Option<&VerifiedMaintenanceToken>,
        commit: bool,
    ) -> Result<(), ReplicaError> {
        self.append_many_inner_with_placement(records, maintenance_token, commit, None)
    }

    fn append_many_inner_with_placement(
        &self,
        records: &[EncryptedRecord],
        maintenance_token: Option<&VerifiedMaintenanceToken>,
        commit: bool,
        placement: Option<&PlacementEpoch>,
    ) -> Result<(), ReplicaError> {
        validate_append_batch(records)?;
        let _placement_lease = if maintenance_token.is_some() {
            None
        } else {
            self.placement_lease(
                records.first().map_or("", EncryptedRecord::stream),
                placement,
            )?
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("node lock is poisoned".to_owned()))?;
        let maintenance_allowed = match (&state.maintenance.active, maintenance_token) {
            (None, None) => true,
            (Some(active), Some(token)) => active == &token.marker(),
            (None, Some(_)) | (Some(_), None) => false,
        };
        if !maintenance_allowed {
            return Err(ReplicaError::WriterFenced);
        }

        let mut new_records = Vec::new();
        for record in records {
            let highest_epoch = state
                .highest_epoch
                .get(record.stream())
                .copied()
                .unwrap_or(0);
            if record.writer_epoch() < highest_epoch {
                return Err(ReplicaError::WriterFenced);
            }
            let trimmed_predecessor = state.trimmed.get(record.stream()).cloned();
            if trimmed_predecessor
                .as_ref()
                .is_some_and(|prefix| record.lsn() <= prefix.archived_lsn)
            {
                return Err(ReplicaError::LsnConflict);
            }
            let key = record_key(record);
            if let Some(existing) = state.records.get(&key) {
                if existing != record {
                    return Err(ReplicaError::LsnConflict);
                }
                continue;
            }

            // A record after the first may use the prior frame in this
            // validated batch as its predecessor. The first new frame still
            // needs a durable record, compacted prefix, or direct manifest
            // boundary on this volume.
            let has_batch_predecessor =
                new_records
                    .iter()
                    .any(|(_, previous): &(_, EncryptedRecord)| {
                        previous.stream() == record.stream()
                            && previous.lsn() == record.lsn().saturating_sub(1)
                    });
            let has_durable_predecessor = state
                .records
                .contains_key(&(record.stream().to_owned(), record.lsn().saturating_sub(1)));
            let has_compacted_predecessor = trimmed_predecessor
                .as_ref()
                .is_some_and(|prefix| prefix.archived_lsn == record.lsn().saturating_sub(1));
            let needs_manifest_boundary = record.lsn() > 1
                && !has_batch_predecessor
                && !has_durable_predecessor
                && !has_compacted_predecessor;
            if needs_manifest_boundary && !self.has_manifest_boundary(record) {
                return Err(ReplicaError::LsnConflict);
            }
            new_records.push((key, record.clone()));
        }

        let mut new_commits = Vec::new();
        if commit {
            for record in records {
                let key = record_key(record);
                if state
                    .trimmed
                    .get(record.stream())
                    .is_some_and(|prefix| record.lsn() <= prefix.archived_lsn)
                {
                    continue;
                }
                if state.committed.get(&key) == Some(record) {
                    continue;
                }
                if state.committed.contains_key(&key) {
                    return Err(ReplicaError::LsnConflict);
                }
                let present = state.records.get(&key) == Some(record)
                    || new_records
                        .iter()
                        .any(|(candidate, existing)| candidate == &key && existing == record);
                if !present {
                    return Err(ReplicaError::LsnConflict);
                }
                new_commits.push((key, record.clone()));
            }
        }

        if new_records.is_empty() && new_commits.is_empty() {
            return Ok(());
        }
        let start_offset = state
            .file
            .metadata()
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            .len();
        let write_result = (|| -> Result<(), ReplicaError> {
            for (_, record) in &new_records {
                write_json_line(&mut state.file, record)?;
            }
            for (_, record) in &new_commits {
                write_json_line(
                    &mut state.file,
                    &NodeCommitEntry {
                        kind: "commit".to_owned(),
                        record: record.clone(),
                    },
                )?;
            }
            state
                .file
                .sync_data()
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
        })();
        if let Err(error) = write_result {
            let _ = state.file.set_len(start_offset);
            let _ = state.file.sync_data();
            return Err(error);
        }

        for (key, record) in new_records {
            state
                .highest_epoch
                .entry(record.stream().to_owned())
                .and_modify(|epoch| *epoch = (*epoch).max(record.writer_epoch()))
                .or_insert(record.writer_epoch());
            state.records.insert(key, record);
        }
        for (key, record) in new_commits {
            state.committed.insert(key, record);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Authenticator {
    root: [u8; 32],
}

impl Authenticator {
    /// Decodes the same root key used by the Cloudflare control plane.
    fn new(encoded: &str) -> Result<Self, ReplicaError> {
        let decoded = STANDARD
            .decode(encoded.trim())
            .map_err(|_| ReplicaError::NodeStorage("root key must be base64".to_owned()))?;
        let root = decoded.try_into().map_err(|_| {
            ReplicaError::NodeStorage("root key must decode to 32 bytes".to_owned())
        })?;
        Ok(Self { root })
    }

    /// Verifies a tenant token for the claimed tenant-qualified stream.
    fn permits(&self, headers: &HeaderMap, stream: &str) -> bool {
        let Some((tenant, _)) = stream.split_once('/') else {
            return false;
        };
        let Some(token) = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .and_then(|value| hex::decode(value).ok())
        else {
            return false;
        };
        let Ok(mut mac) = <HmacSha256 as Mac>::new_from_slice(&self.root) else {
            return false;
        };
        mac.update(
            format!("{DERIVATION_VERSION}\0{tenant}\0replica-gateway-authentication").as_bytes(),
        );
        mac.verify_slice(&token).is_ok()
    }

    #[allow(clippy::too_many_arguments)]
    fn issue_commit_certificate_bound(
        &self,
        record: &EncryptedRecord,
        cohort_id: u64,
        segment_start_lsn: u64,
        manifest_revision: u64,
        manifest_digest: &str,
        segment_end_lsn: Option<u64>,
        member_hash: &str,
        segment_operation_id: &str,
    ) -> Result<String, ReplicaError> {
        let payload = CommitCertificatePayload {
            version: COMMIT_CERTIFICATE_VERSION,
            stream: record.stream().to_owned(),
            writer_epoch: record.writer_epoch(),
            committed_lsn: record.lsn(),
            record_digest: record_digest(record)?,
            cohort_id,
            segment_start_lsn,
            manifest_revision,
            manifest_digest: manifest_digest.to_owned(),
            segment_end_lsn,
            member_hash: member_hash.to_owned(),
            segment_operation_id: segment_operation_id.to_owned(),
        };
        let encoded = serde_json::to_vec(&payload)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.root)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        mac.update(COMMIT_CERTIFICATE_DOMAIN.as_bytes());
        mac.update(&encoded);
        let signature = mac.finalize().into_bytes();
        Ok(format!(
            "{}.{}",
            STANDARD.encode(encoded),
            hex::encode(signature)
        ))
    }

    fn issue_maintenance_token(
        &self,
        owner: impl Into<String>,
        generation: u64,
    ) -> Result<String, ReplicaError> {
        let owner = owner.into();
        if owner.trim().is_empty() || generation == 0 {
            return Err(ReplicaError::Protocol(
                "maintenance owner and generation are required".to_owned(),
            ));
        }
        let payload = MaintenanceTokenPayload {
            version: MAINTENANCE_TOKEN_VERSION,
            owner,
            generation,
        };
        let payload = serde_json::to_vec(&payload)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let encoded = URL_SAFE_NO_PAD.encode(&payload);
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.root)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        mac.update(MAINTENANCE_TOKEN_DOMAIN.as_bytes());
        mac.update(&payload);
        Ok(format!(
            "{encoded}.{}",
            hex::encode(mac.finalize().into_bytes())
        ))
    }

    fn verify_maintenance_token(
        &self,
        token: &str,
    ) -> Result<VerifiedMaintenanceToken, ReplicaError> {
        let unauthorized = || ReplicaError::GatewayUnauthorized;
        let (encoded, signature) = token.trim().split_once('.').ok_or_else(unauthorized)?;
        if encoded.is_empty() || signature.is_empty() {
            return Err(unauthorized());
        }
        let payload = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| unauthorized())?;
        let signature_bytes = hex::decode(signature).map_err(|_| unauthorized())?;
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&self.root).map_err(|_| unauthorized())?;
        mac.update(MAINTENANCE_TOKEN_DOMAIN.as_bytes());
        mac.update(&payload);
        mac.verify_slice(&signature_bytes)
            .map_err(|_| unauthorized())?;
        let payload: MaintenanceTokenPayload =
            serde_json::from_slice(&payload).map_err(|_| unauthorized())?;
        if payload.version != MAINTENANCE_TOKEN_VERSION
            || payload.owner.trim().is_empty()
            || payload.generation == 0
        {
            return Err(unauthorized());
        }
        Ok(VerifiedMaintenanceToken {
            owner: payload.owner,
            generation: payload.generation,
            signature: signature.to_owned(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_commit_certificate(
        &self,
        certificate: &str,
        stream: &str,
        committed_lsn: u64,
        record: &EncryptedRecord,
        expected_cohort_id: Option<u64>,
        expected_segment_start_lsn: Option<u64>,
        expected_manifest_revision: Option<u64>,
        expected_manifest_digest: Option<&str>,
        expected_segment_end_lsn: Option<Option<u64>>,
        expected_member_hash: Option<&str>,
        expected_segment_operation_id: Option<&str>,
    ) -> Result<(), ReplicaError> {
        let unauthorized = || ReplicaError::GatewayUnauthorized;
        let (encoded, signature) = certificate
            .trim()
            .split_once('.')
            .ok_or_else(unauthorized)?;
        let payload_bytes = STANDARD.decode(encoded).map_err(|_| unauthorized())?;
        let signature = hex::decode(signature).map_err(|_| unauthorized())?;
        let mut mac =
            <HmacSha256 as Mac>::new_from_slice(&self.root).map_err(|_| unauthorized())?;
        mac.update(COMMIT_CERTIFICATE_DOMAIN.as_bytes());
        mac.update(&payload_bytes);
        mac.verify_slice(&signature).map_err(|_| unauthorized())?;
        let payload: CommitCertificatePayload =
            serde_json::from_slice(&payload_bytes).map_err(|_| unauthorized())?;
        if payload.version != COMMIT_CERTIFICATE_VERSION
            || payload.stream != stream
            || payload.committed_lsn != committed_lsn
            || payload.writer_epoch != record.writer_epoch()
            || payload.committed_lsn != record.lsn()
            || payload.record_digest != record_digest(record).map_err(|_| unauthorized())?
            || expected_cohort_id.is_some_and(|cohort_id| payload.cohort_id != cohort_id)
            || expected_segment_start_lsn
                .is_some_and(|segment_start_lsn| payload.segment_start_lsn != segment_start_lsn)
            || expected_manifest_revision
                .is_some_and(|revision| payload.manifest_revision != revision)
            || expected_manifest_digest.is_some_and(|digest| payload.manifest_digest != digest)
            || expected_segment_end_lsn.is_some_and(|end_lsn| payload.segment_end_lsn != end_lsn)
            || expected_member_hash.is_some_and(|member_hash| payload.member_hash != member_hash)
            || expected_segment_operation_id
                .is_some_and(|operation_id| payload.segment_operation_id != operation_id)
        {
            return Err(unauthorized());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MaintenanceTokenPayload {
    version: u8,
    owner: String,
    generation: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CommitCertificatePayload {
    version: u8,
    stream: String,
    writer_epoch: u64,
    committed_lsn: u64,
    record_digest: String,
    #[serde(default)]
    cohort_id: u64,
    #[serde(default)]
    segment_start_lsn: u64,
    /// Revision and digest of the quorum manifest used to route the record.
    /// Defaults preserve verification of certificates emitted by the older
    /// static-node compatibility path; direct-mode recovery supplies both
    /// expected values and therefore rejects such a certificate.
    #[serde(default)]
    manifest_revision: u64,
    #[serde(default)]
    manifest_digest: String,
    #[serde(default)]
    segment_end_lsn: Option<u64>,
    #[serde(default)]
    member_hash: String,
    #[serde(default)]
    segment_operation_id: String,
}

#[derive(Clone)]
struct NodeState {
    node: Arc<DiskReplica>,
    auth: Authenticator,
    internal_token: Option<String>,
    control: Option<Arc<DurableControl>>,
}

#[derive(Deserialize)]
struct RecordsQuery {
    stream: String,
}

/// Builds one node's authenticated append, recovery, and health surface.
///
/// Compatibility /v1 routes are tenant-authenticated. /internal/v1 routes
/// require the private token when one was configured for this node.
pub fn router(node: Arc<DiskReplica>, dataplane_root_key: &str) -> Result<Router, ReplicaError> {
    node_router(node, dataplane_root_key, None)
}

/// Builds a node router with an optional private gateway token.
pub fn node_router(
    node: Arc<DiskReplica>,
    dataplane_root_key: &str,
    internal_token: Option<&str>,
) -> Result<Router, ReplicaError> {
    let control = node.control();
    Ok(Router::new()
        .route("/healthz", get(node_health))
        .route("/readyz", get(node_health))
        .route("/v1/append", post(public_append))
        .route("/v1/append-many", post(public_append_many))
        .route("/v1/records", get(public_records))
        .route("/internal/v1/healthz", get(internal_health))
        .route("/internal/v1/metrics", get(internal_status))
        .route("/internal/v1/status", get(internal_status))
        .route("/internal/v1/node/metrics", get(internal_status))
        .route("/internal/v1/node/status", get(internal_status))
        .route("/internal/v1/storage/metrics", get(internal_status))
        .route("/internal/v1/storage/status", get(internal_status))
        .route("/internal/v1/storage/readyz", get(internal_storage_ready))
        .route("/internal/v1/readyz", get(internal_storage_ready))
        .route("/internal/v1/append", post(internal_append))
        .route("/internal/v1/append-many", post(internal_append_many))
        .route("/internal/v1/commit", post(internal_commit))
        .route("/internal/v1/supersede", post(internal_supersede))
        .route("/internal/v1/control", get(internal_control_state))
        .route(
            "/internal/v1/control/metadata/freeze",
            post(metadata_authority::internal_metadata_freeze),
        )
        .route(
            "/internal/v1/control/metadata/adopt",
            post(metadata_authority::internal_metadata_adopt),
        )
        .route(
            "/internal/v1/control/membership",
            post(internal_control_membership),
        )
        .route(
            "/internal/v1/control/membership/online",
            post(internal_control_membership_online),
        )
        .route(
            "/internal/v1/control/placement/fence",
            post(internal_control_placement_fence),
        )
        .route(
            "/internal/v1/control/placement",
            get(internal_control_placement),
        )
        .route(
            "/internal/v1/control/manifest",
            get(internal_control_manifest_read).post(internal_control_manifest),
        )
        .route(
            "/internal/v1/control/manifest/cas",
            post(internal_control_manifest),
        )
        .route(
            "/internal/v1/membership/cas",
            post(internal_control_membership),
        )
        .route(
            "/internal/v1/maintenance/append",
            post(internal_maintenance_append),
        )
        .route(
            "/internal/v1/maintenance/compact",
            post(internal_maintenance_compact),
        )
        .route(
            "/internal/v1/maintenance/commit",
            post(internal_maintenance_commit),
        )
        .route(
            "/internal/v1/maintenance/fence",
            post(internal_maintenance_fence),
        )
        .route(
            "/internal/v1/maintenance/fence/{token}",
            delete(internal_maintenance_release),
        )
        .route("/internal/v1/records", get(internal_records))
        .with_state(NodeState {
            node,
            auth: Authenticator::new(dataplane_root_key)?,
            internal_token: internal_token
                .filter(|token| !token.is_empty())
                .map(ToOwned::to_owned),
            control,
        })
        .layer(DefaultBodyLimit::max(MAX_ENCRYPTED_RECORD_BATCH_BYTES)))
}

async fn node_health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Decodes an append body while retaining JSON as a compatibility transport.
/// Production clients use the bounded binary representation so byte fields do
/// not expand into JSON arrays.
fn encrypted_record_body(headers: &HeaderMap, body: &Bytes) -> Result<EncryptedRecord, StatusCode> {
    let is_binary = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type
                    .trim()
                    .eq_ignore_ascii_case(ENCRYPTED_RECORD_CONTENT_TYPE)
            })
        });
    if is_binary {
        EncryptedRecord::decode_binary(body).map_err(|_| StatusCode::BAD_REQUEST)
    } else {
        serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)
    }
}

/// Decodes the framed binary append-many representation. Batch routes do not
/// accept JSON because a length-delimited binary frame is the bounded,
/// allocation-safe production transport.
fn encrypted_record_batch_body(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<Vec<EncryptedRecord>, StatusCode> {
    let is_binary = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| {
                media_type
                    .trim()
                    .eq_ignore_ascii_case(ENCRYPTED_RECORD_CONTENT_TYPE)
            })
        });
    if !is_binary {
        return Err(StatusCode::BAD_REQUEST);
    }
    EncryptedRecord::decode_binary_batch(body).map_err(|_| StatusCode::BAD_REQUEST)
}

async fn internal_health(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if permits_internal(&headers, state.internal_token.as_deref()) {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn internal_storage_ready(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if permits_internal(&headers, state.internal_token.as_deref()) {
        state
            .node
            .storage_status()
            .map(|_| StatusCode::NO_CONTENT)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

/// Returns the authenticated node identity, log size, and mounted-volume
/// capacity. A failed statvfs/metadata call is unavailable, never a zeroed
/// success response.
async fn internal_status(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> Result<Json<StorageNodeStatus>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .node
        .storage_status()
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

/// Authenticates a tenant record and returns only after its local fsync.
async fn public_append(
    State(state): State<NodeState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let Ok(record) = encrypted_record_body(&headers, &body) else {
        return StatusCode::BAD_REQUEST;
    };
    if !state.auth.permits(&headers, record.stream()) {
        return StatusCode::UNAUTHORIZED;
    }
    append_status(state.node.append(record).await)
}

/// Authenticated compatibility route for a data-only framed batch.
async fn public_append_many(
    State(state): State<NodeState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let Ok(records) = encrypted_record_batch_body(&headers, &body) else {
        return StatusCode::BAD_REQUEST;
    };
    if records
        .iter()
        .any(|record| !state.auth.permits(&headers, record.stream()))
    {
        return StatusCode::UNAUTHORIZED;
    }
    append_status(state.node.append_many(records).await)
}

/// Internal gateway append after tenant authentication at the gateway.
async fn internal_append(
    State(state): State<NodeState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let record = encrypted_record_body(&headers, &body)?;
    Ok(append_status(state.node.append(record).await))
}

/// Internal gateway route for one durable append-and-commit batch.
async fn internal_append_many(
    State(state): State<NodeState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let records = encrypted_record_batch_body(&headers, &body)?;
    let placement = placement_from_headers(&headers)?;
    let result = state
        .node
        .append_and_commit_many_with_placement(&records, placement)
        .await;
    let fenced = matches!(result, Err(ReplicaError::WriterFenced));
    let mut response = append_status(result).into_response();
    if fenced {
        response.headers_mut().insert(
            HeaderName::from_static(PLACEMENT_FENCED_HEADER),
            HeaderValue::from_static("true"),
        );
    }
    Ok(response)
}

fn placement_from_headers(headers: &HeaderMap) -> Result<Option<PlacementEpoch>, StatusCode> {
    let epoch = headers.get(PLACEMENT_EPOCH_HEADER);
    let digest = headers.get(PLACEMENT_DIGEST_HEADER);
    match (epoch, digest) {
        (None, None) => Ok(None),
        (Some(epoch), Some(digest)) => {
            let epoch = epoch
                .to_str()
                .map_err(|_| StatusCode::BAD_REQUEST)?
                .parse::<u64>()
                .map_err(|_| StatusCode::BAD_REQUEST)?;
            let digest = digest
                .to_str()
                .map_err(|_| StatusCode::BAD_REQUEST)?
                .to_owned();
            if epoch == 0 || digest.trim().is_empty() {
                return Err(StatusCode::BAD_REQUEST);
            }
            Ok(Some(PlacementEpoch::new(epoch, digest)))
        }
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PlacementFenceRequest {
    stream: String,
    epoch: u64,
    #[serde(default)]
    route_digest: String,
}

async fn internal_control_placement_fence(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(request): Json<PlacementFenceRequest>,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if request.stream.trim().is_empty()
        || request.epoch == 0
        || request.route_digest.trim().is_empty()
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let node = Arc::clone(&state.node);
    let placement = PlacementEpoch::new(request.epoch, request.route_digest);
    tokio::task::spawn_blocking(move || node.install_placement_fence(&request.stream, placement))
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(placement_status)
}

/// Reads the node-local durable placement token for one stream.  A cutover
/// coordinator uses this as an observation only; the fence endpoint remains
/// the serialization point.  Reading the token avoids deriving a new epoch
/// from a stale manifest revision after a coordinator restart.
async fn internal_control_placement(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Query(query): Query<RecordsQuery>,
) -> Result<Json<PlacementEpoch>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .node
        .current_placement(&query.stream)
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

async fn internal_commit(
    State(state): State<NodeState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let record = encrypted_record_body(&headers, &body)?;
    Ok(append_status(state.node.commit(record).await))
}

#[derive(Deserialize, Serialize)]
struct SupersedeRequest {
    stream: String,
    from_lsn: u64,
    writer_epoch: u64,
}

/// Withdraws a member's orphaned suffix; see [`DiskReplica::supersede`].
async fn internal_supersede(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(request): Json<SupersedeRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    match state
        .node
        .supersede(&request.stream, request.from_lsn, request.writer_epoch)
    {
        Ok(withdrawn) => Ok(Json(serde_json::json!({ "withdrawn": withdrawn }))),
        Err(ReplicaError::WriterFenced | ReplicaError::LsnConflict) => Err(StatusCode::CONFLICT),
        Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}

async fn internal_maintenance_append(
    State(state): State<NodeState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let record = encrypted_record_body(&headers, &body)?;
    let token = required_maintenance_token(&state, &headers)?;
    Ok(append_status(
        state.node.append_with_maintenance(record, &token).await,
    ))
}

/// Installs archived-prefix trim checkpoints under the maintenance fence. A
/// replacement member receives the hot records of its slot, but the design
/// requires the trim checkpoint too: it is what lets the member accept the
/// exact successor of a prefix that now lives only in the archive.
async fn internal_maintenance_compact(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(prefixes): Json<Vec<TrimmedPrefix>>,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token = required_maintenance_token(&state, &headers)?;
    state
        .node
        .verify_fence(&token)
        .map_err(maintenance_status)?;
    let prefixes = prefixes
        .into_iter()
        .map(|prefix| (prefix.stream, prefix.archived_lsn, prefix.writer_epoch))
        .collect::<Vec<_>>();
    state
        .node
        .compact_archived_batch(&prefixes)
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|_| StatusCode::CONFLICT)
}

async fn internal_maintenance_commit(
    State(state): State<NodeState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let record = encrypted_record_body(&headers, &body)?;
    let token = required_maintenance_token(&state, &headers)?;
    Ok(append_status(
        state.node.commit_with_maintenance(record, &token).await,
    ))
}

async fn internal_maintenance_fence(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token = maintenance_token(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let token = state
        .auth
        .verify_maintenance_token(token)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    state
        .node
        .fence(&token)
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(maintenance_status)
}

async fn internal_maintenance_release(
    State(state): State<NodeState>,
    headers: HeaderMap,
    RoutePath(token): RoutePath<String>,
) -> Result<StatusCode, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if let Some(header_token) = maintenance_token(&headers)
        && header_token != token
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token = state
        .auth
        .verify_maintenance_token(&token)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    state
        .node
        .release_fence(&token)
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(maintenance_status)
}

fn required_maintenance_token(
    state: &NodeState,
    headers: &HeaderMap,
) -> Result<VerifiedMaintenanceToken, StatusCode> {
    let token = maintenance_token(headers).ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .auth
        .verify_maintenance_token(token)
        .map_err(|_| StatusCode::UNAUTHORIZED)
}

async fn internal_control_state(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> Result<Json<DurableControlState>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let control = state.control.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    control
        .state()
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

/// Request body used when propagating a direct membership CAS to storage
/// volumes. The gateway wraps this operation in its all-member fence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MembershipCasRequest {
    #[serde(alias = "membership_epoch", alias = "expected_membership_epoch")]
    pub expected_epoch: u64,
    #[serde(default)]
    pub target_epoch: Option<u64>,
    /// A fenced coordinator sets this only when repairing a peer from the
    /// authoritative membership snapshot after an earlier broadcast loss.
    #[serde(default)]
    pub repair: bool,
    pub members: Vec<DurableMember>,
    #[serde(default)]
    pub cohorts: Option<Vec<DurableCohort>>,
    #[serde(default)]
    pub stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
    #[serde(default)]
    pub manifest_revision: Option<u64>,
    #[serde(default)]
    pub manifest_digest: Option<String>,
    /// Future-write cohort committed with an online activation. Omitted by
    /// legacy/fenced membership broadcasts, which preserve the current
    /// target.
    #[serde(default)]
    pub write_cohort_id: Option<u64>,
    /// Whole-cohort handoff mode. Additive online CASes must never be able
    /// to retire an existing source cohort.
    #[serde(default)]
    pub replacement: bool,
    #[serde(default)]
    pub control_authority_epoch: u64,
    #[serde(default)]
    pub control_head_revision: u64,
    #[serde(default)]
    pub control_head_digest: String,
    #[serde(default)]
    pub control_authority_cohort_id: u64,
    #[serde(alias = "op_id", alias = "operation")]
    pub operation_id: String,
}

/// Request used to publish one immutable stream routing range to a peer
/// coordinator. It carries no record data and is safe to retry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamSegmentRequest {
    pub stream: String,
    pub segment: StreamSegment,
    #[serde(alias = "op_id", alias = "operation")]
    pub operation_id: String,
}

/// Full replicated-manifest compare-and-swap request.  A request carries the
/// entire map so a stateless coordinator never relies on a local stream cache.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestCasRequest {
    #[serde(alias = "revision", alias = "expected_manifest_revision")]
    pub expected_revision: u64,
    #[serde(default, alias = "digest", alias = "expected_manifest_digest")]
    pub expected_digest: String,
    #[serde(alias = "manifest")]
    pub stream_segments: BTreeMap<String, Vec<StreamSegment>>,
    #[serde(alias = "op_id", alias = "operation")]
    pub operation_id: String,
    /// Set only by the coordinator after it has observed a two-member quorum;
    /// it lets that quorum heal a divergent third replica at the same
    /// revision without allowing a stale coordinator to overwrite a newer
    /// manifest.
    #[serde(default)]
    pub repair: bool,
    /// Durable LSN at which an existing open stream tail was sealed. This is
    /// required whenever a normal CAS closes an open range.
    #[serde(default)]
    pub cutover_lsn: Option<u64>,
}

async fn internal_control_membership(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(request): Json<MembershipCasRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token = required_maintenance_token(&state, &headers)?;
    state
        .node
        .verify_fence(&token)
        .map_err(maintenance_status)?;
    let control = state.control.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    control
        .cas_membership_document_with_manifest(
            request.expected_epoch,
            request.target_epoch,
            request.repair,
            request.members,
            request.cohorts,
            request.stream_segments,
            request.manifest_revision,
            request.manifest_digest,
            &request.operation_id,
        )
        .map(Json)
        .map_err(control_status)
}

async fn internal_control_membership_online(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(request): Json<MembershipCasRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let control = state.control.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let operation_id = request.operation_id.clone();
    let expected_epoch = request.expected_epoch;
    let target_epoch = request.target_epoch;
    let replacement = request.replacement;
    let node_name = state.node.node_name.clone();
    let current_state = control.state().ok();
    let current_membership_epoch = current_state
        .as_ref()
        .map_or(0, |state| state.membership_epoch);
    let current_authority_epoch = current_state
        .as_ref()
        .map_or(0, |state| state.control_authority_epoch);
    let current_head_revision = current_state
        .as_ref()
        .map_or(0, |state| state.control_head_revision);
    let current_manifest_revision = current_state
        .as_ref()
        .map_or(0, |state| state.manifest_revision);
    let current_manifest_digest_len = current_state
        .as_ref()
        .map_or(0, |state| state.manifest_digest.len());
    let current_member_count = current_state
        .as_ref()
        .map_or(0, |state| state.members.len());
    let current_cohort_count = current_state
        .as_ref()
        .map_or(0, |state| state.cohorts.len());
    let current_stream_count = current_state
        .as_ref()
        .map_or(0, |state| state.stream_segments.len());
    let request_manifest_revision = request.manifest_revision;
    let request_manifest_digest_len = request.manifest_digest.as_ref().map_or(0, String::len);
    let empty_manifest_digest = manifest_digest(&BTreeMap::new());
    let request_manifest_digest_matches_empty = request
        .manifest_digest
        .as_deref()
        .is_some_and(|digest| digest == empty_manifest_digest);
    let request_member_count = request.members.len();
    let request_cohort_count = request.cohorts.as_ref().map_or(0, Vec::len);
    let request_stream_count = request.stream_segments.as_ref().map_or(0, BTreeMap::len);
    let result = if request.replacement {
        control.cas_membership_document_online_replacement(
            request.expected_epoch,
            request.target_epoch,
            request.repair,
            request.members,
            request.cohorts,
            request.stream_segments,
            request.manifest_revision,
            request.manifest_digest,
            request.write_cohort_id,
            &request.operation_id,
        )
    } else {
        control.cas_membership_document_online(
            request.expected_epoch,
            request.target_epoch,
            request.repair,
            request.members,
            request.cohorts,
            request.stream_segments,
            request.manifest_revision,
            request.manifest_digest,
            request.write_cohort_id,
            &request.operation_id,
        )
    };
    result.map(Json).map_err(|error| {
        let status = control_status(error.clone());
        eprintln!(
            "replica_control_membership_online_failed node={} operation_id={} expected_epoch={} target_epoch={:?} current_membership_epoch={} current_authority_epoch={} current_head_revision={} current_manifest_revision={} current_manifest_digest_len={} current_member_count={} current_cohort_count={} current_stream_count={} request_manifest_revision={:?} request_manifest_digest_len={} request_manifest_digest_matches_empty={} request_member_count={} request_cohort_count={} request_stream_count={} request_authority_epoch={} request_head_revision={} request_authority_cohort={} replacement={} status={} error={error:?}",
            node_name,
            operation_id,
            expected_epoch,
            target_epoch,
            current_membership_epoch,
            current_authority_epoch,
            current_head_revision,
            current_manifest_revision,
            current_manifest_digest_len,
            current_member_count,
            current_cohort_count,
            current_stream_count,
            request_manifest_revision,
            request_manifest_digest_len,
            request_manifest_digest_matches_empty,
            request_member_count,
            request_cohort_count,
            request_stream_count,
            request.control_authority_epoch,
            request.control_head_revision,
            request.control_authority_cohort_id,
            replacement,
            status.as_u16(),
        );
        status
    })
}

async fn internal_control_manifest(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(request): Json<ManifestCasRequest>,
) -> Result<Json<ReplicaManifest>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let control = state.control.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    control
        .cas_manifest_with_cutover(
            request.expected_revision,
            &request.expected_digest,
            request.stream_segments,
            &request.operation_id,
            request.repair,
            request.cutover_lsn,
        )
        .map(Json)
        .map_err(control_status)
}

/// Reads the locally durable placement manifest.  Coordinators must combine
/// these reads through the fixed three-member control cohort before using the
/// value for routing; exposing the raw value here is intentional so a
/// restarted stateless coordinator can reconstruct a quorum certificate.
async fn internal_control_manifest_read(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> Result<Json<ReplicaManifest>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let control = state.control.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    // `manifest()` validates the document against the exact state it was
    // built from; re-validating against a second full copy added nothing.
    control.manifest().map(Json).map_err(control_status)
}

fn control_status(error: ReplicaError) -> StatusCode {
    match error {
        ReplicaError::LsnConflict | ReplicaError::WriterFenced => StatusCode::CONFLICT,
        ReplicaError::CapacityExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        ReplicaError::GatewayUnauthorized => StatusCode::UNAUTHORIZED,
        ReplicaError::Protocol(_) | ReplicaError::NodeStorage(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Returns all tenant-authenticated opaque records for compatibility with the
/// direct-node client.
async fn public_records(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Query(query): Query<RecordsQuery>,
) -> Result<Json<Vec<EncryptedRecord>>, StatusCode> {
    if !state.auth.permits(&headers, &query.stream) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(state.node.records(&query.stream).await))
}

async fn internal_records(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Query(query): Query<RecordsQuery>,
) -> Result<Json<NodeSnapshot>, StatusCode> {
    if !permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let mut snapshot = state.node.snapshot();
    if query.stream != "*" {
        snapshot
            .records
            .retain(|record| record.stream() == query.stream);
        snapshot
            .committed
            .retain(|record| record.stream() == query.stream);
        snapshot
            .trimmed
            .retain(|prefix| prefix.stream == query.stream);
    }
    Ok(Json(snapshot))
}

fn permits_internal(headers: &HeaderMap, expected: Option<&str>) -> bool {
    match expected {
        Some(expected) => headers
            .get(INTERNAL_AUTH_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == expected),
        // A missing private credential is never an authorization grant. The
        // no-token constructor remains useful for building public-only test
        // routers, but its internal surface must still fail closed.
        None => false,
    }
}

fn append_status(result: Result<(), ReplicaError>) -> StatusCode {
    match result {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(ReplicaError::WriterFenced | ReplicaError::LsnConflict) => StatusCode::CONFLICT,
        Err(ReplicaError::InvalidWatermark { .. }) => StatusCode::BAD_REQUEST,
        Err(ReplicaError::CapacityExceeded { .. }) => StatusCode::PAYLOAD_TOO_LARGE,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn maintenance_status(error: ReplicaError) -> StatusCode {
    match error {
        ReplicaError::GatewayUnauthorized => StatusCode::UNAUTHORIZED,
        ReplicaError::WriterFenced | ReplicaError::LsnConflict => StatusCode::CONFLICT,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn placement_status(error: ReplicaError) -> StatusCode {
    match error {
        ReplicaError::WriterFenced | ReplicaError::LsnConflict => StatusCode::CONFLICT,
        ReplicaError::Protocol(_) => StatusCode::BAD_REQUEST,
        ReplicaError::NodeStorage(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn record_key(record: &EncryptedRecord) -> (String, u64) {
    (record.stream().to_owned(), record.lsn())
}

fn record_identity(record: &EncryptedRecord) -> Result<Vec<u8>, ReplicaError> {
    serde_json::to_vec(record).map_err(|error| ReplicaError::NodeStorage(error.to_string()))
}

fn record_digest(record: &EncryptedRecord) -> Result<String, ReplicaError> {
    Ok(hex::encode(Sha256::digest(&record_identity(record)?)))
}

fn manifest_from_state(state: &DurableControlState) -> ReplicaManifest {
    let revision = state.manifest_revision;
    let digest = if revision == 0 {
        String::new()
    } else if state.manifest_digest.is_empty() {
        manifest_digest(&state.stream_segments)
    } else {
        state.manifest_digest.clone()
    };
    let cohort_id = if state.write_cohort_id != 0 {
        state.write_cohort_id
    } else {
        state.manifest_cohort_id
    };
    let member_set_hash = if revision == 0 {
        String::new()
    } else if state.manifest_member_hash.is_empty() {
        cohort_member_hash(state, cohort_id).unwrap_or_default()
    } else {
        state.manifest_member_hash.clone()
    };
    ReplicaManifest {
        version: MANIFEST_VERSION,
        revision,
        digest,
        writer_epoch: state.manifest_writer_epoch,
        cohort_id,
        member_set_hash,
        write_cohort_id: state.write_cohort_id,
        stream_segments: state.stream_segments.clone(),
        operation_id: state.manifest_operation_id.clone(),
        tier: if revision == 0 {
            String::new()
        } else {
            manifest_policy_for_state(state, &state.stream_segments, cohort_id).0
        },
        max_append_bytes: if revision == 0 {
            0
        } else {
            manifest_policy_for_state(state, &state.stream_segments, cohort_id).1
        },
    }
}

fn validate_manifest_against_state(
    state: &DurableControlState,
    manifest: &ReplicaManifest,
) -> Result<(), ReplicaError> {
    manifest.validate()?;
    if manifest.revision != state.manifest_revision
        || (manifest.revision > 0 && manifest.digest != manifest_digest(&state.stream_segments))
    {
        return Err(ReplicaError::Protocol(
            "placement manifest revision or digest is inconsistent with control state".to_owned(),
        ));
    }
    if manifest.revision == 0 {
        return Ok(());
    }
    let (writer_epoch, cohort_id, member_hash) =
        manifest_metadata_for_state(state, &state.stream_segments);
    if manifest.writer_epoch != writer_epoch
        || manifest.cohort_id != cohort_id
        || manifest.member_set_hash != member_hash
        || manifest.write_cohort_id != state.write_cohort_id
    {
        return Err(ReplicaError::Protocol(
            "placement manifest cohort binding is inconsistent with control state".to_owned(),
        ));
    }
    let (tier, max_append_bytes) =
        manifest_policy_for_state(state, &manifest.stream_segments, manifest.cohort_id);
    if manifest.tier != tier || manifest.max_append_bytes != max_append_bytes {
        return Err(ReplicaError::Protocol(
            "placement manifest capacity policy is inconsistent with control state".to_owned(),
        ));
    }
    validate_manifest_segments(state, &manifest.stream_segments, manifest.revision)
}

fn manifest_metadata_for_state(
    state: &DurableControlState,
    segments: &BTreeMap<String, Vec<StreamSegment>>,
) -> (u64, u64, String) {
    let (segment_epoch, segment_cohort, segment_hash) = latest_manifest_binding(segments);
    let writer_epoch = state.manifest_writer_epoch.max(segment_epoch);
    if state.write_cohort_id != 0 {
        let member_hash = cohort_member_hash(state, state.write_cohort_id).unwrap_or_default();
        return (writer_epoch, state.write_cohort_id, member_hash);
    }
    // A membership CAS can advance its own epoch while the current stream
    // tail still belongs to an older cohort. Preserve that route binding until
    // the next immutable segment is published; selecting the newest active
    // cohort here would make the unchanged manifest self-inconsistent.
    if !segment_hash.is_empty() {
        return (writer_epoch, segment_cohort, segment_hash);
    }
    let cohort_id = state
        .cohorts
        .values()
        .filter(|cohort| {
            cohort.status == CohortStatus::Active && cohort.members.len() == REPLICATION_FACTOR
        })
        .max_by_key(|cohort| cohort.id)
        .map_or(state.manifest_cohort_id, |cohort| cohort.id);
    let member_hash = cohort_member_hash(state, cohort_id).unwrap_or_default();
    (
        writer_epoch,
        cohort_id,
        if member_hash.is_empty() {
            state.manifest_member_hash.clone()
        } else {
            member_hash
        },
    )
}

fn manifest_policy_for_state(
    state: &DurableControlState,
    segments: &BTreeMap<String, Vec<StreamSegment>>,
    cohort_id: u64,
) -> (String, u64) {
    segments
        .values()
        .flat_map(|ranges| ranges.iter())
        .filter(|segment| segment.cohort_id == cohort_id)
        .max_by_key(|segment| (segment.manifest_revision, segment.start_lsn))
        .map(|segment| (segment.tier.clone(), segment.max_append_bytes))
        .filter(|(tier, max_append_bytes)| !tier.is_empty() || *max_append_bytes != 0)
        .or_else(|| {
            state
                .cohorts
                .get(&cohort_id)
                .map(|cohort| (cohort.tier.clone(), cohort.max_append_bytes))
        })
        .unwrap_or_default()
}

fn validate_stream_segments(
    stream_segments: &BTreeMap<String, Vec<StreamSegment>>,
) -> Result<(), ReplicaError> {
    for (stream, segments) in stream_segments {
        if stream.trim().is_empty() || segments.is_empty() {
            return Err(ReplicaError::Protocol(
                "placement manifest contains an invalid stream range map".to_owned(),
            ));
        }
        let mut expected = 1_u64;
        for (index, segment) in segments.iter().enumerate() {
            if segment.start_lsn != expected
                || segment.end_lsn.is_some_and(|end| end < segment.start_lsn)
                || (segment.end_lsn.is_none() && index + 1 != segments.len())
                || segment
                    .member_ids
                    .windows(2)
                    .any(|window| window[0] >= window[1])
            {
                return Err(ReplicaError::Protocol(
                    "placement manifest stream ranges are not contiguous".to_owned(),
                ));
            }
            if segment.member_ids.is_empty()
                || segment.member_hash.is_empty()
                || segment.manifest_revision == 0
                || segment.operation_id.trim().is_empty()
            {
                return Err(ReplicaError::Protocol(
                    "placement manifest stream range is missing its member binding".to_owned(),
                ));
            }
            if segment.member_ids.len() != REPLICATION_FACTOR {
                return Err(ReplicaError::Protocol(
                    "placement manifest stream range must have exactly three members".to_owned(),
                ));
            }
            expected = match segment.end_lsn {
                Some(end) => end.checked_add(1).ok_or_else(|| {
                    ReplicaError::Protocol("placement manifest LSN range exhausted".to_owned())
                })?,
                None => break,
            };
        }
    }
    Ok(())
}

/// Computes the stable identity digest carried by every manifest segment.
/// Member ids and URLs are included so replacing a disk under an existing id
/// cannot silently inherit historical placement.
fn cohort_member_hash(state: &DurableControlState, cohort_id: u64) -> Option<String> {
    let cohort = state.cohorts.get(&cohort_id)?;
    member_set_hash(state, &cohort.members)
}

/// Computes the same identity digest for an explicit route member set. A
/// stateless coordinator may know the manifest's immutable members before its
/// local membership document has reconstructed the same cohort numbering, so
/// route validation must not depend on the local cohort id.
fn member_set_hash(state: &DurableControlState, member_ids: &[String]) -> Option<String> {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/cohort-members/v1\0");
    for id in member_ids {
        let member = state.members.get(id)?;
        digest.update(member.id.as_bytes());
        digest.update([0]);
        digest.update(member.url.as_bytes());
        digest.update([0]);
    }
    Some(hex::encode(digest.finalize()))
}

/// Computes a canonical digest for the complete stream manifest.  Serde's
/// BTreeMap ordering makes this stable across stateless coordinators.
fn manifest_digest(segments: &BTreeMap<String, Vec<StreamSegment>>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/replica-manifest/v1\0");
    if let Ok(encoded) = serde_json::to_vec(segments) {
        digest.update(encoded);
    }
    hex::encode(digest.finalize())
}

fn control_metadata_from_state(state: &DurableControlState) -> ControlMetadata {
    ControlMetadata {
        membership_epoch: state.membership_epoch,
        members: state.members.clone(),
        cohorts: state.cohorts.clone(),
        manifest: manifest_from_state(state),
        pending_handoffs: BTreeMap::new(),
    }
}

fn membership_snapshot_from_head(
    head: &ControlHead,
    metadata_freeze: Option<authority::MetadataFreeze>,
) -> MembershipSnapshot {
    let manifest = head.metadata.manifest.clone();
    MembershipSnapshot {
        membership_epoch: head.metadata.membership_epoch,
        manifest_revision: manifest.revision,
        manifest_digest: manifest.digest.clone(),
        write_cohort_id: manifest.write_cohort_id,
        control_authority_epoch: head.authority_epoch,
        control_head_revision: head.revision,
        control_head_digest: head.metadata_digest.clone(),
        // This field is a legacy/source hint. The object head itself is the
        // authority and deliberately carries no physical control cohort.
        control_authority_cohort_id: 0,
        metadata_freeze,
        manifest: Some(manifest.clone()),
        members: head.metadata.members.values().cloned().collect(),
        cohorts: head.metadata.cohorts.values().cloned().collect(),
        stream_segments: manifest.stream_segments,
    }
}

/// Computes the deterministic proof that every manifest range owned by a
/// cohort has been sealed. A cohort may not drain or be retired while any
/// stream still has an open tail on it; callers persist this digest alongside
/// their maintenance operation and present it again for the lifecycle CAS.
#[allow(dead_code)]
fn cohort_archive_proof(snapshot: &MembershipSnapshot, cohort_id: u64) -> Option<String> {
    let cohort = snapshot
        .cohorts
        .iter()
        .find(|cohort| cohort.id == cohort_id)?;
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/cohort-archive-proof/v1\0");
    digest.update(cohort_id.to_le_bytes());
    for member_id in &cohort.members {
        digest.update(member_id.as_bytes());
        digest.update([0]);
    }
    digest.update(cohort.tier.as_bytes());
    digest.update([0]);
    digest.update(cohort.max_append_bytes.to_le_bytes());
    for (stream, segments) in &snapshot.stream_segments {
        for segment in segments {
            if segment.cohort_id != cohort_id {
                continue;
            }
            let end = segment.end_lsn?;
            digest.update(stream.as_bytes());
            digest.update([0]);
            digest.update(segment.start_lsn.to_le_bytes());
            digest.update(end.to_le_bytes());
            digest.update(segment.member_hash.as_bytes());
            digest.update([0]);
            digest.update(segment.operation_id.as_bytes());
            digest.update([0]);
        }
    }
    Some(hex::encode(digest.finalize()))
}

#[allow(dead_code)]
fn require_cohort_archive_proof(
    snapshot: &MembershipSnapshot,
    cohort_id: u64,
    supplied: Option<&str>,
) -> Result<(), ReplicaError> {
    let expected = cohort_archive_proof(snapshot, cohort_id).ok_or_else(|| {
        ReplicaError::Protocol(
            "cohort archive proof is unavailable while a range is open".to_owned(),
        )
    })?;
    if supplied != Some(expected.as_str()) {
        return Err(ReplicaError::Protocol(
            "cohort archive proof is required before drain or removal".to_owned(),
        ));
    }
    Ok(())
}

fn latest_manifest_binding(segments: &BTreeMap<String, Vec<StreamSegment>>) -> (u64, u64, String) {
    segments
        .values()
        .flat_map(|ranges| ranges.iter())
        .max_by_key(|segment| (segment.manifest_revision, segment.start_lsn))
        .map_or((0, 0, String::new()), |segment| {
            (
                segment.writer_epoch,
                segment.cohort_id,
                segment.member_hash.clone(),
            )
        })
}

/// Selects a complete active cohort through a rendezvous ring keyed only by
/// the immutable cohort id. Adding or removing a cohort therefore remaps only
/// streams whose winning cohort changes; it never depends on member ordering
/// and cannot move a historical manifest range.
fn cohort_for_stream(state: &DurableControlState, stream: &str) -> Result<u64, ReplicaError> {
    cohort_for_stream_excluding(state, stream, None)
}

/// Rendezvous placement over every complete active cohort except `exclude`.
/// A fenced cutover uses the exclusion so a cohort that is about to drain can
/// never be selected as its own successor.
fn cohort_for_stream_excluding(
    state: &DurableControlState,
    stream: &str,
    exclude: Option<u64>,
) -> Result<u64, ReplicaError> {
    state
        .cohorts
        .values()
        .filter(|cohort| {
            exclude != Some(cohort.id)
                && cohort.status == CohortStatus::Active
                && cohort.members.len() == REPLICATION_FACTOR
                && cohort.members.iter().all(|id| {
                    state
                        .members
                        .get(id)
                        .is_some_and(|member| member.status == MemberStatus::Active)
                })
        })
        .max_by(|left, right| {
            cohort_ring_score(stream, left.id)
                .cmp(&cohort_ring_score(stream, right.id))
                .then_with(|| left.id.cmp(&right.id))
        })
        .map(|cohort| cohort.id)
        .ok_or(ReplicaError::QuorumUnavailable)
}

/// Derives the host placement token from the immutable route identity. This
/// token is intentionally independent from `EncryptedRecord.writer_epoch`:
/// the latter is authenticated client data and must never be rewritten by a
/// gateway during a handoff.
fn placement_for_route(stream: &str, route: &StreamSegment) -> PlacementEpoch {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/stream-placement/v1\0");
    digest.update(stream.as_bytes());
    digest.update([0]);
    digest.update(route.start_lsn.to_le_bytes());
    digest.update(route.cohort_id.to_le_bytes());
    digest.update(route.writer_epoch.to_le_bytes());
    for member in &route.member_ids {
        digest.update(member.as_bytes());
        digest.update([0]);
    }
    PlacementEpoch::new(
        if route.placement_epoch == 0 {
            route.manifest_revision.max(1)
        } else {
            route.placement_epoch
        },
        hex::encode(digest.finalize()),
    )
}

/// Derives a temporary host fence before the certified cutover LSN is known.
/// The digest is deliberately not a route token: it names the operation and
/// the old immutable range, so a stale writer cannot pass the drain barrier,
/// while the final successor receives a distinct route-bound token later.
fn placement_for_transition(
    stream: &str,
    route: &StreamSegment,
    operation_id: &str,
    epoch: u64,
) -> PlacementEpoch {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/stream-placement-transition/v1\0");
    digest.update(stream.as_bytes());
    digest.update([0]);
    digest.update(operation_id.as_bytes());
    digest.update([0]);
    digest.update(route.start_lsn.to_le_bytes());
    digest.update(route.cohort_id.to_le_bytes());
    digest.update(route.writer_epoch.to_le_bytes());
    for member in &route.member_ids {
        digest.update(member.as_bytes());
        digest.update([0]);
    }
    PlacementEpoch::new(epoch.max(1), hex::encode(digest.finalize()))
}

/// Derives a bounded stream-local idempotency key from a cohort operation.
/// A cohort resize owns several streams concurrently; keeping the stream
/// digest in the pending operation id gives each object-head phase its own
/// receipt while the caller's cohort operation id remains the final
/// membership receipt.
fn stream_handoff_operation_id(operation_id: &str, stream: &str) -> String {
    let digest = hex::encode(Sha256::digest(stream.as_bytes()));
    format!("{operation_id}:stream:{}", &digest[..32])
}

/// Returns the rendezvous score for one stream/cohort pair. The domain is a
/// protocol constant: changing it would silently remap every new stream, so
/// future ring versions must use a new explicit domain and migration policy.
fn cohort_ring_score(stream: &str, cohort_id: u64) -> u128 {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/cohort-ring/v1/rendezvous\0");
    digest.update(stream.as_bytes());
    digest.update([0]);
    digest.update(cohort_id.to_le_bytes());
    let digest = digest.finalize();
    let mut score = [0_u8; 16];
    score.copy_from_slice(&digest[..16]);
    u128::from_be_bytes(score)
}

fn manifest_operation_id(
    stream: &str,
    expected_revision: u64,
    start_lsn: u64,
    cohort_id: u64,
    writer_epoch: u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/replica-manifest-operation/v1\0");
    digest.update(stream.as_bytes());
    digest.update([0]);
    digest.update(expected_revision.to_le_bytes());
    digest.update(start_lsn.to_le_bytes());
    digest.update(cohort_id.to_le_bytes());
    digest.update(writer_epoch.to_le_bytes());
    format!("manifest-{}", hex::encode(digest.finalize()))
}

/// Selects the first complete static cohort.  Direct deployments never use
/// this helper: their cohort membership is read from the durable control
/// document, so adding a cohort cannot move an existing stream.
fn select_static_replicas(nodes: &[ReplicaNode]) -> Vec<ReplicaNode> {
    nodes.iter().take(REPLICATION_FACTOR).cloned().collect()
}

fn ensure_exact_watermark(
    after_lsn: u64,
    committed_lsn: u64,
    records: &[EncryptedRecord],
) -> Result<(), ReplicaError> {
    if committed_lsn == after_lsn {
        return Ok(());
    }
    let expected_lsn = records
        .last()
        .map_or(after_lsn.saturating_add(1), |record| {
            record.lsn().saturating_add(1)
        });
    if records
        .last()
        .is_some_and(|record| record.lsn() == committed_lsn)
    {
        Ok(())
    } else {
        Err(ReplicaError::RecoveryIncomplete {
            expected_lsn,
            committed_lsn,
        })
    }
}

/// Returns the watermark the hot tier can actually serve from: the caller's
/// watermark, or the highest archived-prefix trim checkpoint any answering
/// member reports for the stream when that is higher, bounded by the
/// caller's committed watermark. A member writes a trim checkpoint only
/// after it verified that the archive holds the prefix, so every LSN at or
/// below the checkpoint is durable in the archive and absent from that
/// member by design. Reading such a member as a holder that confirms the
/// record absent would let a recover that read the archive head just before
/// the checkpoint was published drop an acknowledged tail.
fn trimmed_floor(
    stream: &str,
    after_lsn: u64,
    committed_lsn: Option<u64>,
    snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
) -> u64 {
    let trimmed = snapshots
        .iter()
        .filter_map(|(_, snapshot)| snapshot.as_ref())
        .flat_map(|snapshot| {
            snapshot
                .trimmed
                .iter()
                .filter(|prefix| prefix.stream == stream)
                .map(|prefix| prefix.archived_lsn)
        })
        .max()
        .unwrap_or(0);
    let trimmed = committed_lsn.map_or(trimmed, |limit| trimmed.min(limit));
    after_lsn.max(trimmed)
}

fn has_durable_marker(
    record: &EncryptedRecord,
    snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
) -> bool {
    snapshots.iter().any(|(_, snapshot)| {
        snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .committed
                .iter()
                .any(|candidate| candidate == record)
        })
    })
}

fn has_quorum_copy(
    record: &EncryptedRecord,
    snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
    quorum: usize,
) -> bool {
    snapshots
        .iter()
        .filter(|(_, snapshot)| {
            snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.records.iter().any(|candidate| candidate == record)
            })
        })
        .count()
        >= quorum
}

type RecordKey = (String, u64);
type EvidenceGroup = (EncryptedRecord, BTreeSet<String>, BTreeSet<String>);
type EvidenceGroups = BTreeMap<RecordKey, BTreeMap<Vec<u8>, EvidenceGroup>>;
type WatermarkGroup = (EncryptedRecord, BTreeSet<String>, BTreeSet<String>);
type CandidateMap = BTreeMap<RecordKey, EncryptedRecord>;
type CandidateResult = Result<(CandidateMap, usize), ReplicaError>;

#[derive(Default)]
struct StreamWriterState {
    writer_epoch: u64,
    committed_lsn: u64,
    records: BTreeMap<u64, EncryptedRecord>,
}

#[derive(Default)]
struct GatewayWriterState {
    /// Set only after a quorum snapshot has reconstructed the committed tail.
    initialized: bool,
    /// Membership used for reconstruction. A durable direct membership epoch
    /// change forces a fresh fence/prefix scan before the next append or
    /// readiness response.
    membership: Vec<ReplicaNode>,
    streams: BTreeMap<String, StreamWriterState>,
}

/// The durable boundary assembled for one stream handoff.  The source range
/// is never rewritten in place: `successor` is published only after the
/// source placement fence has drained and `archived_lsn` has been certified.
struct StreamCutover {
    stream: String,
    source: StreamSegment,
    successor: StreamSegment,
    archived_lsn: u64,
    pending_handoff: Option<PendingHandoffDescriptor>,
}

/// Snapshot I/O may overlap acknowledgements for other streams. Merge those
/// locally certified suffixes before publishing a reconstructed cache.
fn preserve_acknowledged_suffixes(
    cached: &GatewayWriterState,
    rebuilt: &mut GatewayWriterState,
) -> Result<(), ReplicaError> {
    for (stream, cached_stream) in &cached.streams {
        let target = rebuilt.streams.entry(stream.clone()).or_default();
        for (lsn, record) in &cached_stream.records {
            if target
                .records
                .get(lsn)
                .is_some_and(|candidate| candidate != record)
            {
                return Err(ReplicaError::LsnConflict);
            }
            if *lsn > target.committed_lsn {
                target.records.insert(*lsn, record.clone());
            }
        }
        target.committed_lsn = target.committed_lsn.max(cached_stream.committed_lsn);
        target.writer_epoch = target.writer_epoch.max(cached_stream.writer_epoch);
    }
    Ok(())
}

/// One sealed range whose records the archive does not yet cover.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UnarchivedRange {
    pub stream: String,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub archived_lsn: u64,
}

/// Outcome of one fenced cohort archive pass.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CohortArchiveReport {
    pub cohort_id: u64,
    pub archived_records: usize,
    pub complete: bool,
    pub unarchived: Vec<UnarchivedRange>,
}

/// Result of one anti-entropy pass.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RebalanceReport {
    /// Distinct records for which durable evidence was found.
    pub committed_records: usize,
    /// Records already present byte-for-byte on a target node.
    pub already_durable: usize,
    /// Records copied and locally fsynced on a target node.
    pub repaired: usize,
    /// Records that could not be copied because a target was unavailable or
    /// rejected the write. The pass remains safe and can be retried.
    pub failed: usize,
    /// Records deliberately ignored because they had neither quorum evidence
    /// nor a bounded authenticated watermark from a quorum acknowledgement.
    pub unsafe_records_skipped: usize,
    /// Every retained member of every inspected cohort answered its snapshot
    /// request. Quorum certification counts the members that hold a record,
    /// so a silent member makes committed records look like minority tails;
    /// a pass that could not hear every member proves nothing about what may
    /// be destroyed afterwards.
    #[serde(default)]
    pub quorum_proven: bool,
}

impl RebalanceReport {
    /// Whether every eligible record was repaired or already present.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.failed == 0
            && self.unsafe_records_skipped == 0
            && self.repaired + self.already_durable >= self.committed_records
    }
}

#[derive(Clone)]
struct GatewayState {
    gateway: Arc<ReplicaGateway>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GatewayMaintenanceFence {
    token: String,
    owner: String,
    generation: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum LocalMaintenanceState {
    #[default]
    Open,
    /// The gateway could not prove that every discovered node is open. It
    /// therefore rejects writes until a later status reconciliation succeeds.
    Unknown,
    Fenced(GatewayMaintenanceFence),
}

/// Admission state shared by all requests in one gateway process. The mutex
/// closes the check/increment race between an append and a maintenance fence;
/// the transition mutex serializes network reconciliation/acquisition with
/// admission so a gateway cannot write while its durable state is unknown.
struct WriteAdmission {
    fence: tokio::sync::Mutex<LocalMaintenanceState>,
    transition: tokio::sync::Mutex<()>,
    active: AtomicUsize,
    operations: AtomicUsize,
    next_fence: AtomicU64,
    drained: tokio::sync::Notify,
}

/// Process-local gateway counters. Every field except `active_requests` is a
/// monotonic total for one `boot_id`; atomics keep the counters correct when
/// callers use the library directly rather than going through Axum.
struct GatewayMetricsState {
    boot_id: String,
    started_at_ms: u64,
    append_attempts: AtomicU64,
    append_acks: AtomicU64,
    append_failures: AtomicU64,
    acked_bytes: AtomicU64,
    append_latency_nanos: AtomicU64,
    updated_at_ms: AtomicU64,
    rates: Mutex<GatewayRateSample>,
}

#[derive(Default)]
struct GatewayRateSample {
    observed_at_ms: u64,
    acked_bytes: u64,
    append_attempts: u64,
    append_latency_nanos: u64,
}

impl Default for GatewayMetricsState {
    fn default() -> Self {
        let now = unix_time_ms();
        Self {
            boot_id: new_boot_id(),
            started_at_ms: now,
            append_attempts: AtomicU64::new(0),
            append_acks: AtomicU64::new(0),
            append_failures: AtomicU64::new(0),
            acked_bytes: AtomicU64::new(0),
            append_latency_nanos: AtomicU64::new(0),
            updated_at_ms: AtomicU64::new(now),
            rates: Mutex::new(GatewayRateSample::default()),
        }
    }
}

impl GatewayMetricsState {
    fn mark_updated(&self) {
        self.updated_at_ms.store(unix_time_ms(), Ordering::Release);
    }

    fn counters(&self) -> GatewayCounters {
        let append_attempts = self.append_attempts.load(Ordering::Acquire);
        let append_acks = self.append_acks.load(Ordering::Acquire);
        let append_failures = self.append_failures.load(Ordering::Acquire);
        let acked_bytes = self.acked_bytes.load(Ordering::Acquire);
        let append_latency_nanos = self.append_latency_nanos.load(Ordering::Acquire);
        GatewayCounters {
            append_attempts,
            append_acks,
            append_failures,
            acked_bytes,
            append_latency_nanos,
            append_attempts_total: append_attempts,
            append_acks_total: append_acks,
            append_failures_total: append_failures,
            acked_bytes_total: acked_bytes,
            append_latency_nanos_total: append_latency_nanos,
        }
    }

    fn rates(&self, counters: &GatewayCounters, now_ms: u64) -> GatewayRateValues {
        let Ok(mut previous) = self.rates.lock() else {
            return GatewayRateValues::default();
        };
        let elapsed_ms = now_ms.saturating_sub(previous.observed_at_ms);
        let window_ms = elapsed_ms.max(1);
        let bytes_delta = counters.acked_bytes.saturating_sub(previous.acked_bytes);
        let attempts_delta = counters
            .append_attempts
            .saturating_sub(previous.append_attempts);
        let latency_delta = counters
            .append_latency_nanos
            .saturating_sub(previous.append_latency_nanos);
        let bytes_per_second = bytes_delta.saturating_mul(1_000) / window_ms;
        let latency_ms = if attempts_delta == 0 {
            0.0
        } else {
            latency_delta as f64 / attempts_delta as f64 / 1_000_000.0
        };
        previous.observed_at_ms = now_ms;
        previous.acked_bytes = counters.acked_bytes;
        previous.append_attempts = counters.append_attempts;
        previous.append_latency_nanos = counters.append_latency_nanos;
        GatewayRateValues {
            window_ms,
            bytes_per_second,
            latency_ms,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct GatewayRateValues {
    window_ms: u64,
    bytes_per_second: u64,
    latency_ms: f64,
}

impl Default for WriteAdmission {
    fn default() -> Self {
        Self {
            fence: tokio::sync::Mutex::new(LocalMaintenanceState::default()),
            transition: tokio::sync::Mutex::new(()),
            active: AtomicUsize::new(0),
            operations: AtomicUsize::new(0),
            next_fence: AtomicU64::new(1),
            drained: tokio::sync::Notify::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct DurableFenceObservation {
    active: Option<(String, u64)>,
    /// Number of statuses carrying the active owner/generation pair.
    fenced_storage: usize,
    /// Highest generation retained by any node, including released fences.
    generation: u64,
    /// True only when every supplied node reports the same active marker.
    complete: bool,
}

fn observe_durable_fence(
    statuses: &[StorageNodeStatus],
) -> Result<DurableFenceObservation, ReplicaError> {
    let mut observed = DurableFenceObservation {
        complete: true,
        ..DurableFenceObservation::default()
    };
    let mut active = BTreeSet::<(String, u64)>::new();
    for status in statuses {
        observed.generation = observed.generation.max(status.maintenance_generation);
        if status.maintenance {
            let owner = status
                .maintenance_owner
                .as_deref()
                .filter(|owner| !owner.trim().is_empty())
                .ok_or_else(|| {
                    ReplicaError::Protocol("active storage fence is missing its owner".to_owned())
                })?;
            if status.maintenance_generation == 0 {
                return Err(ReplicaError::Protocol(
                    "active storage fence has zero generation".to_owned(),
                ));
            }
            active.insert((owner.to_owned(), status.maintenance_generation));
        }
    }
    if active.len() > 1 {
        observed.complete = false;
        return Ok(observed);
    }
    if let Some(pair) = active.into_iter().next() {
        observed.fenced_storage = statuses
            .iter()
            .filter(|status| {
                status.maintenance
                    && status.maintenance_owner.as_deref() == Some(pair.0.as_str())
                    && status.maintenance_generation == pair.1
            })
            .count();
        observed.active = Some(pair);
        observed.complete = observed.fenced_storage == statuses.len();
    }
    Ok(observed)
}

/// Keeps one append counted until its quorum response has been decided.
struct AppendAdmission {
    admission: Arc<WriteAdmission>,
    active_requests: Arc<AtomicUsize>,
}

/// Keeps a recovery request visible to maintenance fencing until its streamed
/// response has been assembled. Reads are admitted through the same durable
/// fence as appends so whole-cell suspension cannot interrupt a large replay.
struct ReadAdmission {
    admission: Arc<WriteAdmission>,
    active_requests: Arc<AtomicUsize>,
}

impl Drop for AppendAdmission {
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::AcqRel);
        self.admission.active.fetch_sub(1, Ordering::AcqRel);
        self.admission.drained.notify_waiters();
    }
}

impl Drop for ReadAdmission {
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::AcqRel);
        self.admission.drained.notify_waiters();
    }
}

/// Prevents a fence from being released while a rebalance request that holds
/// the fence is still operating.
struct MaintenanceOperation {
    admission: Arc<WriteAdmission>,
}

impl Drop for MaintenanceOperation {
    fn drop(&mut self) {
        self.admission.operations.fetch_sub(1, Ordering::AcqRel);
        self.admission.drained.notify_waiters();
    }
}

#[derive(Deserialize)]
struct GatewayRecordsQuery {
    stream: String,
    after_lsn: Option<u64>,
    committed_lsn: Option<u64>,
}

/// An authenticated client watermark used for conservative recovery/repair.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairRequest {
    /// Tenant-qualified stream to repair.
    pub stream: String,
    /// Highest contiguous LSN the authenticated caller says is committed.
    /// This bounds repair; a one-copy certified record must still be the unique
    /// surviving payload in a contiguous cLSN prefix.
    pub committed_lsn: u64,
}

#[derive(Deserialize)]
struct RebalanceRequest {
    target: Option<String>,
}

/// Private operational view consumed by the cluster lifecycle scripts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GatewayStatus {
    /// Configured number of nodes that must durably append and commit before
    /// an append is acknowledged.
    pub quorum: usize,
    /// Number of nodes in this gateway's current membership snapshot.
    pub storage_nodes: usize,
    /// Number of those nodes responding to the private health probe.
    pub healthy_storage: usize,
    /// Requests currently in the gateway's append path.
    pub active_requests: usize,
    /// Whether new appends are fenced for a maintenance operation.
    #[serde(default)]
    pub maintenance: bool,
    /// Number of nodes that have persisted the active maintenance marker.
    #[serde(default)]
    pub fenced_storage: usize,
    /// Active durable fence generation, when known.
    #[serde(default)]
    pub maintenance_generation: u64,
    /// Active durable fence owner, when known.
    #[serde(default)]
    pub maintenance_owner: Option<String>,
    #[serde(default)]
    pub membership_epoch: u64,
}

/// Records appended to a catching-up member per request.
const CATCH_UP_BATCH: usize = 256;

/// Client-facing replica gateway.
///
/// [`Self::new_direct`] is the combined Fly constructor. It uses a durable
/// per-volume membership document. Every combined process is an equivalent,
/// stateless coordinator; storage nodes provide the per-LSN conflict fence.
pub struct ReplicaGateway {
    membership: Membership,
    local_member_id: String,
    quorum: usize,
    auth: Authenticator,
    internal_token: String,
    admin_token: String,
    active_requests: Arc<AtomicUsize>,
    admission: Arc<WriteAdmission>,
    metrics: Arc<GatewayMetricsState>,
    provider_cas_verified: AtomicBool,
    metrics_max_age: Duration,
    writer_state: tokio::sync::Mutex<GatewayWriterState>,
    /// Independent streams may append concurrently. Weak entries retain no
    /// stream lock once its admitted writers finish.
    stream_writers: Mutex<BTreeMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    /// Process-local proofs that a route manifest reached its exact member
    /// quorum. A hit skips only the redundant manifest CAS; data-node
    /// placement admission still runs for every append.
    route_repair_cache: Mutex<RouteRepairCache>,
    client: reqwest::Client,
    archive: Arc<OpaqueArchive>,
    /// Direct mode shares the local Machine's durable control document with
    /// its storage listener. Legacy static gateways leave this unset and
    /// retain their original process-local admission behavior.
    control: Option<Arc<DurableControl>>,
    /// Object-store authority head used when the original control cohort is
    /// handed off to a freshly prepared cohort. Legacy/static gateways leave
    /// this unset.
    control_head: Option<ControlHeadStore>,
}

impl ReplicaGateway {
    /// Archives every committed stream visible on one local member.
    ///
    /// The local snapshot is used only for stream discovery. The published
    /// bytes come from the gateway's conservative hot-tail recovery across
    /// the owning cohort, so one partial member can never advance S3.
    pub async fn archive_local_commits(&self, node: &DiskReplica) -> Result<usize, ReplicaError> {
        self.archive_local_commits_inner(node).await
    }

    /// Record every archived stream prefix as a durable trim fence on a
    /// member that has no local history for it. A member booting on an empty
    /// volume otherwise refuses the writer's next append at
    /// `archived_lsn + 1` for want of a predecessor, even though the archive
    /// on object storage is that predecessor. Also carries the archived
    /// writer epoch forward so a stale writer stays fenced across the restart.
    /// Returns the number of streams seeded.
    pub async fn seed_from_archive(&self, node: &DiskReplica) -> Result<usize, ReplicaError> {
        let snapshot = node.snapshot();
        let known = snapshot
            .trimmed
            .iter()
            .map(|prefix| prefix.stream.clone())
            .chain(
                snapshot
                    .records
                    .iter()
                    .chain(snapshot.committed.iter())
                    .map(|record| record.stream().to_owned()),
            )
            .collect::<BTreeSet<_>>();
        let prefixes = self
            .archive
            .stream_heads()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            .into_iter()
            .filter(|(stream, _, _)| !known.contains(stream))
            .collect::<Vec<_>>();
        if prefixes.is_empty() {
            return Ok(0);
        }
        let count = prefixes.len();
        node.compact_archived_batch(&prefixes)?;
        Ok(count)
    }

    async fn archive_local_commits_inner(&self, node: &DiskReplica) -> Result<usize, ReplicaError> {
        let snapshot = node.snapshot();
        let trimmed = snapshot
            .trimmed
            .into_iter()
            .map(|prefix| (prefix.stream, prefix.archived_lsn))
            .collect::<BTreeMap<_, _>>();
        let streams = snapshot
            .records
            .into_iter()
            .chain(snapshot.committed)
            .map(|record| record.stream().to_owned())
            .collect::<BTreeSet<_>>();
        let mut archived_records = 0_usize;
        let mut compacted_prefixes = Vec::new();
        for stream in streams {
            let archived_lsn = self
                .archive
                .archived_lsn(&stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            let (records, _) = self
                .recover_hot_tail(&stream, archived_lsn, None, None)
                .await?;
            if !records.is_empty() {
                archived_records = archived_records.saturating_add(records.len());
                self.archive
                    .archive_committed(&records)
                    .await
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            }
            if let Some((published_lsn, writer_epoch)) = self
                .archive
                .archived_tail(&stream, trimmed.get(&stream).copied().unwrap_or(0))
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            {
                compacted_prefixes.push((stream, published_lsn, writer_epoch));
            }
        }
        node.compact_archived_batch(&compacted_prefixes)?;
        Ok(archived_records)
    }

    /// Creates a gateway from an immutable explicit membership snapshot.
    /// Direct Fly deployments should use [`Self::new_direct`].
    pub fn new(
        nodes: Vec<ReplicaNode>,
        quorum: usize,
        dataplane_root_key: &str,
        internal_token: impl Into<String>,
        archive: Arc<OpaqueArchive>,
    ) -> Result<Self, ReplicaError> {
        Self::build_static(nodes, quorum, dataplane_root_key, internal_token, archive)
    }

    /// Creates a stateless direct Fly coordinator backed by a durable control
    /// document. Any coordinator can serve client traffic immediately.
    pub fn new_direct(
        nodes: Vec<ReplicaNode>,
        quorum: usize,
        dataplane_root_key: &str,
        internal_token: impl Into<String>,
        control_path: impl AsRef<Path>,
        archive: Arc<OpaqueArchive>,
    ) -> Result<Self, ReplicaError> {
        let control = DurableControl::open(control_path, &nodes)?;
        Self::new_direct_with_control(
            nodes,
            quorum,
            dataplane_root_key,
            internal_token,
            control,
            archive,
        )
    }

    /// Direct constructor used by the combined process so storage and gateway
    /// share one in-memory handle as well as one volume file.
    pub fn new_direct_with_control(
        nodes: Vec<ReplicaNode>,
        quorum: usize,
        dataplane_root_key: &str,
        internal_token: impl Into<String>,
        control: Arc<DurableControl>,
        archive: Arc<OpaqueArchive>,
    ) -> Result<Self, ReplicaError> {
        let control_head = ControlHeadStore::new(
            archive.object_store(),
            format!("{}/control-head", archive.prefix()),
        )
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let persisted_nodes = control.active_members()?;
        let nodes = if persisted_nodes.is_empty() {
            if !nodes.is_empty() {
                let bootstrap_members = nodes
                    .iter()
                    .map(|node| DurableMember {
                        id: node.id.clone(),
                        url: node.url.clone(),
                        status: MemberStatus::Active,
                        cohort_id: 0,
                        ..DurableMember::default()
                    })
                    .collect();
                let _ = control.cas_membership(0, bootstrap_members, "bootstrap")?;
            }
            nodes
        } else {
            persisted_nodes
        };
        if nodes.len() < 3 {
            return Err(ReplicaError::NodeStorage(
                "direct membership must contain at least three members".to_owned(),
            ));
        }
        if control.membership()?.active_nodes().len() < 3 {
            return Err(ReplicaError::NodeStorage(
                "direct membership must have at least three active members".to_owned(),
            ));
        }
        let mut gateway =
            Self::build_static(nodes, quorum, dataplane_root_key, internal_token, archive)?;
        gateway.membership = Membership::Direct(DirectMembership {
            control: Arc::clone(&control),
        });
        gateway.control = Some(control);
        gateway.control_head = Some(control_head);
        Ok(gateway)
    }

    fn build_static(
        nodes: Vec<ReplicaNode>,
        quorum: usize,
        dataplane_root_key: &str,
        internal_token: impl Into<String>,
        archive: Arc<OpaqueArchive>,
    ) -> Result<Self, ReplicaError> {
        if quorum != ACK_QUORUM {
            return Err(ReplicaError::NodeStorage(
                "replica quorum is fixed at two durable acknowledgements".to_owned(),
            ));
        }
        let mut members = BTreeMap::new();
        let mut urls = BTreeSet::new();
        for node in nodes {
            validate_node(&node)?;
            if !urls.insert(node.url.clone()) {
                return Err(ReplicaError::NodeStorage(
                    "replica node URLs must be unique".to_owned(),
                ));
            }
            if members.insert(node.id.clone(), node).is_some() {
                return Err(ReplicaError::NodeStorage(
                    "replica node ids must be unique".to_owned(),
                ));
            }
        }
        if members.len() < quorum {
            return Err(ReplicaError::NodeStorage(
                "replica membership must contain at least quorum nodes".to_owned(),
            ));
        }
        Self::build_membership(
            Membership::Static(Arc::new(RwLock::new(members))),
            quorum,
            dataplane_root_key,
            internal_token,
            archive,
        )
    }

    fn build_membership(
        membership: Membership,
        quorum: usize,
        dataplane_root_key: &str,
        internal_token: impl Into<String>,
        archive: Arc<OpaqueArchive>,
    ) -> Result<Self, ReplicaError> {
        if quorum != ACK_QUORUM {
            return Err(ReplicaError::NodeStorage(
                "replica quorum is fixed at two durable acknowledgements".to_owned(),
            ));
        }
        Ok(Self {
            membership,
            local_member_id: String::new(),
            quorum,
            auth: Authenticator::new(dataplane_root_key)?,
            internal_token: internal_token.into(),
            admin_token: String::new(),
            active_requests: Arc::new(AtomicUsize::new(0)),
            admission: Arc::new(WriteAdmission::default()),
            metrics: Arc::new(GatewayMetricsState::default()),
            provider_cas_verified: AtomicBool::new(false),
            metrics_max_age: DEFAULT_METRICS_MAX_AGE,
            writer_state: tokio::sync::Mutex::new(GatewayWriterState::default()),
            stream_writers: Mutex::new(BTreeMap::new()),
            route_repair_cache: Mutex::new(RouteRepairCache::default()),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
            archive,
            control: None,
            control_head: None,
        })
    }

    /// Sets the private operator token used by the status and membership API.
    ///
    /// Gateway-to-node traffic continues to use `internal_token`; keeping the
    /// two credentials separate prevents an operator bearer from becoming a
    /// tenant or node credential by accident.
    #[must_use]
    pub fn with_admin_token(mut self, token: impl Into<String>) -> Self {
        self.admin_token = token.into();
        self
    }

    /// Identifies the storage member colocated with this gateway. Fly service
    /// routing must exclude a joining Machine until membership activation has
    /// made that exact member active in its durable control document.
    #[must_use]
    pub fn with_local_member_id(mut self, member_id: impl Into<String>) -> Self {
        self.local_member_id = member_id.into();
        self
    }

    /// Sets the maximum age accepted for a storage node status sample.
    ///
    /// A zero duration is useful in tests that need to prove stale samples are
    /// rejected; production callers should leave the conservative default.
    #[must_use]
    pub fn with_metrics_max_age(mut self, max_age: Duration) -> Self {
        self.metrics_max_age = max_age;
        self
    }

    /// Returns whether this gateway uses a durable direct membership document.
    #[must_use]
    pub fn direct_mode(&self) -> bool {
        self.control.is_some()
    }

    /// Marks the provider CAS capability as verified for this gateway
    /// process. The background provider probe owns the verification; metrics
    /// keeps the capability false until that probe succeeds.
    pub fn mark_provider_cas_verified(&self) {
        self.provider_cas_verified.store(true, Ordering::Release);
    }

    /// Returns one coherent, authenticated metrics sample after collecting a
    /// fresh status from every current storage member.
    pub async fn metrics(&self) -> Result<GatewayMetrics, ReplicaError> {
        self.sync_control_head().await?;
        let nodes = self.nodes_snapshot().await?;
        if nodes.len() < self.quorum {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let storage = self.fetch_storage_statuses(&nodes).await?;
        // `nodes_snapshot` is the complete set of members in every active
        // cohort. A mixed old/new fleet therefore remains unavailable for
        // online reconfiguration until every active member reports version 1.
        let online_reconfiguration = online_reconfiguration_ready(
            self.direct_mode(),
            self.provider_cas_verified.load(Ordering::Acquire),
            nodes.len(),
            &storage,
        );
        let durable_fence = self
            .apply_durable_observation(&storage, nodes.len())
            .await?;
        let (maintenance_owner, maintenance_generation) = durable_fence
            .active
            .as_ref()
            .map_or((None, durable_fence.generation), |(owner, generation)| {
                (Some(owner.clone()), *generation)
            });
        let counters = self.metrics.counters();
        let timestamp_ms = unix_time_ms();
        let rates = self.metrics.rates(&counters, timestamp_ms);
        let (disk_free_bytes, disk_total_bytes) = aggregate_storage_capacity(&storage)?;
        let metrics = GatewayMetrics {
            version: 1,
            boot_id: self.metrics.boot_id.clone(),
            started_at_ms: self.metrics.started_at_ms,
            timestamp_ms,
            observed_at_ms: timestamp_ms,
            updated_at_ms: self.metrics.updated_at_ms.load(Ordering::Acquire),
            append_attempts: counters.append_attempts,
            append_acks: counters.append_acks,
            append_failures: counters.append_failures,
            acked_bytes: counters.acked_bytes,
            append_latency_nanos: counters.append_latency_nanos,
            append_attempts_total: counters.append_attempts_total,
            append_acks_total: counters.append_acks_total,
            append_failures_total: counters.append_failures_total,
            acked_bytes_total: counters.acked_bytes_total,
            append_latency_nanos_total: counters.append_latency_nanos_total,
            cpu_utilization: 0.0,
            memory_utilization: 0.0,
            disk_free_bytes,
            disk_total_bytes,
            throughput_bytes_per_second: rates.bytes_per_second,
            acked_bytes_per_second: rates.bytes_per_second,
            latency_p95_ms: rates.latency_ms,
            latency_p50_ms: rates.latency_ms,
            window_ms: rates.window_ms,
            counters,
            storage_nodes: nodes.len(),
            healthy_storage: storage.len(),
            active_requests: self.active_requests.load(Ordering::Acquire),
            maintenance: self.maintenance_active().await,
            online_reconfiguration,
            fenced_storage: durable_fence.fenced_storage,
            maintenance_generation,
            maintenance_owner,
            storage,
        };
        Ok(metrics)
    }

    /// Compatibility alias for callers that call a point-in-time sample a
    /// snapshot instead of metrics.
    pub async fn metrics_snapshot(&self) -> Result<GatewayMetrics, ReplicaError> {
        self.metrics().await
    }

    /// Issues an HMAC-bound receipt for a record this gateway has already
    /// acknowledged at quorum. The receipt is returned by the HTTP append
    /// route and can be persisted by a tenant cursor for one-copy recovery.
    pub async fn issue_commit_certificate(
        &self,
        record: &EncryptedRecord,
    ) -> Result<String, ReplicaError> {
        let state = self.writer_state.lock().await;
        let committed = state
            .streams
            .get(record.stream())
            .is_some_and(|stream_state| {
                stream_state.committed_lsn >= record.lsn()
                    && stream_state.records.get(&record.lsn()) == Some(record)
            });
        drop(state);
        if !committed {
            return Err(ReplicaError::Protocol(
                "commit certificate requested for an unacknowledged record".to_owned(),
            ));
        }
        // One quorum read serves both the route lookup and the manifest
        // binding below; they describe the same certified revision.
        let manifest = if self.direct_mode() {
            Some(
                self.read_manifest_quorum()
                    .await?
                    .ok_or(ReplicaError::QuorumUnavailable)?,
            )
        } else {
            None
        };
        let route = manifest
            .as_ref()
            .and_then(|manifest| manifest_route(manifest, record.stream(), record.lsn()));
        let (
            cohort_id,
            segment_start_lsn,
            manifest_revision,
            manifest_digest,
            segment_end_lsn,
            member_hash,
            segment_operation_id,
        ) = if let Some(segment) = route {
            let (revision, digest) = if let Some(manifest) = manifest {
                (manifest.revision, manifest.digest)
            } else {
                (segment.manifest_revision, String::new())
            };
            (
                segment.cohort_id,
                segment.start_lsn,
                revision,
                digest,
                segment.end_lsn,
                segment.member_hash,
                segment.operation_id,
            )
        } else {
            (0, 0, 0, String::new(), None, String::new(), String::new())
        };
        self.auth.issue_commit_certificate_bound(
            record,
            cohort_id,
            segment_start_lsn,
            manifest_revision,
            &manifest_digest,
            segment_end_lsn,
            &member_hash,
            &segment_operation_id,
        )
    }

    /// Returns the configured quorum (two for the staging three-node cell).
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.quorum
    }

    /// Returns the currently held maintenance token, including after a
    /// partial acquisition. This recovery hook lets an operator complete a
    /// release after a node failed while fencing; it never creates or opens a
    /// fence and callers must still pass the token through [`Self::end_maintenance`].
    pub async fn maintenance_token(&self) -> Option<String> {
        let _ = self.refresh_durable_fence().await;
        match self.admission.fence.lock().await.clone() {
            LocalMaintenanceState::Fenced(fence) => Some(fence.token),
            LocalMaintenanceState::Open | LocalMaintenanceState::Unknown => None,
        }
    }

    /// Fences new appends and waits for every already-admitted append to
    /// finish. The returned token must be presented to every rebalance call
    /// and to [`Self::end_maintenance`] after the destructive operation has
    /// completed.
    pub async fn begin_maintenance(&self) -> Result<String, ReplicaError> {
        self.begin_maintenance_owned(None).await
    }

    /// Acquires or resumes the durable fence owned by one operation. The
    /// operation id is embedded in the signed token and in every storage-node
    /// marker, so a gateway restart can recover a fence left behind by a
    /// crashed coordinator without inventing a competing owner.
    pub async fn begin_maintenance_for(&self, operation_id: &str) -> Result<String, ReplicaError> {
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "maintenance operation id must not be empty".to_owned(),
            ));
        }
        self.begin_maintenance_owned(Some(operation_id)).await
    }

    async fn begin_maintenance_owned(
        &self,
        requested_owner: Option<&str>,
    ) -> Result<String, ReplicaError> {
        // An anonymous acquisition is owned by this gateway process. Naming
        // it up front lets a retry from the same process resume a partial
        // fence it left on some members after another member was
        // unreachable, instead of conflicting with its own marker forever.
        let anonymous = requested_owner.is_none();
        let owner_name = requested_owner
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.metrics.boot_id.clone());
        let requested_owner = Some(owner_name.as_str());
        // Keep the transition lock across discovery and node writes. Existing
        // admitted appends are allowed to finish, but a new append cannot
        // pass admission while this durable acquisition is in flight.
        let transition = self.admission.transition.lock().await;
        // A release that could not reach every member leaves this gateway
        // fail-closed with a stale local fence. Once every member answers
        // and none carries a marker, the durable state is the authority and
        // the local fence opens again; otherwise the stale fence stays and
        // the conflict below is real.
        self.reconcile_durable_fence_locked().await?;
        let local_fence = self.admission.fence.lock().await.clone();
        if !matches!(&local_fence, LocalMaintenanceState::Open)
            && !matches!(
                (&local_fence, requested_owner),
                (
                    LocalMaintenanceState::Fenced(fence),
                    Some(owner)
                ) if fence.owner == owner
            )
        {
            return Err(ReplicaError::LsnConflict);
        }
        let nodes = self.all_members_snapshot().await?;
        let active_nodes = self.nodes_snapshot().await?;
        if active_nodes.len() < self.quorum {
            self.set_unknown_fence().await;
            drop(transition);
            self.wait_for_active_requests().await;
            return Err(ReplicaError::QuorumUnavailable);
        }

        // Status gives us the monotonic generation and any fence that another
        // gateway/node has already persisted. An unavailable status does not
        // let us assume the node is open; we still try the acquisition with a
        // fresh wall-clock generation, and retain a local fail-closed fence if
        // any node cannot persist it.
        let status_results = self.fetch_durable_statuses(&nodes).await;
        let statuses = status_results
            .iter()
            .filter_map(|result| result.as_ref().ok().cloned())
            .collect::<Vec<_>>();
        let observation = match observe_durable_fence(&statuses) {
            Ok(observation) => observation,
            Err(error) => {
                self.set_unknown_fence().await;
                drop(transition);
                self.wait_for_active_requests().await;
                return Err(error);
            }
        };
        if let Some((active_owner, generation)) = observation.active.clone() {
            self.apply_durable_observation(&statuses, nodes.len())
                .await?;
            // A named operation resumes its own fence at any time. An
            // anonymous request resumes only a fence this process left
            // half-installed; a fully installed fence is someone's live
            // maintenance window and a second anonymous caller must not
            // share it.
            if requested_owner != Some(active_owner.as_str()) || (anonymous && observation.complete)
            {
                drop(transition);
                self.wait_for_active_requests().await;
                return Err(ReplicaError::LsnConflict);
            }
            let token = self
                .auth
                .issue_maintenance_token(active_owner.clone(), generation)?;
            self.admission
                .next_fence
                .store(generation, Ordering::Release);
            let results = join_all(nodes.iter().cloned().map(|node| {
                let http = &self.client;
                let internal_token = &self.internal_token;
                let token = token.clone();
                async move {
                    let result = NodeClient::new(node.clone(), http, internal_token)
                        .fence(&token)
                        .await;
                    (node, result)
                }
            }))
            .await;
            let all_fenced = results.iter().all(|(_, result)| result.is_ok());
            self.set_local_fence(GatewayMaintenanceFence {
                token: token.clone(),
                owner: active_owner,
                generation,
            })
            .await;
            drop(transition);
            self.wait_for_active_requests().await;
            return if all_fenced {
                Ok(token)
            } else {
                Err(ReplicaError::QuorumUnavailable)
            };
        }
        if observation.fenced_storage == 0 && !observation.complete && !statuses.is_empty() {
            self.set_unknown_fence().await;
            drop(transition);
            self.wait_for_active_requests().await;
            return Err(ReplicaError::QuorumUnavailable);
        }

        let mut generation = observation
            .generation
            .max(self.admission.next_fence.load(Ordering::Acquire));
        generation = generation.max(unix_time_ms());
        generation = generation
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Protocol("maintenance generation exhausted".to_owned()))?;
        self.admission
            .next_fence
            .store(generation, Ordering::Release);
        let owner = owner_name.clone();
        let token = self
            .auth
            .issue_maintenance_token(owner.clone(), generation)?;
        let results = join_all(nodes.iter().cloned().map(|node| {
            let http = &self.client;
            let internal_token = &self.internal_token;
            let token = token.clone();
            async move {
                let result = NodeClient::new(node.clone(), http, internal_token)
                    .fence(&token)
                    .await;
                (node, result)
            }
        }))
        .await;
        let all_fenced = results.iter().all(|(_, result)| result.is_ok());
        // Even a partial acquisition is an availability boundary. Keeping the
        // token locally prevents this gateway from acknowledging writes on a
        // node that did not answer the fence request.
        self.set_local_fence(GatewayMaintenanceFence {
            token: token.clone(),
            owner,
            generation,
        })
        .await;
        drop(transition);
        self.wait_for_active_requests().await;
        if all_fenced {
            Ok(token)
        } else {
            Err(ReplicaError::QuorumUnavailable)
        }
    }

    /// Releases a maintenance fence after all fence-owned operations have
    /// returned. A stale or missing token never opens write admission.
    pub async fn end_maintenance(&self, token: &str) -> Result<(), ReplicaError> {
        let token_text = token.to_owned();
        let _token = self.auth.verify_maintenance_token(&token_text)?;
        let transition = self.admission.transition.lock().await;
        self.reconcile_durable_fence_locked().await?;
        let local = self.admission.fence.lock().await.clone();
        let expected = match local {
            LocalMaintenanceState::Fenced(expected) => expected,
            LocalMaintenanceState::Open | LocalMaintenanceState::Unknown => {
                return Err(ReplicaError::GatewayUnauthorized);
            }
        };
        if expected.token != token_text {
            return Err(ReplicaError::GatewayUnauthorized);
        }
        if self.admission.operations.load(Ordering::Acquire) != 0 {
            return Err(ReplicaError::LsnConflict);
        }
        let nodes = self.all_members_snapshot().await?;
        let results = join_all(nodes.iter().cloned().map(|node| {
            let http = &self.client;
            let internal_token = &self.internal_token;
            let token_text = token_text.clone();
            async move {
                let result = NodeClient::new(node.clone(), http, internal_token)
                    .release(&token_text)
                    .await;
                (node, result)
            }
        }))
        .await;
        if results.iter().any(|(_, result)| result.is_err()) {
            // The gateway stays closed until every node has acknowledged the
            // same owner/generation release. A retry is idempotent on nodes
            // that already cleared their marker.
            drop(transition);
            return Err(ReplicaError::NodeUnavailable);
        }
        *self.admission.fence.lock().await = LocalMaintenanceState::Open;
        self.admission.drained.notify_waiters();
        drop(transition);
        Ok(())
    }

    /// Runs one rebalance under a previously acquired maintenance fence. The
    /// fence remains held for the caller so node drain and removal can follow
    /// without reopening write admission between the two phases.
    pub async fn rebalance_with_maintenance(
        &self,
        target: Option<&str>,
        token: &str,
    ) -> Result<RebalanceReport, ReplicaError> {
        let _operation = self.maintenance_operation(token).await?;
        self.rebalance_unfenced(target, Some(token)).await
    }

    async fn maintenance_operation(
        &self,
        token: &str,
    ) -> Result<MaintenanceOperation, ReplicaError> {
        let token_text = token.to_owned();
        let _token = self.auth.verify_maintenance_token(&token_text)?;
        let expected_token = token_text;
        let transition = self.admission.transition.lock().await;
        self.reconcile_durable_fence_locked().await?;
        let fence = self.admission.fence.lock().await;
        let LocalMaintenanceState::Fenced(expected) = &*fence else {
            return Err(ReplicaError::GatewayUnauthorized);
        };
        if expected.token != expected_token {
            return Err(ReplicaError::GatewayUnauthorized);
        }
        self.admission.operations.fetch_add(1, Ordering::AcqRel);
        drop(transition);
        Ok(MaintenanceOperation {
            admission: Arc::clone(&self.admission),
        })
    }

    /// Reads every member's durable fence status. The phase ends once a
    /// quorum answered and the stragglers had their grace; a member that
    /// stays silent is reported unavailable, exactly as one whose request
    /// timed out, so the observation stays incomplete rather than wrong.
    async fn fetch_durable_statuses(
        &self,
        nodes: &[ReplicaNode],
    ) -> Vec<Result<StorageNodeStatus, ReplicaError>> {
        let quorum = self.quorum;
        fan_out(
            nodes,
            |node| {
                let client = self.client.clone();
                let internal_token = self.internal_token.clone();
                async move {
                    NodeClient::new(node, &client, &internal_token)
                        .storage_status()
                        .await
                }
            },
            |answers| answers.iter().flatten().filter(|r| r.is_ok()).count() >= quorum,
            Some(straggler_grace),
        )
        .await
        .into_iter()
        .map(|answer| answer.unwrap_or(Err(ReplicaError::NodeUnavailable)))
        .collect()
    }

    async fn set_local_fence(&self, fence: GatewayMaintenanceFence) {
        *self.admission.fence.lock().await = LocalMaintenanceState::Fenced(fence);
    }

    async fn set_unknown_fence(&self) {
        let mut local = self.admission.fence.lock().await;
        if !matches!(*local, LocalMaintenanceState::Fenced(_)) {
            *local = LocalMaintenanceState::Unknown;
        }
    }

    async fn wait_for_active_requests(&self) {
        loop {
            let drained = self.admission.drained.notified();
            if self.active_requests.load(Ordering::Acquire) == 0 {
                return;
            }
            drained.await;
        }
    }

    async fn apply_durable_observation(
        &self,
        statuses: &[StorageNodeStatus],
        expected_nodes: usize,
    ) -> Result<DurableFenceObservation, ReplicaError> {
        let observation = match observe_durable_fence(statuses) {
            Ok(observation) => observation,
            Err(error) => {
                let mut local = self.admission.fence.lock().await;
                if !matches!(*local, LocalMaintenanceState::Fenced(_)) {
                    *local = LocalMaintenanceState::Unknown;
                }
                return Err(error);
            }
        };
        let mut local = self.admission.fence.lock().await;
        if statuses.len() != expected_nodes {
            if let Some((owner, generation)) = &observation.active {
                let token = self
                    .auth
                    .issue_maintenance_token(owner.clone(), *generation)?;
                *local = LocalMaintenanceState::Fenced(GatewayMaintenanceFence {
                    token,
                    owner: owner.clone(),
                    generation: *generation,
                });
            } else if !matches!(*local, LocalMaintenanceState::Fenced(_))
                && !matches!(*local, LocalMaintenanceState::Unknown)
            {
                // A best-effort status refresh on a newly constructed
                // gateway must not turn an otherwise healthy quorum write
                // into an outage merely because one discovered node is down.
                // Acquisition itself remains strict and records a local
                // fail-closed fence when any node rejects it.
                *local = LocalMaintenanceState::Open;
            }
            return Ok(observation);
        }
        match &observation.active {
            Some((owner, generation)) => {
                let token = self
                    .auth
                    .issue_maintenance_token(owner.clone(), *generation)?;
                *local = LocalMaintenanceState::Fenced(GatewayMaintenanceFence {
                    token,
                    owner: owner.clone(),
                    generation: *generation,
                });
            }
            None if observation.complete => *local = LocalMaintenanceState::Open,
            None => *local = LocalMaintenanceState::Unknown,
        }
        Ok(observation)
    }

    /// Reconstructs local admission from the durable node markers. This is
    /// called on gateway startup's first status/write operation and whenever a
    /// transition needs to prove the current marker set.
    async fn reconcile_durable_fence_locked(
        &self,
    ) -> Result<DurableFenceObservation, ReplicaError> {
        let nodes = self.all_members_snapshot().await?;
        let statuses = self.fetch_durable_statuses(&nodes).await;
        let successful = statuses
            .iter()
            .filter_map(|result| result.as_ref().ok().cloned())
            .collect::<Vec<_>>();
        self.apply_durable_observation(&successful, nodes.len())
            .await
    }

    async fn refresh_durable_fence(&self) -> Result<DurableFenceObservation, ReplicaError> {
        let _transition = self.admission.transition.lock().await;
        self.reconcile_durable_fence_locked().await
    }

    async fn local_fence_view(&self) -> DurableFenceObservation {
        match self.admission.fence.lock().await.clone() {
            LocalMaintenanceState::Open => DurableFenceObservation::default(),
            LocalMaintenanceState::Unknown => DurableFenceObservation {
                complete: false,
                ..DurableFenceObservation::default()
            },
            LocalMaintenanceState::Fenced(fence) => DurableFenceObservation {
                active: Some((fence.owner, fence.generation)),
                fenced_storage: 0,
                generation: fence.generation,
                complete: false,
            },
        }
    }

    async fn admit_append(&self) -> Result<AppendAdmission, ReplicaError> {
        // Direct gateways are stateless coordinators.  Their storage nodes
        // perform the authoritative maintenance and placement checks while
        // holding the append state lock, so a normal append must not first
        // serialize behind a three-node status fanout.  Explicit maintenance
        // transitions still take `transition` and reconcile every member;
        // this branch only removes that control-plane round trip from the
        // ordinary data path.
        if self.direct_mode() {
            let fence = self.admission.fence.lock().await;
            if !matches!(*fence, LocalMaintenanceState::Open) {
                // A maintenance fence is an intentional availability
                // boundary; do not admit a request already known to this
                // gateway to be fenced or uncertain.
                return Err(ReplicaError::QuorumUnavailable);
            }
            self.admission.active.fetch_add(1, Ordering::AcqRel);
            drop(fence);
            self.active_requests.fetch_add(1, Ordering::AcqRel);
            return Ok(AppendAdmission {
                admission: Arc::clone(&self.admission),
                active_requests: Arc::clone(&self.active_requests),
            });
        }

        let transition = self.admission.transition.lock().await;
        if self.reconcile_durable_fence_locked().await.is_err() {
            drop(transition);
            return Err(ReplicaError::QuorumUnavailable);
        }
        let fence = self.admission.fence.lock().await;
        if !matches!(*fence, LocalMaintenanceState::Open) {
            // A maintenance fence is an intentional availability boundary;
            // do not let an append race a node removal or get acknowledged
            // against a membership that is about to disappear.
            return Err(ReplicaError::QuorumUnavailable);
        }
        self.admission.active.fetch_add(1, Ordering::AcqRel);
        drop(fence);
        drop(transition);
        self.active_requests.fetch_add(1, Ordering::AcqRel);
        Ok(AppendAdmission {
            admission: Arc::clone(&self.admission),
            active_requests: Arc::clone(&self.active_requests),
        })
    }

    async fn admit_read(&self) -> Result<ReadAdmission, ReplicaError> {
        let transition = self.admission.transition.lock().await;
        if self.reconcile_durable_fence_locked().await.is_err() {
            drop(transition);
            return Err(ReplicaError::QuorumUnavailable);
        }
        let fence = self.admission.fence.lock().await;
        if !matches!(*fence, LocalMaintenanceState::Open) {
            return Err(ReplicaError::QuorumUnavailable);
        }
        self.active_requests.fetch_add(1, Ordering::AcqRel);
        drop(fence);
        drop(transition);
        Ok(ReadAdmission {
            admission: Arc::clone(&self.admission),
            active_requests: Arc::clone(&self.active_requests),
        })
    }

    async fn maintenance_active(&self) -> bool {
        !matches!(
            *self.admission.fence.lock().await,
            LocalMaintenanceState::Open
        )
    }

    /// Returns the configured membership for a static or direct gateway.
    pub fn nodes(&self) -> Result<Vec<ReplicaNode>, ReplicaError> {
        match &self.membership {
            Membership::Static(nodes) => nodes
                .read()
                .map(|nodes| nodes.values().cloned().collect())
                .map_err(|_| ReplicaError::NodeStorage("membership lock is poisoned".to_owned())),
            Membership::Direct(direct) => direct.control.active_members(),
        }
    }

    async fn nodes_snapshot(&self) -> Result<Vec<ReplicaNode>, ReplicaError> {
        match &self.membership {
            Membership::Static(nodes) => nodes
                .read()
                .map(|nodes| nodes.values().cloned().collect())
                .map_err(|_| ReplicaError::NodeStorage("membership lock is poisoned".to_owned())),
            Membership::Direct(direct) => direct.control.active_members(),
        }
    }

    /// Returns every retained member for recovery and historical repair. It is
    /// deliberately distinct from [`Self::nodes_snapshot`], which exposes
    /// complete active cohorts for the write path and excludes joining or
    /// removed members.
    async fn recovery_nodes_snapshot(&self) -> Result<Vec<ReplicaNode>, ReplicaError> {
        self.all_members_snapshot().await
    }

    async fn all_members_snapshot(&self) -> Result<Vec<ReplicaNode>, ReplicaError> {
        match &self.membership {
            Membership::Static(nodes) => nodes
                .read()
                .map(|nodes| nodes.values().cloned().collect())
                .map_err(|_| ReplicaError::NodeStorage("membership lock is poisoned".to_owned())),
            Membership::Direct(direct) => direct.control.all_members(),
        }
    }

    /// Adds a node to an explicit static test membership. Direct membership
    /// changes must use the fenced asynchronous operation API.
    pub fn add_node(&self, node: ReplicaNode) -> Result<(), ReplicaError> {
        validate_node(&node)?;
        let Membership::Static(nodes) = &self.membership else {
            return Err(ReplicaError::NodeStorage(
                "direct membership requires a fenced operation".to_owned(),
            ));
        };
        let mut nodes = nodes
            .write()
            .map_err(|_| ReplicaError::NodeStorage("membership lock is poisoned".to_owned()))?;
        if let Some(existing) = nodes.get(&node.id) {
            return if existing == &node {
                Ok(())
            } else {
                Err(ReplicaError::LsnConflict)
            };
        }
        if nodes.values().any(|existing| existing.url == node.url) {
            return Err(ReplicaError::NodeStorage(
                "replica node URLs must be unique".to_owned(),
            ));
        }
        nodes.insert(node.id.clone(), node);
        Ok(())
    }

    /// Removes a node from an explicit static membership while preserving the
    /// configured quorum. Direct membership changes use the fenced API.
    pub fn remove_node(&self, id: &str) -> Result<ReplicaNode, ReplicaError> {
        let Membership::Static(nodes) = &self.membership else {
            return Err(ReplicaError::NodeStorage(
                "direct membership requires a fenced operation".to_owned(),
            ));
        };
        let mut nodes = nodes
            .write()
            .map_err(|_| ReplicaError::NodeStorage("membership lock is poisoned".to_owned()))?;
        if nodes.len().saturating_sub(1) < self.quorum {
            return Err(ReplicaError::NodeStorage(
                "removing this node would leave fewer than quorum members".to_owned(),
            ));
        }
        nodes
            .remove(id)
            .ok_or_else(|| ReplicaError::NodeStorage(format!("replica node {id} is not a member")))
    }

    /// Returns the durable direct membership view. Static compatibility
    /// gateways synthesize an epoch-zero active snapshot.
    pub async fn membership(&self) -> Result<MembershipSnapshot, ReplicaError> {
        match &self.membership {
            Membership::Direct(direct) => {
                if let Some(head_store) = self.control_head.as_ref()
                    && let Some(head) = head_store
                        .load()
                        .await
                        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
                {
                    direct.control.adopt_authoritative_head(&head.head)?;
                    let freeze = direct.control.metadata_freeze().ok().flatten();
                    return Ok(membership_snapshot_from_head(&head.head, freeze));
                }
                if self.control_head.is_some() && direct.control.state()?.control_head_revision != 0
                {
                    return Err(ReplicaError::NodeStorage(
                        "authoritative control head is missing after migration".to_owned(),
                    ));
                }
                direct.control.membership()
            }
            Membership::Static(nodes) => {
                let members: Vec<DurableMember> = nodes
                    .read()
                    .map_err(|_| {
                        ReplicaError::NodeStorage("membership lock is poisoned".to_owned())
                    })?
                    .values()
                    .cloned()
                    .map(|node| DurableMember {
                        id: node.id,
                        url: node.url,
                        status: MemberStatus::Active,
                        cohort_id: 0,
                        ..DurableMember::default()
                    })
                    .collect();
                Ok(MembershipSnapshot {
                    membership_epoch: 0,
                    manifest_revision: 0,
                    manifest_digest: String::new(),
                    write_cohort_id: 0,
                    control_authority_epoch: 1,
                    control_head_revision: 0,
                    control_head_digest: String::new(),
                    control_authority_cohort_id: 0,
                    metadata_freeze: None,
                    manifest: None,
                    members: members.clone(),
                    cohorts: vec![DurableCohort {
                        id: 0,
                        members: members.iter().map(|member| member.id.clone()).collect(),
                        status: CohortStatus::Active,
                        tier: String::new(),
                        max_append_bytes: 0,
                    }],
                    stream_segments: BTreeMap::new(),
                })
            }
        }
    }

    /// Returns the archive proof required before a cohort member may be
    /// drained or removed. The proof is derived from the quorum-committed
    /// immutable ranges, so an open range deliberately has no proof.
    pub async fn cohort_archive_proof(&self, cohort_id: u64) -> Result<String, ReplicaError> {
        let snapshot = self.membership().await?;
        cohort_archive_proof(&snapshot, cohort_id).ok_or_else(|| {
            ReplicaError::Protocol(
                "cohort archive proof is unavailable while a range is open".to_owned(),
            )
        })
    }

    /// Applies an arbitrary direct membership CAS under the durable
    /// all-member maintenance fence. The operation id is persisted and a
    /// retry returns the exact prior snapshot without incrementing the epoch.
    pub async fn membership_cas(
        &self,
        expected_epoch: u64,
        members: Vec<DurableMember>,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.membership_cas_document(expected_epoch, members, None, None, operation_id)
            .await
    }

    /// Applies a direct membership CAS with an explicitly carried cohort and
    /// stream-routing document.  This is the stateless-coordinator form: a
    /// coordinator that did not originate the operation can persist exactly
    /// the same immutable placement before it serves traffic.
    pub async fn membership_cas_document(
        &self,
        expected_epoch: u64,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if self.control().is_none() {
            return Err(ReplicaError::Protocol(
                "membership CAS is available only in direct mode".to_owned(),
            ));
        }
        self.direct_membership_document_operation(
            operation_id,
            move |current| {
                if current.membership_epoch != expected_epoch {
                    return Err(ReplicaError::LsnConflict);
                }
                Ok(members)
            },
            cohorts,
            stream_segments,
        )
        .await
    }

    /// Applies the complete membership document under an already acquired
    /// maintenance fence. Provisioning holds one fence across bootstrap
    /// policy persistence and cohort activation; attempting to acquire a
    /// second fence inside the CAS would fail closed with a conflicting owner.
    pub async fn membership_cas_document_with_maintenance(
        &self,
        expected_epoch: u64,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        operation_id: &str,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if self.control().is_none() {
            return Err(ReplicaError::Protocol(
                "membership CAS is available only in direct mode".to_owned(),
            ));
        }
        self.direct_membership_document_operation_with_maintenance(
            operation_id,
            maintenance_token,
            move |current| {
                if current.membership_epoch != expected_epoch {
                    return Err(ReplicaError::LsnConflict);
                }
                Ok(members)
            },
            cohorts,
            stream_segments,
        )
        .await
    }

    /// Joins a new member in `joining` state. Activation is a separate fenced
    /// operation so a replacement can be repaired before it enters quorum.
    pub async fn join_member(
        &self,
        operation_id: &str,
        node: ReplicaNode,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.join_member_in_cohort(operation_id, node, None).await
    }

    /// Joins a node into an explicitly selected cohort.  Autoscalers use this
    /// form so three machines created by one operation cannot accidentally be
    /// split across different scale-out units.
    pub async fn join_member_in_cohort(
        &self,
        operation_id: &str,
        node: ReplicaNode,
        requested_cohort_id: Option<u64>,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.join_member_in_cohort_with_identity(
            operation_id,
            node,
            requested_cohort_id,
            DurableMemberIdentity::default(),
        )
        .await
    }

    /// Joins a member while retaining the exact provider resource identity
    /// and ordinal in the durable control document.
    pub async fn join_member_in_cohort_with_identity(
        &self,
        operation_id: &str,
        node: ReplicaNode,
        requested_cohort_id: Option<u64>,
        identity: DurableMemberIdentity,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        validate_node(&node)?;
        self.direct_membership_operation(operation_id, move |current| {
            let mut members = current
                .members
                .iter()
                .cloned()
                .map(|member| (member.id.clone(), member))
                .collect::<BTreeMap<_, _>>();
            if let Some(existing) = members.get(&node.id) {
                if existing.url != node.url
                    || existing.status == MemberStatus::Removed
                    || (!identity.name.is_empty()
                        && !existing.name.is_empty()
                        && existing.name != identity.name)
                    || (!identity.machine_id.is_empty()
                        && !existing.machine_id.is_empty()
                        && existing.machine_id != identity.machine_id)
                    || (!identity.volume_id.is_empty()
                        && !existing.volume_id.is_empty()
                        && existing.volume_id != identity.volume_id)
                    || (identity.ordinal != 0
                        && existing.ordinal != 0
                        && existing.ordinal != identity.ordinal)
                    || (!identity.tier.is_empty()
                        && !existing.tier.is_empty()
                        && existing.tier != identity.tier)
                    || (identity.max_append_bytes != 0
                        && existing.max_append_bytes != 0
                        && existing.max_append_bytes != identity.max_append_bytes)
                {
                    return Err(ReplicaError::LsnConflict);
                }
                return Ok(members.into_values().collect());
            }
            if members.values().any(|member| member.url == node.url) {
                return Err(ReplicaError::LsnConflict);
            }
            let cohort_id = requested_cohort_id.unwrap_or_else(|| {
                current
                    .cohorts
                    .iter()
                    .filter(|cohort| {
                        cohort.status == CohortStatus::Joining
                            && cohort.members.len() < REPLICATION_FACTOR
                    })
                    .map(|cohort| cohort.id)
                    .max()
                    .unwrap_or_else(|| {
                        current
                            .cohorts
                            .iter()
                            .map(|cohort| cohort.id)
                            .max()
                            .map_or(0, |id| id.saturating_add(1))
                    })
            });
            if let Some(cohort) = current.cohorts.iter().find(|cohort| cohort.id == cohort_id)
                && cohort.members.len() >= REPLICATION_FACTOR
            {
                return Err(ReplicaError::LsnConflict);
            }
            members.insert(
                node.id.clone(),
                DurableMember {
                    id: node.id,
                    url: node.url,
                    status: MemberStatus::Joining,
                    cohort_id,
                    name: identity.name,
                    machine_id: identity.machine_id,
                    volume_id: identity.volume_id,
                    ordinal: identity.ordinal,
                    tier: identity.tier,
                    max_append_bytes: identity.max_append_bytes,
                },
            );
            Ok(members.into_values().collect())
        })
        .await
    }

    /// Registers a prepared member without taking the cell-wide maintenance
    /// fence. A zero requested cohort is treated as a replacement-candidate
    /// sentinel and receives a fresh immutable cohort id; the replacement
    /// handoff later decides whether that cohort becomes the writer.
    pub async fn join_member_in_cohort_online_with_identity(
        &self,
        operation_id: &str,
        node: ReplicaNode,
        requested_cohort_id: Option<u64>,
        identity: DurableMemberIdentity,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        validate_node(&node)?;
        let requested_cohort_id = requested_cohort_id.filter(|cohort_id| *cohort_id != 0);
        self.direct_membership_operation_online(operation_id, move |current| {
            let mut members = current
                .members
                .iter()
                .cloned()
                .map(|member| (member.id.clone(), member))
                .collect::<BTreeMap<_, _>>();
            if let Some(existing) = members.get(&node.id) {
                if existing.url != node.url
                    || existing.status == MemberStatus::Removed
                    || (!identity.name.is_empty()
                        && !existing.name.is_empty()
                        && existing.name != identity.name)
                    || (!identity.machine_id.is_empty()
                        && !existing.machine_id.is_empty()
                        && existing.machine_id != identity.machine_id)
                    || (!identity.volume_id.is_empty()
                        && !existing.volume_id.is_empty()
                        && existing.volume_id != identity.volume_id)
                    || (identity.ordinal != 0
                        && existing.ordinal != 0
                        && existing.ordinal != identity.ordinal)
                    || (!identity.tier.is_empty()
                        && !existing.tier.is_empty()
                        && existing.tier != identity.tier)
                    || (identity.max_append_bytes != 0
                        && existing.max_append_bytes != 0
                        && existing.max_append_bytes != identity.max_append_bytes)
                {
                    return Err(ReplicaError::LsnConflict);
                }
                return Ok(members.into_values().collect());
            }
            if members.values().any(|member| member.url == node.url) {
                return Err(ReplicaError::LsnConflict);
            }
            let cohort_id = requested_cohort_id.unwrap_or_else(|| {
                current
                    .cohorts
                    .iter()
                    .filter(|cohort| {
                        cohort.status == CohortStatus::Joining
                            && cohort.members.len() < REPLICATION_FACTOR
                    })
                    .map(|cohort| cohort.id)
                    .max()
                    .unwrap_or_else(|| {
                        current
                            .cohorts
                            .iter()
                            .map(|cohort| cohort.id)
                            .max()
                            .map_or(1, |id| id.saturating_add(1).max(1))
                    })
            });
            if cohort_id == 0 {
                return Err(ReplicaError::LsnConflict);
            }
            if let Some(cohort) = current.cohorts.iter().find(|cohort| cohort.id == cohort_id)
                && cohort.members.len() >= REPLICATION_FACTOR
            {
                return Err(ReplicaError::LsnConflict);
            }
            members.insert(
                node.id.clone(),
                DurableMember {
                    id: node.id,
                    url: node.url,
                    status: MemberStatus::Joining,
                    cohort_id,
                    name: identity.name,
                    machine_id: identity.machine_id,
                    volume_id: identity.volume_id,
                    ordinal: identity.ordinal,
                    tier: identity.tier,
                    max_append_bytes: identity.max_append_bytes,
                },
            );
            Ok(members.into_values().collect())
        })
        .await
    }

    /// Marks a joined member active after its repair has completed.
    pub async fn activate_member(
        &self,
        operation_id: &str,
        id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let id = id.to_owned();
        self.direct_membership_operation(operation_id, move |current| {
            let mut members = current
                .members
                .iter()
                .cloned()
                .map(|member| (member.id.clone(), member))
                .collect::<BTreeMap<_, _>>();
            let cohort_id = members
                .get(&id)
                .map(|member| member.cohort_id)
                .ok_or_else(|| {
                    ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                })?;
            let cohort_members = members
                .values()
                .filter(|member| member.cohort_id == cohort_id)
                .collect::<Vec<_>>();
            if cohort_members.len() != REPLICATION_FACTOR
                || cohort_members.iter().any(|member| {
                    !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
                })
            {
                return Err(ReplicaError::QuorumUnavailable);
            }
            for member in members
                .values_mut()
                .filter(|member| member.cohort_id == cohort_id)
            {
                match member.status {
                    MemberStatus::Joining | MemberStatus::Active => {
                        member.status = MemberStatus::Active
                    }
                    MemberStatus::Draining | MemberStatus::Removed => {
                        return Err(ReplicaError::LsnConflict);
                    }
                }
            }
            Ok(members.into_values().collect())
        })
        .await
    }

    /// Activates one replacement while the caller already owns the durable
    /// maintenance fence. This endpoint is reserved for the repair path; it
    /// must not acquire a second fence or release the caller's fence.
    pub async fn activate_member_with_maintenance(
        &self,
        operation_id: &str,
        id: &str,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let id = id.to_owned();
        self.direct_membership_operation_with_maintenance(
            operation_id,
            maintenance_token,
            move |current| {
                let mut members = current
                    .members
                    .iter()
                    .cloned()
                    .map(|member| (member.id.clone(), member))
                    .collect::<BTreeMap<_, _>>();
                let cohort_id =
                    members
                        .get(&id)
                        .map(|member| member.cohort_id)
                        .ok_or_else(|| {
                            ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                        })?;
                let cohort_members = members
                    .values()
                    .filter(|member| member.cohort_id == cohort_id)
                    .collect::<Vec<_>>();
                if cohort_members.len() != REPLICATION_FACTOR
                    || cohort_members.iter().any(|member| {
                        !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
                    })
                {
                    return Err(ReplicaError::QuorumUnavailable);
                }
                for member in members
                    .values_mut()
                    .filter(|member| member.cohort_id == cohort_id)
                {
                    member.status = MemberStatus::Active;
                }
                Ok(members.into_values().collect())
            },
        )
        .await
    }

    /// Copies every quorum-committed record owned by one cohort to a fresh
    /// member, then atomically swaps that member into the old slot. The
    /// placement manifest changes in the same durable CAS, so historical
    /// records never point at the retired member after the operation commits.
    #[allow(clippy::too_many_arguments)]
    pub async fn replace_member_with_maintenance(
        &self,
        operation_id: &str,
        old_member_id: &str,
        replacement: ReplicaNode,
        cohort_id: u64,
        identity: DurableMemberIdentity,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if operation_id.trim().is_empty()
            || old_member_id.trim().is_empty()
            || replacement.id.trim().is_empty()
            || old_member_id == replacement.id
        {
            return Err(ReplicaError::Protocol(
                "replacement requires distinct complete member identities".to_owned(),
            ));
        }
        validate_node(&replacement)?;
        let _operation = self.maintenance_operation(maintenance_token).await?;
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("member replacement is available only in direct mode".to_owned())
        })?;
        if let Some(snapshot) = control.operation_snapshot(operation_id)? {
            self.propagate_membership(&snapshot, maintenance_token, operation_id)
                .await?;
            self.install_current_route_placements().await?;
            return Ok(snapshot);
        }
        // The successor manifest is derived from the quorum manifest, never
        // from this coordinator's cache. Routes are published through any
        // coordinator, and a replacement member adopts whatever document it
        // is handed; a stale cache propagated through two swaps would become
        // the new quorum and erase every route published since.
        self.sync_manifest_cache().await?;

        let current_state = control.state()?;
        let old = current_state
            .members
            .get(old_member_id)
            .filter(|member| member.status == MemberStatus::Active)
            .ok_or_else(|| {
                ReplicaError::NodeStorage("replacement source is not active".to_owned())
            })?;
        // The original prototype bootstrap document predates durable Fly
        // identity fields, so those members decode with empty resource
        // identities and ordinal zero. The privileged operator has already
        // resolved and verified the exact Fly Machine before reaching this
        // fenced endpoint. Permit its real ordinal only for that fully sparse
        // shape; once replaced, the complete identity is persisted below.
        let sparse_bootstrap_identity =
            old.name.is_empty() && old.machine_id.is_empty() && old.volume_id.is_empty();
        if old.cohort_id != cohort_id
            || (!sparse_bootstrap_identity && old.ordinal != identity.ordinal)
        {
            return Err(ReplicaError::LsnConflict);
        }
        let source_nodes = current_state
            .cohorts
            .get(&cohort_id)
            .filter(|cohort| cohort.status == CohortStatus::Active)
            .ok_or(ReplicaError::QuorumUnavailable)?
            .members
            .iter()
            .filter_map(|id| current_state.members.get(id))
            .filter(|member| member.status == MemberStatus::Active)
            .map(DurableMember::node)
            .collect::<Vec<_>>();
        if source_nodes.len() != REPLICATION_FACTOR {
            return Err(ReplicaError::QuorumUnavailable);
        }

        let replacement_client =
            NodeClient::new(replacement.clone(), &self.client, &self.internal_token);
        replacement_client.fence(maintenance_token).await?;
        // Every retained source member must answer before anything is
        // judged. Quorum certification below counts the nodes that hold a
        // record, so a source that merely failed to respond would make every
        // committed record look like a minority tail and the replacement
        // would be activated empty. Refusing is safe: the operator retries
        // the same operation id once the member is back.
        let source_results = self.fetch_snapshot_results(&source_nodes, None).await;
        if source_results.iter().any(|(_, result)| result.is_err()) {
            return Err(ReplicaError::NodeUnavailable);
        }
        let source_snapshots = source_results
            .into_iter()
            .map(|(node, result)| (node, result.ok()))
            .collect::<Vec<_>>();
        let (candidates, _unsafe_records) =
            self.collect_candidates_from_snapshots("*", &source_snapshots)?;
        // The maintenance fence has stopped new appends. Records present on
        // fewer than a quorum are therefore definitively uncommitted tails,
        // not recoverable client acknowledgements. Copy only quorum-proven
        // candidates; carrying a minority tail into the replacement would be
        // unsafe, while refusing it would deadlock this operation forever.
        let replacement_snapshot = replacement_client.snapshot(None).await?;
        for record in candidates.into_values().filter(|record| {
            current_state
                .stream_segments
                .get(record.stream())
                .and_then(|segments| {
                    segments.iter().find(|segment| {
                        segment.start_lsn <= record.lsn()
                            && segment.end_lsn.is_none_or(|end| record.lsn() <= end)
                    })
                })
                .is_some_and(|segment| segment.cohort_id == cohort_id)
        }) {
            let already_present = replacement_snapshot
                .records
                .iter()
                .any(|candidate| candidate == &record)
                && replacement_snapshot
                    .committed
                    .iter()
                    .any(|candidate| candidate == &record);
            if !already_present {
                replacement_client
                    .append_with_maintenance(&record, maintenance_token)
                    .await?;
                replacement_client
                    .commit_with_maintenance(&record, maintenance_token)
                    .await?;
            }
        }
        // Hot records are only half of a member's state. Every prefix the
        // sources have trimmed after archival is carried as a checkpoint,
        // verified against the archive head, so the replacement can fence
        // stale writers and accept the exact successor of an archived prefix
        // exactly as the member it replaces could.
        let mut checkpoints: BTreeMap<String, TrimmedPrefix> = BTreeMap::new();
        for (_, snapshot) in &source_snapshots {
            let Some(snapshot) = snapshot else { continue };
            for prefix in &snapshot.trimmed {
                let entry = checkpoints
                    .entry(prefix.stream.clone())
                    .or_insert_with(|| prefix.clone());
                if prefix.archived_lsn > entry.archived_lsn {
                    entry.archived_lsn = prefix.archived_lsn;
                }
                entry.writer_epoch = entry.writer_epoch.max(prefix.writer_epoch);
            }
        }
        let mut verified = Vec::with_capacity(checkpoints.len());
        for prefix in checkpoints.into_values() {
            let archived_lsn = self
                .archive
                .archived_lsn(&prefix.stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if archived_lsn < prefix.archived_lsn {
                return Err(ReplicaError::NodeStorage(format!(
                    "archive head {} trails the trimmed prefix {} of {}",
                    archived_lsn, prefix.archived_lsn, prefix.stream
                )));
            }
            verified.push(prefix);
        }
        if !verified.is_empty() {
            replacement_client
                .compact_with_maintenance(&verified, maintenance_token)
                .await?;
        }

        let mut next = current_state.clone();
        next.members.remove(old_member_id);
        next.members.insert(
            replacement.id.clone(),
            DurableMember {
                id: replacement.id.clone(),
                url: replacement.url.clone(),
                status: MemberStatus::Active,
                cohort_id,
                name: identity.name,
                machine_id: identity.machine_id,
                volume_id: identity.volume_id,
                ordinal: identity.ordinal,
                tier: identity.tier,
                max_append_bytes: identity.max_append_bytes,
            },
        );
        let member_ids = {
            let cohort = next
                .cohorts
                .get_mut(&cohort_id)
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let slot = cohort
                .members
                .iter_mut()
                .find(|id| id.as_str() == old_member_id)
                .ok_or(ReplicaError::LsnConflict)?;
            *slot = replacement.id.clone();
            cohort.members.sort();
            let member_tiers = cohort
                .members
                .iter()
                .filter_map(|id| next.members.get(id))
                .map(|member| (member.tier.clone(), member.max_append_bytes))
                .collect::<BTreeSet<_>>();
            if member_tiers.len() == 1
                && let Some((tier, max_append_bytes)) = member_tiers.into_iter().next()
            {
                cohort.tier = tier;
                cohort.max_append_bytes = max_append_bytes;
            }
            cohort.members.clone()
        };
        // Normalize legacy empty tier metadata before computing the new
        // manifest. Clearing the cloned digest is safe here because `next`
        // is only a candidate document; the durable CAS below still verifies
        // the exact previous revision and incoming digest.
        next.manifest_digest.clear();
        reconcile_cohorts(&mut next)?;
        let (cohort_tier, cohort_max_append_bytes) = next
            .cohorts
            .get(&cohort_id)
            .map(|cohort| (cohort.tier.clone(), cohort.max_append_bytes))
            .ok_or(ReplicaError::QuorumUnavailable)?;
        let member_hash = member_set_hash(&next, &member_ids).ok_or(ReplicaError::LsnConflict)?;
        let next_manifest_revision = current_state
            .manifest_revision
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
        for segment in next.stream_segments.values_mut().flatten() {
            if segment.cohort_id == cohort_id {
                segment.member_ids = member_ids.clone();
                segment.member_hash = member_hash.clone();
                segment.tier = cohort_tier.clone();
                segment.max_append_bytes = cohort_max_append_bytes;
                segment.manifest_revision = next_manifest_revision;
                segment.operation_id = operation_id.to_owned();
            }
        }
        let next_members = next.members.values().cloned().collect::<Vec<_>>();
        let next_cohorts = next.cohorts.values().cloned().collect::<Vec<_>>();
        let next_segments = next.stream_segments;
        let snapshot = control.cas_membership_document_with_manifest(
            current_state.membership_epoch,
            current_state.membership_epoch.checked_add(1),
            true,
            next_members,
            Some(next_cohorts),
            Some(next_segments.clone()),
            Some(next_manifest_revision),
            Some(manifest_digest(&next_segments)),
            operation_id,
        )?;
        self.propagate_membership(&snapshot, maintenance_token, operation_id)
            .await?;
        self.install_current_route_placements().await?;
        Ok(snapshot)
    }

    /// Activates all three members of one complete joining cohort in a single
    /// durable membership transition.  The active routing cohort changes only
    /// after this CAS has propagated to every coordinator.
    pub async fn activate_cohort(
        &self,
        operation_id: &str,
        cohort_id: u64,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let operation_id = operation_id.to_owned();
        self.direct_membership_operation(&operation_id, move |current| {
            let mut members = current
                .members
                .iter()
                .cloned()
                .map(|member| (member.id.clone(), member))
                .collect::<BTreeMap<_, _>>();
            let cohort_members = members
                .values()
                .filter(|member| member.cohort_id == cohort_id)
                .collect::<Vec<_>>();
            if cohort_members.len() != REPLICATION_FACTOR
                || cohort_members.iter().any(|member| {
                    !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
                })
            {
                return Err(ReplicaError::QuorumUnavailable);
            }
            for member in members
                .values_mut()
                .filter(|member| member.cohort_id == cohort_id)
            {
                member.status = MemberStatus::Active;
            }
            Ok(members.into_values().collect())
        })
        .await
    }

    /// Activates a complete prepared cohort without stopping the data plane.
    ///
    /// The object-store control head is the only authority for this
    /// transition. Candidate health is established before the candidate is
    /// made eligible, then each stream whose rendezvous winner changes is
    /// moved through the durable stream-handoff state machine. Existing
    /// immutable ranges remain on their original cohort until their own
    /// placement fence, archive watermark, and successor route are
    /// committed; a failed stream therefore leaves all other streams
    /// writable and resumable.
    pub async fn activate_cohort_online(
        &self,
        operation_id: &str,
        cohort_id: u64,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if operation_id.trim().is_empty() || cohort_id == 0 {
            return Err(ReplicaError::Protocol(
                "online cohort activation requires an operation id and nonzero cohort id"
                    .to_owned(),
            ));
        }
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol(
                "online cohort activation is available only in direct mode".to_owned(),
            )
        })?;

        // Migration is idempotent. Once a head exists, all retries adopt it
        // rather than consulting the legacy metadata quorum.
        self.migrate_control_authority(&format!("{operation_id}:authority"))
            .await?;
        if let Some(head) = self.load_authoritative_head().await?
            && head.head.completed_operations.contains_key(operation_id)
        {
            let snapshot = self.membership().await?;
            self.propagate_authoritative_head().await?;
            return Ok(snapshot);
        }

        let current = self.membership().await?;
        let mut members = current
            .members
            .iter()
            .cloned()
            .map(|member| (member.id.clone(), member))
            .collect::<BTreeMap<_, _>>();
        let target = current
            .cohorts
            .iter()
            .find(|cohort| cohort.id == cohort_id)
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if target.members.len() != REPLICATION_FACTOR
            || !matches!(target.status, CohortStatus::Joining | CohortStatus::Active)
            || target.members.iter().any(|id| {
                members.get(id).is_none_or(|member| {
                    !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
                })
            })
        {
            return Err(ReplicaError::QuorumUnavailable);
        }

        let target_nodes = target
            .members
            .iter()
            .map(|id| {
                members
                    .get(id)
                    .map(DurableMember::node)
                    .ok_or(ReplicaError::QuorumUnavailable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let readiness = join_all(target_nodes.iter().cloned().map(|node| {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            async move { NodeClient::new(node, &client, &token).health().await }
        }))
        .await;
        if readiness.iter().any(Result::is_err) {
            return Err(ReplicaError::NodeUnavailable);
        }

        // This is the sole eligibility CAS. It carries the complete existing
        // manifest unchanged, so no stream is rerouted merely because the
        // cohort became active. The new target cannot win rendezvous until
        // every candidate has passed the readiness check above.
        for id in &target.members {
            members
                .get_mut(id)
                .ok_or(ReplicaError::QuorumUnavailable)?
                .status = MemberStatus::Active;
        }
        let mut cohorts = current
            .cohorts
            .iter()
            .cloned()
            .map(|cohort| (cohort.id, cohort))
            .collect::<BTreeMap<_, _>>();
        cohorts
            .get_mut(&cohort_id)
            .ok_or(ReplicaError::QuorumUnavailable)?
            .status = CohortStatus::Active;
        let activation_operation_id = format!("{operation_id}:target-active");
        let activated = self
            .cas_authoritative_membership(
                current.membership_epoch,
                members.into_values().collect(),
                Some(cohorts.into_values().collect()),
                Some(current.stream_segments.clone()),
                Some(current.manifest_revision),
                Some(current.manifest_digest.clone()),
                None,
                true,
                false,
                &activation_operation_id,
            )
            .await?;
        let _ = activated;
        self.propagate_authoritative_head().await?;

        // Capture the streams that remap under the now-active rendezvous
        // ring. The set is a work list only; every stream is reloaded before
        // its handoff so a restarted coordinator resumes the exact durable
        // claim rather than reusing stale route state.
        let state = control.state()?;
        let mut streams = state
            .stream_segments
            .iter()
            .filter_map(|(stream, ranges)| {
                let route = ranges.last()?;
                if route.end_lsn.is_some() || route.cohort_id == cohort_id {
                    return None;
                }
                (cohort_for_stream(&state, stream).ok() == Some(cohort_id)).then(|| stream.clone())
            })
            .collect::<BTreeSet<_>>();
        if let Some(head) = self.load_authoritative_head().await? {
            for stream in head.head.metadata.pending_handoffs.keys() {
                let stream_operation_id = stream_handoff_operation_id(operation_id, stream);
                if head.head.metadata.pending_handoffs[stream].operation_id == stream_operation_id {
                    streams.insert(stream.clone());
                }
            }
        }

        for stream in streams {
            let stream_operation_id = stream_handoff_operation_id(operation_id, &stream);
            if let Some(claim) = self.pending_handoff_claim_for_stream(&stream).await? {
                if claim.operation_id != stream_operation_id {
                    return Err(ReplicaError::Protocol(format!(
                        "stream {stream} is already owned by online handoff {}",
                        claim.operation_id
                    )));
                }
                if claim.stage == PendingHandoffStage::RoutePublished {
                    self.finish_pending_stream_handoff(&stream).await?;
                    continue;
                }
            }

            let state = control.state()?;
            let Some(source) = state
                .stream_segments
                .get(&stream)
                .and_then(|ranges| ranges.last())
                .filter(|route| route.end_lsn.is_none())
                .cloned()
            else {
                continue;
            };
            let target_cohort_id = cohort_for_stream(&state, &stream)?;
            if target_cohort_id != cohort_id || source.cohort_id == target_cohort_id {
                continue;
            }
            let target = state
                .cohorts
                .get(&target_cohort_id)
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let target_ids = target.members.clone();
            let target_nodes = target_ids
                .iter()
                .map(|id| {
                    state
                        .members
                        .get(id)
                        .map(DurableMember::node)
                        .ok_or(ReplicaError::QuorumUnavailable)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let target_member_hash =
                member_set_hash(&state, &target_ids).ok_or(ReplicaError::LsnConflict)?;
            let next_manifest_revision = state
                .manifest_revision
                .checked_add(1)
                .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
            let cutover = self
                .cutover_stream(
                    &stream,
                    &source,
                    target_cohort_id,
                    &target_ids,
                    &target_nodes,
                    &target_member_hash,
                    &target.tier,
                    target.max_append_bytes,
                    next_manifest_revision,
                    &stream_operation_id,
                )
                .await?;
            self.publish_one_pending_handoff(&cutover).await?;
        }

        // A final operation receipt is a metadata-only object-head CAS. It
        // does not advance membership or rewrite the manifest, and therefore
        // cannot make a concurrent append reject merely because activation is
        // completing. It also proves that no claim owned by this operation
        // was silently abandoned.
        let final_state = control.state()?;
        for (stream, ranges) in &final_state.stream_segments {
            let Some(route) = ranges.last() else { continue };
            if route.end_lsn.is_none()
                && route.cohort_id != cohort_id
                && cohort_for_stream(&final_state, stream)? == cohort_id
            {
                return Err(ReplicaError::LsnConflict);
            }
        }
        let head_store = self.control_head_store()?;
        let expected = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if expected
            .head
            .metadata
            .pending_handoffs
            .iter()
            .any(|(stream, claim)| {
                claim.operation_id == stream_handoff_operation_id(operation_id, stream)
            })
        {
            return Err(ReplicaError::LsnConflict);
        }
        if expected
            .head
            .completed_operations
            .contains_key(operation_id)
        {
            return Ok(membership_snapshot_from_head(
                &expected.head,
                control.metadata_freeze().ok().flatten(),
            ));
        }
        let next = ControlHead::new(
            expected.head.authority_epoch,
            expected.head.revision.checked_add(1).ok_or_else(|| {
                ReplicaError::Protocol("control-head revision exhausted".to_owned())
            })?,
            operation_id,
            expected.head.source_marker.clone(),
            expected.head.metadata.clone(),
        )
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let committed = self.cas_authoritative_head(&expected, next).await?;
        self.propagate_authoritative_head().await?;
        Ok(membership_snapshot_from_head(
            &committed.head,
            control.metadata_freeze().ok().flatten(),
        ))
    }

    /// Installs one placement value on a node set.  Placement is a host-side
    /// admission boundary, so every member that could still receive the old
    /// route must persist the value before the coordinator publishes a
    /// successor.  This fan-out is deliberately one bounded operation; it is
    /// not a retry or polling loop.
    async fn install_placement_on_nodes(
        &self,
        stream: &str,
        placement: &PlacementEpoch,
        nodes: &[ReplicaNode],
    ) -> Result<(), ReplicaError> {
        let mut unique = BTreeMap::new();
        for node in nodes {
            unique
                .entry(node.id.clone())
                .or_insert_with(|| node.clone());
        }
        let results = join_all(unique.into_values().map(|node| {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            let stream = stream.to_owned();
            let placement = placement.clone();
            async move {
                NodeClient::new(node, &client, &token)
                    .install_placement_fence(&stream, &placement)
                    .await
            }
        }))
        .await;
        let acknowledgements = results.iter().filter(|result| result.is_ok()).count();
        if acknowledgements < self.quorum {
            return Err(results
                .into_iter()
                .find_map(Result::err)
                .unwrap_or(ReplicaError::QuorumUnavailable));
        }
        Ok(())
    }

    /// Loads the stream-local handoff claim, when the object-store control
    /// head has already been initialized. A claim is the durable ownership
    /// boundary for a cutover; retries must reuse its placement epochs rather
    /// than deriving a fresh value from whatever a node reports now.
    async fn pending_handoff_claim(
        &self,
        stream: &str,
        operation_id: &str,
    ) -> Result<Option<PendingHandoffDescriptor>, ReplicaError> {
        let Some(head_store) = self.control_head_store().ok() else {
            return Ok(None);
        };
        let Some(snapshot) = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        else {
            return Ok(None);
        };
        if let Some(control) = self.control() {
            control.adopt_authoritative_head(&snapshot.head)?;
        }
        let Some(descriptor) = snapshot.head.metadata.pending_handoffs.get(stream) else {
            return Ok(None);
        };
        if descriptor.operation_id != operation_id {
            return Err(ReplicaError::Protocol(format!(
                "stream {stream} is already owned by online handoff {}",
                descriptor.operation_id
            )));
        }
        Ok(Some(descriptor.clone()))
    }

    /// Claims one stream before touching node admission. The claim and every
    /// later phase are object-store CASes; the old control cohort remains a
    /// metadata reader while a handoff is in progress.
    async fn claim_pending_handoff(
        &self,
        descriptor: PendingHandoffDescriptor,
    ) -> Result<bool, ReplicaError> {
        let Some(head_store) = self.control_head_store().ok() else {
            return Ok(false);
        };
        let Some(expected) = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        else {
            return Ok(false);
        };
        if let Some(existing) = expected
            .head
            .metadata
            .pending_handoffs
            .get(&descriptor.stream)
            && existing.operation_id != descriptor.operation_id
        {
            return Err(ReplicaError::Protocol(format!(
                "stream {} is already owned by online handoff {}",
                descriptor.stream, existing.operation_id
            )));
        }
        let snapshot = head_store
            .claim_pending_handoff(&expected, descriptor)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if let Some(control) = self.control() {
            control.adopt_authoritative_head(&snapshot.head)?;
        }
        Ok(true)
    }

    /// Advances a claimed stream phase using the current object-head ETag.
    async fn advance_pending_handoff(
        &self,
        descriptor: PendingHandoffDescriptor,
    ) -> Result<bool, ReplicaError> {
        let Some(head_store) = self.control_head_store().ok() else {
            return Ok(false);
        };
        let Some(expected) = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        else {
            return Ok(false);
        };
        let snapshot = head_store
            .advance_pending_handoff(&expected, descriptor)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if let Some(control) = self.control() {
            control.adopt_authoritative_head(&snapshot.head)?;
        }
        Ok(true)
    }

    /// Publishes and completes one stream claim while leaving the source
    /// cohort lifecycle unchanged. This is the foreground completion path:
    /// an append that encounters a claimed handoff can finish that exact
    /// successor route before sending its original ciphertext.
    async fn publish_one_pending_handoff(
        &self,
        cutover: &StreamCutover,
    ) -> Result<(), ReplicaError> {
        let Some(base) = cutover.pending_handoff.as_ref() else {
            return Ok(());
        };
        let head_store = self.control_head_store()?;
        let mut expected = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            .ok_or_else(|| {
                ReplicaError::Protocol(
                    "authoritative membership head is not initialized".to_owned(),
                )
            })?;
        let mut metadata = expected.head.metadata.clone();
        if let Some(existing) = metadata.pending_handoffs.get(&cutover.stream) {
            if existing.operation_id == base.operation_id
                && existing.stage == PendingHandoffStage::RoutePublished
            {
                let published = existing
                    .successor
                    .as_ref()
                    .ok_or(ReplicaError::LsnConflict)?;
                let route_present = metadata
                    .manifest
                    .stream_segments
                    .get(&cutover.stream)
                    .is_some_and(|ranges| ranges.iter().any(|range| range == published));
                if !route_present {
                    return Err(ReplicaError::LsnConflict);
                }
                expected = head_store
                    .complete_pending_handoff(&expected, &cutover.stream, &base.operation_id)
                    .await
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
                if let Some(control) = self.control() {
                    control.adopt_authoritative_head(&expected.head)?;
                }
                return Ok(());
            }
        } else if metadata
            .manifest
            .stream_segments
            .get(&cutover.stream)
            .is_some_and(|ranges| {
                ranges.iter().any(|range| {
                    range.start_lsn == cutover.successor.start_lsn
                        && range.cohort_id == cutover.successor.cohort_id
                        && range.member_ids == cutover.successor.member_ids
                        && range.member_hash == cutover.successor.member_hash
                        && range.placement_epoch == cutover.successor.placement_epoch
                        && range.end_lsn.is_none()
                })
            })
        {
            // A concurrent foreground completion removed the claim after
            // publishing the exact route. The route identity is the durable
            // proof; do not report a conflict merely because cleanup won.
            return Ok(());
        }
        let ranges = metadata
            .manifest
            .stream_segments
            .get_mut(&cutover.stream)
            .ok_or(ReplicaError::LsnConflict)?;
        let last = ranges.last_mut().ok_or(ReplicaError::LsnConflict)?;
        let next_manifest_revision = expected
            .head
            .metadata
            .manifest
            .revision
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
        let published_successor = base
            .successor
            .clone()
            .unwrap_or_else(|| cutover.successor.clone());
        if published_successor.manifest_revision == 0
            || published_successor.manifest_revision > next_manifest_revision
            || published_successor.operation_id.trim().is_empty()
        {
            return Err(ReplicaError::LsnConflict);
        }
        if *last != cutover.source {
            return Err(ReplicaError::LsnConflict);
        }
        let archived_lsn = base.archived_lsn.unwrap_or(cutover.archived_lsn);
        if archived_lsn != cutover.archived_lsn {
            return Err(ReplicaError::LsnConflict);
        }
        if archived_lsn < cutover.source.start_lsn {
            ranges.pop();
            ranges.push(published_successor.clone());
        } else {
            last.end_lsn = Some(archived_lsn);
            ranges.push(published_successor.clone());
        }
        metadata.manifest.revision = next_manifest_revision;
        metadata.manifest.digest = manifest_digest(&metadata.manifest.stream_segments);
        metadata.manifest.operation_id = format!("{}:route", base.operation_id);
        metadata.manifest.digest = manifest_digest(&metadata.manifest.stream_segments);

        let mut descriptor = base.clone();
        descriptor.stage = PendingHandoffStage::RoutePublished;
        descriptor.archived_lsn = Some(archived_lsn);
        descriptor.successor = Some(published_successor.clone());
        descriptor.successor_placement = Some(
            base.successor_placement
                .clone()
                .unwrap_or_else(|| placement_for_route(&cutover.stream, &published_successor)),
        );
        metadata
            .pending_handoffs
            .insert(cutover.stream.clone(), descriptor.clone());
        expected = head_store
            .publish_pending_handoff(&expected, metadata, descriptor)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        expected = head_store
            .complete_pending_handoff(&expected, &cutover.stream, &base.operation_id)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if let Some(control) = self.control() {
            control.adopt_authoritative_head(&expected.head)?;
        }
        Ok(())
    }

    /// Completes one claimed stream handoff for an append or stale-route
    /// request. The caller supplies no retry token: the persisted descriptor
    /// determines the exact source, target, epochs, and archive boundary.
    pub async fn finish_pending_stream_handoff(&self, stream: &str) -> Result<bool, ReplicaError> {
        let Some(claim) = self.pending_handoff_claim_for_stream(stream).await? else {
            return Ok(false);
        };
        if claim.stage == PendingHandoffStage::RoutePublished {
            let head_store = self.control_head_store()?;
            let expected = head_store
                .load()
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let snapshot = head_store
                .complete_pending_handoff(&expected, stream, &claim.operation_id)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if let Some(control) = self.control() {
                control.adopt_authoritative_head(&snapshot.head)?;
            }
            return Ok(true);
        }
        let control = self.control().ok_or(ReplicaError::QuorumUnavailable)?;
        let current = control.state()?;
        let target_ids = claim
            .target_members
            .iter()
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        let target_nodes = claim
            .target_members
            .iter()
            .map(DurableMember::node)
            .collect::<Vec<_>>();
        let next_revision = current
            .manifest_revision
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
        let cutover = self
            .cutover_stream(
                stream,
                &claim.source,
                claim.target_cohort_id,
                &target_ids,
                &target_nodes,
                &claim.target_member_hash,
                &claim.target_tier,
                claim.target_max_append_bytes,
                next_revision,
                &claim.operation_id,
            )
            .await?;
        self.publish_one_pending_handoff(&cutover).await?;
        Ok(true)
    }

    async fn pending_handoff_claim_for_stream(
        &self,
        stream: &str,
    ) -> Result<Option<PendingHandoffDescriptor>, ReplicaError> {
        let Some(head_store) = self.control_head_store().ok() else {
            return Ok(None);
        };
        let Some(snapshot) = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        else {
            return Ok(None);
        };
        if let Some(control) = self.control() {
            control.adopt_authoritative_head(&snapshot.head)?;
        }
        Ok(snapshot.head.metadata.pending_handoffs.get(stream).cloned())
    }

    /// Makes a cohort ineligible for new rendezvous assignments while
    /// retaining its active members and every existing route.  This metadata
    /// boundary is deliberately separate from the per-stream placement
    /// fences: a stream that is already on the source cohort continues to be
    /// served until its own handoff has certified an archive watermark.
    async fn begin_cohort_handoff(
        &self,
        operation_id: &str,
        source_cohort_id: u64,
        target_cohort: Option<(u64, Vec<DurableMember>)>,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let control = self.control().ok_or(ReplicaError::QuorumUnavailable)?;
        let current = control.state()?;
        let source = current
            .cohorts
            .get(&source_cohort_id)
            .filter(|cohort| matches!(cohort.status, CohortStatus::Active | CohortStatus::Draining))
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if source.members.len() != REPLICATION_FACTOR
            || source.members.iter().any(|id| {
                current.members.get(id).is_none_or(|member| {
                    !matches!(member.status, MemberStatus::Active | MemberStatus::Draining)
                })
            })
        {
            return Err(ReplicaError::QuorumUnavailable);
        }

        let mut members = current.members.clone();
        let mut cohorts = current.cohorts.clone();
        cohorts
            .get_mut(&source_cohort_id)
            .ok_or(ReplicaError::QuorumUnavailable)?
            .status = CohortStatus::Draining;

        if let Some((target_cohort_id, target_specs)) = target_cohort {
            if target_cohort_id == 0 || target_cohort_id == source_cohort_id {
                return Err(ReplicaError::LsnConflict);
            }
            let mut target_specs = target_specs;
            target_specs.sort_by(|left, right| left.id.cmp(&right.id));
            if target_specs.len() != REPLICATION_FACTOR
                || target_specs.windows(2).any(|pair| pair[0].id == pair[1].id)
                || target_specs.iter().any(|member| {
                    member.id.trim().is_empty()
                        || member.cohort_id != target_cohort_id
                        || source.members.iter().any(|id| id == &member.id)
                })
            {
                return Err(ReplicaError::QuorumUnavailable);
            }
            let target_tier = target_specs
                .first()
                .map(|member| member.tier.clone())
                .unwrap_or_default();
            let target_max_append_bytes = target_specs
                .first()
                .map_or(0, |member| member.max_append_bytes);
            if target_specs.iter().any(|member| {
                member.tier != target_tier || member.max_append_bytes != target_max_append_bytes
            }) {
                return Err(ReplicaError::LsnConflict);
            }
            let sorted_target_ids = target_specs
                .iter()
                .map(|member| member.id.clone())
                .collect::<Vec<_>>();
            for candidate in target_specs {
                validate_node(&candidate.node())?;
                let mut candidate = candidate;
                candidate.status = MemberStatus::Active;
                if let Some(existing) = members.get(&candidate.id) {
                    if existing.status == MemberStatus::Removed {
                        return Err(ReplicaError::LsnConflict);
                    }
                    let mut existing_identity = existing.clone();
                    existing_identity.status = MemberStatus::Active;
                    if existing_identity != candidate {
                        return Err(ReplicaError::LsnConflict);
                    }
                }
                members.insert(candidate.id.clone(), candidate);
            }
            let target = cohorts
                .entry(target_cohort_id)
                .or_insert_with(|| DurableCohort {
                    id: target_cohort_id,
                    members: Vec::new(),
                    status: CohortStatus::Joining,
                    tier: target_tier.clone(),
                    max_append_bytes: target_max_append_bytes,
                });
            if !target.members.is_empty() && target.members != sorted_target_ids {
                return Err(ReplicaError::LsnConflict);
            }
            target.members = sorted_target_ids;
            target.status = CohortStatus::Active;
            target.tier = target_tier;
            target.max_append_bytes = target_max_append_bytes;
        }

        let prepare_operation = format!("{operation_id}:prepare-handoff");
        let snapshot = self
            .cas_authoritative_membership(
                current.membership_epoch,
                members.into_values().collect(),
                Some(cohorts.into_values().collect()),
                Some(current.stream_segments.clone()),
                Some(current.manifest_revision),
                Some(current.manifest_digest.clone()),
                None,
                true,
                true,
                &prepare_operation,
            )
            .await?;
        self.propagate_membership_online(&snapshot, &prepare_operation, true)
            .await?;
        Ok(snapshot)
    }

    /// Retires a source cohort after every open source range has published a
    /// successor and the archive verifier has covered all finite source
    /// ranges.  The final CAS carries the caller's operation id, so a
    /// restarted controller can prove completion from the object head rather
    /// than inferring it from a partially propagated node set.
    async fn finish_cohort_handoff(
        &self,
        operation_id: &str,
        source_cohort_id: u64,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let control = self.control().ok_or(ReplicaError::QuorumUnavailable)?;
        let head_store = self.control_head_store()?;
        let head = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            .ok_or(ReplicaError::QuorumUnavailable)?;
        control.adopt_authoritative_head(&head.head)?;
        if head
            .head
            .metadata
            .pending_handoffs
            .values()
            .any(|handoff| handoff.source.cohort_id == source_cohort_id)
        {
            return Err(ReplicaError::LsnConflict);
        }
        let current = control.state()?;
        let source = current
            .cohorts
            .get(&source_cohort_id)
            .filter(|cohort| cohort.status == CohortStatus::Draining)
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if current
            .stream_segments
            .values()
            .flatten()
            .any(|segment| segment.cohort_id == source_cohort_id && segment.end_lsn.is_none())
        {
            return Err(ReplicaError::LsnConflict);
        }

        let mut members = current.members.clone();
        for id in &source.members {
            members.get_mut(id).ok_or(ReplicaError::LsnConflict)?.status = MemberStatus::Removed;
        }
        let mut cohorts = current.cohorts.clone();
        cohorts
            .get_mut(&source_cohort_id)
            .ok_or(ReplicaError::QuorumUnavailable)?
            .status = CohortStatus::Retired;
        let snapshot = self
            .cas_authoritative_membership(
                current.membership_epoch,
                members.into_values().collect(),
                Some(cohorts.into_values().collect()),
                Some(current.stream_segments.clone()),
                Some(current.manifest_revision),
                Some(current.manifest_digest.clone()),
                None,
                true,
                true,
                operation_id,
            )
            .await?;
        self.propagate_authoritative_head().await?;
        Ok(snapshot)
    }

    /// Performs the data-plane half of one handoff.  The source route remains
    /// in the manifest while this runs.  First every source and target volume
    /// is checked, then the source placement gate is advanced and drained,
    /// then a quorum-certified source tail is archived, and only then is the
    /// exact successor placement token installed on both sides.
    #[allow(clippy::too_many_arguments)]
    async fn cutover_stream(
        &self,
        stream: &str,
        source: &StreamSegment,
        target_cohort_id: u64,
        target_ids: &[String],
        target_nodes: &[ReplicaNode],
        target_member_hash: &str,
        target_tier: &str,
        target_max_append_bytes: u64,
        next_manifest_revision: u64,
        operation_id: &str,
    ) -> Result<StreamCutover, ReplicaError> {
        let source_nodes = source
            .member_ids
            .iter()
            .map(|id| {
                self.control()
                    .and_then(|control| control.state().ok())
                    .and_then(|state| state.members.get(id).map(DurableMember::node))
                    .ok_or(ReplicaError::QuorumUnavailable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if source_nodes.len() != REPLICATION_FACTOR
            || target_ids.len() != REPLICATION_FACTOR
            || target_nodes.len() != REPLICATION_FACTOR
        {
            return Err(ReplicaError::QuorumUnavailable);
        }

        let mut all_nodes = source_nodes.clone();
        all_nodes.extend(target_nodes.iter().cloned());
        let existing_claim = self.pending_handoff_claim(stream, operation_id).await?;
        // A persisted claim owns its exact transition and successor epochs.
        // Resuming it must not read a higher local placement and mint another
        // fence; the claim is the durable state machine boundary. A fresh
        // claim can derive its first epoch from a quorum of the source and
        // target nodes, allowing one volume to be unavailable without
        // blocking the whole cohort.
        let (transition_epoch, transition) = if let Some(claim) = &existing_claim {
            (
                claim.transition_placement.epoch,
                claim.transition_placement.clone(),
            )
        } else {
            let read_placements = |nodes: &[ReplicaNode]| {
                let nodes = nodes.to_vec();
                let client = self.client.clone();
                let token = self.internal_token.clone();
                let stream = stream.to_owned();
                async move {
                    join_all(nodes.into_iter().map(|node| {
                        let client = client.clone();
                        let token = token.clone();
                        let stream = stream.clone();
                        async move {
                            NodeClient::new(node, &client, &token)
                                .current_placement(&stream)
                                .await
                        }
                    }))
                    .await
                }
            };
            let source_placements = read_placements(&source_nodes).await;
            let target_placements = read_placements(target_nodes).await;
            if source_placements
                .iter()
                .filter(|result| result.is_ok())
                .count()
                < self.quorum
                || target_placements
                    .iter()
                    .filter(|result| result.is_ok())
                    .count()
                    < self.quorum
            {
                return Err(ReplicaError::NodeUnavailable);
            }
            let source_epoch = source_placements
                .iter()
                .chain(target_placements.iter())
                .filter_map(|result| result.as_ref().ok())
                .map(PlacementEpoch::epoch)
                .max()
                .unwrap_or(0)
                .max(source.placement_epoch)
                .max(source.manifest_revision);
            let transition_epoch = source_epoch.saturating_add(1).max(1);
            (
                transition_epoch,
                placement_for_transition(stream, source, operation_id, transition_epoch),
            )
        };
        let target_members = self
            .control()
            .and_then(|control| control.state().ok())
            .ok_or(ReplicaError::QuorumUnavailable)?
            .members
            .values()
            .filter(|member| target_ids.contains(&member.id))
            .cloned()
            .collect::<Vec<_>>();
        if target_members.len() != REPLICATION_FACTOR {
            return Err(ReplicaError::QuorumUnavailable);
        }
        if let Some(claim) = &existing_claim
            && (claim.source != *source
                || claim.target_cohort_id != target_cohort_id
                || claim.target_members != target_members
                || claim.target_member_hash != target_member_hash)
        {
            return Err(ReplicaError::LsnConflict);
        }
        let mut pending = existing_claim;
        if pending.is_none() && self.load_authoritative_head().await?.is_some() {
            let descriptor = PendingHandoffDescriptor {
                operation_id: operation_id.to_owned(),
                stream: stream.to_owned(),
                source: source.clone(),
                target_cohort_id,
                target_members,
                target_member_hash: target_member_hash.to_owned(),
                target_tier: target_tier.to_owned(),
                target_max_append_bytes,
                target_writer_epoch: source.writer_epoch,
                transition_placement: transition.clone(),
                successor_placement_epoch: transition_epoch.saturating_add(1).max(1),
                successor_placement: None,
                stage: PendingHandoffStage::Prepared,
                archived_lsn: None,
                successor: None,
            };
            self.claim_pending_handoff(descriptor.clone()).await?;
            pending = Some(descriptor);
        }

        // This is the only point that changes source admission.  A resumed
        // claim already records whether this fence completed; never install a
        // new transition epoch over an ArchiveVerified claim.
        if pending
            .as_ref()
            .is_none_or(|claim| claim.stage.rank() < PendingHandoffStage::SourceFenced.rank())
        {
            self.install_placement_on_nodes(stream, &transition, &source_nodes)
                .await?;
            if let Some(claim) = &pending {
                let mut fenced = claim.clone();
                fenced.stage = PendingHandoffStage::SourceFenced;
                self.advance_pending_handoff(fenced.clone()).await?;
                pending = Some(fenced);
            }
        }

        let (archived_lsn, cutover_lsn) = if let Some(claim) = &pending
            && claim.stage.rank() >= PendingHandoffStage::ArchiveVerified.rank()
        {
            let archived_lsn = claim.archived_lsn.ok_or(ReplicaError::LsnConflict)?;
            (archived_lsn, archived_lsn)
        } else {
            // A source cohort is a three-member write quorum. One unavailable
            // source volume is safe here: the placement fence and commit
            // evidence on the other two still define the certified tail.
            let source_results = self
                .fetch_snapshot_results(&source_nodes, Some(stream))
                .await;
            let available = source_results
                .iter()
                .filter(|(_, result)| result.is_ok())
                .count();
            if available < self.quorum {
                return Err(ReplicaError::NodeUnavailable);
            }
            let source_snapshots = source_results
                .into_iter()
                .map(|(node, result)| (node, result.ok()))
                .collect::<Vec<_>>();
            let archived_before = self
                .archive
                .archived_lsn(stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            // A matching data quorum without durable commit markers is still
            // ambiguous, even when every source answered.  Keep that legacy
            // safety check ahead of the handoff-specific partial-tail rule;
            // the latter only permits a single-copy, fully observed tail to
            // remain outside the successor's certified prefix.
            self.reject_ambiguous_quorum_records(stream, archived_before, &source_snapshots)?;
            let (candidates, unsafe_records) =
                self.collect_candidates_from_snapshots(stream, &source_snapshots)?;
            if unsafe_records != 0 && available < source_nodes.len() {
                // With a source member unavailable, an unsafe tail may be
                // the one copy that completed an acknowledged append. Keep
                // the handoff fail-closed until an authenticated recovery
                // certificate supplies that missing evidence. When every
                // source volume answered after the placement fence drained,
                // the same evidence is a fully observed partial tail and can
                // be left behind while the successor starts at the last
                // certified contiguous LSN.
                return Err(ReplicaError::RecoveryAmbiguous {
                    lsn: 0,
                    committed_nodes: 0,
                    quorum: self.quorum,
                });
            }

            // The archive head is already a certified immutable prefix. Read
            // only its tail after the watermark instead of replaying the
            // stream's complete history for every source handoff.
            let archived = self
                .archive
                .recover(stream, archived_before)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            let mut records_by_lsn = BTreeMap::<u64, EncryptedRecord>::new();
            for record in archived {
                records_by_lsn.insert(record.lsn(), record);
            }
            for record in candidates.into_values() {
                if record.lsn() <= archived_before {
                    continue;
                }
                if let Some(existing) = records_by_lsn.get(&record.lsn())
                    && existing != &record
                {
                    return Err(ReplicaError::LsnConflict);
                }
                records_by_lsn.insert(record.lsn(), record);
            }
            let mut certified = Vec::new();
            let mut expected = source.start_lsn.max(archived_before.saturating_add(1));
            let mut expected_epoch = source.writer_epoch;
            while let Some(record) = records_by_lsn.get(&expected) {
                if record.committed_lsn() != expected.saturating_sub(1)
                    || record.writer_epoch() < expected_epoch
                {
                    return Err(ReplicaError::RecoveryAmbiguous {
                        lsn: expected,
                        committed_nodes: 0,
                        quorum: self.quorum,
                    });
                }
                expected_epoch = record.writer_epoch();
                certified.push(record.clone());
                expected = expected.saturating_add(1);
            }
            if records_by_lsn.keys().any(|lsn| *lsn > expected) {
                return Err(ReplicaError::RecoveryGap {
                    expected_lsn: expected,
                    received_lsn: records_by_lsn
                        .keys()
                        .find(|lsn| **lsn > expected)
                        .copied()
                        .unwrap_or(expected),
                });
            }
            let new_archive_records = certified
                .iter()
                .filter(|record| record.lsn() > archived_before)
                .cloned()
                .collect::<Vec<_>>();
            if !new_archive_records.is_empty() {
                self.archive
                    .archive_committed(&new_archive_records)
                    .await
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            }
            let archived_lsn = self
                .archive
                .archived_lsn(stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            let cutover_lsn = expected.saturating_sub(1);
            if archived_lsn < cutover_lsn {
                return Err(ReplicaError::NodeStorage(format!(
                    "archive ended at {archived_lsn}, cutover requires {cutover_lsn}"
                )));
            }
            (archived_lsn, cutover_lsn)
        };

        let successor_start = cutover_lsn.saturating_add(1).max(source.start_lsn);
        let final_epoch = pending.as_ref().map_or_else(
            || transition_epoch.saturating_add(1).max(1),
            |claim| claim.successor_placement_epoch,
        );
        let computed_successor = || StreamSegment {
            start_lsn: successor_start,
            end_lsn: None,
            cohort_id: target_cohort_id,
            member_ids: target_ids.to_vec(),
            member_hash: target_member_hash.to_owned(),
            // The client-authenticated writer epoch is preserved. Placement
            // fencing is the host-side stale-writer boundary.
            writer_epoch: source.writer_epoch,
            manifest_revision: next_manifest_revision,
            placement_epoch: final_epoch,
            operation_id: manifest_operation_id(
                stream,
                next_manifest_revision,
                successor_start,
                target_cohort_id,
                source.writer_epoch,
            ),
            tier: target_tier.to_owned(),
            max_append_bytes: target_max_append_bytes,
        };
        // ArchiveVerified persists the complete successor route before any
        // target placement is installed. A crash after the sidecar write can
        // therefore resume the exact route and token without depending on a
        // later manifest revision or a node-local tail guess.
        let successor = pending
            .as_ref()
            .and_then(|claim| claim.successor.clone())
            .unwrap_or_else(computed_successor);
        let final_placement = pending
            .as_ref()
            .and_then(|claim| claim.successor_placement.clone())
            .unwrap_or_else(|| placement_for_route(stream, &successor));
        if let Some(claim) = &pending {
            if claim.stage.rank() >= PendingHandoffStage::ArchiveVerified.rank() {
                if claim.successor.as_ref() != Some(&successor)
                    || claim.successor_placement.as_ref() != Some(&final_placement)
                    || claim.archived_lsn != Some(archived_lsn)
                {
                    return Err(ReplicaError::LsnConflict);
                }
            } else {
                let mut archived_claim = claim.clone();
                archived_claim.stage = PendingHandoffStage::ArchiveVerified;
                archived_claim.archived_lsn = Some(archived_lsn);
                archived_claim.successor_placement = Some(final_placement.clone());
                archived_claim.successor = Some(successor.clone());
                self.advance_pending_handoff(archived_claim.clone()).await?;
                pending = Some(archived_claim);
            }
        }
        // Each physical cohort must persist the successor token on its own
        // write quorum. Counting a combined source+target response set
        // could otherwise let two old source nodes satisfy the CAS while no
        // target volume is ready to accept the first successor write.
        self.install_placement_on_nodes(stream, &final_placement, &source_nodes)
            .await?;
        self.install_placement_on_nodes(stream, &final_placement, target_nodes)
            .await?;
        Ok(StreamCutover {
            stream: stream.to_owned(),
            source: source.clone(),
            successor,
            archived_lsn,
            pending_handoff: pending,
        })
    }

    /// Replaces every member of one active cohort while the data plane keeps
    /// serving the old route during candidate preparation. The source route is
    /// fenced per stream, its certified prefix is archived, and an exact
    /// successor range is published in the final membership CAS. No historical
    /// record is copied peer-to-peer into the candidate volumes.
    pub async fn replace_cohort_online(
        &self,
        operation_id: &str,
        requested_source_cohort_id: u64,
        requested_target_cohort_id: u64,
        old_member_ids: Vec<String>,
        candidate_specs: Vec<DurableMember>,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "online cohort replacement requires an operation id".to_owned(),
            ));
        }
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol(
                "online cohort replacement is available only in direct mode".to_owned(),
            )
        })?;
        self.migrate_control_authority(&format!("{operation_id}:authority"))
            .await?;
        if let Some(head) = self.load_authoritative_head().await?
            && head.head.completed_operations.contains_key(operation_id)
        {
            let snapshot = self.membership().await?;
            self.propagate_authoritative_head().await?;
            return Ok(snapshot);
        }
        self.sync_manifest_cache().await?;
        let current = control.state()?;
        let mut old_ids = old_member_ids;
        if old_ids.is_empty() {
            old_ids = current
                .cohorts
                .get(&requested_source_cohort_id)
                .map(|cohort| cohort.members.clone())
                .ok_or(ReplicaError::QuorumUnavailable)?;
        }
        old_ids.sort();
        old_ids.dedup();
        if old_ids.len() != REPLICATION_FACTOR {
            return Err(ReplicaError::LsnConflict);
        }
        let source_cohort_id = if requested_source_cohort_id != 0 {
            requested_source_cohort_id
        } else {
            let ids = old_ids
                .iter()
                .filter_map(|id| current.members.get(id).map(|member| member.cohort_id))
                .collect::<BTreeSet<_>>();
            if ids.len() != 1 {
                return Err(ReplicaError::LsnConflict);
            }
            *ids.first().ok_or(ReplicaError::LsnConflict)?
        };
        let source_cohort = current
            .cohorts
            .get(&source_cohort_id)
            .filter(|cohort| matches!(cohort.status, CohortStatus::Active | CohortStatus::Draining))
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if source_cohort.members != old_ids
            || old_ids.iter().any(|id| {
                current.members.get(id).is_none_or(|member| {
                    !matches!(member.status, MemberStatus::Active | MemberStatus::Draining)
                })
            })
        {
            return Err(ReplicaError::LsnConflict);
        }

        let mut candidate_ids = candidate_specs
            .iter()
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        if candidate_ids.is_empty() {
            candidate_ids = current
                .members
                .values()
                .filter(|member| {
                    member.status == MemberStatus::Joining && member.cohort_id != source_cohort_id
                })
                .map(|member| member.id.clone())
                .collect();
        }
        candidate_ids.sort();
        candidate_ids.dedup();
        if candidate_ids.len() != REPLICATION_FACTOR
            || candidate_ids.iter().any(|id| old_ids.contains(id))
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let candidate_cohort_id = if requested_target_cohort_id != 0 {
            requested_target_cohort_id
        } else {
            candidate_ids
                .iter()
                .filter_map(|id| current.members.get(id).map(|member| member.cohort_id))
                .find(|id| *id != 0 && *id != source_cohort_id)
                .ok_or(ReplicaError::QuorumUnavailable)?
        };
        if candidate_cohort_id == source_cohort_id || candidate_cohort_id == 0 {
            return Err(ReplicaError::LsnConflict);
        }
        let target_members = candidate_ids
            .iter()
            .map(|id| {
                candidate_specs
                    .iter()
                    .find(|member| member.id == *id)
                    .cloned()
                    .or_else(|| current.members.get(id).cloned())
                    .ok_or(ReplicaError::QuorumUnavailable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let target_nodes = target_members
            .iter()
            .map(DurableMember::node)
            .collect::<Vec<_>>();
        let readiness = join_all(target_nodes.iter().cloned().map(|node| {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            async move { NodeClient::new(node, &client, &token).health().await }
        }))
        .await;
        if readiness.iter().any(Result::is_err) {
            return Err(ReplicaError::NodeUnavailable);
        }

        // Make the prepared cohort eligible and remove the source cohort from
        // the rendezvous ring before enumerating streams. Existing routes keep
        // using the source members; only streams created after this CAS are
        // assigned elsewhere.
        self.begin_cohort_handoff(
            operation_id,
            source_cohort_id,
            Some((candidate_cohort_id, target_members.clone())),
        )
        .await?;

        // Handoffs are published one stream at a time. Each iteration reloads
        // the authoritative route because the preceding stream's manifest CAS
        // is allowed to advance independently of unrelated streams.
        let open_streams = control
            .state()?
            .stream_segments
            .iter()
            .filter_map(|(stream, ranges)| {
                let route = ranges.last()?;
                (route.cohort_id == source_cohort_id && route.end_lsn.is_none())
                    .then(|| stream.clone())
            })
            .collect::<Vec<_>>();
        for stream in open_streams {
            let current = control.state()?;
            let Some(source) = current
                .stream_segments
                .get(&stream)
                .and_then(|ranges| ranges.last())
                .filter(|route| route.cohort_id == source_cohort_id && route.end_lsn.is_none())
                .cloned()
            else {
                // A previous attempt may have completed this stream after the
                // initial enumeration. Its durable successor is authoritative.
                continue;
            };
            let target = current
                .cohorts
                .get(&candidate_cohort_id)
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let target_ids = target.members.clone();
            let target_nodes = target_ids
                .iter()
                .map(|id| {
                    current
                        .members
                        .get(id)
                        .map(DurableMember::node)
                        .ok_or(ReplicaError::QuorumUnavailable)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let target_member_hash =
                member_set_hash(&current, &target_ids).ok_or(ReplicaError::LsnConflict)?;
            let next_manifest_revision = current
                .manifest_revision
                .checked_add(1)
                .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
            let stream_operation_id = stream_handoff_operation_id(operation_id, &stream);
            let cutover = self
                .cutover_stream(
                    &stream,
                    &source,
                    candidate_cohort_id,
                    &target_ids,
                    &target_nodes,
                    &target_member_hash,
                    &target.tier,
                    target.max_append_bytes,
                    next_manifest_revision,
                    &stream_operation_id,
                )
                .await?;
            self.publish_one_pending_handoff(&cutover).await?;
        }

        // All source routes are now finite and their archive proofs can be
        // checked while the draining members are still readable. Only after
        // that proof does the final membership CAS remove the source cohort.
        let current = control.state()?;
        let proposed_manifest = manifest_from_state(&current);
        self.archive_source_finite_ranges(&proposed_manifest, source_cohort_id)
            .await?;
        let snapshot = self
            .finish_cohort_handoff(operation_id, source_cohort_id)
            .await?;
        self.propagate_authoritative_head().await?;
        Ok(snapshot)
    }

    /// Retires one complete active cohort in place. Existing active cohorts
    /// remain in the rendezvous ring; each source stream is handed to the
    /// winner among those survivors after its per-stream placement fence has
    /// drained and its immutable prefix has reached the archive. The source
    /// members stay in the durable document as Removed tombstones so a stale
    /// coordinator cannot resurrect them.
    pub async fn retire_cohort_online(
        &self,
        operation_id: &str,
        source_cohort_id: u64,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "online cohort retirement requires an operation id".to_owned(),
            ));
        }
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol(
                "online cohort retirement is available only in direct mode".to_owned(),
            )
        })?;
        self.migrate_control_authority(&format!("{operation_id}:authority"))
            .await?;
        if let Some(head) = self.load_authoritative_head().await?
            && head.head.completed_operations.contains_key(operation_id)
        {
            let snapshot = self.membership().await?;
            self.propagate_authoritative_head().await?;
            return Ok(snapshot);
        }

        self.sync_manifest_cache().await?;
        let current = control.state()?;
        let source_cohort = current
            .cohorts
            .get(&source_cohort_id)
            .filter(|cohort| matches!(cohort.status, CohortStatus::Active | CohortStatus::Draining))
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if source_cohort.members.len() != REPLICATION_FACTOR
            || source_cohort.members.iter().any(|id| {
                current.members.get(id).is_none_or(|member| {
                    !matches!(member.status, MemberStatus::Active | MemberStatus::Draining)
                })
            })
        {
            return Err(ReplicaError::QuorumUnavailable);
        }

        // Mark the source ineligible for new streams while preserving its
        // existing routes. If a prior attempt made this CAS, the idempotent
        // operation is simply adopted and the per-stream loop resumes.
        self.begin_cohort_handoff(operation_id, source_cohort_id, None)
            .await?;

        let open_streams = control
            .state()?
            .stream_segments
            .iter()
            .filter_map(|(stream, ranges)| {
                let route = ranges.last()?;
                (route.cohort_id == source_cohort_id && route.end_lsn.is_none())
                    .then(|| stream.clone())
            })
            .collect::<Vec<_>>();
        for stream in open_streams {
            let current = control.state()?;
            let Some(source) = current
                .stream_segments
                .get(&stream)
                .and_then(|ranges| ranges.last())
                .filter(|route| route.cohort_id == source_cohort_id && route.end_lsn.is_none())
                .cloned()
            else {
                continue;
            };
            let target_cohort_id =
                cohort_for_stream_excluding(&current, &stream, Some(source_cohort_id))?;
            let target_cohort = current
                .cohorts
                .get(&target_cohort_id)
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let target_ids = target_cohort.members.clone();
            let target_nodes = target_ids
                .iter()
                .map(|id| {
                    current
                        .members
                        .get(id)
                        .map(DurableMember::node)
                        .ok_or(ReplicaError::QuorumUnavailable)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let target_member_hash =
                member_set_hash(&current, &target_ids).ok_or(ReplicaError::LsnConflict)?;
            let next_manifest_revision = current
                .manifest_revision
                .checked_add(1)
                .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
            let stream_operation_id = stream_handoff_operation_id(operation_id, &stream);
            let cutover = self
                .cutover_stream(
                    &stream,
                    &source,
                    target_cohort_id,
                    &target_ids,
                    &target_nodes,
                    &target_member_hash,
                    &target_cohort.tier,
                    target_cohort.max_append_bytes,
                    next_manifest_revision,
                    &stream_operation_id,
                )
                .await?;
            self.publish_one_pending_handoff(&cutover).await?;
        }

        let current = control.state()?;
        let proposed_manifest = manifest_from_state(&current);
        self.archive_source_finite_ranges(&proposed_manifest, source_cohort_id)
            .await?;
        let snapshot = self
            .finish_cohort_handoff(operation_id, source_cohort_id)
            .await?;
        self.propagate_authoritative_head().await?;
        Ok(snapshot)
    }

    /// Activates a complete cohort while the caller already holds the
    /// gateway's durable maintenance fence.  This is the autoscaler path:
    /// reacquiring the fence would reject a valid in-progress operation and
    /// would leave a crash between fencing and activation unrecoverable.
    pub async fn activate_cohort_with_maintenance(
        &self,
        operation_id: &str,
        cohort_id: u64,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if cohort_id == 0 {
            return Err(ReplicaError::Protocol(
                "cohort activation requires a nonzero cohort id".to_owned(),
            ));
        }
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("cohort activation is available only in direct mode".to_owned())
        })?;
        if self.load_authoritative_head().await?.is_some() {
            let current = self.membership().await?;
            let mut members = current
                .members
                .iter()
                .cloned()
                .map(|member| (member.id.clone(), member))
                .collect::<BTreeMap<_, _>>();
            let cohort_members = members
                .values()
                .filter(|member| member.cohort_id == cohort_id)
                .collect::<Vec<_>>();
            if cohort_members.len() != REPLICATION_FACTOR
                || cohort_members.iter().any(|member| {
                    !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
                })
            {
                return Err(ReplicaError::QuorumUnavailable);
            }
            for member in members
                .values_mut()
                .filter(|member| member.cohort_id == cohort_id)
            {
                member.status = MemberStatus::Active;
            }
            let snapshot = self
                .cas_authoritative_membership(
                    current.membership_epoch,
                    members.into_values().collect(),
                    None,
                    None,
                    None,
                    None,
                    Some(cohort_id),
                    true,
                    false,
                    operation_id,
                )
                .await?;
            self.propagate_authoritative_head().await?;
            return Ok(snapshot);
        }
        let _operation = self.maintenance_operation(maintenance_token).await?;
        if let Some(snapshot) = control.operation_snapshot(operation_id)? {
            // Keep the same durable fence until every member has adopted the
            // operation. If propagation fails, releasing the reachable
            // members would leave a mixed-generation fence: a retry could no
            // longer re-fence those members because their release tombstones
            // correctly reject the old token. The caller can retry this
            // operation id after the missing member returns, or explicitly
            // recover/release the retained fence.
            self.propagate_membership(&snapshot, maintenance_token, operation_id)
                .await?;
            // `end_maintenance` waits for all fence-owned operations to have
            // returned.  The guard above is no longer needed after the full
            // propagation succeeds, and must be dropped before releasing the
            // caller's durable fence.  On propagation failure `?` returns
            // while the guard is dropped normally and the fence remains held.
            drop(_operation);
            self.end_maintenance(maintenance_token).await?;
            return Ok(snapshot);
        }
        let current = control.membership()?;
        let mut members = current
            .members
            .iter()
            .cloned()
            .map(|member| (member.id.clone(), member))
            .collect::<BTreeMap<_, _>>();
        let cohort_members = members
            .values()
            .filter(|member| member.cohort_id == cohort_id)
            .collect::<Vec<_>>();
        if cohort_members.len() != REPLICATION_FACTOR
            || cohort_members.iter().any(|member| {
                !matches!(member.status, MemberStatus::Joining | MemberStatus::Active)
            })
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        for member in members
            .values_mut()
            .filter(|member| member.cohort_id == cohort_id)
        {
            member.status = MemberStatus::Active;
        }
        let snapshot = control.cas_membership(
            current.membership_epoch,
            members.into_values().collect(),
            operation_id,
        )?;
        self.propagate_membership(&snapshot, maintenance_token, operation_id)
            .await?;
        Ok(snapshot)
    }

    /// Marks an active member draining. At least three active members must
    /// remain after every lifecycle operation.
    pub async fn drain_member(
        &self,
        operation_id: &str,
        id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.drain_member_with_archive_proof(operation_id, id, None)
            .await
    }

    /// Marks an active member draining after the caller proves that every
    /// immutable range owned by its cohort has been archived and sealed.
    pub async fn drain_member_with_archive_proof(
        &self,
        operation_id: &str,
        id: &str,
        archive_proof: Option<&str>,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let id = id.to_owned();
        let archive_proof = archive_proof.map(ToOwned::to_owned);
        self.direct_membership_operation(operation_id, move |current| {
            let mut members = current
                .members
                .iter()
                .cloned()
                .map(|member| (member.id.clone(), member))
                .collect::<BTreeMap<_, _>>();
            let cohort_id = members
                .get(&id)
                .map(|member| member.cohort_id)
                .ok_or_else(|| {
                    ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                })?;
            require_cohort_archive_proof(current, cohort_id, archive_proof.as_deref())?;
            let member = members.get_mut(&id).ok_or_else(|| {
                ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
            })?;
            if member.status == MemberStatus::Active {
                member.status = MemberStatus::Draining;
            } else if member.status != MemberStatus::Draining {
                return Err(ReplicaError::LsnConflict);
            }
            if members
                .values()
                .filter(|member| member.status == MemberStatus::Active)
                .count()
                < 3
            {
                return Err(ReplicaError::QuorumUnavailable);
            }
            Ok(members.into_values().collect())
        })
        .await
    }

    /// Certified durable tail of one stream for a fenced cutover: the
    /// quorum-proven hot records merged with the immutable archive and walked
    /// as one contiguous prefix from LSN 1. The archive is the authority for
    /// any prefix the hot logs have trimmed, and it still counts when a
    /// member lost its trim watermark, so a cutover never seals below a
    /// record the cell already acknowledged.
    async fn certified_stream_tail(
        &self,
        stream: &str,
        candidates: &CandidateMap,
        hot: &GatewayWriterState,
    ) -> Result<(u64, u64), ReplicaError> {
        let hot_state = hot.streams.get(stream);
        let mut committed = hot_state.map_or(0, |state| state.committed_lsn);
        let mut writer_epoch = hot_state.map_or(0, |state| state.writer_epoch);
        let mut records = self
            .archive
            .recover(stream, 0)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        records.extend(
            candidates
                .iter()
                .filter(|((candidate_stream, _), _)| candidate_stream == stream)
                .map(|(_, record)| record.clone()),
        );
        records.sort_by_key(EncryptedRecord::lsn);
        records.dedup_by_key(|record| record.lsn());
        let mut expected_lsn = 1_u64;
        let mut prefix_epoch = 0_u64;
        let mut merged = 0_u64;
        for record in records {
            if record.lsn() != expected_lsn
                || record.committed_lsn() != expected_lsn.saturating_sub(1)
                || record.writer_epoch() < prefix_epoch
            {
                break;
            }
            prefix_epoch = record.writer_epoch();
            writer_epoch = writer_epoch.max(prefix_epoch);
            merged = record.lsn();
            expected_lsn = expected_lsn.saturating_add(1);
        }
        committed = committed.max(merged);
        Ok((committed, writer_epoch))
    }

    /// Seals every open stream range still owned by `cohort_id` at its
    /// quorum-certified durable LSN and opens the successor range on the
    /// rendezvous winner among the remaining active cohorts.
    ///
    /// A live stream cuts over on its next append, but a quiet stream keeps
    /// its open tail on the cohort it was placed on and would pin that cohort
    /// in the cell forever. The caller holds the durable maintenance fence,
    /// so no append can race the boundary, and the successor begins exactly
    /// at the next LSN: the same proof an append-path cutover carries. Each
    /// stream is one manifest CAS, so the operation is idempotent by
    /// construction and a retry after a fault resumes with the next open
    /// tail. A range that never carried a durable record cannot be sealed;
    /// once every cohort member has confirmed it is empty, it is dropped and
    /// the stream's next append publishes a fresh route.
    pub async fn cutover_cohort_with_maintenance(
        &self,
        operation_id: &str,
        cohort_id: u64,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "cutover operation_id must not be empty".to_owned(),
            ));
        }
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("cohort cutover is available only in direct mode".to_owned())
        })?;
        let _operation = self.maintenance_operation(maintenance_token).await?;
        let source_members = control
            .state()?
            .cohorts
            .get(&cohort_id)
            .map(|cohort| cohort.members.iter().cloned().collect::<BTreeSet<_>>())
            .ok_or_else(|| {
                ReplicaError::Protocol(format!("cohort {cohort_id} is not part of the membership"))
            })?;
        // Certify every stream's durable tail from the retained cohorts
        // before choosing a boundary. A seal published from a stale
        // watermark would strand durable records behind it, so a quorum of
        // the source cohort itself must have answered.
        let nodes = self.recovery_nodes_snapshot().await?;
        let snapshots = self.fetch_snapshots(&nodes, None).await;
        let answering = snapshots
            .iter()
            .filter(|(node, snapshot)| snapshot.is_some() && source_members.contains(&node.id))
            .count();
        if answering < self.quorum {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let (candidates, _) = self.collect_candidates_from_snapshots("*", &snapshots)?;
        let certified = rebuild_writer_state(&candidates, &snapshots, self.quorum);
        loop {
            let expected = self
                .read_manifest_quorum()
                .await?
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let local = control.manifest().ok();
            if local.as_ref().is_none_or(|local| {
                local.revision != expected.revision || local.digest != expected.digest
            }) {
                control.adopt_manifest(&expected, true)?;
            }
            let state = control.state()?;
            let Some((stream, open)) =
                expected
                    .stream_segments
                    .iter()
                    .find_map(|(stream, segments)| {
                        segments
                            .last()
                            .filter(|segment| {
                                segment.end_lsn.is_none() && segment.cohort_id == cohort_id
                            })
                            .map(|segment| (stream.clone(), segment.clone()))
                    })
            else {
                break;
            };
            let (committed, certified_epoch) = self
                .certified_stream_tail(&stream, &candidates, &certified)
                .await?;
            if committed < open.start_lsn {
                // The range is a reservation that never held a durable
                // record: a writer published the route and then went away.
                // It cannot be sealed, so drop it. Every member of the cohort
                // must have answered for that judgement, not just a quorum:
                // a record present on one silent member is still not
                // committed, but the manifest must never discard a range
                // this coordinator could not fully observe.
                if answering < source_members.len() {
                    return Err(ReplicaError::NodeUnavailable);
                }
                let mut desired = expected.stream_segments.clone();
                if let Some(ranges) = desired.get_mut(&stream) {
                    ranges.pop();
                    if ranges.is_empty() {
                        desired.remove(&stream);
                    }
                }
                let cas_operation_id = manifest_operation_id(
                    &stream,
                    expected.revision,
                    open.start_lsn,
                    cohort_id,
                    open.writer_epoch,
                );
                let expected_digest = if expected.revision == 0 {
                    String::new()
                } else {
                    expected.digest.clone()
                };
                self.cas_manifest_quorum(
                    expected.revision,
                    &expected_digest,
                    desired,
                    &cas_operation_id,
                    Some(open.start_lsn.saturating_sub(1)),
                )
                .await?;
                continue;
            }
            let target_id = cohort_for_stream_excluding(&state, &stream, Some(cohort_id))?;
            let target = state
                .cohorts
                .get(&target_id)
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let member_hash = cohort_member_hash(&state, target_id).ok_or_else(|| {
                ReplicaError::NodeStorage("active cohort member hash is unavailable".to_owned())
            })?;
            let writer_epoch = open.writer_epoch.max(certified_epoch);
            let start_lsn = committed.saturating_add(1);
            let cas_operation_id = manifest_operation_id(
                &stream,
                expected.revision,
                start_lsn,
                target_id,
                writer_epoch,
            );
            let mut desired = expected.stream_segments.clone();
            let ranges = desired.entry(stream.clone()).or_default();
            if let Some(last) = ranges.last_mut() {
                last.end_lsn = Some(committed);
            }
            ranges.push(StreamSegment {
                start_lsn,
                end_lsn: None,
                cohort_id: target_id,
                member_ids: target.members.clone(),
                member_hash,
                writer_epoch,
                manifest_revision: expected.revision.saturating_add(1),
                placement_epoch: 0,
                operation_id: cas_operation_id.clone(),
                tier: target.tier.clone(),
                max_append_bytes: target.max_append_bytes,
            });
            let expected_digest = if expected.revision == 0 {
                String::new()
            } else {
                expected.digest.clone()
            };
            self.cas_manifest_quorum(
                expected.revision,
                &expected_digest,
                desired,
                &cas_operation_id,
                Some(committed),
            )
            .await?;
        }
        let snapshot = self.membership().await?;
        self.install_current_route_placements().await?;
        Ok(snapshot)
    }

    /// Sealed ranges the cohort owns whose records the archive does not yet
    /// cover. The archive head is the only authority for `archived_lsn`;
    /// draining a member trims its copies, so every range must be archived
    /// first, exactly as the trim watermark is bounded by the archived one.
    async fn cohort_unarchived_ranges(
        &self,
        cohort_id: u64,
    ) -> Result<Vec<UnarchivedRange>, ReplicaError> {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("cohort archival is available only in direct mode".to_owned())
        })?;
        let state = control.state()?;
        let mut unarchived = Vec::new();
        for (stream, segments) in &state.stream_segments {
            for segment in segments {
                if segment.cohort_id != cohort_id {
                    continue;
                }
                let Some(end_lsn) = segment.end_lsn else {
                    continue;
                };
                let archived_lsn = self
                    .archive
                    .archived_lsn(stream)
                    .await
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
                if archived_lsn < end_lsn {
                    unarchived.push(UnarchivedRange {
                        stream: stream.clone(),
                        start_lsn: segment.start_lsn,
                        end_lsn,
                        archived_lsn,
                    });
                }
            }
        }
        Ok(unarchived)
    }

    async fn require_cohort_archived(&self, cohort_id: u64) -> Result<(), ReplicaError> {
        let unarchived = self.cohort_unarchived_ranges(cohort_id).await?;
        if let Some(range) = unarchived.first() {
            return Err(ReplicaError::Protocol(format!(
                "cohort {cohort_id} range {} {}-{} is archived only through LSN {} ({} ranges pending); archive the cohort before draining it",
                range.stream,
                range.start_lsn,
                range.end_lsn,
                range.archived_lsn,
                unarchived.len()
            )));
        }
        Ok(())
    }

    fn member_cohort(&self, member_id: &str) -> Result<u64, ReplicaError> {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("member lookup is available only in direct mode".to_owned())
        })?;
        control
            .state()?
            .members
            .get(member_id)
            .map(|member| member.cohort_id)
            .ok_or_else(|| {
                ReplicaError::NodeStorage(format!("replica node {member_id} is not joined"))
            })
    }

    /// Archives every sealed range the cohort still owns under the caller's
    /// maintenance fence and reports what remains uncovered. Each stream's
    /// records are recovered from a quorum of the retained cohorts and
    /// published in order behind the archive head, so the step is idempotent
    /// and a retry after a fault resumes at the archived watermark. A range
    /// that cannot be fully recovered is reported, never trimmed.
    pub async fn archive_cohort_with_maintenance(
        &self,
        operation_id: &str,
        cohort_id: u64,
        maintenance_token: &str,
    ) -> Result<CohortArchiveReport, ReplicaError> {
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "archive operation_id must not be empty".to_owned(),
            ));
        }
        let _operation = self.maintenance_operation(maintenance_token).await?;
        let mut archived_records = 0_usize;
        for range in self.cohort_unarchived_ranges(cohort_id).await? {
            let (records, _) = self
                .recover_hot_tail(&range.stream, range.archived_lsn, None, None)
                .await?;
            let batch = records
                .into_iter()
                .filter(|record| record.lsn() <= range.end_lsn)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                continue;
            }
            self.archive
                .archive_committed(&batch)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            archived_records = archived_records.saturating_add(batch.len());
        }
        let unarchived = self.cohort_unarchived_ranges(cohort_id).await?;
        Ok(CohortArchiveReport {
            cohort_id,
            archived_records,
            complete: unarchived.is_empty(),
            unarchived,
        })
    }

    /// Drains one member while retaining the caller's existing maintenance
    /// fence. The operation is idempotent by `operation_id`.
    pub async fn drain_member_with_maintenance(
        &self,
        operation_id: &str,
        id: &str,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.drain_member_with_maintenance_and_archive_proof(
            operation_id,
            id,
            maintenance_token,
            None,
        )
        .await
    }

    /// Drains one member while retaining the caller's existing maintenance
    /// fence and presenting an archive proof for its cohort.
    pub async fn drain_member_with_maintenance_and_archive_proof(
        &self,
        operation_id: &str,
        id: &str,
        maintenance_token: &str,
        archive_proof: Option<&str>,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.require_cohort_archived(self.member_cohort(id)?)
            .await?;
        let id = id.to_owned();
        let archive_proof = archive_proof.map(ToOwned::to_owned);
        self.direct_membership_operation_with_maintenance(
            operation_id,
            maintenance_token,
            move |current| {
                let mut members = current
                    .members
                    .iter()
                    .cloned()
                    .map(|member| (member.id.clone(), member))
                    .collect::<BTreeMap<_, _>>();
                let cohort_id =
                    members
                        .get(&id)
                        .map(|member| member.cohort_id)
                        .ok_or_else(|| {
                            ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                        })?;
                require_cohort_archive_proof(current, cohort_id, archive_proof.as_deref())?;
                let member = members.get_mut(&id).ok_or_else(|| {
                    ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                })?;
                if member.status == MemberStatus::Active {
                    member.status = MemberStatus::Draining;
                } else if member.status != MemberStatus::Draining {
                    return Err(ReplicaError::LsnConflict);
                }
                if members
                    .values()
                    .filter(|member| member.status == MemberStatus::Active)
                    .count()
                    < 3
                {
                    return Err(ReplicaError::QuorumUnavailable);
                }
                Ok(members.into_values().collect())
            },
        )
        .await
    }

    /// Removes a drained member while retaining its tombstone in the durable
    /// control document. Reusing its id is intentionally forbidden.
    pub async fn remove_member(
        &self,
        operation_id: &str,
        id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.remove_member_with_archive_proof(operation_id, id, None)
            .await
    }

    /// Removes a drained member only after the member's cohort has an archive
    /// proof. The status check prevents a joining member from being silently
    /// tombstoned before it ever joined a complete cohort.
    pub async fn remove_member_with_archive_proof(
        &self,
        operation_id: &str,
        id: &str,
        archive_proof: Option<&str>,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let id = id.to_owned();
        let archive_proof = archive_proof.map(ToOwned::to_owned);
        self.direct_membership_operation(operation_id, move |current| {
            let mut members = current
                .members
                .iter()
                .cloned()
                .map(|member| (member.id.clone(), member))
                .collect::<BTreeMap<_, _>>();
            let cohort_id = members
                .get(&id)
                .map(|member| member.cohort_id)
                .ok_or_else(|| {
                    ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                })?;
            require_cohort_archive_proof(current, cohort_id, archive_proof.as_deref())?;
            let member = members.get_mut(&id).ok_or_else(|| {
                ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
            })?;
            if member.status != MemberStatus::Draining {
                return Err(ReplicaError::LsnConflict);
            }
            member.status = MemberStatus::Removed;
            if members
                .values()
                .filter(|member| member.status == MemberStatus::Active)
                .count()
                < 3
            {
                return Err(ReplicaError::QuorumUnavailable);
            }
            Ok(members.into_values().collect())
        })
        .await
    }

    /// Removes one drained member while retaining the caller's existing
    /// maintenance fence. Individual member removal is never used by normal
    /// cohort scale-out; this exists only for an explicit repair operation.
    pub async fn remove_member_with_maintenance(
        &self,
        operation_id: &str,
        id: &str,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.remove_member_with_maintenance_and_archive_proof(
            operation_id,
            id,
            maintenance_token,
            None,
        )
        .await
    }

    /// Removes a drained member while retaining the caller's maintenance
    /// fence and presenting an archive proof for its cohort.
    pub async fn remove_member_with_maintenance_and_archive_proof(
        &self,
        operation_id: &str,
        id: &str,
        maintenance_token: &str,
        archive_proof: Option<&str>,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.require_cohort_archived(self.member_cohort(id)?)
            .await?;
        let id = id.to_owned();
        let archive_proof = archive_proof.map(ToOwned::to_owned);
        self.direct_membership_operation_with_maintenance(
            operation_id,
            maintenance_token,
            move |current| {
                let mut members = current
                    .members
                    .iter()
                    .cloned()
                    .map(|member| (member.id.clone(), member))
                    .collect::<BTreeMap<_, _>>();
                let cohort_id =
                    members
                        .get(&id)
                        .map(|member| member.cohort_id)
                        .ok_or_else(|| {
                            ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                        })?;
                require_cohort_archive_proof(current, cohort_id, archive_proof.as_deref())?;
                let member = members.get_mut(&id).ok_or_else(|| {
                    ReplicaError::NodeStorage(format!("replica node {id} is not joined"))
                })?;
                if member.status != MemberStatus::Draining {
                    return Err(ReplicaError::LsnConflict);
                }
                member.status = MemberStatus::Removed;
                if members
                    .values()
                    .filter(|member| member.status == MemberStatus::Active)
                    .count()
                    < 3
                {
                    return Err(ReplicaError::QuorumUnavailable);
                }
                Ok(members.into_values().collect())
            },
        )
        .await
    }

    /// Short aliases for clients that use lifecycle verbs without the
    /// `_member` suffix.
    pub async fn join(
        &self,
        operation_id: &str,
        node: ReplicaNode,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.join_member(operation_id, node).await
    }

    pub async fn activate(
        &self,
        operation_id: &str,
        id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.activate_member(operation_id, id).await
    }

    pub async fn drain(
        &self,
        operation_id: &str,
        id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.drain_member(operation_id, id).await
    }

    pub async fn remove(
        &self,
        operation_id: &str,
        id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        self.remove_member(operation_id, id).await
    }

    fn control(&self) -> Option<Arc<DurableControl>> {
        match &self.membership {
            Membership::Direct(direct) => Some(Arc::clone(&direct.control)),
            Membership::Static(_) => self.control.clone(),
        }
    }

    /// Returns the immutable three-store control cohort. Data cohorts may be
    /// added indefinitely, but placement CASes always use this original set
    /// so a scale-out cannot change the quorum intersection.
    fn control_cohort_nodes(&self) -> Result<Vec<ReplicaNode>, ReplicaError> {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("placement manifest requires direct mode".to_owned())
        })?;
        let nodes = control.with_state(|state| {
            let authority_cohort_id = state.control_authority_cohort_id;
            let ids = state
                .cohorts
                .get(&authority_cohort_id)
                .map(|cohort| cohort.members.clone())
                .unwrap_or_else(|| {
                    state
                        .members
                        .keys()
                        .take(CONTROL_COHORT_SIZE)
                        .cloned()
                        .collect()
                });
            if ids.len() != CONTROL_COHORT_SIZE {
                return Vec::new();
            }
            ids.iter()
                .filter_map(|id| state.members.get(id))
                .filter(|member| member.status != MemberStatus::Removed)
                .map(DurableMember::node)
                .collect::<Vec<_>>()
        })?;
        if nodes.len() != CONTROL_COHORT_SIZE {
            return Err(ReplicaError::QuorumUnavailable);
        }
        Ok(nodes)
    }

    /// Reads a manifest only after a two-of-three control quorum agrees on
    /// the exact bytes. A same-revision disagreement is a protocol failure,
    /// never a value to resolve by timestamp or coordinator identity.
    async fn read_manifest_quorum(&self) -> Result<Option<ReplicaManifest>, ReplicaError> {
        Ok(self
            .read_manifest_quorum_agreement()
            .await?
            .map(|quorum| quorum.manifest))
    }

    /// Reads the manifest from the fixed control cohort and returns as soon
    /// as two members answer with the same document. Two identical durable
    /// copies are the quorum certificate and a third response cannot change
    /// the value, so a slow or suspended member no longer gates every read.
    /// Members whose answer had already arrived when the quorum formed are
    /// recorded in `agreed` so the route repair can skip them.
    async fn read_manifest_quorum_agreement(&self) -> Result<Option<QuorumManifest>, ReplicaError> {
        let Some(control) = self.control() else {
            return Ok(None);
        };

        // Once the immutable control head exists, object storage is the sole
        // metadata authority. The old control cohort is only a migration
        // source and may already be retired, so every route/recovery/status
        // read must follow the same head instead of falling back to a fixed
        // cohort quorum.
        if let Some(head_store) = self.control_head.as_ref()
            && let Some(snapshot) = head_store
                .load()
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        {
            control.adopt_authoritative_head(&snapshot.head)?;
            return Ok(Some(QuorumManifest {
                manifest: snapshot.head.metadata.manifest,
                // The object CAS is the certificate. There are no legacy
                // node ids to treat as already repaired.
                agreed: BTreeSet::new(),
            }));
        }
        if self.control_head.is_some() && control.state()?.control_head_revision != 0 {
            return Err(ReplicaError::NodeStorage(
                "authoritative control head is missing after migration".to_owned(),
            ));
        }

        // A coordinator-local cache may be stale or tampered. It is useful
        // only for the explicitly legacy all-404 fallback; modern admission
        // counts responses from the fixed control cohort itself.
        let local = control.manifest().ok();
        let peers = self.control_cohort_nodes()?;
        // `NodeClient::manifest` already validated each document; grouping by
        // value equality avoids a clone, serialization, and hash per answer.
        fn agreed_manifests(
            answers: &[Option<Result<Option<ReplicaManifest>, ReplicaError>>],
        ) -> Vec<&ReplicaManifest> {
            answers
                .iter()
                .flatten()
                .filter_map(|result| result.as_ref().ok()?.as_ref())
                .collect()
        }
        let answers = fan_out(
            &peers,
            |node| {
                let client = self.client.clone();
                let internal_token = self.internal_token.clone();
                async move {
                    NodeClient::new(node, &client, &internal_token)
                        .manifest()
                        .await
                }
            },
            |answers| {
                let manifests = agreed_manifests(answers);
                manifests.iter().any(|manifest| {
                    manifests
                        .iter()
                        .filter(|candidate| *candidate == manifest)
                        .count()
                        >= ACK_QUORUM
                })
            },
            None,
        )
        .await;
        let mut groups = Vec::<(ReplicaManifest, BTreeSet<String>)>::new();
        let mut supported = 0_usize;
        let mut unavailable = 0_usize;
        for (node, answer) in peers.iter().zip(answers) {
            match answer {
                Some(Ok(Some(manifest))) => {
                    supported += 1;
                    let index = match groups
                        .iter()
                        .position(|(candidate, _)| candidate == &manifest)
                    {
                        Some(index) => index,
                        None => {
                            groups.push((manifest, BTreeSet::new()));
                            groups.len() - 1
                        }
                    };
                    groups[index].1.insert(node.id.clone());
                }
                Some(Ok(None)) => {}
                Some(Err(_)) | None => unavailable += 1,
            }
        }
        if let Some(index) = groups
            .iter()
            .position(|(_, agreed)| agreed.len() >= ACK_QUORUM)
        {
            let (manifest, agreed) = groups.swap_remove(index);
            return Ok(Some(QuorumManifest { manifest, agreed }));
        }
        if supported == 0 && unavailable == 0 {
            // Isolated compatibility fixtures expose no control endpoint on
            // their storage nodes. The local direct document remains the
            // authority in that explicitly legacy configuration.
            return local
                .map(|manifest| QuorumManifest {
                    manifest,
                    agreed: BTreeSet::new(),
                })
                .ok_or(ReplicaError::QuorumUnavailable)
                .map(Some);
        }
        Err(ReplicaError::QuorumUnavailable)
    }

    /// Refreshes a stateless coordinator's local cache from the fixed control
    /// quorum.  The cache is never counted as quorum evidence, but candidate
    /// filtering still needs the immutable route map when a coordinator was
    /// restarted with a fresh local volume.  Without this adoption, a new
    /// coordinator would see the nodes yet reject every historical record as
    /// having no locally-known cohort.
    async fn sync_manifest_cache(&self) -> Result<Option<QuorumManifest>, ReplicaError> {
        let Some(control) = self.control() else {
            return Ok(None);
        };
        let Some(quorum) = self.read_manifest_quorum_agreement().await? else {
            return Ok(None);
        };
        let manifest = &quorum.manifest;
        let local = control.manifest().ok();
        if local.as_ref().is_none_or(|local| {
            local.revision != manifest.revision || local.digest != manifest.digest
        }) {
            control.adopt_manifest(manifest, true)?;
        }
        Ok(Some(quorum))
    }

    /// CASes one complete manifest to the fixed control cohort. All requests
    /// start together; the call returns as soon as two durable CAS responses
    /// agree, while the third request remains detached and can finish after
    /// the caller has resumed normal data traffic.
    async fn cas_manifest_quorum(
        &self,
        expected_revision: u64,
        expected_digest: &str,
        stream_segments: BTreeMap<String, Vec<StreamSegment>>,
        operation_id: &str,
        cutover_lsn: Option<u64>,
    ) -> Result<ReplicaManifest, ReplicaError> {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("placement manifest requires direct mode".to_owned())
        })?;

        // After migration the mutable manifest lives inside the object-store
        // control head. Build the complete next metadata value and commit it
        // with one ETag CAS; the retired control cohort is never consulted or
        // asked to acknowledge a write.
        if let Some(head_store) = self.control_head.as_ref()
            && let Some(current) = head_store
                .load()
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        {
            control.adopt_authoritative_head(&current.head)?;
            if current.head.completed_operations.contains_key(operation_id) {
                return Ok(current.head.metadata.manifest);
            }
            let current_manifest = &current.head.metadata.manifest;
            if current_manifest.revision != expected_revision
                || (!expected_digest.is_empty() && current_manifest.digest != expected_digest)
            {
                return Err(ReplicaError::LsnConflict);
            }
            let next_revision = expected_revision
                .checked_add(1)
                .ok_or_else(|| ReplicaError::Protocol("manifest revision exhausted".to_owned()))?;
            let digest = manifest_digest(&stream_segments);
            let state = control.state()?;
            validate_manifest_segments(&state, &stream_segments, next_revision)?;
            validate_manifest_transition(&state.stream_segments, &stream_segments)?;
            validate_manifest_cutover(&state.stream_segments, &stream_segments, cutover_lsn)?;
            let (writer_epoch, cohort_id, member_set_hash) =
                manifest_metadata_for_state(&state, &stream_segments);
            let (tier, max_append_bytes) =
                manifest_policy_for_state(&state, &stream_segments, cohort_id);
            let manifest = ReplicaManifest {
                version: MANIFEST_VERSION,
                revision: next_revision,
                digest,
                writer_epoch,
                cohort_id,
                member_set_hash,
                write_cohort_id: state.write_cohort_id,
                stream_segments,
                operation_id: operation_id.to_owned(),
                tier,
                max_append_bytes,
            };
            manifest.validate()?;
            let mut metadata = current.head.metadata.clone();
            metadata.manifest = manifest.clone();
            let next = ControlHead::new(
                current.head.authority_epoch,
                current.head.revision.checked_add(1).ok_or_else(|| {
                    ReplicaError::Protocol("control-head revision exhausted".to_owned())
                })?,
                operation_id.to_owned(),
                current.head.source_marker.clone(),
                metadata,
            )
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            let snapshot = head_store
                .update(&current, next)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            control.adopt_authoritative_head(&snapshot.head)?;
            return Ok(snapshot.head.metadata.manifest);
        }
        if self.control_head.is_some() && control.state()?.control_head_revision != 0 {
            return Err(ReplicaError::NodeStorage(
                "authoritative control head is missing after migration".to_owned(),
            ));
        }

        let request = ManifestCasRequest {
            expected_revision,
            expected_digest: expected_digest.to_owned(),
            stream_segments,
            operation_id: operation_id.to_owned(),
            repair: false,
            cutover_lsn,
        };
        // The coordinator's local cache is not itself a quorum member. Send
        // the exact CAS to all three fixed control stores and count only
        // durable node responses; a stale cache must not prevent two stores
        // from committing a value it has not observed yet.
        let peers = self.control_cohort_nodes()?;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        for node in peers.clone() {
            let sender = sender.clone();
            let request = request.clone();
            let client = self.client.clone();
            let internal_token = self.internal_token.clone();
            tokio::spawn(async move {
                let result = NodeClient::new(node, &client, &internal_token)
                    .cas_manifest(&request)
                    .await;
                let _ = sender.send(result);
            });
        }
        drop(sender);
        let mut acknowledgements = 0_usize;
        let mut unsupported = 0_usize;
        let mut failures = 0_usize;
        let mut committed: Option<ReplicaManifest> = None;
        while let Some(result) = receiver.recv().await {
            match result {
                Ok(Some(manifest)) => {
                    manifest.validate()?;
                    if manifest.revision != expected_revision.saturating_add(1)
                        || manifest.digest != manifest_digest(&request.stream_segments)
                    {
                        return Err(ReplicaError::LsnConflict);
                    }
                    if let Some(previous) = &committed
                        && previous != &manifest
                    {
                        return Err(ReplicaError::LsnConflict);
                    }
                    committed = Some(manifest);
                    acknowledgements += 1;
                }
                Ok(None) => unsupported += 1,
                Err(_) => failures += 1,
            }
            if acknowledgements >= ACK_QUORUM {
                let manifest = committed.ok_or(ReplicaError::QuorumUnavailable)?;
                // Reconciliation is intentionally detached. The first two
                // durable responses are the commit certificate; a lagging or
                // divergent third store receives an adoption request and can
                // heal after the client has resumed normal traffic.
                let repair_request = ManifestCasRequest {
                    expected_revision: manifest.revision,
                    expected_digest: manifest.digest.clone(),
                    stream_segments: manifest.stream_segments.clone(),
                    operation_id: manifest.operation_id.clone(),
                    repair: true,
                    cutover_lsn: None,
                };
                for node in peers {
                    let request = repair_request.clone();
                    let client = self.client.clone();
                    let internal_token = self.internal_token.clone();
                    tokio::spawn(async move {
                        let _ = NodeClient::new(node, &client, &internal_token)
                            .cas_manifest(&request)
                            .await;
                    });
                }
                // Persist the quorum value only as a local cache after the
                // quorum has been proven. A stateless coordinator can restart
                // and recover this exact manifest without becoming a fourth
                // authority.
                control.adopt_manifest(&manifest, true)?;
                return Ok(manifest);
            }
            // Saturating on purpose: the cohort is exactly CONTROL_COHORT_SIZE
            // members, enforced where the peer list is built, so the
            // subtraction cannot go below zero today. It is 300 lines from the
            // invariant that guarantees it, and an underflow here would panic
            // a gateway rather than merely skip an early break.
            let outstanding = CONTROL_COHORT_SIZE
                .saturating_sub(unsupported)
                .saturating_sub(failures);
            if acknowledgements + outstanding < ACK_QUORUM {
                // Drain all responses when every observed peer is a legacy
                // node without a control endpoint. Only after the final
                // response can the compatibility path distinguish that case
                // from a modern quorum outage.
                if unsupported == 0 {
                    break;
                }
            }
        }
        if unsupported == CONTROL_COHORT_SIZE {
            // Isolated legacy fixtures have no control listener. Keep the
            // local compatibility document usable, but never count it as a
            // modern quorum acknowledgement.
            let local = control.manifest()?;
            let local_manifest = control.cas_manifest(
                local.revision,
                if local.revision == 0 {
                    ""
                } else {
                    local.digest.as_str()
                },
                request.stream_segments,
                operation_id,
                false,
            )?;
            return Ok(local_manifest);
        }
        Err(ReplicaError::QuorumUnavailable)
    }

    #[allow(clippy::too_many_arguments)]
    async fn cas_authoritative_membership(
        &self,
        expected_epoch: u64,
        members: Vec<DurableMember>,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        manifest_revision: Option<u64>,
        manifest_digest_value: Option<String>,
        write_cohort_id: Option<u64>,
        online: bool,
        replacement: bool,
        operation_id: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol("membership CAS is available only in direct mode".to_owned())
        })?;
        let head_store = self.control_head_store()?;
        let current = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            .ok_or_else(|| {
                ReplicaError::Protocol(
                    "authoritative membership head is not initialized".to_owned(),
                )
            })?;
        control.adopt_authoritative_head(&current.head)?;
        let current_state = control.state()?;
        if current.head.completed_operations.contains_key(operation_id) {
            return Ok(membership_snapshot_from_head(
                &current.head,
                current_state.metadata_freeze,
            ));
        }
        if current_state.membership_epoch != expected_epoch {
            return Err(ReplicaError::LsnConflict);
        }
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "membership operation_id must not be empty".to_owned(),
            ));
        }

        let mut next_members = BTreeMap::new();
        for member in members {
            let node = member.node();
            validate_node(&node)?;
            if next_members.insert(member.id.clone(), member).is_some() {
                return Err(ReplicaError::LsnConflict);
            }
        }
        if cohorts.is_none() {
            normalize_members_for_cas(&mut next_members, &current_state)?;
        }
        validate_members_state(&next_members)?;
        for (id, member) in &next_members {
            if let Some(previous) = current_state.members.get(id) {
                if previous.status == MemberStatus::Removed
                    && member.status != MemberStatus::Removed
                {
                    return Err(ReplicaError::LsnConflict);
                }
                if previous.cohort_id != member.cohort_id {
                    return Err(ReplicaError::LsnConflict);
                }
            }
        }
        let adjacent_epoch = expected_epoch
            .checked_add(1)
            .ok_or_else(|| ReplicaError::Protocol("membership epoch exhausted".to_owned()))?;
        let next_epoch = adjacent_epoch;

        let mut next = current_state.clone();
        next.membership_epoch = next_epoch;
        next.members = next_members;
        if let Some(cohorts) = cohorts {
            next.cohorts = cohorts
                .into_iter()
                .map(|cohort| (cohort.id, cohort))
                .collect();
        }
        if let Some(write_cohort_id) = write_cohort_id {
            if write_cohort_id == 0 {
                return Err(ReplicaError::LsnConflict);
            }
            next.write_cohort_id = write_cohort_id;
        }

        let current_manifest = &current.head.metadata.manifest;
        let current_digest = current_manifest.digest.clone();
        let (next_revision, next_segments) = match stream_segments {
            Some(incoming_segments) => {
                let incoming_revision = manifest_revision.unwrap_or(current_manifest.revision);
                let incoming_digest = manifest_digest_value
                    .filter(|digest| !digest.is_empty())
                    .unwrap_or_else(|| manifest_digest(&incoming_segments));
                if incoming_revision == current_manifest.revision {
                    if incoming_digest != current_digest
                        || incoming_segments != current_manifest.stream_segments
                    {
                        return Err(ReplicaError::LsnConflict);
                    }
                    (
                        current_manifest.revision,
                        current_manifest.stream_segments.clone(),
                    )
                } else if incoming_revision > current_manifest.revision {
                    if incoming_digest != manifest_digest(&incoming_segments) {
                        return Err(ReplicaError::LsnConflict);
                    }
                    validate_manifest_segments(&next, &incoming_segments, incoming_revision)?;
                    validate_manifest_transition(
                        &current_state.stream_segments,
                        &incoming_segments,
                    )?;
                    (incoming_revision, incoming_segments)
                } else {
                    return Err(ReplicaError::LsnConflict);
                }
            }
            None => (
                current_manifest.revision.checked_add(1).ok_or_else(|| {
                    ReplicaError::Protocol("manifest revision exhausted".to_owned())
                })?,
                current_manifest.stream_segments.clone(),
            ),
        };
        next.stream_segments = next_segments.clone();
        if online {
            if replacement {
                validate_online_replacement_transition(&current_state, &next)?;
            } else {
                validate_online_membership_transition(&current_state, &next)?;
            }
        }
        next.manifest_revision = next_revision;
        next.manifest_digest = manifest_digest(&next_segments);
        reconcile_cohorts(&mut next)?;
        let (writer_epoch, cohort_id, member_set_hash) =
            manifest_metadata_for_state(&next, &next_segments);
        let (tier, max_append_bytes) = manifest_policy_for_state(&next, &next_segments, cohort_id);
        let next_manifest = ReplicaManifest {
            version: MANIFEST_VERSION,
            revision: next_revision,
            digest: next.manifest_digest.clone(),
            writer_epoch,
            cohort_id,
            member_set_hash,
            write_cohort_id: next.write_cohort_id,
            stream_segments: next_segments,
            operation_id: operation_id.to_owned(),
            tier,
            max_append_bytes,
        };
        next.manifest_operation_id = operation_id.to_owned();
        next.manifest_writer_epoch = writer_epoch;
        next.manifest_cohort_id = cohort_id;
        next.manifest_member_hash = next_manifest.member_set_hash.clone();
        next.manifest_operations.insert(
            operation_id.to_owned(),
            serde_json::to_string(&next_manifest)
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?,
        );
        validate_control_state(&next)?;
        let metadata = ControlMetadata {
            membership_epoch: next.membership_epoch,
            members: next.members.clone(),
            cohorts: next.cohorts.clone(),
            manifest: next_manifest,
            pending_handoffs: current.head.metadata.pending_handoffs.clone(),
        };
        let next_head = ControlHead::new(
            current.head.authority_epoch,
            current.head.revision.checked_add(1).ok_or_else(|| {
                ReplicaError::Protocol("control-head revision exhausted".to_owned())
            })?,
            operation_id.to_owned(),
            current.head.source_marker.clone(),
            metadata,
        )
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let snapshot = head_store
            .update(&current, next_head)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        control.adopt_authoritative_head(&snapshot.head)?;
        let freeze = control.metadata_freeze().ok().flatten();
        Ok(membership_snapshot_from_head(&snapshot.head, freeze))
    }

    async fn direct_membership_operation<F>(
        &self,
        operation_id: &str,
        mutate: F,
    ) -> Result<MembershipSnapshot, ReplicaError>
    where
        F: FnOnce(&MembershipSnapshot) -> Result<Vec<DurableMember>, ReplicaError>,
    {
        self.direct_membership_document_operation(operation_id, mutate, None, None)
            .await
    }

    async fn direct_membership_operation_online<F>(
        &self,
        operation_id: &str,
        mutate: F,
    ) -> Result<MembershipSnapshot, ReplicaError>
    where
        F: FnOnce(&MembershipSnapshot) -> Result<Vec<DurableMember>, ReplicaError>,
    {
        self.direct_membership_document_operation_online(operation_id, mutate, None, None, None)
            .await
    }

    async fn direct_membership_document_operation_online<F>(
        &self,
        operation_id: &str,
        mutate: F,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
        write_cohort_id: Option<u64>,
    ) -> Result<MembershipSnapshot, ReplicaError>
    where
        F: FnOnce(&MembershipSnapshot) -> Result<Vec<DurableMember>, ReplicaError>,
    {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol(
                "online membership operation is available only in direct mode".to_owned(),
            )
        })?;

        if let Some(head) = self.load_authoritative_head().await? {
            if head.head.completed_operations.contains_key(operation_id) {
                control.adopt_authoritative_head(&head.head)?;
                let snapshot = self.membership().await?;
                self.propagate_membership_online(&snapshot, operation_id, false)
                    .await?;
                return Ok(snapshot);
            }
            let current = self.membership().await?;
            let members = mutate(&current)?;
            let candidate_nodes = members
                .iter()
                .filter(|member| member.status != MemberStatus::Removed)
                .map(DurableMember::node)
                .collect::<Vec<_>>();
            let readiness = join_all(candidate_nodes.iter().cloned().map(|node| {
                let client = self.client.clone();
                let internal_token = self.internal_token.clone();
                async move {
                    NodeClient::new(node, &client, &internal_token)
                        .health()
                        .await
                }
            }))
            .await;
            if readiness.iter().any(Result::is_err) {
                return Err(ReplicaError::NodeUnavailable);
            }
            let snapshot = self
                .cas_authoritative_membership(
                    current.membership_epoch,
                    members,
                    cohorts,
                    stream_segments,
                    Some(current.manifest_revision),
                    Some(current.manifest_digest.clone()),
                    write_cohort_id,
                    true,
                    false,
                    operation_id,
                )
                .await?;
            self.propagate_membership_online(&snapshot, operation_id, false)
                .await?;
            return Ok(snapshot);
        }
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "membership operation_id must not be empty".to_owned(),
            ));
        }
        if let Some(snapshot) = control.operation_snapshot(operation_id)? {
            self.propagate_membership_online(&snapshot, operation_id, false)
                .await?;
            return Ok(snapshot);
        }
        // Build the candidate against the quorum manifest before making any
        // durable local change. Appends admitted before this lock continue;
        // new appends wait briefly for the document CAS and are then routed
        // from the same target.
        self.sync_manifest_cache().await?;
        let current_state = control.state()?;
        let current = MembershipSnapshot::from_state(&current_state);
        let members = mutate(&current)?;
        if members
            .iter()
            .filter(|member| member.status == MemberStatus::Active)
            .count()
            < 3
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        // A failed prepare must leave the current authority untouched. Probe
        // every candidate volume before the local CAS; joining volumes are
        // not yet write targets but must be able to persist the exact control
        // snapshot before activation succeeds.
        let candidate_nodes = members
            .iter()
            .filter(|member| member.status != MemberStatus::Removed)
            .map(DurableMember::node)
            .collect::<Vec<_>>();
        let readiness = join_all(candidate_nodes.iter().cloned().map(|node| {
            let client = self.client.clone();
            let internal_token = self.internal_token.clone();
            async move {
                NodeClient::new(node, &client, &internal_token)
                    .health()
                    .await
            }
        }))
        .await;
        if readiness.iter().any(Result::is_err) {
            return Err(ReplicaError::NodeUnavailable);
        }
        let snapshot = control.cas_membership_document_online(
            current.membership_epoch,
            current.membership_epoch.checked_add(1),
            false,
            members,
            cohorts,
            stream_segments,
            Some(current.manifest_revision),
            Some(current.manifest_digest.clone()),
            write_cohort_id,
            operation_id,
        )?;
        if write_cohort_id.is_some() {
            let next_state = control.state()?;
            self.install_transition_placement_fences(&current_state, &next_state)
                .await?;
        }
        self.propagate_membership_online(&snapshot, operation_id, false)
            .await?;
        Ok(snapshot)
    }

    /// Fences only streams whose rendezvous winner changes after an additive
    /// cohort becomes active. The operation is per stream: unrelated streams
    /// keep their old placement and no global admission lock is held while
    /// the node-side durable fence waits for an in-flight append to drain.
    async fn install_transition_placement_fences(
        &self,
        previous: &DurableControlState,
        next: &DurableControlState,
    ) -> Result<(), ReplicaError> {
        let mut requests = Vec::new();
        let writer_state = self.writer_state.lock().await;
        for (stream, ranges) in &previous.stream_segments {
            let Some(last) = ranges.last() else { continue };
            if last.end_lsn.is_some() {
                continue;
            }
            let target_id = cohort_for_stream(next, stream)?;
            if target_id == last.cohort_id {
                continue;
            }
            let Some(target) = next.cohorts.get(&target_id) else {
                return Err(ReplicaError::QuorumUnavailable);
            };
            let tail = writer_state
                .streams
                .get(stream)
                .map_or(last.start_lsn.saturating_sub(1), |state| {
                    state.committed_lsn
                });
            if tail < last.start_lsn.saturating_sub(1) {
                continue;
            }
            let successor = StreamSegment {
                start_lsn: tail.saturating_add(1),
                end_lsn: None,
                cohort_id: target_id,
                member_ids: target.members.clone(),
                member_hash: cohort_member_hash(next, target_id)
                    .ok_or(ReplicaError::LsnConflict)?,
                writer_epoch: last.writer_epoch,
                manifest_revision: next.manifest_revision.saturating_add(1),
                placement_epoch: 0,
                operation_id: String::from("placement-pending"),
                tier: target.tier.clone(),
                max_append_bytes: target.max_append_bytes,
            };
            requests.push((
                stream.clone(),
                placement_for_route(stream, &successor),
                last.member_ids.clone(),
                target.members.clone(),
            ));
        }
        drop(writer_state);
        for (stream, placement, old_ids, target_ids) in requests {
            let mut nodes = Vec::new();
            for id in old_ids.into_iter().chain(target_ids) {
                if nodes.iter().any(|node: &ReplicaNode| node.id == id) {
                    continue;
                }
                if let Some(member) = next.members.get(&id) {
                    nodes.push(member.node());
                } else if let Some(member) = previous.members.get(&id) {
                    nodes.push(member.node());
                }
            }
            let results = join_all(nodes.into_iter().map(|node| {
                let client = self.client.clone();
                let token = self.internal_token.clone();
                let stream = stream.clone();
                let placement = placement.clone();
                async move {
                    NodeClient::new(node, &client, &token)
                        .install_placement_fence(&stream, &placement)
                        .await
                }
            }))
            .await;
            if results.iter().any(Result::is_err) {
                return Err(ReplicaError::NodeUnavailable);
            }
        }
        Ok(())
    }

    async fn direct_membership_operation_with_maintenance<F>(
        &self,
        operation_id: &str,
        maintenance_token: &str,
        mutate: F,
    ) -> Result<MembershipSnapshot, ReplicaError>
    where
        F: FnOnce(&MembershipSnapshot) -> Result<Vec<DurableMember>, ReplicaError>,
    {
        self.direct_membership_document_operation_with_maintenance(
            operation_id,
            maintenance_token,
            mutate,
            None,
            None,
        )
        .await
    }

    async fn direct_membership_document_operation_with_maintenance<F>(
        &self,
        operation_id: &str,
        maintenance_token: &str,
        mutate: F,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
    ) -> Result<MembershipSnapshot, ReplicaError>
    where
        F: FnOnce(&MembershipSnapshot) -> Result<Vec<DurableMember>, ReplicaError>,
    {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol(
                "membership operation is available only in direct mode".to_owned(),
            )
        })?;

        if let Some(head) = self.load_authoritative_head().await? {
            if head.head.completed_operations.contains_key(operation_id) {
                control.adopt_authoritative_head(&head.head)?;
                let snapshot = self.membership().await?;
                self.propagate_membership(&snapshot, maintenance_token, operation_id)
                    .await?;
                return Ok(snapshot);
            }
            let current = self.membership().await?;
            let members = mutate(&current)?;
            let snapshot = self
                .cas_authoritative_membership(
                    current.membership_epoch,
                    members,
                    cohorts,
                    stream_segments,
                    Some(current.manifest_revision),
                    Some(current.manifest_digest.clone()),
                    None,
                    false,
                    false,
                    operation_id,
                )
                .await?;
            self.propagate_membership(&snapshot, maintenance_token, operation_id)
                .await?;
            return Ok(snapshot);
        }
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "membership operation_id must not be empty".to_owned(),
            ));
        }
        let _operation = self.maintenance_operation(maintenance_token).await?;
        if let Some(snapshot) = control.operation_snapshot(operation_id)? {
            self.propagate_membership(&snapshot, maintenance_token, operation_id)
                .await?;
            return Ok(snapshot);
        }
        // Every membership document carries the route map, so it must be
        // built on the quorum manifest rather than this coordinator's cache.
        self.sync_manifest_cache().await?;
        let current = control.membership()?;
        let members = mutate(&current)?;
        if members
            .iter()
            .filter(|member| member.status == MemberStatus::Active)
            .count()
            < 3
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let snapshot = control.cas_membership_document(
            current.membership_epoch,
            members,
            cohorts,
            stream_segments,
            operation_id,
        )?;
        self.propagate_membership(&snapshot, maintenance_token, operation_id)
            .await?;
        Ok(snapshot)
    }

    async fn direct_membership_document_operation<F>(
        &self,
        operation_id: &str,
        mutate: F,
        cohorts: Option<Vec<DurableCohort>>,
        stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
    ) -> Result<MembershipSnapshot, ReplicaError>
    where
        F: FnOnce(&MembershipSnapshot) -> Result<Vec<DurableMember>, ReplicaError>,
    {
        let control = self.control().ok_or_else(|| {
            ReplicaError::Protocol(
                "membership operation is available only in direct mode".to_owned(),
            )
        })?;

        if let Some(head) = self.load_authoritative_head().await? {
            if head.head.completed_operations.contains_key(operation_id) {
                control.adopt_authoritative_head(&head.head)?;
                let snapshot = self.membership().await?;
                self.propagate_membership(&snapshot, "", operation_id)
                    .await?;
                return Ok(snapshot);
            }
            let current = self.membership().await?;
            let members = mutate(&current)?;
            let snapshot = self
                .cas_authoritative_membership(
                    current.membership_epoch,
                    members,
                    cohorts,
                    stream_segments,
                    Some(current.manifest_revision),
                    Some(current.manifest_digest.clone()),
                    None,
                    false,
                    false,
                    operation_id,
                )
                .await?;
            self.propagate_membership(&snapshot, "", operation_id)
                .await?;
            return Ok(snapshot);
        }
        if operation_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "membership operation_id must not be empty".to_owned(),
            ));
        }
        if let Some(snapshot) = control.operation_snapshot(operation_id)? {
            // A retry still performs a full durable fence/release around the
            // propagation pass below; this makes a crash during a previous
            // partial broadcast recoverable without incrementing the epoch.
            let token = self.begin_maintenance_for(operation_id).await?;
            let propagation = self
                .propagate_membership(&snapshot, &token, operation_id)
                .await;
            let release = self.end_maintenance(&token).await;
            propagation?;
            release?;
            return Ok(snapshot);
        }
        let token = self.begin_maintenance_for(operation_id).await?;
        // Build the document on the quorum manifest, not this coordinator's
        // cache, so the route map it carries cannot regress.
        if let Err(error) = self.sync_manifest_cache().await {
            let _ = self.end_maintenance(&token).await;
            return Err(error);
        }
        let current = control.membership()?;
        let members = match mutate(&current) {
            Ok(members) => members,
            Err(error) => {
                let _ = self.end_maintenance(&token).await;
                return Err(error);
            }
        };
        let active = members
            .iter()
            .filter(|member| member.status == MemberStatus::Active)
            .count();
        if active < 3 {
            let _ = self.end_maintenance(&token).await;
            return Err(ReplicaError::QuorumUnavailable);
        }
        let snapshot = match control.cas_membership_document(
            current.membership_epoch,
            members,
            cohorts,
            stream_segments,
            operation_id,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let _ = self.end_maintenance(&token).await;
                return Err(error);
            }
        };
        // Do not release a partially propagated operation. Keeping the
        // all-member fence active makes retrying the same idempotency key
        // safe: members that already adopted the operation accept the exact
        // same fence marker, while the failed member can catch up after a
        // restart. A successful propagation is the only point at which write
        // admission may reopen.
        self.propagate_membership(&snapshot, &token, operation_id)
            .await?;
        self.end_maintenance(&token).await?;
        Ok(snapshot)
    }

    async fn propagate_authoritative_head(&self) -> Result<(), ReplicaError> {
        let head = self
            .load_authoritative_head()
            .await?
            .ok_or(ReplicaError::QuorumUnavailable)?;
        let mut nodes = self.all_members_snapshot().await?;
        nodes.retain(|node| {
            head.head
                .metadata
                .members
                .get(&node.id)
                .is_some_and(|member| member.status != MemberStatus::Removed)
        });
        for member in head
            .head
            .metadata
            .members
            .values()
            .filter(|member| member.status != MemberStatus::Removed)
        {
            let node = member.node();
            if !nodes.iter().any(|candidate| candidate.id == node.id) {
                nodes.push(node);
            }
        }
        let results = join_all(nodes.into_iter().map(|node| {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            let head = head.head.clone();
            async move {
                NodeClient::new(node, &client, &token)
                    .adopt_control_head(&head)
                    .await
            }
        }))
        .await;
        if results.iter().any(Result::is_err) {
            return Err(ReplicaError::NodeUnavailable);
        }
        Ok(())
    }

    async fn propagate_membership(
        &self,
        snapshot: &MembershipSnapshot,
        token: &str,
        operation_id: &str,
    ) -> Result<(), ReplicaError> {
        if self.load_authoritative_head().await?.is_some() {
            return self.propagate_authoritative_head().await;
        }
        let mut nodes = self.all_members_snapshot().await?;
        for node in snapshot.all_nodes() {
            if !nodes.iter().any(|candidate| candidate.id == node.id) {
                nodes.push(node);
            }
        }
        // New members are fenced before their control state is used. Existing
        // members are already fenced by begin_maintenance; duplicate fence
        // calls are idempotent.
        let fenced = join_all(nodes.iter().cloned().map(|node| {
            let token = token.to_owned();
            async move {
                let client = NodeClient::new(node, &self.client, &self.internal_token);
                client.fence(&token).await
            }
        }))
        .await;
        if fenced.iter().any(Result::is_err) {
            return Err(ReplicaError::NodeUnavailable);
        }
        let request = MembershipCasRequest {
            expected_epoch: snapshot.membership_epoch.saturating_sub(1),
            target_epoch: Some(snapshot.membership_epoch),
            repair: true,
            members: snapshot.members.clone(),
            cohorts: Some(snapshot.cohorts.clone()),
            stream_segments: Some(snapshot.stream_segments.clone()),
            manifest_revision: Some(snapshot.manifest_revision),
            manifest_digest: Some(snapshot.manifest_digest.clone()),
            write_cohort_id: (snapshot.write_cohort_id != 0).then_some(snapshot.write_cohort_id),
            control_authority_epoch: snapshot.control_authority_epoch,
            control_head_revision: snapshot.control_head_revision,
            control_head_digest: snapshot.control_head_digest.clone(),
            control_authority_cohort_id: snapshot.control_authority_cohort_id,
            replacement: false,
            operation_id: operation_id.to_owned(),
        };
        // A local node can already contain this exact idempotency result. All
        // other members must persist the same epoch before the fence opens.
        let results = join_all(nodes.iter().cloned().map(|node| {
            let mut request = request.clone();
            async move {
                let client = NodeClient::new(node.clone(), &self.client, &self.internal_token);
                // A cohort is provisioned in parallel. A member that has not
                // yet joined the authoritative document will therefore have
                // an older epoch than the snapshot being propagated. Read its
                // fenced control state and CAS from that exact epoch; never
                // relax the check for a member that is already ahead.
                if let Some(state) = client.control_state().await? {
                    // The coordinator's local control store already contains
                    // this operation before propagation reaches the local
                    // storage URL. Let the storage CAS return its persisted
                    // idempotency result when it is ahead; a genuinely
                    // conflicting operation still fails its expected epoch
                    // check. Only lower the expectation for a newly joined
                    // peer whose bootstrap epoch trails the snapshot.
                    if state.membership_epoch < request.expected_epoch {
                        request.expected_epoch = state.membership_epoch;
                    }
                }
                client.cas_membership(&request, token).await
            }
        }))
        .await;
        if results.iter().any(Result::is_err) {
            return Err(ReplicaError::NodeUnavailable);
        }
        Ok(())
    }

    /// Propagates an additive/activation snapshot without a maintenance
    /// marker. Every member must durably adopt it before the caller exposes
    /// the new target; a failed propagation leaves the previous write target
    /// usable and returns an unavailable result to the operator.
    async fn propagate_membership_online(
        &self,
        snapshot: &MembershipSnapshot,
        operation_id: &str,
        replacement: bool,
    ) -> Result<(), ReplicaError> {
        if self.load_authoritative_head().await?.is_some() {
            return self.propagate_authoritative_head().await;
        }
        let mut nodes = self.all_members_snapshot().await?;
        for node in snapshot.all_nodes() {
            if !nodes.iter().any(|candidate| candidate.id == node.id) {
                nodes.push(node);
            }
        }
        let request = MembershipCasRequest {
            expected_epoch: snapshot.membership_epoch.saturating_sub(1),
            target_epoch: Some(snapshot.membership_epoch),
            repair: true,
            members: snapshot.members.clone(),
            cohorts: Some(snapshot.cohorts.clone()),
            stream_segments: Some(snapshot.stream_segments.clone()),
            manifest_revision: Some(snapshot.manifest_revision),
            manifest_digest: Some(snapshot.manifest_digest.clone()),
            write_cohort_id: (snapshot.write_cohort_id != 0).then_some(snapshot.write_cohort_id),
            control_authority_epoch: snapshot.control_authority_epoch,
            control_head_revision: snapshot.control_head_revision,
            control_head_digest: snapshot.control_head_digest.clone(),
            control_authority_cohort_id: snapshot.control_authority_cohort_id,
            replacement,
            operation_id: operation_id.to_owned(),
        };
        let results = join_all(nodes.iter().cloned().map(|node| {
            let mut request = request.clone();
            async move {
                let client = NodeClient::new(node, &self.client, &self.internal_token);
                if let Some(state) = client.control_state().await?
                    && state.membership_epoch < request.expected_epoch
                {
                    request.expected_epoch = state.membership_epoch;
                }
                client.cas_membership_online(&request).await
            }
        }))
        .await;
        if results.iter().any(Result::is_err) {
            return Err(ReplicaError::NodeUnavailable);
        }
        Ok(())
    }

    /// Repairs a replacement in an explicit static membership and removes the
    /// old member only after the target is complete. On a failed repair the
    /// old member remains active. Direct deployments should use the fenced
    /// membership operation endpoints when changing the persisted set.
    pub async fn replace_node(
        &self,
        old_id: &str,
        replacement: ReplicaNode,
    ) -> Result<RebalanceReport, ReplicaError> {
        if replacement.id == old_id {
            return Err(ReplicaError::NodeStorage(
                "replacement must have a distinct node id".to_owned(),
            ));
        }
        let token = self.begin_maintenance().await?;
        if let Err(error) = self.add_node(replacement.clone()) {
            let _ = self.end_maintenance(&token).await;
            return Err(error);
        }
        let report = match self
            .rebalance_with_maintenance(Some(&replacement.id), &token)
            .await
        {
            Ok(report) => report,
            Err(error) => {
                let _ = self.remove_node(&replacement.id);
                let _ = self.end_maintenance(&token).await;
                return Err(error);
            }
        };
        if !report.complete() {
            let _ = self.remove_node(&replacement.id);
            let _ = self.end_maintenance(&token).await;
            return Err(ReplicaError::NodeUnavailable);
        }
        if let Err(error) = self.remove_node(old_id) {
            let _ = self.end_maintenance(&token).await;
            return Err(error);
        }
        self.end_maintenance(&token).await?;
        Ok(report)
    }

    /// Appends one already-encrypted record to the three members selected for
    /// its stream and returns after two fsyncs. Every coordinator may run this
    /// path; its local sequence cache is only an optimization reconstructed
    /// from quorum node evidence before the append, while storage nodes remain
    /// the authoritative per-LSN conflict fence. Node commit markers are
    /// durable survivor evidence; the cache is never used as recovery
    /// authority.
    pub async fn append(&self, record: EncryptedRecord) -> Result<usize, ReplicaError> {
        self.metrics.append_attempts.fetch_add(1, Ordering::AcqRel);
        self.metrics.mark_updated();
        let started = Instant::now();
        let result = match self.admit_append().await {
            Ok(_admission) => self.append_serialized(record.clone()).await,
            Err(error) => Err(error),
        };
        let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.metrics
            .append_latency_nanos
            .fetch_add(elapsed, Ordering::AcqRel);
        match &result {
            Ok(_) => {
                self.metrics.append_acks.fetch_add(1, Ordering::AcqRel);
                self.metrics.acked_bytes.fetch_add(
                    record.ciphertext().len().min(u64::MAX as usize) as u64,
                    Ordering::AcqRel,
                );
            }
            Err(_) => {
                self.metrics.append_failures.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.metrics.mark_updated();
        result
    }

    /// Appends one ordered encrypted batch to the selected cohort. Each node
    /// receives the complete batch and performs one group data/commit sync,
    /// while the gateway acknowledges after the configured node quorum.
    pub async fn append_many(&self, records: Vec<EncryptedRecord>) -> Result<usize, ReplicaError> {
        validate_append_batch(&records)?;
        let acked_bytes = records.iter().fold(0_u64, |total, record| {
            total.saturating_add(record.ciphertext().len().min(u64::MAX as usize) as u64)
        });
        self.metrics.append_attempts.fetch_add(1, Ordering::AcqRel);
        self.metrics.mark_updated();
        let started = Instant::now();
        let result = match self.admit_append().await {
            Ok(_admission) => self.append_many_serialized(records).await,
            Err(error) => Err(error),
        };
        let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.metrics
            .append_latency_nanos
            .fetch_add(elapsed, Ordering::AcqRel);
        match &result {
            Ok(_) => {
                self.metrics.append_acks.fetch_add(1, Ordering::AcqRel);
                self.metrics
                    .acked_bytes
                    .fetch_add(acked_bytes, Ordering::AcqRel);
            }
            Err(_) => {
                self.metrics.append_failures.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.metrics.mark_updated();
        result
    }

    /// Reconstructs the writer fence and committed prefix from the current
    /// membership before serving a request. This deliberately requires a
    /// quorum of successful snapshots, even for an empty stream, so a gateway
    /// restarted while the cell is unavailable cannot start a divergent log.
    async fn ensure_initialized(&self) -> Result<(), ReplicaError> {
        self.ensure_initialized_through(None).await.map(|_| ())
    }

    /// Synchronizes a restarted stateless coordinator from the conditional
    /// object-store control head before it asks any legacy replica quorum for
    /// a manifest. This is what lets the successor cohort become the metadata
    /// authority after the original control cohort is retired.
    async fn sync_control_head(&self) -> Result<(), ReplicaError> {
        let (Some(control), Some(head_store)) = (self.control(), self.control_head.as_ref()) else {
            return Ok(());
        };
        let Some(snapshot) = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        else {
            if control.state()?.control_head_revision != 0 {
                return Err(ReplicaError::NodeStorage(
                    "authoritative control head is missing after migration".to_owned(),
                ));
            }
            return Ok(());
        };
        let local = control.state()?;
        if snapshot.head.authority_epoch > local.control_authority_epoch
            || snapshot.head.revision > local.control_head_revision
            || local.control_head_digest.is_empty()
        {
            control.adopt_control_head(&snapshot.head)?;
        }
        Ok(())
    }

    /// Refreshes a stateless coordinator when an authenticated successor
    /// proves another coordinator has advanced this stream beyond its cache.
    /// Returns the quorum manifest the cache was synchronized from so the
    /// append that follows can route and repair against the same certified
    /// value without reading it again.
    async fn ensure_initialized_through(
        &self,
        minimum: Option<(&str, u64)>,
    ) -> Result<Option<QuorumManifest>, ReplicaError> {
        // A stateless coordinator may have a newly-created local cache while
        // the fixed control cohort already owns a non-empty route manifest.
        // Adopt that quorum value before filtering recovery evidence or
        // rebuilding the writer prefix.
        self.sync_control_head().await?;
        let quorum = self.sync_manifest_cache().await?;
        // Rebuild from every retained cohort. Once a new cohort is activated
        // it is intentionally empty, so using only the newest cohort would
        // lose the committed prefix and make the next append fork at LSN 1.
        let nodes = self.recovery_nodes_snapshot().await?;
        let state = self.writer_state.lock().await;
        let initialized = state.initialized;
        let same_membership = state.membership == nodes;
        let has_minimum = minimum.is_none_or(|(stream, committed_lsn)| {
            state
                .streams
                .get(stream)
                .is_some_and(|stream_state| stream_state.committed_lsn >= committed_lsn)
                || committed_lsn == 0
        });
        drop(state);
        if initialized && same_membership && has_minimum {
            return Ok(quorum);
        }
        if nodes.len() < self.quorum {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let snapshots = self.fetch_snapshots(&nodes, None).await;
        let available = snapshots
            .iter()
            .filter(|(_, snapshot)| snapshot.is_some())
            .count();
        if available < self.quorum {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let (candidates, _) = self.collect_candidates_from_snapshots("*", &snapshots)?;
        let mut rebuilt = rebuild_writer_state(&candidates, &snapshots, self.quorum);
        // Removed data cohorts are intentionally absent from `nodes`, so a
        // writer reconstructed after scale-in cannot infer their committed
        // prefix from hot snapshots alone. The immutable archive is the
        // authority for that prefix. Merge it with quorum-certified hot
        // candidates for the stream the caller is about to extend.
        if let Some((stream, _)) = minimum {
            // Every member has already proven the archive holds the prefix
            // below its trim checkpoint, so the merge starts there: the
            // watermark never regresses below what the cohort has trimmed.
            let floor = rebuilt.streams.get(stream).map_or(0, |state| {
                state
                    .records
                    .keys()
                    .next()
                    .map_or(state.committed_lsn, |first| first.saturating_sub(1))
            });
            let archived = self
                .archive
                .recover(stream, floor)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if !archived.is_empty() {
                let mut records = archived;
                records.extend(
                    candidates
                        .iter()
                        .filter(|((candidate_stream, lsn), _)| {
                            candidate_stream == stream && *lsn > floor
                        })
                        .map(|(_, record)| record.clone()),
                );
                records.sort_by_key(EncryptedRecord::lsn);
                records.dedup_by_key(|record| record.lsn());
                let stream_state = rebuilt.streams.entry(stream.to_owned()).or_default();
                stream_state.committed_lsn = floor;
                stream_state.records.clear();
                let mut expected_lsn = floor.saturating_add(1);
                // Seed below every epoch, as the rebuild from hot snapshots
                // does. The stream's writer epoch is the highest any member
                // reported, not the epoch in force at the floor, so seeding
                // with it would reject the first archived record written
                // before the last failover and abandon the prefix this
                // merge has already cleared.
                let mut prefix_epoch = 0;
                for record in records {
                    if record.lsn() != expected_lsn
                        || record.committed_lsn() != expected_lsn.saturating_sub(1)
                        || record.writer_epoch() < prefix_epoch
                    {
                        break;
                    }
                    prefix_epoch = record.writer_epoch();
                    stream_state.writer_epoch = stream_state.writer_epoch.max(prefix_epoch);
                    stream_state.committed_lsn = record.lsn();
                    stream_state.records.insert(record.lsn(), record);
                    expected_lsn = expected_lsn.saturating_add(1);
                }
            }
        }
        // A certified record that only one answering owner still holds is
        // durable but no longer quorum-replicated. Restore its copies on the
        // owners that answered without it, in LSN order so every predecessor
        // lands first, before the writer extends the log past it.
        if let Some((stream, _)) = minimum
            && let Some(stream_state) = rebuilt.streams.get(stream)
        {
            self.repair_under_replicated(stream, &snapshots, &stream_state.records)
                .await;
        }
        rebuilt.membership = nodes;
        let mut state = self.writer_state.lock().await;
        let still_missing_minimum = minimum.is_some_and(|(stream, committed_lsn)| {
            state
                .streams
                .get(stream)
                .is_none_or(|stream_state| stream_state.committed_lsn < committed_lsn)
        });
        if !state.initialized || state.membership != rebuilt.membership || still_missing_minimum {
            preserve_acknowledged_suffixes(&state, &mut rebuilt)?;
            *state = rebuilt;
        }
        Ok(quorum)
    }

    async fn stream_writer(
        &self,
        stream: &str,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, ReplicaError> {
        let lock = {
            let mut writers = self.stream_writers.lock().map_err(|_| {
                ReplicaError::Protocol("stream writer locks are poisoned".to_owned())
            })?;
            if writers.len() >= 256 {
                writers.retain(|_, lock| lock.strong_count() != 0);
            }
            if let Some(lock) = writers.get(stream).and_then(std::sync::Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                writers.insert(stream.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        Ok(lock.lock_owned().await)
    }

    async fn append_serialized(&self, record: EncryptedRecord) -> Result<usize, ReplicaError> {
        self.append_many_serialized(vec![record]).await
    }

    async fn append_many_serialized(
        &self,
        mut records: Vec<EncryptedRecord>,
    ) -> Result<usize, ReplicaError> {
        validate_append_batch(&records)?;
        let first = records
            .first()
            .cloned()
            .expect("append-many validation requires one record");
        let last = records
            .last()
            .cloned()
            .expect("append-many validation requires one record");
        let stream = first.stream().to_owned();
        // Sequence validation, route selection, and quorum acknowledgement
        // are ordered for this stream. Network and archive I/O never hold the
        // global writer-state map or block an unrelated stream's append.
        let _writer = self.stream_writer(&stream).await?;
        self.finish_pending_stream_handoff(&stream).await?;
        let quorum = self
            .ensure_initialized_through(Some((&stream, first.committed_lsn())))
            .await?;
        let (current_lsn, mut archived_lsns) = {
            let mut writer_state = self.writer_state.lock().await;
            let stream_state = writer_state.streams.entry(stream.clone()).or_default();
            let mut archived = BTreeSet::new();
            for record in &records {
                if record.writer_epoch() < stream_state.writer_epoch {
                    return Err(ReplicaError::WriterFenced);
                }
                if record.lsn() <= stream_state.committed_lsn {
                    match stream_state.records.get(&record.lsn()) {
                        Some(cached) if cached == record => {}
                        Some(_) => return Err(ReplicaError::LsnConflict),
                        None => {
                            archived.insert(record.lsn());
                        }
                    }
                }
            }
            (stream_state.committed_lsn, archived)
        };
        // A hot cache may still hold a prefix whose owners have retired.
        // Resends of that prefix are proved against the archive even when
        // this coordinator remembers their original acknowledgement.
        if first.lsn() <= current_lsn {
            let archived_lsn = self
                .archive
                .archived_lsn(&stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            archived_lsns.extend(
                records
                    .iter()
                    .filter(|record| record.lsn() <= archived_lsn)
                    .map(EncryptedRecord::lsn),
            );
        }
        for record in &records {
            if archived_lsns.contains(&record.lsn())
                && !self.archived_record_matches(record).await?
            {
                return Err(ReplicaError::LsnConflict);
            }
        }
        // Archived resends are proven by the durable archive, not submitted
        // to nodes whose trim checkpoints have removed that prefix.
        records.retain(|record| !archived_lsns.contains(&record.lsn()));
        if records.is_empty() {
            return Ok(self.quorum);
        }
        let has_new = records.iter().any(|record| record.lsn() > current_lsn);
        if let Some(first_new) = records.iter().find(|record| record.lsn() > current_lsn) {
            if first_new.lsn() != current_lsn.saturating_add(1) {
                return Err(ReplicaError::LsnConflict);
            }
            if first_new.committed_lsn() != current_lsn {
                return Err(ReplicaError::InvalidWatermark {
                    lsn: first_new.lsn(),
                    committed_lsn: first_new.committed_lsn(),
                });
            }
        }
        let routed_first = records.first().expect("unarchived batch is nonempty");
        let (route, quorum) = self
            .ensure_stream_route(routed_first, current_lsn, quorum)
            .await?;
        let last_route = quorum
            .as_ref()
            .and_then(|quorum| manifest_route(&quorum.manifest, last.stream(), last.lsn()));
        if route != last_route {
            return Err(ReplicaError::LsnConflict);
        }
        let acknowledgements = match self
            .append_nodes_many(&records, route.as_ref(), quorum.as_ref())
            .await
        {
            Ok(acknowledgements) => acknowledgements,
            Err(ReplicaError::WriterFenced) => {
                self.forward_after_stream_handoff(&records, route.as_ref())
                    .await?
            }
            Err(error) => return Err(error),
        };
        if has_new {
            let mut writer_state = self.writer_state.lock().await;
            let stream_state = writer_state.streams.entry(stream).or_default();
            // A concurrent reconstruction may have learned a newer prefix
            // from another coordinator. An acknowledgement never regresses it.
            stream_state.writer_epoch = stream_state.writer_epoch.max(last.writer_epoch());
            stream_state.committed_lsn = stream_state.committed_lsn.max(last.lsn());
            for record in records {
                if stream_state
                    .records
                    .get(&record.lsn())
                    .is_some_and(|cached| cached != &record)
                {
                    return Err(ReplicaError::LsnConflict);
                }
                stream_state.records.insert(record.lsn(), record);
            }
        }
        Ok(acknowledgements)
    }

    /// Resolves an explicit placement fence against the permanent metadata
    /// authority. Dispatches the unchanged ciphertext only when the original
    /// route has a committed successor; client writer fences remain errors.
    async fn forward_after_stream_handoff(
        &self,
        records: &[EncryptedRecord],
        previous: Option<&StreamSegment>,
    ) -> Result<usize, ReplicaError> {
        let previous = previous.ok_or(ReplicaError::WriterFenced)?;
        let first = records.first().ok_or(ReplicaError::LsnConflict)?;
        self.finish_pending_stream_handoff(first.stream()).await?;
        self.sync_control_head().await?;
        let quorum = self
            .sync_manifest_cache()
            .await?
            .ok_or(ReplicaError::QuorumUnavailable)?;
        let archived_lsn = self
            .archive
            .archived_lsn(first.stream())
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let mut remaining = Vec::new();
        for record in records {
            if record.lsn() <= archived_lsn {
                if !self.archived_record_matches(record).await? {
                    return Err(ReplicaError::LsnConflict);
                }
            } else {
                remaining.push(record.clone());
            }
        }
        if remaining.is_empty() {
            return Ok(self.quorum);
        }
        let first = remaining.first().expect("unarchived batch is nonempty");
        let last = remaining.last().expect("unarchived batch is nonempty");
        let successor = manifest_route(&quorum.manifest, first.stream(), first.lsn())
            .ok_or(ReplicaError::WriterFenced)?;
        if &successor == previous && successor.placement_epoch == 0 {
            // A committed legacy reservation can survive a lost CAS response
            // before its initial placement installation. Repair that exact
            // reservation once; a newer stream fence rejects this token.
            self.install_route_placement(first.stream(), &successor)
                .await?;
            return self
                .append_nodes_many(&remaining, Some(&successor), Some(&quorum))
                .await;
        }
        if successor.placement_epoch <= previous.placement_epoch
            || (successor.cohort_id == previous.cohort_id
                && successor.member_hash == previous.member_hash)
            || manifest_route(&quorum.manifest, last.stream(), last.lsn()).as_ref()
                != Some(&successor)
        {
            return Err(ReplicaError::WriterFenced);
        }
        self.append_nodes_many(&remaining, Some(&successor), Some(&quorum))
            .await
    }

    async fn route_for_record(
        &self,
        record: &EncryptedRecord,
    ) -> Result<Option<StreamSegment>, ReplicaError> {
        let Some(_control) = self.control() else {
            return Ok(None);
        };
        // A local control volume is only a cache for a stateless coordinator.
        // When the fixed control cohort is available, route recovery follows
        // the two-member manifest quorum so a stale or tampered local copy
        // cannot steer historical data into the wrong cohort. Legacy node
        // fixtures return the local document through the compatibility path.
        let manifest = self
            .read_manifest_quorum()
            .await?
            .ok_or(ReplicaError::QuorumUnavailable)?;
        Ok(manifest_route(&manifest, record.stream(), record.lsn()))
    }

    /// Checks whether a node is a member of the immutable cohort that owns a
    /// record.  Direct control files created before segment metadata existed
    /// treat the bootstrap cohort (zero) as the owner of legacy history.  A
    /// record is never allowed to contribute recovery evidence from a
    /// different cohort: otherwise one old copy and one new copy could be
    /// mistaken for a quorum after scale-out.
    fn node_owns_record(
        &self,
        state: Option<&DurableControlState>,
        node: &ReplicaNode,
        record: &EncryptedRecord,
    ) -> bool {
        let Some(state) = state else {
            return true;
        };
        let Some(segment) = state
            .stream_segments
            .get(record.stream())
            .and_then(|segments| {
                segments.iter().find(|segment| {
                    segment.start_lsn <= record.lsn()
                        && segment.end_lsn.is_none_or(|end| record.lsn() <= end)
                })
            })
        else {
            // A direct node must not contribute evidence for a stream/LSN
            // that the quorum manifest has never assigned. Falling back to
            // cohort zero would let an unknown or gapped route masquerade as
            // historical bootstrap data after a restart.
            return false;
        };
        // Ownership is bound to the exact member set published for this
        // range. A locally tampered cohort list must not turn a copy on an
        // unrelated disk into recovery evidence.
        if segment.member_ids.len() != REPLICATION_FACTOR
            || segment
                .member_ids
                .windows(2)
                .any(|window| window[0] >= window[1])
            || member_set_hash(state, &segment.member_ids).as_deref()
                != Some(segment.member_hash.as_str())
        {
            return false;
        }
        segment.member_ids.iter().any(|id| id == &node.id)
    }

    fn snapshots_for_record(
        &self,
        record: &EncryptedRecord,
        snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
    ) -> Result<Vec<(ReplicaNode, Option<NodeSnapshot>)>, ReplicaError> {
        let control_state = self.control().map(|control| control.state()).transpose()?;
        Ok(snapshots
            .iter()
            .filter(|(node, _)| self.node_owns_record(control_state.as_ref(), node, record))
            .cloned()
            .collect())
    }

    /// Builds the exact identity used to decide whether a route-manifest
    /// repair may be skipped. A legacy/local control document has no object
    /// head receipt, so it simply keeps the existing repair path.
    fn route_repair_context(&self, route: &StreamSegment) -> Option<RouteRepairContext> {
        let control = self.control()?;
        let state = control.state().ok()?;
        let members = route
            .member_ids
            .iter()
            .map(|id| state.members.get(id).cloned())
            .collect::<Option<Vec<_>>>()?;
        RouteRepairContext::new(
            state.control_authority_epoch,
            state.control_head_revision,
            state.control_head_digest,
            route.clone(),
            members,
        )
        .ok()
    }

    fn route_repair_cache_hit(&self, context: &RouteRepairContext) -> bool {
        self.route_repair_cache
            .lock()
            .ok()
            .and_then(|mut cache| cache.contains(context).ok())
            .unwrap_or(false)
    }

    /// Stores a proof only after the caller has established the configured
    /// manifest-repair quorum. Cache failures are deliberately best effort:
    /// the next append can repeat the durable repair without changing the
    /// write authority or placement checks.
    fn record_route_repair_proof(
        &self,
        context: Option<&RouteRepairContext>,
        nodes: &[ReplicaNode],
        results: &[Result<Option<ReplicaManifest>, ReplicaError>],
    ) {
        let Some(context) = context else { return };
        if nodes.len() != results.len() {
            return;
        }
        let acknowledged_member_ids = nodes
            .iter()
            .zip(results)
            .filter(|(_, result)| result.is_ok())
            .map(|(node, _)| node.id.clone())
            .collect::<Vec<_>>();
        if acknowledged_member_ids.len() < self.quorum {
            return;
        }
        let proof = RouteRepairProof {
            context: context.clone(),
            acknowledged_member_ids,
            quorum: self.quorum,
        };
        if let Ok(mut cache) = self.route_repair_cache.lock() {
            let _ = cache.record_quorum_repair(proof);
        }
    }

    /// /// Publishes a deterministic stream range for an append.  The range is
    /// is first committed locally and then fanned out to retained peers so a
    /// restarted stateless coordinator can recover the same route from any
    /// healthy control volume.  Legacy node fixtures without a control
    /// listener are accepted; their gateway still has the authoritative local
    /// document.
    ///
    /// The caller normally passes the quorum manifest it just synchronized
    /// its cache from; reading it again from three members would only repeat
    /// the same round trip. The returned manifest is the one the route was
    /// taken from, so the data fan-out can verify and repair against it.
    async fn ensure_stream_route(
        &self,
        record: &EncryptedRecord,
        durable_old_lsn: u64,
        quorum: Option<QuorumManifest>,
    ) -> Result<(Option<StreamSegment>, Option<QuorumManifest>), ReplicaError> {
        let Some(control) = self.control() else {
            return Ok((None, None));
        };
        let QuorumManifest {
            manifest: expected,
            agreed,
        } = match quorum {
            Some(quorum) => quorum,
            None => self
                .read_manifest_quorum_agreement()
                .await?
                .ok_or(ReplicaError::QuorumUnavailable)?,
        };
        // A gateway may have restarted with a stale local control volume even
        // though the fixed control cohort has already committed a newer
        // manifest. Adopt that quorum value before constructing the next
        // immutable range, otherwise the local CAS would fork the revision.
        let local = control.manifest().ok();
        if local.as_ref().is_none_or(|local| {
            local.revision != expected.revision || local.digest != expected.digest
        }) {
            control.adopt_manifest(&expected, true)?;
        }
        let state = control.state()?;
        if let Some(existing) = manifest_route(&expected, record.stream(), record.lsn()) {
            if existing.max_append_bytes != 0
                && record.ciphertext().len() as u64 > existing.max_append_bytes
            {
                return Err(ReplicaError::CapacityExceeded {
                    cohort_id: existing.cohort_id,
                    requested_bytes: record.ciphertext().len() as u64,
                    max_append_bytes: existing.max_append_bytes,
                });
            }
            // An existing range is immutable from the append path. A live
            // cohort handoff owns the placement fence, archive watermark, and
            // successor CAS; a rendezvous change must never synthesize that
            // boundary merely because a new cohort now wins the ring.
            return Ok((
                Some(existing),
                Some(QuorumManifest {
                    manifest: expected,
                    agreed,
                }),
            ));
        }
        let selected_cohort_id = cohort_for_stream(&state, record.stream())?;
        let cohort_id = selected_cohort_id;
        let cohort = state
            .cohorts
            .get(&cohort_id)
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if cohort.status != CohortStatus::Active || cohort.members.len() != REPLICATION_FACTOR {
            return Err(ReplicaError::QuorumUnavailable);
        }
        if cohort.max_append_bytes != 0
            && record.ciphertext().len() as u64 > cohort.max_append_bytes
        {
            return Err(ReplicaError::CapacityExceeded {
                cohort_id,
                requested_bytes: record.ciphertext().len() as u64,
                max_append_bytes: cohort.max_append_bytes,
            });
        }
        let member_hash = cohort_member_hash(&state, cohort_id).ok_or_else(|| {
            ReplicaError::NodeStorage("active cohort member hash is unavailable".to_owned())
        })?;
        let mut desired = expected.stream_segments.clone();
        let mut cutover_lsn = None;
        let ranges = desired.entry(record.stream().to_owned()).or_default();
        if ranges.is_empty() {
            if durable_old_lsn > 0 && record.lsn() != durable_old_lsn.saturating_add(1) {
                return Err(ReplicaError::LsnConflict);
            }
        } else if let Some(last) = ranges.last_mut() {
            if last.end_lsn.is_none() {
                if durable_old_lsn < last.start_lsn.saturating_sub(1) {
                    // The open range was only a reservation and no durable
                    // record used it. Replace that empty reservation at LSN1.
                    ranges.clear();
                } else {
                    last.end_lsn = Some(durable_old_lsn);
                    cutover_lsn = Some(durable_old_lsn);
                }
            }
            if let Some(last) = ranges.last()
                && record.lsn() != last.end_lsn.unwrap_or(0).saturating_add(1)
            {
                return Err(ReplicaError::LsnConflict);
            }
        }
        let start_lsn = if ranges.is_empty() { 1 } else { record.lsn() };
        let operation_id = manifest_operation_id(
            record.stream(),
            expected.revision,
            start_lsn,
            cohort_id,
            record.writer_epoch(),
        );
        ranges.push(StreamSegment {
            start_lsn,
            end_lsn: None,
            cohort_id,
            member_ids: cohort.members.clone(),
            member_hash,
            writer_epoch: record.writer_epoch(),
            manifest_revision: expected.revision.saturating_add(1),
            placement_epoch: 0,
            operation_id: operation_id.clone(),
            tier: cohort.tier.clone(),
            max_append_bytes: cohort.max_append_bytes,
        });
        let expected_digest = if expected.revision == 0 {
            String::new()
        } else {
            expected.digest.clone()
        };
        let manifest = self
            .cas_manifest_quorum(
                expected.revision,
                &expected_digest,
                desired,
                &operation_id,
                cutover_lsn,
            )
            .await?;
        let route = manifest_route(&manifest, record.stream(), record.lsn()).ok_or_else(|| {
            ReplicaError::NodeStorage("published manifest omitted route".to_owned())
        })?;
        // A newly published route has no node-side admission state yet.  The
        // route CAS is durable, but each target volume must persist the exact
        // placement token before the first append can pass its stream gate.
        // This is one bounded fan-out, not a retry or polling mechanism; a
        // partial installation fails the append and leaves the route intact
        // for an explicit repair operation.
        self.install_route_placement(record.stream(), &route)
            .await?;
        // A freshly published route carries no agreement set: every data
        // member must still receive the repair before its first append.
        Ok((
            Some(route),
            Some(QuorumManifest {
                manifest,
                agreed: BTreeSet::new(),
            }),
        ))
    }

    async fn install_route_placement(
        &self,
        stream: &str,
        route: &StreamSegment,
    ) -> Result<(), ReplicaError> {
        let placement = placement_for_route(stream, route);
        let nodes = self.replicas_for_route(stream, Some(route)).await?;
        let results = join_all(nodes.into_iter().map(|node| {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            let stream = stream.to_owned();
            let placement = placement.clone();
            async move {
                NodeClient::new(node, &client, &token)
                    .install_placement_fence(&stream, &placement)
                    .await
            }
        }))
        .await;
        Self::require_manifest_sync_quorum(&results, self.quorum)
    }

    /// Installs the host admission token for every currently writable stream
    /// route after a legacy maintenance CAS. The route document and the
    /// node-local placement sidecar are separate durable records; publishing
    /// only the former would make the next append fail closed on a restarted
    /// or replaced volume. Closed historical ranges are intentionally skipped:
    /// only the current tail may receive a new append token.
    async fn install_current_route_placements(&self) -> Result<(), ReplicaError> {
        let snapshot = self.membership().await?;
        let routes = snapshot
            .stream_segments
            .iter()
            .filter_map(|(stream, segments)| {
                segments.last().map(|route| (stream.clone(), route.clone()))
            })
            .collect::<Vec<_>>();
        for (stream, route) in routes {
            self.install_route_placement(&stream, &route).await?;
        }
        Ok(())
    }

    async fn newest_active_cohort_id(&self) -> Result<u64, ReplicaError> {
        if let Some(control) = self.control() {
            return control
                .newest_active_cohort()?
                .map(|cohort| cohort.id)
                .ok_or(ReplicaError::QuorumUnavailable);
        }
        Ok(0)
    }

    async fn replicas_for_route(
        &self,
        stream: &str,
        route: Option<&StreamSegment>,
    ) -> Result<Vec<ReplicaNode>, ReplicaError> {
        self.replicas_for_route_internal(stream, route, false).await
    }

    async fn replicas_for_repair_route(
        &self,
        stream: &str,
        route: Option<&StreamSegment>,
    ) -> Result<Vec<ReplicaNode>, ReplicaError> {
        self.replicas_for_route_internal(stream, route, true).await
    }

    async fn replicas_for_route_internal(
        &self,
        stream: &str,
        route: Option<&StreamSegment>,
        allow_joining: bool,
    ) -> Result<Vec<ReplicaNode>, ReplicaError> {
        let Some(control) = self.control() else {
            let nodes = self.recovery_nodes_snapshot().await?;
            let replicas = select_static_replicas(&nodes);
            let required = REPLICATION_FACTOR.min(nodes.len());
            if replicas.len() != required {
                return Err(ReplicaError::QuorumUnavailable);
            }
            return Ok(replicas);
        };
        let state = control.state()?;
        let (member_ids, cohort) = if let Some(route) = route {
            if route.member_ids.len() != REPLICATION_FACTOR
                || route
                    .member_ids
                    .windows(2)
                    .any(|window| window[0] >= window[1])
                || route.member_hash.is_empty()
                || member_set_hash(&state, &route.member_ids).as_deref()
                    != Some(route.member_hash.as_str())
            {
                return Err(ReplicaError::LsnConflict);
            }
            let cohort = state.cohorts.get(&route.cohort_id);
            (route.member_ids.as_slice(), cohort)
        } else {
            let cohort_id = self.newest_active_cohort_id().await?;
            let cohort = state
                .cohorts
                .get(&cohort_id)
                .ok_or(ReplicaError::QuorumUnavailable)?;
            if cohort.status != CohortStatus::Active {
                return Err(ReplicaError::QuorumUnavailable);
            }
            (cohort.members.as_slice(), Some(cohort))
        };
        if let Some(cohort) = cohort
            && route.is_none()
            && (cohort.status != CohortStatus::Active || cohort.members.len() != REPLICATION_FACTOR)
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let nodes = member_ids
            .iter()
            .filter_map(|id| state.members.get(id))
            .filter(|member| {
                member.status != MemberStatus::Removed
                    && (allow_joining || member.status != MemberStatus::Joining)
            })
            .map(DurableMember::node)
            .collect::<Vec<_>>();
        if nodes.len() != REPLICATION_FACTOR {
            return Err(ReplicaError::QuorumUnavailable);
        }
        let _ = stream;
        Ok(nodes)
    }

    /// Returns the three members assigned to a stream. A stream's existing
    /// manifest tail remains authoritative; a stream with no history is
    /// assigned through the stable cohort-identity ring.
    pub async fn replicas_for_stream(
        &self,
        stream: &str,
    ) -> Result<Vec<ReplicaNode>, ReplicaError> {
        if let Some(control) = self.control() {
            let manifest = self
                .read_manifest_quorum()
                .await?
                .ok_or(ReplicaError::QuorumUnavailable)?;
            let state = control.state()?;
            let route = manifest
                .stream_segments
                .get(stream)
                .and_then(|segments| segments.last())
                .cloned();
            let route = if let Some(route) = route {
                Some(route)
            } else {
                let cohort_id = cohort_for_stream(&state, stream)?;
                let cohort = state
                    .cohorts
                    .get(&cohort_id)
                    .ok_or(ReplicaError::QuorumUnavailable)?;
                Some(StreamSegment {
                    start_lsn: 1,
                    end_lsn: None,
                    cohort_id,
                    member_ids: cohort.members.clone(),
                    member_hash: cohort_member_hash(&state, cohort_id)
                        .ok_or(ReplicaError::QuorumUnavailable)?,
                    writer_epoch: 0,
                    manifest_revision: manifest.revision,
                    placement_epoch: 0,
                    operation_id: String::new(),
                    tier: cohort.tier.clone(),
                    max_append_bytes: cohort.max_append_bytes,
                })
            };
            return self.replicas_for_route(stream, route.as_ref()).await;
        }
        let nodes = self.nodes_snapshot().await?;
        let replicas = select_static_replicas(&nodes);
        let required = if self.direct_mode() {
            REPLICATION_FACTOR
        } else {
            REPLICATION_FACTOR.min(nodes.len())
        };
        if replicas.len() != required {
            return Err(ReplicaError::QuorumUnavailable);
        }
        Ok(replicas)
    }

    /// Returns the exact cohort assigned to one record, including historical
    /// segments.  It is public so recovery/repair operators can inspect the
    /// immutable placement without changing it.
    pub async fn replicas_for_record(
        &self,
        record: &EncryptedRecord,
    ) -> Result<Vec<ReplicaNode>, ReplicaError> {
        let route = self.route_for_record(record).await?;
        if self.direct_mode() && route.is_none() {
            return Err(ReplicaError::QuorumUnavailable);
        }
        self.replicas_for_route(record.stream(), route.as_ref())
            .await
    }

    async fn append_nodes_many(
        &self,
        records: &[EncryptedRecord],
        route: Option<&StreamSegment>,
        quorum: Option<&QuorumManifest>,
    ) -> Result<usize, ReplicaError> {
        validate_append_batch(records)?;
        for record in records {
            if let Some(route) = route
                && route.max_append_bytes != 0
                && record.ciphertext().len() as u64 > route.max_append_bytes
            {
                return Err(ReplicaError::CapacityExceeded {
                    cohort_id: route.cohort_id,
                    requested_bytes: record.ciphertext().len() as u64,
                    max_append_bytes: route.max_append_bytes,
                });
            }
        }
        let first = records
            .first()
            .expect("append-many validation requires one record");
        let nodes = self.replicas_for_route(first.stream(), route).await?;
        if nodes.len() < self.quorum {
            return Err(ReplicaError::QuorumUnavailable);
        }
        if self.direct_mode() {
            let route = route.ok_or(ReplicaError::QuorumUnavailable)?;
            let quorum = quorum.ok_or(ReplicaError::QuorumUnavailable)?;
            let manifest = &quorum.manifest;
            let authoritative_route = manifest_route(manifest, first.stream(), first.lsn())
                .ok_or(ReplicaError::QuorumUnavailable)?;
            if &authoritative_route != route {
                return Err(ReplicaError::LsnConflict);
            }
            let repair_request = ManifestCasRequest {
                expected_revision: manifest.revision,
                expected_digest: manifest.digest.clone(),
                stream_segments: manifest.stream_segments.clone(),
                operation_id: manifest.operation_id.clone(),
                repair: true,
                cutover_lsn: None,
            };
            let route_context = self.route_repair_context(route);
            let sync_results = self
                .repair_route_members(&nodes, quorum, repair_request, route_context.as_ref())
                .await;
            Self::require_manifest_sync_quorum(&sync_results, self.quorum)?;
            self.record_route_repair_proof(route_context.as_ref(), &nodes, &sync_results);
        }

        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        let placement = route.map(|route| placement_for_route(first.stream(), route));
        for node in nodes {
            let records = records.to_vec();
            let placement = placement.clone();
            let http = self.client.clone();
            let internal_token = self.internal_token.clone();
            let result_tx = result_tx.clone();
            tokio::spawn(async move {
                let client = NodeClient::new(node.clone(), &http, &internal_token);
                let result = client
                    .append_and_commit_many_with_placement(&records, placement)
                    .await;
                let _ = result_tx.send((node.clone(), result));
            });
        }
        drop(result_tx);

        let mut acknowledgements = 0;
        let mut saw_fence = false;
        let mut saw_conflict = false;
        let mut behind = Vec::new();
        while let Some((node, result)) = result_rx.recv().await {
            match result {
                Ok(()) => {
                    acknowledgements += 1;
                    if acknowledgements >= self.quorum {
                        return Ok(acknowledgements);
                    }
                }
                Err(ReplicaError::WriterFenced) => saw_fence = true,
                Err(ReplicaError::LsnConflict) => {
                    saw_conflict = true;
                    behind.push(node);
                }
                Err(_) => {}
            }
        }
        // Short of a quorum, a member that refused for want of a predecessor
        // may simply be behind: one that was down while its peers wrote.
        // Bring it up to date from the committed copies and offer it the
        // batch again, rather than failing a write two members could carry.
        if !saw_fence {
            for node in behind {
                match self.catch_up_member(first.stream(), &node).await {
                    Ok(Some(_)) => {}
                    _ => continue,
                }
                let client = NodeClient::new(node.clone(), &self.client, &self.internal_token);
                if client
                    .append_and_commit_many_with_placement(records, placement.clone())
                    .await
                    .is_ok()
                {
                    acknowledgements += 1;
                    if acknowledgements >= self.quorum {
                        return Ok(acknowledgements);
                    }
                }
            }
        }
        if saw_fence {
            return Err(ReplicaError::WriterFenced);
        }
        if saw_conflict {
            return Err(ReplicaError::LsnConflict);
        }
        Err(ReplicaError::QuorumUnavailable)
    }

    /// Sends the route repair to the members of a route that did not answer
    /// the quorum read with this exact manifest. A member that did already
    /// holds the value durably and is reported as an immediate success, so
    /// with one healthy cohort the repair usually reaches nobody at all.
    async fn repair_route_members(
        &self,
        nodes: &[ReplicaNode],
        quorum: &QuorumManifest,
        request: ManifestCasRequest,
        context: Option<&RouteRepairContext>,
    ) -> Vec<Result<Option<ReplicaManifest>, ReplicaError>> {
        if let Some(context) = context
            && self.route_repair_cache_hit(context)
        {
            return nodes.iter().map(|_| Ok(None)).collect();
        }
        // A migrated node rejects legacy manifest CAS, including repairs.
        // Repair its cache by adopting the complete authoritative head.
        let authoritative = if self.control_head.is_some() {
            match self.load_authoritative_head().await {
                Ok(snapshot) => snapshot.map(|snapshot| snapshot.head),
                Err(_) => {
                    return nodes
                        .iter()
                        .map(|_| Err(ReplicaError::NodeUnavailable))
                        .collect();
                }
            }
        } else {
            None
        };
        let mut results = (0..nodes.len())
            .map(|_| None)
            .collect::<Vec<Option<Result<Option<ReplicaManifest>, ReplicaError>>>>();
        let mut pending = Vec::new();
        let mut pending_indices = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            if quorum.agreed.contains(&node.id) {
                results[index] = Some(Ok(None));
            } else {
                pending.push(node.clone());
                pending_indices.push(index);
            }
        }
        // The phase ends as soon as enough members hold the route; a repair
        // still in flight to a slow member completes detached, and the data
        // fan-out that follows requires the same quorum anyway.
        let acknowledged = results.iter().filter(|result| result.is_some()).count();
        let needed = self.quorum.saturating_sub(acknowledged);
        let answers = fan_out(
            &pending,
            |node| {
                let request = request.clone();
                let authoritative = authoritative.clone();
                let client = self.client.clone();
                let internal_token = self.internal_token.clone();
                async move {
                    let node = NodeClient::new(node, &client, &internal_token);
                    if let Some(head) = authoritative {
                        node.adopt_control_head(&head).await.map(|()| None)
                    } else {
                        node.cas_manifest(&request).await
                    }
                }
            },
            |answers| answers.iter().flatten().filter(|r| r.is_ok()).count() >= needed,
            None,
        )
        .await;
        for (index, answer) in pending_indices.into_iter().zip(answers) {
            results[index] = Some(answer.unwrap_or(Err(ReplicaError::NodeUnavailable)));
        }
        results
            .into_iter()
            .map(|result| result.unwrap_or(Err(ReplicaError::NodeUnavailable)))
            .collect()
    }

    /// Accepts a route-manifest repair once the configured quorum persisted it.
    /// An unavailable trailing member must not turn a durable quorum into an
    /// outage; the subsequent append fanout still requires the same quorum.
    fn require_manifest_sync_quorum<T>(
        results: &[Result<T, ReplicaError>],
        quorum: usize,
    ) -> Result<(), ReplicaError> {
        if results.iter().filter(|result| result.is_ok()).count() >= quorum {
            return Ok(());
        }
        if results
            .iter()
            .any(|result| matches!(result, Err(ReplicaError::WriterFenced)))
        {
            return Err(ReplicaError::WriterFenced);
        }
        if results
            .iter()
            .any(|result| matches!(result, Err(ReplicaError::LsnConflict)))
        {
            return Err(ReplicaError::LsnConflict);
        }
        Err(ReplicaError::QuorumUnavailable)
    }

    /// Recovers only an unambiguous quorum of byte-identical node copies after
    /// every owning cohort has supplied a read quorum. A caller-supplied
    /// authenticated watermark enables the certified recovery path
    /// below, where one unique contiguous copy is sufficient even if the
    /// gateway crashed before asynchronous commit markers reached the nodes.
    pub async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.recover_with_watermark(stream, after_lsn, None).await
    }

    /// Recovers a conservative contiguous prefix when an authenticated caller
    /// supplies a committed LSN watermark. The watermark is external commit
    /// evidence for this certified recovery; each one-copy record must
    /// still have one unique payload and an exact preceding cLSN. Only records
    /// at or below the watermark are eligible, and a gap/conflict stops the
    /// prefix.
    pub async fn recover_with_watermark(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: Option<u64>,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        self.recover_with_watermark_certificate(stream, after_lsn, committed_lsn, None)
            .await
    }

    /// Recovers with an optional gateway-issued commit certificate. Quorum
    /// candidates and durable node markers remain valid evidence on their own;
    /// a one-copy candidate without a marker must present a certificate bound
    /// to the exact record at the requested watermark.
    pub async fn recover_with_watermark_certificate(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: Option<u64>,
        certificate: Option<&str>,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        let mut archived = self
            .archive
            .recover(stream, after_lsn)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if let Some(limit) = committed_lsn {
            archived.retain(|record| record.lsn() <= limit);
        }
        let hot_after = archived
            .last()
            .map_or(after_lsn, EncryptedRecord::lsn)
            .max(after_lsn);
        let (mut hot, floor) = self
            .recover_hot_tail(stream, hot_after, committed_lsn, certificate)
            .await?;
        if floor > hot_after {
            // A member reported a trim checkpoint above the archive head this
            // call read: its archive loop published and trimmed the tail
            // between the archive pass and the hot pass. That member no
            // longer holds the record and is not a reader that confirms it
            // absent; a checkpoint is only written after the archive
            // verifiably holds the prefix, so the archive is re-read from
            // the stale watermark and must reach the checkpoint.
            let mut late = self
                .archive
                .recover(stream, hot_after)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if let Some(limit) = committed_lsn {
                late.retain(|record| record.lsn() <= limit);
            }
            let reached = late.last().map_or(hot_after, EncryptedRecord::lsn);
            if reached < floor {
                return Err(ReplicaError::NodeStorage(format!(
                    "member trim checkpoint {floor} for {stream} exceeds the archive tail {reached}"
                )));
            }
            hot.retain(|record| record.lsn() > reached);
            archived.append(&mut late);
        }
        archived.append(&mut hot);
        if let Some(limit) = committed_lsn {
            ensure_exact_watermark(after_lsn, limit, &archived)?;
        }
        Ok(archived)
    }

    /// Recovers the quorum-resident tail after the caller's archived
    /// watermark. Returns the tail together with the floor it starts after:
    /// the caller's watermark, or the highest trim checkpoint an answering
    /// member reported when that is higher, in which case the archive holds
    /// the LSNs between the two and the caller must read them from there.
    async fn recover_hot_tail(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: Option<u64>,
        certificate: Option<&str>,
    ) -> Result<(Vec<EncryptedRecord>, u64), ReplicaError> {
        if let Some(committed_lsn) = committed_lsn
            && committed_lsn < after_lsn
        {
            return Err(ReplicaError::RecoveryGap {
                expected_lsn: after_lsn.saturating_add(1),
                received_lsn: committed_lsn,
            });
        }
        // Ordinary recovery is the fresh/no-certificate resurrection path.
        // It establishes the writer fence and reads a quorum from every
        // cohort that can own the requested tail. Records still need quorum
        // commit markers; an unavailable third copy is not treated as empty.
        let (candidates, floor) = if committed_lsn.is_none() {
            self.ensure_initialized().await?;
            let snapshots = self
                .fetch_ordinary_recovery_snapshots(stream, after_lsn)
                .await?;
            let floor = trimmed_floor(stream, after_lsn, committed_lsn, &snapshots);
            self.reject_ambiguous_quorum_records(stream, floor, &snapshots)?;
            let (candidates, _) = self.collect_candidates_from_snapshots(stream, &snapshots)?;
            (candidates, floor)
        } else {
            // A certified handoff still needs the quorum manifest to bind a
            // fresh coordinator to immutable stream placement, but it must
            // not require a full data-cohort snapshot before using its exact
            // watermark path.
            self.sync_manifest_cache().await?;
            let (candidates, _) = self.collect_candidates(stream).await?;
            (candidates, after_lsn)
        };
        let mut records = candidates
            .values()
            .filter(|record| record.lsn() > floor)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(committed_lsn) = committed_lsn {
            // Watermark recovery is also a resurrection path.  It must inspect
            // every retained cohort so a cutover cannot hide the historical
            // prefix that still lives on cohort zero (or an older draining
            // cohort).
            let nodes = self.recovery_nodes_snapshot().await?;
            let snapshots = self.fetch_snapshots(&nodes, Some(stream)).await;
            let floor = trimmed_floor(stream, after_lsn, Some(committed_lsn), &snapshots);
            let watermark_records =
                self.watermark_candidates(stream, floor, committed_lsn, &snapshots)?;
            records.retain(|record| record.lsn() > floor);
            records.extend(watermark_records);
            records.sort_by_key(EncryptedRecord::lsn);
            records.dedup_by_key(|record| record.lsn());
            let mut contiguous = Vec::new();
            let mut expected = floor.saturating_add(1);
            let mut expected_epoch = 0_u64;
            for record in records {
                if record.lsn() < expected {
                    continue;
                }
                if record.lsn() != expected
                    || record.lsn() > committed_lsn
                    || record.committed_lsn() != expected.saturating_sub(1)
                    || record.writer_epoch() < expected_epoch
                {
                    break;
                }
                contiguous.push(record);
                expected_epoch = contiguous
                    .last()
                    .map_or(expected_epoch, EncryptedRecord::writer_epoch);
                expected = expected.saturating_add(1);
            }
            ensure_exact_watermark(floor, committed_lsn, &contiguous)?;
            if let Some(last) = contiguous.last()
                && contiguous.iter().any(|record| {
                    let key = record_key(record);
                    !candidates
                        .get(&key)
                        .is_some_and(|candidate| candidate == record)
                        && !has_durable_marker(record, &snapshots)
                })
            {
                let certificate = certificate.ok_or(ReplicaError::CommitCertificateMissing)?;
                let route = self.route_for_record(last).await?;
                let (
                    cohort_id,
                    segment_start_lsn,
                    segment_end_lsn,
                    member_hash,
                    segment_operation_id,
                ) = route
                    .as_ref()
                    .map(|segment| {
                        (
                            Some(segment.cohort_id),
                            Some(segment.start_lsn),
                            Some(segment.end_lsn),
                            Some(segment.member_hash.as_str()),
                            Some(segment.operation_id.as_str()),
                        )
                    })
                    .unwrap_or((None, None, None, None, None));
                let manifest = self.read_manifest_quorum().await?;
                if self.direct_mode() && manifest.is_none() {
                    return Err(ReplicaError::QuorumUnavailable);
                }
                let manifest_revision = manifest.as_ref().map(|value| value.revision);
                let manifest_digest = manifest.as_ref().map(|value| value.digest.as_str());
                self.auth.verify_commit_certificate(
                    certificate,
                    stream,
                    committed_lsn,
                    last,
                    cohort_id,
                    segment_start_lsn,
                    manifest_revision,
                    manifest_digest,
                    segment_end_lsn,
                    member_hash,
                    segment_operation_id,
                )?;
            }
            return Ok((contiguous, floor));
        }
        records.sort_by_key(EncryptedRecord::lsn);
        let mut contiguous = Vec::new();
        let mut expected = floor.saturating_add(1);
        let mut expected_epoch = 0_u64;
        for record in records {
            if record.lsn() < expected {
                continue;
            }
            if record.lsn() != expected {
                // A lone markerless partial may be ignored at the end of a
                // stream, but a later committed candidate proves that the
                // partial is a hidden predecessor. Returning a shorter tail
                // here would let a fresh writer fork at the wrong LSN.
                return Err(ReplicaError::RecoveryGap {
                    expected_lsn: expected,
                    received_lsn: record.lsn(),
                });
            }
            if record.committed_lsn() != expected.saturating_sub(1)
                || record.writer_epoch() < expected_epoch
            {
                break;
            }
            contiguous.push(record);
            expected_epoch = contiguous
                .last()
                .map_or(expected_epoch, EncryptedRecord::writer_epoch);
            expected = expected.saturating_add(1);
        }
        Ok((contiguous, floor))
    }

    /// Repairs all active nodes, or only target when a replacement is being
    /// brought online. Eligibility is computed from pre-repair snapshots, so a
    /// new target cannot bootstrap a commit from its own lone partial copy.
    pub async fn rebalance(&self, target: Option<&str>) -> Result<RebalanceReport, ReplicaError> {
        let token = self.begin_maintenance().await?;
        let result = self.rebalance_with_maintenance(target, &token).await;
        let release = self.end_maintenance(&token).await;
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(report), Ok(())) => Ok(report),
        }
    }

    async fn rebalance_unfenced(
        &self,
        target: Option<&str>,
        maintenance_token: Option<&str>,
    ) -> Result<RebalanceReport, ReplicaError> {
        self.ensure_initialized().await?;
        let nodes = self.recovery_nodes_snapshot().await?;
        let target_cohort = if let Some(target) = target {
            let control = self.control().ok_or_else(|| {
                ReplicaError::NodeStorage("rebalance target is not an active member".to_owned())
            })?;
            Some(
                control
                    .state()?
                    .members
                    .get(target)
                    .map(|member| member.cohort_id)
                    .ok_or_else(|| {
                        ReplicaError::NodeStorage(
                            "rebalance target is not a retained member".to_owned(),
                        )
                    })?,
            )
        } else {
            None
        };
        if let Some(token) = maintenance_token {
            // A replacement can be added after the gateway acquired its
            // original cell-wide fence. Bring that newly discovered node
            // under the same durable marker before using the privileged
            // maintenance write surface.
            let fenced = join_all(nodes.iter().cloned().map(|node| {
                let http = &self.client;
                let internal_token = &self.internal_token;
                let token = token.to_owned();
                async move {
                    NodeClient::new(node, http, internal_token)
                        .fence(&token)
                        .await
                }
            }))
            .await;
            if fenced.iter().any(Result::is_err) {
                return Err(ReplicaError::NodeUnavailable);
            }
        }
        let groups = if let Some(control) = self.control() {
            let state = control.state()?;
            state
                .cohorts
                .values()
                .filter(|cohort| cohort.status != CohortStatus::Joining)
                .map(|cohort| {
                    let cohort_nodes = cohort
                        .members
                        .iter()
                        .filter_map(|id| state.members.get(id))
                        .filter(|member| member.status != MemberStatus::Removed)
                        .map(DurableMember::node)
                        .collect::<Vec<_>>();
                    let targets = cohort
                        .members
                        .iter()
                        .filter_map(|id| state.members.get(id))
                        .filter(|member| {
                            member.status == MemberStatus::Active
                                && target.is_none_or(|wanted| wanted == member.id)
                        })
                        .map(DurableMember::node)
                        .collect::<Vec<_>>();
                    (cohort.id, cohort_nodes, targets)
                })
                .collect::<Vec<_>>()
        } else {
            let targets = target
                .map(|wanted| {
                    nodes
                        .iter()
                        .filter(|node| node.id == wanted)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| nodes.clone());
            vec![(0, nodes.clone(), targets)]
        };
        if target.is_some() && !groups.iter().any(|(_, _, targets)| !targets.is_empty()) {
            return Err(ReplicaError::NodeStorage(
                "rebalance target is not an active member".to_owned(),
            ));
        }
        let mut report = RebalanceReport {
            quorum_proven: true,
            ..RebalanceReport::default()
        };
        for (cohort_id, cohort_nodes, targets) in groups {
            if target_cohort.is_some_and(|target_cohort| cohort_id != target_cohort) {
                continue;
            }
            if cohort_nodes.is_empty() || targets.is_empty() {
                continue;
            }
            let snapshots = self.fetch_snapshots(&cohort_nodes, None).await;
            if snapshots.iter().any(|(_, snapshot)| snapshot.is_none()) {
                report.quorum_proven = false;
            }
            let (all_candidates, unsafe_records_skipped) =
                self.collect_candidates_from_snapshots("*", &snapshots)?;
            let candidates = all_candidates
                .into_values()
                .filter(|record| {
                    self.control()
                        .and_then(|control| {
                            control.stream_segment(record.stream(), record.lsn()).ok()
                        })
                        .flatten()
                        .is_none_or(|segment| segment.cohort_id == cohort_id)
                })
                .collect::<Vec<_>>();
            report.committed_records += candidates.len();
            report.unsafe_records_skipped += unsafe_records_skipped;
            for record in candidates {
                let key = record_key(&record);
                let identity = record_identity(&record)?;
                let source_exists = snapshots.iter().any(|(_, snapshot)| {
                    snapshot.as_ref().is_some_and(|snapshot| {
                        snapshot.records.iter().any(|candidate| {
                            record_key(candidate) == key
                                && record_identity(candidate).ok().as_deref() == Some(&identity)
                        })
                    })
                });
                if !source_exists {
                    report.failed += 1;
                    continue;
                }
                let placement = if maintenance_token.is_some() {
                    None
                } else {
                    self.route_for_record(&record)
                        .await?
                        .as_ref()
                        .map(|route| placement_for_route(record.stream(), route))
                };
                for node in &targets {
                    let already = snapshots.iter().any(|(snapshot_node, snapshot)| {
                        snapshot_node.id == node.id
                            && snapshot.as_ref().is_some_and(|snapshot| {
                                snapshot.records.iter().any(|candidate| {
                                    record_key(candidate) == key
                                        && record_identity(candidate).ok().as_deref()
                                            == Some(&identity)
                                })
                            })
                    });
                    if already {
                        report.already_durable += 1;
                        continue;
                    }
                    let client = NodeClient::new(node.clone(), &self.client, &self.internal_token);
                    let append = match maintenance_token {
                        Some(token) => client.append_with_maintenance(&record, token).await,
                        None => {
                            client
                                .append_and_commit_many_with_placement(
                                    std::slice::from_ref(&record),
                                    placement.clone(),
                                )
                                .await
                        }
                    };
                    match append {
                        Ok(()) => {
                            if let Some(token) = maintenance_token {
                                match client.commit_with_maintenance(&record, token).await {
                                    Ok(()) => report.repaired += 1,
                                    Err(_) => report.failed += 1,
                                }
                            } else {
                                report.repaired += 1;
                            }
                        }
                        Err(_) => report.failed += 1,
                    }
                }
            }
        }
        Ok(report)
    }

    /// Repairs records for one tenant using an authenticated caller's
    /// committed prefix. Unlike ordinary rebalancing, a record present on only
    /// one survivor is eligible only when the caller supplies the watermark
    /// and that survivor is the sole source of an unambiguous contiguous copy;
    /// records above it are never copied. This is the deliberate recovery
    /// path for a gateway crash before asynchronous node commit markers.
    pub async fn repair(
        &self,
        stream: &str,
        committed_lsn: u64,
    ) -> Result<RebalanceReport, ReplicaError> {
        self.repair_with_certificate(stream, committed_lsn, None)
            .await
    }

    /// Repairs a bounded prefix, accepting a commit certificate when a
    /// surviving record has no quorum copy or durable node marker.
    pub async fn repair_with_certificate(
        &self,
        stream: &str,
        committed_lsn: u64,
        certificate: Option<&str>,
    ) -> Result<RebalanceReport, ReplicaError> {
        // Repair is serialized with appends for this stream only. The global
        // writer cache is published after the node I/O below, so recovery of
        // one stream cannot stall unrelated streams on a slow member.
        let _writer = self.stream_writer(stream).await?;
        self.ensure_initialized().await?;
        // Repair may be invoked after scale-out.  A target in the newest
        // cohort cannot be used as the sole source for the old prefix, and a
        // draining cohort still owns its immutable historical segments.
        let nodes = self.recovery_nodes_snapshot().await?;
        let snapshots = self.fetch_snapshots(&nodes, Some(stream)).await;
        let (records, unsafe_records_skipped) =
            self.watermark_repair_candidates(stream, committed_lsn, &snapshots)?;
        ensure_exact_watermark(0, committed_lsn, &records)?;
        if let Some(last) = records.last()
            && records.iter().any(|record| {
                let scoped = self
                    .snapshots_for_record(record, &snapshots)
                    .unwrap_or_default();
                !has_quorum_copy(record, &scoped, self.quorum)
                    && !has_durable_marker(record, &scoped)
            })
        {
            let certificate = certificate.ok_or(ReplicaError::CommitCertificateMissing)?;
            let route = self.route_for_record(last).await?;
            let (cohort_id, segment_start_lsn, segment_end_lsn, member_hash, segment_operation_id) =
                route
                    .as_ref()
                    .map(|segment| {
                        (
                            Some(segment.cohort_id),
                            Some(segment.start_lsn),
                            Some(segment.end_lsn),
                            Some(segment.member_hash.as_str()),
                            Some(segment.operation_id.as_str()),
                        )
                    })
                    .unwrap_or((None, None, None, None, None));
            let manifest = self.read_manifest_quorum().await?;
            if self.direct_mode() && manifest.is_none() {
                return Err(ReplicaError::QuorumUnavailable);
            }
            let manifest_revision = manifest.as_ref().map(|value| value.revision);
            let manifest_digest = manifest.as_ref().map(|value| value.digest.as_str());
            self.auth.verify_commit_certificate(
                certificate,
                stream,
                committed_lsn,
                last,
                cohort_id,
                segment_start_lsn,
                manifest_revision,
                manifest_digest,
                segment_end_lsn,
                member_hash,
                segment_operation_id,
            )?;
        }
        let repaired_records = records.clone();
        let mut report = RebalanceReport {
            committed_records: records.len(),
            unsafe_records_skipped,
            ..RebalanceReport::default()
        };
        for record in records {
            let key = record_key(&record);
            let identity = record_identity(&record)?;
            let scoped_snapshots = self.snapshots_for_record(&record, &snapshots)?;
            // A legacy control document has no segment entries. Its records
            // belong to bootstrap cohort zero, so use that cohort as the
            // repair target rather than accidentally copying history into a
            // newly activated cohort.
            let route = self.route_for_record(&record).await?;
            let placement = route
                .as_ref()
                .map(|route| placement_for_route(record.stream(), route));
            let repair_route = route.clone().or_else(|| {
                self.control().and_then(|control| {
                    control.state().ok().and_then(|state| {
                        state.cohorts.contains_key(&0).then_some(StreamSegment {
                            start_lsn: 1,
                            end_lsn: None,
                            cohort_id: 0,
                            member_ids: Vec::new(),
                            member_hash: String::new(),
                            writer_epoch: 0,
                            manifest_revision: 0,
                            placement_epoch: 0,
                            operation_id: String::new(),
                            tier: String::new(),
                            max_append_bytes: 0,
                        })
                    })
                })
            });
            let targets = self
                .replicas_for_repair_route(record.stream(), repair_route.as_ref())
                .await?;
            for node in &targets {
                let already = scoped_snapshots.iter().any(|(snapshot_node, snapshot)| {
                    snapshot_node.id == node.id
                        && snapshot.as_ref().is_some_and(|snapshot| {
                            snapshot.records.iter().any(|candidate| {
                                record_key(candidate) == key
                                    && record_identity(candidate).ok().as_deref() == Some(&identity)
                            })
                        })
                });
                if already {
                    report.already_durable += 1;
                    continue;
                }
                let client = NodeClient::new(node.clone(), &self.client, &self.internal_token);
                match client
                    .append_and_commit_many_with_placement(
                        std::slice::from_ref(&record),
                        placement.clone(),
                    )
                    .await
                {
                    Ok(()) => report.repaired += 1,
                    Err(_) => report.failed += 1,
                }
            }
        }
        if report.failed == 0 && report.unsafe_records_skipped == 0 {
            let mut writer_state = self.writer_state.lock().await;
            let stream_state = writer_state.streams.entry(stream.to_owned()).or_default();
            for record in repaired_records {
                if record.lsn() != stream_state.committed_lsn.saturating_add(1)
                    || record.committed_lsn() != stream_state.committed_lsn
                {
                    continue;
                }
                if record.writer_epoch() < stream_state.writer_epoch {
                    continue;
                }
                stream_state.writer_epoch = record.writer_epoch();
                stream_state.committed_lsn = record.lsn();
                stream_state.records.insert(record.lsn(), record);
            }
        }
        Ok(report)
    }

    async fn collect_candidates(&self, stream: &str) -> CandidateResult {
        let nodes = self.recovery_nodes_snapshot().await?;
        let snapshots = self
            .fetch_snapshots(&nodes, (stream != "*").then_some(stream))
            .await;
        self.collect_candidates_from_snapshots(stream, &snapshots)
    }

    fn collect_candidates_from_snapshots(
        &self,
        stream: &str,
        snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
    ) -> CandidateResult {
        let mut candidates = BTreeMap::new();
        let control_state = self.control().map(|control| control.state()).transpose()?;

        // Group exact bytes by LSN and count distinct node ids. A key with two
        // different quorum-sized values is intentionally ambiguous.
        let mut groups: EvidenceGroups = BTreeMap::new();
        let mut invalid_keys = BTreeSet::new();
        for (node, snapshot) in snapshots.iter() {
            let Some(snapshot) = snapshot else { continue };
            for record in &snapshot.records {
                if stream != "*" && record.stream() != stream {
                    continue;
                }
                if !self.node_owns_record(control_state.as_ref(), node, record) {
                    continue;
                }
                let key = record_key(record);
                if record.committed_lsn() >= record.lsn() {
                    // The prior-cLSN relation is part of the record's
                    // authenticated ordering evidence. A malformed or
                    // speculative record is never a repair source, even if
                    // it happens to occur on multiple nodes.
                    invalid_keys.insert(key);
                    continue;
                }
                let identity = record_identity(record)?;
                let entry = groups
                    .entry(key)
                    .or_default()
                    .entry(identity)
                    .or_insert_with(|| (record.clone(), BTreeSet::new(), BTreeSet::new()));
                entry.1.insert(node.id.clone());
                if snapshot
                    .committed
                    .iter()
                    .any(|committed| committed == record)
                {
                    entry.2.insert(node.id.clone());
                }
            }
        }
        let mut unsafe_records_skipped = invalid_keys
            .iter()
            .filter(|key| !groups.contains_key(*key))
            .count();
        for (key, identities) in groups {
            // The owners that answered this pass. A member that is silent
            // (fenced for a swap, restarting) contributes no evidence either
            // way, so an LSN is discarded only when a read quorum of the
            // members that did answer confirms it absent.
            let sample = identities
                .values()
                .next()
                .map(|(record, _, _)| record.clone());
            let answering_owners = sample.as_ref().map_or(0, |record| {
                snapshots
                    .iter()
                    .filter(|(node, snapshot)| {
                        snapshot.is_some()
                            && self.node_owns_record(control_state.as_ref(), node, record)
                    })
                    .count()
            });
            let eligible = identities
                .into_values()
                // Data copies are only a speculative append until their local
                // durable commit markers reach quorum. In particular, two
                // matching records without markers must not be resurrected
                // as an ordinary committed tail after a gateway restart. A
                // record whose durable marker one answering owner holds is
                // only discarded once a read quorum of answering owners has
                // confirmed it absent; while a member is silent, its copy may
                // be the one that completed the acknowledged quorum.
                .filter(|(_, data_nodes, committed_nodes)| {
                    committed_nodes.len() >= self.quorum
                        || (!committed_nodes.is_empty()
                            && answering_owners.saturating_sub(data_nodes.len()) < self.quorum)
                })
                .collect::<Vec<_>>();
            if eligible.len() == 1 {
                candidates.insert(key, eligible[0].0.clone());
            } else {
                // Both conflicting quorum values and records without enough
                // evidence are excluded. The latter is the partial-write
                // safety rule.
                unsafe_records_skipped += 1;
            }
        }
        Ok((candidates, unsafe_records_skipped))
    }

    /// Rejects a matching data quorum that never reached the durable marker
    /// boundary. Such evidence is stronger than a harmless one-node partial,
    /// but still cannot be treated as committed: omitting it would let a
    /// fresh writer reuse the LSN and fork an acknowledged pre-49a6 tail.
    fn reject_ambiguous_quorum_records(
        &self,
        stream: &str,
        after_lsn: u64,
        snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
    ) -> Result<(), ReplicaError> {
        let control_state = self.control().map(|control| control.state()).transpose()?;
        let mut groups: EvidenceGroups = BTreeMap::new();
        for (node, snapshot) in snapshots {
            let Some(snapshot) = snapshot else { continue };
            for record in &snapshot.records {
                if record.lsn() <= after_lsn
                    || stream != "*" && record.stream() != stream
                    || record.committed_lsn() >= record.lsn()
                    || !self.node_owns_record(control_state.as_ref(), node, record)
                {
                    continue;
                }
                let key = record_key(record);
                let identity = record_identity(record)?;
                let entry = groups
                    .entry(key)
                    .or_default()
                    .entry(identity)
                    .or_insert_with(|| (record.clone(), BTreeSet::new(), BTreeSet::new()));
                entry.1.insert(node.id.clone());
                if snapshot
                    .committed
                    .iter()
                    .any(|committed| committed == record)
                {
                    entry.2.insert(node.id.clone());
                }
            }
        }
        for ((_, lsn), identities) in groups {
            for (_, data_nodes, committed_nodes) in identities.into_values() {
                if data_nodes.len() >= self.quorum && committed_nodes.len() < self.quorum {
                    return Err(ReplicaError::RecoveryAmbiguous {
                        lsn,
                        committed_nodes: committed_nodes.len(),
                        quorum: self.quorum,
                    });
                }
            }
        }
        Ok(())
    }

    fn watermark_candidates(
        &self,
        stream: &str,
        after_lsn: u64,
        committed_lsn: u64,
        snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        let mut groups: BTreeMap<u64, BTreeMap<Vec<u8>, WatermarkGroup>> = BTreeMap::new();
        let control_state = self.control().map(|control| control.state()).transpose()?;
        for (node, snapshot) in snapshots {
            let Some(snapshot) = snapshot else { continue };
            for record in &snapshot.records {
                if record.stream() != stream
                    || record.lsn() <= after_lsn
                    || record.lsn() > committed_lsn
                    || record.committed_lsn() >= record.lsn()
                    || record.committed_lsn() > committed_lsn
                {
                    continue;
                }
                if !self.node_owns_record(control_state.as_ref(), node, record) {
                    continue;
                }
                let entry = groups
                    .entry(record.lsn())
                    .or_default()
                    .entry(record_identity(record)?);
                let entry =
                    entry.or_insert_with(|| (record.clone(), BTreeSet::new(), BTreeSet::new()));
                entry.1.insert(node.id.clone());
                if snapshot
                    .committed
                    .iter()
                    .any(|committed| committed == record)
                {
                    entry.2.insert(node.id.clone());
                }
            }
        }
        Ok(groups
            .into_values()
            .filter_map(|values| {
                if values.len() != 1 {
                    return None;
                }
                let (record, data_nodes, _) = values.into_values().next()?;
                (!data_nodes.is_empty()).then_some(record)
            })
            .collect())
    }

    fn watermark_repair_candidates(
        &self,
        stream: &str,
        committed_lsn: u64,
        snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
    ) -> Result<(Vec<EncryptedRecord>, usize), ReplicaError> {
        let mut groups: BTreeMap<u64, BTreeMap<Vec<u8>, WatermarkGroup>> = BTreeMap::new();
        let control_state = self.control().map(|control| control.state()).transpose()?;
        for (node, snapshot) in snapshots {
            let Some(snapshot) = snapshot else { continue };
            for record in &snapshot.records {
                if record.stream() != stream
                    || record.lsn() > committed_lsn
                    || record.committed_lsn() >= record.lsn()
                    || record.committed_lsn() > committed_lsn
                {
                    continue;
                }
                if !self.node_owns_record(control_state.as_ref(), node, record) {
                    continue;
                }
                let entry = groups
                    .entry(record.lsn())
                    .or_default()
                    .entry(record_identity(record)?);
                let entry =
                    entry.or_insert_with(|| (record.clone(), BTreeSet::new(), BTreeSet::new()));
                entry.1.insert(node.id.clone());
                if snapshot
                    .committed
                    .iter()
                    .any(|committed| committed == record)
                {
                    entry.2.insert(node.id.clone());
                }
            }
        }
        // A caller's watermark is a claim about a contiguous prefix, not a
        // license to copy an isolated higher LSN. Stop at the first missing,
        // conflicting, or incorrectly chained record and report the pass as
        // incomplete so an operator can retry after recovering its source.
        let mut skipped = 0;
        let mut records = Vec::new();
        let mut expected = 1_u64;
        let mut expected_epoch = 0_u64;
        while expected <= committed_lsn {
            let Some(values) = groups.get(&expected) else {
                skipped += 1;
                break;
            };
            if values.len() != 1 {
                skipped += 1;
                break;
            }
            let Some((record, data_nodes, _)) = values.values().next() else {
                skipped += 1;
                break;
            };
            if data_nodes.is_empty() {
                skipped += 1;
                break;
            }
            if record.committed_lsn() != expected.saturating_sub(1)
                || record.writer_epoch() < expected_epoch
            {
                skipped += 1;
                break;
            }
            records.push(record.clone());
            expected_epoch = record.writer_epoch();
            if expected == committed_lsn {
                break;
            }
            expected = expected.saturating_add(1);
        }
        Ok((records, skipped))
    }

    async fn fetch_snapshots(
        &self,
        nodes: &[ReplicaNode],
        stream: Option<&str>,
    ) -> Vec<(ReplicaNode, Option<NodeSnapshot>)> {
        self.fetch_snapshot_results(nodes, stream)
            .await
            .into_iter()
            .map(|(node, result)| (node, result.ok()))
            .collect()
    }

    /// Fetches node snapshots while retaining the transport/protocol error.
    /// Recovery uses this to distinguish an unavailable owner from a healthy
    /// owner whose stream is empty; callers doing best-effort anti-entropy can
    /// continue to use [`Self::fetch_snapshots`].
    /// Whether the archive holds exactly this record at its LSN. A writer that
    /// lost the acknowledgement of a record the cohort has since archived and
    /// trimmed resends it; the archive, not the emptied hot log, answers.
    async fn archived_record_matches(
        &self,
        record: &EncryptedRecord,
    ) -> Result<bool, ReplicaError> {
        let archived = self
            .archive
            .recover(record.stream(), record.lsn().saturating_sub(1))
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        Ok(archived
            .iter()
            .find(|candidate| candidate.lsn() == record.lsn())
            .is_some_and(|candidate| candidate == record))
    }

    /// Re-replicates certified hot records to the answering owners whose
    /// snapshot lacks them. Failures are left to the next append, which
    /// simply fails to reach quorum and is retried by the writer; nothing
    /// here can fork the log because every record is already certified.
    async fn repair_under_replicated(
        &self,
        stream: &str,
        snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
        certified: &BTreeMap<u64, EncryptedRecord>,
    ) {
        let control_state = self.control().and_then(|control| control.state().ok());
        for (node, snapshot) in snapshots {
            let Some(snapshot) = snapshot else { continue };
            let present = snapshot
                .committed
                .iter()
                .filter(|record| record.stream() == stream)
                .map(EncryptedRecord::lsn)
                .collect::<BTreeSet<_>>();
            let missing = certified
                .values()
                .filter(|record| {
                    !present.contains(&record.lsn())
                        && self.node_owns_record(control_state.as_ref(), node, record)
                })
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                continue;
            }
            let client = NodeClient::new(node.clone(), &self.client, &self.internal_token);
            for record in missing {
                let placement = match self.route_for_record(&record).await {
                    Ok(route) => route
                        .as_ref()
                        .map(|route| placement_for_route(record.stream(), route)),
                    Err(_) => break,
                };
                if client
                    .append_and_commit_many_with_placement(
                        std::slice::from_ref(&record),
                        placement.clone(),
                    )
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    /// Bring one member of a stream's cohort up to date: append, in LSN order,
    /// every record it is missing that is known to be committed.
    ///
    /// A member that was down or cut off while its peers kept writing holds a
    /// prefix of the log. It refuses the next append for want of a
    /// predecessor, correctly - a member counts towards a quorum only for
    /// positions it actually holds - so until it catches up the cohort runs on
    /// two copies, and losing either stops writes. This closes the gap.
    ///
    /// A record is copied only if it is committed: a later record carries a
    /// commit watermark at or above it, or this coordinator acknowledged it.
    /// Copies of one LSN must agree byte for byte, the chain must be
    /// contiguous with each record naming its predecessor as committed, and
    /// writer epochs must not go backwards; the member's own predecessor and
    /// epoch checks apply to every append as well. A range the peers have
    /// already trimmed is read back from the archive. Returns the LSN range
    /// appended, none when the member was already complete.
    pub async fn catch_up_member(
        &self,
        stream: &str,
        member: &ReplicaNode,
    ) -> Result<Option<std::ops::RangeInclusive<u64>>, ReplicaError> {
        let cohort = self.replicas_for_stream(stream).await?;
        if !cohort.iter().any(|node| node.id == member.id) {
            return Ok(None);
        }
        let snapshots = self.fetch_snapshots(&cohort, Some(stream)).await;
        let Some(own) = snapshots
            .iter()
            .find(|(node, _)| node.id == member.id)
            .and_then(|(_, snapshot)| snapshot.as_ref())
        else {
            return Err(ReplicaError::NodeUnavailable);
        };
        let floor = own
            .trimmed
            .iter()
            .filter(|prefix| prefix.stream == stream)
            .map(|prefix| prefix.archived_lsn)
            .max()
            .unwrap_or(0);
        let own_records: BTreeMap<u64, &EncryptedRecord> = own
            .records
            .iter()
            .filter(|record| record.stream() == stream && record.lsn() > floor)
            .map(|record| (record.lsn(), record))
            .collect();

        // Every copy the peers hold above the member's floor, with the
        // highest commit watermark any of them proves.
        let mut copies: BTreeMap<u64, EncryptedRecord> = BTreeMap::new();
        let mut certified = 0_u64;
        let mut peers_trimmed = 0_u64;
        let mut disagreement: Option<u64> = None;
        for (node, snapshot) in &snapshots {
            if node.id == member.id {
                continue;
            }
            let Some(snapshot) = snapshot else { continue };
            peers_trimmed = peers_trimmed.max(
                snapshot
                    .trimmed
                    .iter()
                    .filter(|prefix| prefix.stream == stream)
                    .map(|prefix| prefix.archived_lsn)
                    .max()
                    .unwrap_or(0),
            );
            for record in snapshot
                .records
                .iter()
                .filter(|record| record.stream() == stream && record.lsn() > floor)
            {
                certified = certified.max(record.committed_lsn());
                match copies.get(&record.lsn()) {
                    // Two peers disagree: nothing from here on is certain
                    // enough to copy.
                    Some(existing) if existing != record => {
                        disagreement =
                            Some(disagreement.map_or(record.lsn(), |at: u64| at.min(record.lsn())));
                    }
                    Some(_) => {}
                    None => {
                        copies.insert(record.lsn(), record.clone());
                    }
                }
            }
        }
        if let Some(at) = disagreement {
            copies.split_off(&at);
        }
        {
            let state = self.writer_state.lock().await;
            if let Some(acknowledged) = state.streams.get(stream) {
                certified = certified.max(acknowledged.committed_lsn);
                for (lsn, record) in acknowledged.records.range(floor.saturating_add(1)..) {
                    if *lsn > acknowledged.committed_lsn {
                        break;
                    }
                    if copies.get(lsn).is_some_and(|existing| existing != record) {
                        return Err(ReplicaError::LsnConflict);
                    }
                    copies.entry(*lsn).or_insert_with(|| record.clone());
                }
            }
        }
        // What the peers have already archived and trimmed comes back from
        // the archive, which holds exactly the committed prefix.
        if peers_trimmed > floor {
            for record in self
                .archive
                .recover(stream, floor)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
            {
                if record.lsn() > peers_trimmed {
                    break;
                }
                certified = certified.max(record.lsn());
                if copies
                    .get(&record.lsn())
                    .is_some_and(|existing| existing != &record)
                {
                    return Err(ReplicaError::LsnConflict);
                }
                copies.insert(record.lsn(), record);
            }
        }

        // How far the member agrees with the committed log. Where it holds a
        // record the committed log replaced, left by an older writer that
        // never reached a quorum, that suffix is withdrawn first.
        let mut position = floor;
        let mut own_epoch = own
            .trimmed
            .iter()
            .filter(|prefix| prefix.stream == stream)
            .map(|prefix| prefix.writer_epoch)
            .max()
            .unwrap_or(0);
        let client = NodeClient::new(member.clone(), &self.client, &self.internal_token);
        for (lsn, record) in &own_records {
            if *lsn != position.saturating_add(1) {
                break;
            }
            match copies.get(lsn) {
                Some(copy) if copy == *record => {
                    position = *lsn;
                    own_epoch = own_epoch.max(record.writer_epoch());
                }
                Some(copy)
                    if *lsn <= certified
                        && own_records
                            .range(lsn..)
                            .all(|(_, orphan)| orphan.writer_epoch() < copy.writer_epoch()) =>
                {
                    client.supersede(stream, *lsn, copy.writer_epoch()).await?;
                    eprintln!(
                        "lakeday.replica catch_up stream={stream} member={} superseded_from_lsn={lsn}",
                        member.id
                    );
                    break;
                }
                // Holding a record nobody else can vouch for, or one the
                // committed log differs on for a writer at least as new:
                // nothing to do here that is certainly right.
                _ => break,
            }
        }

        let mut missing = Vec::new();
        let mut expected = position.saturating_add(1);
        for (lsn, record) in copies.range(expected..) {
            if *lsn != expected
                || *lsn > certified
                || record.committed_lsn() != lsn.saturating_sub(1)
                || record.writer_epoch() < own_epoch
            {
                break;
            }
            own_epoch = record.writer_epoch();
            missing.push(record.clone());
            expected = expected.saturating_add(1);
        }
        let Some(first) = missing.first().map(EncryptedRecord::lsn) else {
            return Ok(None);
        };
        let last = missing.last().map_or(first, EncryptedRecord::lsn);
        // The member was away while the stream's route moved on, so its
        // placement fence may name a route that is gone. Committed history is
        // copied under the stream's current route: the member is fenced to it
        // first, exactly as a route change fences every member, and an older
        // coordinator's late append is refused from then on.
        let placement = match self.read_manifest_quorum().await? {
            Some(manifest) => {
                let route = manifest
                    .stream_segments
                    .get(stream)
                    .and_then(|segments| segments.last())
                    .cloned()
                    .ok_or(ReplicaError::QuorumUnavailable)?;
                let placement = placement_for_route(stream, &route);
                client.install_placement_fence(stream, &placement).await?;
                Some(placement)
            }
            None => None,
        };
        // An append is one writer epoch.
        let mut batch: Vec<EncryptedRecord> = Vec::new();
        for record in missing {
            if batch.last().is_some_and(|previous: &EncryptedRecord| {
                previous.writer_epoch() != record.writer_epoch()
            }) || batch.len() >= CATCH_UP_BATCH
            {
                client
                    .append_and_commit_many_with_placement(&batch, placement.clone())
                    .await?;
                batch.clear();
            }
            batch.push(record);
        }
        if !batch.is_empty() {
            client
                .append_and_commit_many_with_placement(&batch, placement)
                .await?;
        }
        eprintln!(
            "lakeday.replica catch_up stream={stream} member={} from_lsn={first} to_lsn={last}",
            member.id
        );
        Ok(Some(first..=last))
    }

    /// Bring the local member up to date on every stream its cohort routes to
    /// it, as a restarted replica does before it can count towards a quorum
    /// again. Returns how many records it appended.
    pub async fn catch_up_local(&self) -> Result<u64, ReplicaError> {
        let Some(control) = self.control() else {
            return Ok(0);
        };
        let me = self
            .nodes_snapshot()
            .await?
            .into_iter()
            .find(|member| member.id == self.local_member_id)
            .ok_or(ReplicaError::NodeUnavailable)?;
        let streams = control.with_state(|state| {
            state
                .stream_segments
                .iter()
                .filter(|(_, segments)| {
                    segments.last().is_some_and(|segment| {
                        segment
                            .member_ids
                            .iter()
                            .any(|id| id == &self.local_member_id)
                    })
                })
                .map(|(stream, _)| stream.clone())
                .collect::<Vec<_>>()
        })?;
        let mut appended = 0_u64;
        for stream in streams {
            if let Some(range) = self.catch_up_member(&stream, &me).await? {
                appended = appended.saturating_add(range.count() as u64);
            }
        }
        Ok(appended)
    }

    /// Fetches every member's snapshot. The phase ends once a quorum answered
    /// and the stragglers had their grace; a member that stays silent is
    /// reported unavailable, exactly as one whose request timed out, so the
    /// recovery rules that tolerate one unavailable owner apply unchanged.
    async fn fetch_snapshot_results(
        &self,
        nodes: &[ReplicaNode],
        stream: Option<&str>,
    ) -> Vec<(ReplicaNode, Result<NodeSnapshot, ReplicaError>)> {
        let quorum = self.quorum;
        let stream = stream.map(ToOwned::to_owned);
        let answers = fan_out(
            nodes,
            |node| {
                let client = self.client.clone();
                let internal_token = self.internal_token.clone();
                let stream = stream.clone();
                async move {
                    NodeClient::new(node, &client, &internal_token)
                        .snapshot(stream.as_deref())
                        .await
                }
            },
            |answers| answers.iter().flatten().filter(|r| r.is_ok()).count() >= quorum,
            Some(straggler_grace),
        )
        .await;
        nodes
            .iter()
            .cloned()
            .zip(answers)
            .map(|(node, answer)| (node, answer.unwrap_or(Err(ReplicaError::NodeUnavailable))))
            .collect()
    }

    /// Fetches snapshots for ordinary no-certificate recovery. Every owning
    /// cohort must supply a quorum, while an unavailable trailing member is
    /// retained as missing evidence instead of being treated as empty.
    async fn fetch_ordinary_recovery_snapshots(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<(ReplicaNode, Option<NodeSnapshot>)>, ReplicaError> {
        let nodes = self.recovery_nodes_snapshot().await?;
        let owner_groups = self.owning_member_groups(stream, &nodes, after_lsn).await?;
        let results = self
            .fetch_snapshot_results(&nodes, (stream != "*").then_some(stream))
            .await;
        for owners in &owner_groups {
            let available = results
                .iter()
                .filter(|(node, result)| owners.contains(&node.id) && result.is_ok())
                .count();
            if available < self.quorum {
                return Err(results
                    .iter()
                    .find_map(|(node, result)| {
                        owners
                            .contains(&node.id)
                            .then(|| result.as_ref().err().cloned())
                            .flatten()
                    })
                    .unwrap_or(ReplicaError::QuorumUnavailable));
            }
        }
        Ok(results
            .into_iter()
            .map(|(node, result)| (node, result.ok()))
            .collect())
    }

    /// Returns every member group that can own the requested stream tail.
    /// Historical cohorts remain separate so each must independently provide
    /// enough evidence for safe recovery.
    async fn owning_member_groups(
        &self,
        stream: &str,
        nodes: &[ReplicaNode],
        after_lsn: u64,
    ) -> Result<Vec<BTreeSet<String>>, ReplicaError> {
        let mut owner_groups: Vec<BTreeSet<String>> = Vec::new();
        if self.control().is_none() {
            owner_groups.push(
                select_static_replicas(nodes)
                    .into_iter()
                    .map(|node| node.id)
                    .collect(),
            );
        } else {
            let manifest = self
                .read_manifest_quorum()
                .await?
                .ok_or(ReplicaError::QuorumUnavailable)?;
            if let Some(segments) = manifest.stream_segments.get(stream) {
                for segment in segments {
                    if segment.member_ids.len() != REPLICATION_FACTOR {
                        return Err(ReplicaError::Protocol(
                            "placement manifest stream range has an incomplete owner set"
                                .to_owned(),
                        ));
                    }
                    // A sealed range at or before the caller's archive
                    // watermark is already covered by immutable object-store
                    // evidence. Its retired owners are deliberately absent
                    // from the hot membership and must not block tail
                    // recovery; only ranges that can still contribute records
                    // after `after_lsn` require live owner responses.
                    if segment.end_lsn.is_some_and(|end| end <= after_lsn) {
                        continue;
                    }
                    owner_groups.push(segment.member_ids.iter().cloned().collect());
                }
            } else {
                let control = self.control().ok_or(ReplicaError::QuorumUnavailable)?;
                let state = control.state()?;
                let cohort_id = cohort_for_stream(&state, stream)?;
                let cohort = state
                    .cohorts
                    .get(&cohort_id)
                    .ok_or(ReplicaError::QuorumUnavailable)?;
                if cohort.members.len() != REPLICATION_FACTOR {
                    return Err(ReplicaError::QuorumUnavailable);
                }
                owner_groups.push(cohort.members.iter().cloned().collect());
            }
        }
        if owner_groups
            .iter()
            .flatten()
            .any(|id| !nodes.iter().any(|node| node.id == *id))
        {
            return Err(ReplicaError::QuorumUnavailable);
        }
        Ok(owner_groups)
    }

    async fn fetch_storage_statuses(
        &self,
        nodes: &[ReplicaNode],
    ) -> Result<Vec<StorageNodeStatus>, ReplicaError> {
        let sampled_at = unix_time_ms();
        let statuses = join_all(nodes.iter().cloned().map(|node| async move {
            let client = NodeClient::new(node, &self.client, &self.internal_token);
            client.storage_status().await
        }))
        .await;
        let now = unix_time_ms().max(sampled_at);
        statuses
            .into_iter()
            .map(|status| {
                let status = status?;
                if !fresh_storage_status(&status, now, self.metrics_max_age) {
                    return Err(ReplicaError::NodeUnavailable);
                }
                Ok(status)
            })
            .collect()
    }

    /// Builds the client-facing and private membership router.
    pub fn router(self: Arc<Self>) -> Router {
        gateway_router(self)
    }
}

/// Rebuilds the writer fence and only the largest contiguous committed prefix
/// represented by quorum candidates. The highest observed epoch is retained
/// even when its record is a one-node partial: that partial is not returned or
/// promoted, but it must still fence an older writer after gateway restart.
fn rebuild_writer_state(
    candidates: &CandidateMap,
    snapshots: &[(ReplicaNode, Option<NodeSnapshot>)],
    quorum: usize,
) -> GatewayWriterState {
    let mut rebuilt = GatewayWriterState {
        initialized: true,
        membership: Vec::new(),
        streams: BTreeMap::new(),
    };
    for (_, snapshot) in snapshots {
        let Some(snapshot) = snapshot else { continue };
        for prefix in &snapshot.trimmed {
            let stream_state = rebuilt.streams.entry(prefix.stream.clone()).or_default();
            stream_state.writer_epoch = stream_state.writer_epoch.max(prefix.writer_epoch);
        }
        for record in &snapshot.records {
            let stream_state = rebuilt
                .streams
                .entry(record.stream().to_owned())
                .or_default();
            stream_state.writer_epoch = stream_state.writer_epoch.max(record.writer_epoch());
        }
    }

    let mut trim_watermarks: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (_, snapshot) in snapshots {
        let Some(snapshot) = snapshot else { continue };
        for prefix in &snapshot.trimmed {
            trim_watermarks
                .entry(prefix.stream.clone())
                .or_default()
                .push(prefix.archived_lsn);
        }
    }
    for (stream, mut watermarks) in trim_watermarks {
        watermarks.sort_unstable_by(|left, right| right.cmp(left));
        if let Some(quorum_watermark) = watermarks.get(quorum.saturating_sub(1)).copied() {
            rebuilt.streams.entry(stream).or_default().committed_lsn = quorum_watermark;
        }
    }

    let mut by_stream: BTreeMap<String, Vec<EncryptedRecord>> = BTreeMap::new();
    for ((stream, _), record) in candidates {
        by_stream
            .entry(stream.clone())
            .or_default()
            .push(record.clone());
    }
    for (stream, mut records) in by_stream {
        records.sort_by_key(EncryptedRecord::lsn);
        let stream_state = rebuilt.streams.entry(stream).or_default();
        let mut expected_lsn = stream_state.committed_lsn.saturating_add(1);
        let mut prefix_epoch = 0_u64;
        for record in records {
            if record.lsn() < expected_lsn {
                continue;
            }
            if record.lsn() != expected_lsn
                || record.committed_lsn() != expected_lsn.saturating_sub(1)
                || record.writer_epoch() < prefix_epoch
            {
                break;
            }
            prefix_epoch = record.writer_epoch();
            stream_state.committed_lsn = record.lsn();
            stream_state.records.insert(record.lsn(), record);
            expected_lsn = expected_lsn.saturating_add(1);
        }
    }
    rebuilt
}

#[async_trait]
impl walleye_bitr::ReplicaGateway for ReplicaGateway {
    async fn append(&self, record: EncryptedRecord) -> Result<Option<String>, ReplicaError> {
        ReplicaGateway::append(self, record.clone()).await?;
        self.issue_commit_certificate(&record).await.map(Some)
    }

    async fn append_many(
        &self,
        records: Vec<EncryptedRecord>,
    ) -> Result<Option<String>, ReplicaError> {
        let last = records
            .last()
            .cloned()
            .ok_or_else(|| ReplicaError::Protocol("append batch must not be empty".to_owned()))?;
        ReplicaGateway::append_many(self, records).await?;
        self.issue_commit_certificate(&last).await.map(Some)
    }

    async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ReplicaError> {
        ReplicaGateway::recover(self, stream, after_lsn).await
    }

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
        ReplicaGateway::recover_with_watermark_certificate(
            self,
            stream,
            after_lsn,
            Some(committed_lsn),
            Some(certificate),
        )
        .await
    }
}

/// Builds the gateway router around a shared gateway state.
pub fn gateway_router(gateway: Arc<ReplicaGateway>) -> Router {
    Router::new()
        .route("/healthz", get(gateway_health))
        .route("/controlz", get(gateway_control_ready))
        .route("/readyz", get(gateway_ready))
        .route("/v1/append", post(gateway_append))
        .route("/v1/append-many", post(gateway_append_many))
        .route("/v1/records", get(gateway_records))
        .route("/v1/admin/repair", post(gateway_repair))
        .route("/v1/admin/status", get(admin_status))
        .route("/v1/admin/rebalance", post(admin_rebalance))
        .route("/internal/v1/maintenance/fence", post(begin_maintenance))
        .route(
            "/internal/v1/maintenance/fence/{token}",
            delete(end_maintenance),
        )
        .route("/internal/v1/nodes", get(list_nodes))
        .route("/internal/v1/membership", get(membership_status_route))
        .route("/internal/v1/control", get(gateway_control_state))
        .route("/internal/v1/membership/cas", post(membership_cas_route))
        .route("/internal/v1/membership/join", post(membership_join_route))
        .route(
            "/internal/v1/membership/join-online",
            post(membership_join_online_route),
        )
        .route(
            "/internal/v1/membership/replace",
            post(membership_replace_route),
        )
        .route(
            "/internal/v1/membership/activate/{id}",
            post(membership_activate_route),
        )
        .route(
            "/internal/v1/membership/activate-cohort/{id}",
            post(membership_activate_cohort_route),
        )
        .route(
            "/internal/v1/membership/activate-cohort",
            post(membership_activate_cohort_body_route),
        )
        .route(
            "/internal/v1/membership/activate-cohort-online",
            post(membership_activate_cohort_online_route),
        )
        .route(
            "/internal/v1/membership/replace-cohort-online",
            post(membership_replace_cohort_online_route),
        )
        .route(
            "/internal/v1/membership/retire-cohort-online",
            post(membership_retire_cohort_online_route),
        )
        .route(
            "/internal/v1/membership/drain/{id}",
            post(membership_drain_route),
        )
        .route(
            "/internal/v1/membership/cutover/{id}",
            post(membership_cutover_cohort_route),
        )
        .route(
            "/internal/v1/membership/archive/{id}",
            post(membership_archive_cohort_route),
        )
        .route(
            "/internal/v1/membership/remove/{id}",
            post(membership_remove_route).delete(membership_remove_route),
        )
        .route("/internal/v1/metrics", get(gateway_metrics))
        .route("/internal/v1/status", get(gateway_metrics))
        .route("/internal/v1/node/metrics", get(gateway_metrics))
        .route("/internal/v1/node/status", get(gateway_metrics))
        .route("/internal/v1/storage/metrics", get(gateway_metrics))
        .route("/internal/v1/storage/status", get(gateway_metrics))
        .route("/internal/v1/rebalance", post(rebalance))
        .with_state(GatewayState { gateway })
        .layer(DefaultBodyLimit::max(MAX_ENCRYPTED_RECORD_BATCH_BYTES))
}

async fn gateway_health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn gateway_control_ready(State(state): State<GatewayState>) -> StatusCode {
    if state.gateway.ensure_initialized().await.is_err() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if !state.gateway.local_member_id.is_empty() {
        let Ok(membership) = state.gateway.membership().await else {
            return StatusCode::SERVICE_UNAVAILABLE;
        };
        if !membership.members.iter().any(|member| {
            member.id == state.gateway.local_member_id && member.status == MemberStatus::Active
        }) {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    StatusCode::NO_CONTENT
}

async fn gateway_metrics(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<GatewayMetrics>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .gateway
        .metrics()
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

/// Readiness to accept durable writes: at least `quorum` members answering,
/// initialized, and not fenced for maintenance. Members are probed first,
/// because a quorum shortage is both the usual reason a gateway is not ready
/// and the reason it cannot initialize; a refusal names the members that did
/// not answer so an operator can see which node is at fault.
/// `?require=all` makes readiness strict: every configured member must be
/// serving, not just a quorum. A caller that reaches the cluster through one
/// address cannot otherwise tell a whole cluster from a quorum of one.
#[derive(Deserialize)]
struct ReadyQuery {
    #[serde(default)]
    require: Option<String>,
}

async fn gateway_ready(
    State(state): State<GatewayState>,
    Query(query): Query<ReadyQuery>,
) -> Response {
    let strict = query.require.as_deref() == Some("all");
    let gateway = Arc::clone(&state.gateway);
    let quorum = gateway.quorum;
    let members = gateway.nodes_snapshot().await;
    let probes = match &members {
        Ok(nodes) => {
            join_all(nodes.iter().map(|node| {
                let node = node.clone();
                let gateway = Arc::clone(&gateway);
                async move {
                    let healthy =
                        NodeClient::new(node.clone(), &gateway.client, &gateway.internal_token)
                            .health()
                            .await
                            .is_ok();
                    (node.id, healthy)
                }
            }))
            .await
        }
        Err(_) => Vec::new(),
    };
    let (healthy, unreachable): (Vec<_>, Vec<_>) =
        probes.into_iter().partition(|(_, healthy)| *healthy);
    let healthy = healthy.into_iter().map(|(id, _)| id).collect::<Vec<_>>();
    let unreachable = unreachable
        .into_iter()
        .map(|(id, _)| id)
        .collect::<Vec<_>>();
    let not_ready = |reason: String| -> Response {
        let mut body = serde_json::json!({"ready": false, "reason": reason});
        if members.is_ok() {
            body["quorum"] = serde_json::json!(quorum);
            body["healthy"] = serde_json::json!(healthy);
            body["unreachable"] = serde_json::json!(unreachable);
        }
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    };
    if members.is_err() {
        return not_ready("the member list could not be read".to_owned());
    }
    if healthy.len() < quorum {
        return not_ready(format!(
            "{} of {quorum} required members are reachable",
            healthy.len()
        ));
    }
    if strict && !unreachable.is_empty() {
        return not_ready(format!(
            "{} of {} members are serving; strict readiness requires all",
            healthy.len(),
            healthy.len() + unreachable.len()
        ));
    }
    if state.gateway.ensure_initialized().await.is_err() {
        return not_ready("the gateway is still initializing".to_owned());
    }
    if state.gateway.refresh_durable_fence().await.is_err() {
        return not_ready("the durable fence could not be refreshed".to_owned());
    }
    if state.gateway.maintenance_active().await {
        return not_ready("maintenance is fencing writes on this gateway".to_owned());
    }
    // A ready answer names who is serving, as a refusal names who is not: a
    // caller with one address can then tell a whole cluster from a quorum.
    // It also names the log itself, by where its committed segments are
    // archived: every coordinator of one log answers the same whatever
    // address the caller reached it at and however its membership changes,
    // and no other log does. A writer names the log it appends to by it.
    let log = log_digest(&state.gateway.archive.location());
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ready": true,
            "quorum": quorum,
            "healthy": healthy,
            "unreachable": unreachable,
            "log": log,
        })),
    )
        .into_response()
}

/// A stable name for one log: where its committed segments are archived.
fn log_digest(location: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lakeday-cloud/log/v1\0");
    digest.update(location.as_bytes());
    hex::encode(digest.finalize())
}

#[derive(Serialize)]
struct CapacityExceededResponse {
    code: &'static str,
    retryable: bool,
    requested_bytes: u64,
    max_append_bytes: u64,
    cohort_id: u64,
}

fn gateway_append_error(error: ReplicaError) -> Response {
    let status = match &error {
        ReplicaError::WriterFenced | ReplicaError::LsnConflict => StatusCode::CONFLICT,
        ReplicaError::InvalidWatermark { .. } => StatusCode::BAD_REQUEST,
        ReplicaError::CapacityExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    if let ReplicaError::CapacityExceeded {
        cohort_id,
        requested_bytes,
        max_append_bytes,
    } = error
    {
        let mut response = Json(CapacityExceededResponse {
            code: CAPACITY_EXCEEDED_CODE,
            retryable: false,
            requested_bytes,
            max_append_bytes,
            cohort_id,
        })
        .into_response();
        *response.status_mut() = status;
        return response;
    }
    // A writer must be able to tell a fence from an ordering conflict and
    // from a transient outage: the first is terminal for its epoch, the
    // second means its view of the tail is wrong, the third is retried.
    let mut response = Json(serde_json::json!({
        "code": append_error_code(&error),
        "retryable": matches!(status, StatusCode::SERVICE_UNAVAILABLE),
        "error": error.to_string(),
    }))
    .into_response();
    *response.status_mut() = status;
    response
}

/// Stable machine-readable code for one append rejection.
fn append_error_code(error: &ReplicaError) -> &'static str {
    match error {
        ReplicaError::WriterFenced => "writer_fenced",
        ReplicaError::LsnConflict => "lsn_conflict",
        ReplicaError::InvalidWatermark { .. } => "invalid_watermark",
        ReplicaError::CapacityExceeded { .. } => CAPACITY_EXCEEDED_CODE,
        ReplicaError::QuorumUnavailable => "quorum_unavailable",
        ReplicaError::NodeUnavailable => "node_unavailable",
        ReplicaError::GatewayUnauthorized => "unauthorized",
        _ => "unavailable",
    }
}

async fn gateway_append(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let record = match encrypted_record_body(&headers, &body) {
        Ok(record) => record,
        Err(status) => return (status, HeaderMap::new()).into_response(),
    };
    if !state.gateway.auth.permits(&headers, record.stream()) {
        return (StatusCode::UNAUTHORIZED, HeaderMap::new()).into_response();
    }
    match state.gateway.append(record.clone()).await {
        Ok(_) => {
            let certificate = match state.gateway.issue_commit_certificate(&record).await {
                Ok(certificate) => certificate,
                Err(_) => {
                    return (StatusCode::SERVICE_UNAVAILABLE, HeaderMap::new()).into_response();
                }
            };
            let Ok(value) = HeaderValue::from_str(&certificate) else {
                return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new()).into_response();
            };
            let mut response_headers = HeaderMap::new();
            response_headers.insert(HeaderName::from_static(COMMIT_CERTIFICATE_HEADER), value);
            (StatusCode::NO_CONTENT, response_headers).into_response()
        }
        Err(error) => gateway_append_error(error),
    }
}

async fn gateway_append_many(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let records = match encrypted_record_batch_body(&headers, &body) {
        Ok(records) => records,
        Err(status) => return (status, HeaderMap::new()).into_response(),
    };
    if records
        .iter()
        .any(|record| !state.gateway.auth.permits(&headers, record.stream()))
    {
        return (StatusCode::UNAUTHORIZED, HeaderMap::new()).into_response();
    }
    let last = records
        .last()
        .cloned()
        .expect("batch decoder rejects empty body");
    match state.gateway.append_many(records).await {
        Ok(_) => {
            let certificate = match state.gateway.issue_commit_certificate(&last).await {
                Ok(certificate) => certificate,
                Err(_) => {
                    return (StatusCode::SERVICE_UNAVAILABLE, HeaderMap::new()).into_response();
                }
            };
            let Ok(value) = HeaderValue::from_str(&certificate) else {
                return (StatusCode::INTERNAL_SERVER_ERROR, HeaderMap::new()).into_response();
            };
            let mut response_headers = HeaderMap::new();
            response_headers.insert(HeaderName::from_static(COMMIT_CERTIFICATE_HEADER), value);
            (StatusCode::NO_CONTENT, response_headers).into_response()
        }
        Err(error) => gateway_append_error(error),
    }
}

async fn gateway_records(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(query): Query<GatewayRecordsQuery>,
) -> Response {
    if !state.gateway.auth.permits(&headers, &query.stream) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let _admission = match state.gateway.admit_read().await {
        Ok(admission) => admission,
        Err(error) => return gateway_recovery_error(error, &query, "admission"),
    };
    let certificate = headers
        .get(HeaderName::from_static(COMMIT_CERTIFICATE_HEADER))
        .and_then(|value| value.to_str().ok());
    match state
        .gateway
        .recover_with_watermark_certificate(
            &query.stream,
            query.after_lsn.unwrap_or(0),
            query.committed_lsn,
            certificate,
        )
        .await
    {
        Ok(records) => Json(records).into_response(),
        Err(error) => gateway_recovery_error(error, &query, "recovery"),
    }
}

/// Retains recovery diagnostics without returning provider error text to a
/// tenant. Storage details remain in the private replica operator logs.
fn gateway_recovery_error(
    error: ReplicaError,
    query: &GatewayRecordsQuery,
    phase: &str,
) -> Response {
    let status = recovery_status(error.clone());
    eprintln!(
        "{}",
        serde_json::json!({
            "event":"replica_recovery_failed", "phase":phase, "stream":query.stream,
            "after_lsn":query.after_lsn, "committed_lsn":query.committed_lsn,
            "status":status.as_u16(), "error":error.to_string(),
        })
    );
    let detail = match &error {
        ReplicaError::NodeStorage(_) => {
            "replica storage recovery failed; see operator logs".to_owned()
        }
        ReplicaError::Protocol(_) => {
            "replica recovery protocol failed; see operator logs".to_owned()
        }
        _ => error.to_string(),
    };
    (
        status,
        Json(serde_json::json!({
            "code":"recovery_failed", "error":detail.chars().take(1024).collect::<String>(),
        })),
    )
        .into_response()
}

async fn gateway_repair(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<RepairRequest>,
) -> Result<Json<RebalanceReport>, StatusCode> {
    if !state.gateway.auth.permits(&headers, &request.stream) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let certificate = headers
        .get(HeaderName::from_static(COMMIT_CERTIFICATE_HEADER))
        .and_then(|value| value.to_str().ok());
    let report = state
        .gateway
        .repair_with_certificate(&request.stream, request.committed_lsn, certificate)
        .await
        .map_err(recovery_status)?;
    if report.failed > 0 {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Json(report))
}

fn maintenance_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(MAINTENANCE_AUTH_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
}

fn internal_authorized(headers: &HeaderMap, internal_token: &str, admin_token: &str) -> bool {
    let internal = !internal_token.is_empty()
        && headers
            .get(INTERNAL_AUTH_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == internal_token);
    let admin = !admin_token.is_empty()
        && headers
            .get(ADMIN_AUTH_HEADER)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == admin_token);
    internal || admin
}

#[derive(Serialize)]
struct MaintenanceLease {
    /// Opaque HMAC-bound owner/generation token required for rebalance and
    /// release. It remains valid across gateway restarts while its node
    /// markers are active.
    token: String,
    /// Number of admitted reads or appends still completing when the fence was acquired.
    active_requests: usize,
}

async fn begin_maintenance(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<MaintenanceLease>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let operation_id = if body.iter().all(u8::is_ascii_whitespace) {
        None
    } else {
        let value: Value = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
        match value
            .get("operation_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
        {
            Some(operation_id) if !operation_id.trim().is_empty() => Some(operation_id),
            Some(_) => return Err(StatusCode::BAD_REQUEST),
            None => return Err(StatusCode::BAD_REQUEST),
        }
    };
    let token = match operation_id {
        Some(operation_id) => state.gateway.begin_maintenance_for(&operation_id).await,
        None => state.gateway.begin_maintenance().await,
    }
    .map_err(membership_status)?;
    Ok(Json(MaintenanceLease {
        token,
        active_requests: state.gateway.active_requests.load(Ordering::Acquire),
    }))
}

async fn end_maintenance(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    RoutePath(token): RoutePath<String>,
) -> Result<StatusCode, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .gateway
        .end_maintenance(&token)
        .await
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(membership_status)
}

async fn admin_status(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<GatewayStatus>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let nodes = state
        .gateway
        .nodes_snapshot()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let durable_fence = state.gateway.refresh_durable_fence().await.ok();
    let durable_fence = match durable_fence {
        Some(observation) => observation,
        None => state.gateway.local_fence_view().await,
    };
    let (maintenance_owner, maintenance_generation) = durable_fence
        .active
        .as_ref()
        .map_or((None, durable_fence.generation), |(owner, generation)| {
            (Some(owner.clone()), *generation)
        });
    let membership_epoch = state
        .gateway
        .membership()
        .await
        .map_or(0, |membership| membership.membership_epoch);
    let healthy = join_all(nodes.iter().map(|node| {
        let node = node.clone();
        let gateway = Arc::clone(&state.gateway);
        async move {
            NodeClient::new(node, &gateway.client, &gateway.internal_token)
                .health()
                .await
                .is_ok()
        }
    }))
    .await
    .into_iter()
    .filter(|healthy| *healthy)
    .count();
    Ok(Json(GatewayStatus {
        quorum: state.gateway.quorum,
        storage_nodes: nodes.len(),
        healthy_storage: healthy,
        active_requests: state.gateway.active_requests.load(Ordering::Acquire),
        maintenance: state.gateway.maintenance_active().await,
        fenced_storage: durable_fence.fenced_storage,
        maintenance_generation,
        maintenance_owner,
        membership_epoch,
    }))
}

async fn admin_rebalance(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<RebalanceReport>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let request = if body.iter().all(u8::is_ascii_whitespace) {
        RebalanceRequest { target: None }
    } else {
        serde_json::from_slice::<RebalanceRequest>(&body).map_err(|_| StatusCode::BAD_REQUEST)?
    };
    let report = if let Some(token) = maintenance_token(&headers) {
        state
            .gateway
            .rebalance_with_maintenance(request.target.as_deref(), token)
            .await
    } else {
        state.gateway.rebalance(request.target.as_deref()).await
    }
    .map_err(membership_status)?;
    if report.failed > 0 || report.unsafe_records_skipped > 0 {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Json(report))
}

async fn list_nodes(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ReplicaNode>>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .gateway
        .nodes_snapshot()
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MembershipOperationRequest {
    #[serde(alias = "op_id", alias = "operation")]
    pub operation_id: String,
    #[serde(default)]
    pub old_member_id: Option<String>,
    #[serde(default)]
    pub old_member_ids: Vec<String>,
    #[serde(default)]
    pub source_cohort_id: Option<u64>,
    #[serde(default, alias = "member")]
    pub node: Option<ReplicaNode>,
    /// Complete replacement candidates for an online whole-cohort handoff.
    /// When omitted, candidates previously registered as joining are selected
    /// by id from the durable membership document.
    #[serde(default)]
    pub nodes: Vec<DurableMember>,
    #[serde(default, alias = "cohort")]
    pub cohort_id: Option<u64>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub machine_id: Option<String>,
    #[serde(default)]
    pub volume_id: Option<String>,
    #[serde(default)]
    pub ordinal: Option<u64>,
    #[serde(default)]
    pub tier: Option<String>,
    #[serde(default)]
    pub max_append_bytes: Option<u64>,
    #[serde(default)]
    pub expected_epoch: Option<u64>,
    #[serde(default)]
    pub members: Option<Vec<DurableMember>>,
    /// Optional complete cohort document.  Supplying it makes a CAS fully
    /// self-describing for stateless coordinators; omitting it preserves the
    /// legacy wire form and derives assignments for new members.
    #[serde(default)]
    pub cohorts: Option<Vec<DurableCohort>>,
    #[serde(default)]
    pub stream_segments: Option<BTreeMap<String, Vec<StreamSegment>>>,
    /// Digest proving that every immutable range owned by a cohort has been
    /// archived and sealed before a member is drained or removed.
    #[serde(default)]
    pub archive_proof: Option<String>,
}

async fn membership_status_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .gateway
        .membership()
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn gateway_control_state(
    State(state): State<GatewayState>,
    headers: HeaderMap,
) -> Result<Json<DurableControlState>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state
        .gateway
        .control
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?
        .state()
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

async fn membership_cas_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let expected_epoch = request.expected_epoch.ok_or(StatusCode::BAD_REQUEST)?;
    let members = request.members.ok_or(StatusCode::BAD_REQUEST)?;
    let result = if let Some(token) = maintenance_token(&headers) {
        state
            .gateway
            .membership_cas_document_with_maintenance(
                expected_epoch,
                members,
                request.cohorts,
                request.stream_segments,
                &request.operation_id,
                token,
            )
            .await
    } else {
        state
            .gateway
            .membership_cas_document(
                expected_epoch,
                members,
                request.cohorts,
                request.stream_segments,
                &request.operation_id,
            )
            .await
    };
    result.map(Json).map_err(membership_status)
}

async fn membership_join_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let node = request.node.ok_or(StatusCode::BAD_REQUEST)?;
    state
        .gateway
        .join_member_in_cohort_with_identity(
            &request.operation_id,
            node,
            request.cohort_id,
            DurableMemberIdentity {
                name: request.name.unwrap_or_default(),
                machine_id: request.machine_id.unwrap_or_default(),
                volume_id: request.volume_id.unwrap_or_default(),
                ordinal: request.ordinal.unwrap_or_default(),
                tier: request.tier.unwrap_or_default(),
                max_append_bytes: request.max_append_bytes.unwrap_or_default(),
            },
        )
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_join_online_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let node = request.node.ok_or(StatusCode::BAD_REQUEST)?;
    let operation_id = request.operation_id.clone();
    let node_id = node.id.clone();
    let requested_cohort = request.cohort_id;
    let result = state
        .gateway
        .join_member_in_cohort_online_with_identity(
            &request.operation_id,
            node,
            request.cohort_id,
            DurableMemberIdentity {
                name: request.name.unwrap_or_default(),
                machine_id: request.machine_id.unwrap_or_default(),
                volume_id: request.volume_id.unwrap_or_default(),
                ordinal: request.ordinal.unwrap_or_default(),
                tier: request.tier.unwrap_or_default(),
                max_append_bytes: request.max_append_bytes.unwrap_or_default(),
            },
        )
        .await;
    result.map(Json).map_err(|error| {
        let status = membership_status(error.clone());
        eprintln!(
            "replica_membership_join_online_failed operation_id={} node_id={} requested_cohort={:?} status={} error={error:?}",
            operation_id,
            node_id,
            requested_cohort,
            status.as_u16(),
        );
        status
    })
}

async fn membership_replace_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token = maintenance_token(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    let old_member_id = request.old_member_id.ok_or(StatusCode::BAD_REQUEST)?;
    let node = request.node.ok_or(StatusCode::BAD_REQUEST)?;
    let cohort_id = request.cohort_id.ok_or(StatusCode::BAD_REQUEST)?;
    state
        .gateway
        .replace_member_with_maintenance(
            &request.operation_id,
            &old_member_id,
            node,
            cohort_id,
            DurableMemberIdentity {
                name: request.name.unwrap_or_default(),
                machine_id: request.machine_id.unwrap_or_default(),
                volume_id: request.volume_id.unwrap_or_default(),
                ordinal: request.ordinal.unwrap_or_default(),
                tier: request.tier.unwrap_or_default(),
                max_append_bytes: request.max_append_bytes.unwrap_or_default(),
            },
            token,
        )
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_activate_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    RoutePath(id): RoutePath<String>,
    body: Bytes,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let request = membership_operation_body(&headers, &body)?;
    let token = maintenance_token(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .gateway
        .activate_member_with_maintenance(&request.operation_id, &id, token)
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_activate_cohort_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    RoutePath(id): RoutePath<String>,
    body: Bytes,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let cohort_id = id.parse::<u64>().map_err(|_| StatusCode::BAD_REQUEST)?;
    let request = membership_operation_body(&headers, &body)?;
    let token = maintenance_token(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .gateway
        .activate_cohort_with_maintenance(&request.operation_id, cohort_id, token)
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_activate_cohort_body_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let cohort_id = request.cohort_id.ok_or(StatusCode::BAD_REQUEST)?;
    let token = maintenance_token(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .gateway
        .activate_cohort_with_maintenance(&request.operation_id, cohort_id, token)
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_activate_cohort_online_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let cohort_id = request.cohort_id.ok_or(StatusCode::BAD_REQUEST)?;
    state
        .gateway
        .activate_cohort_online(&request.operation_id, cohort_id)
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_replace_cohort_online_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<OnlineCohortReplacementReceipt>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let cohort_id = request.cohort_id.ok_or(StatusCode::BAD_REQUEST)?;
    let source_cohort_id = request.source_cohort_id.ok_or(StatusCode::BAD_REQUEST)?;
    let operation_id = request.operation_id.clone();
    state
        .gateway
        .replace_cohort_online(
            &request.operation_id,
            source_cohort_id,
            cohort_id,
            request.old_member_ids,
            request.nodes,
        )
        .await
        .map_err(|error| {
            let status = membership_status(error.clone());
            eprintln!(
                "replica_membership_replace_cohort_online_failed operation_id={} source_cohort={} target_cohort={} status={} error={error:?}",
                operation_id,
                source_cohort_id,
                cohort_id,
                status.as_u16(),
            );
            status
        })?;
    state
        .gateway
        .verified_online_retirement_receipt(&request.operation_id, source_cohort_id, cohort_id)
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_retire_cohort_online_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<MembershipOperationRequest>,
) -> Result<Json<OnlineCohortReplacementReceipt>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let source_cohort_id = request.source_cohort_id.ok_or(StatusCode::BAD_REQUEST)?;
    state
        .gateway
        .retire_cohort_online(&request.operation_id, source_cohort_id)
        .await
        .map_err(membership_status)?;
    state
        .gateway
        .verified_online_retirement_receipt(
            &request.operation_id,
            source_cohort_id,
            source_cohort_id,
        )
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_drain_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    RoutePath(id): RoutePath<String>,
    body: Bytes,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let request = membership_operation_body(&headers, &body)?;
    let token = maintenance_token(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .gateway
        .drain_member_with_maintenance_and_archive_proof(
            &request.operation_id,
            &id,
            token,
            request.archive_proof.as_deref(),
        )
        .await
        .map(Json)
        .map_err(membership_status)
}

async fn membership_cutover_cohort_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    RoutePath(id): RoutePath<String>,
    body: Bytes,
) -> Result<Json<MembershipSnapshot>, (StatusCode, Json<Value>)> {
    let bare = |status: StatusCode| {
        (
            status,
            Json(serde_json::json!({"error": status.to_string()})),
        )
    };
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(bare(StatusCode::UNAUTHORIZED));
    }
    let cohort_id = id
        .parse::<u64>()
        .map_err(|_| bare(StatusCode::BAD_REQUEST))?;
    let request = membership_operation_body(&headers, &body).map_err(bare)?;
    let token = maintenance_token(&headers).ok_or_else(|| bare(StatusCode::UNAUTHORIZED))?;
    state
        .gateway
        .cutover_cohort_with_maintenance(&request.operation_id, cohort_id, token)
        .await
        .map(Json)
        .map_err(|error| {
            // The operator surfaces this reason verbatim when it aborts, so a
            // stream that cannot be sealed is named rather than a bare 400.
            let status = membership_status(error.clone());
            (
                status,
                Json(serde_json::json!({"error": error.to_string()})),
            )
        })
}

async fn membership_archive_cohort_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    RoutePath(id): RoutePath<String>,
    body: Bytes,
) -> Result<Json<CohortArchiveReport>, (StatusCode, Json<Value>)> {
    let bare = |status: StatusCode| {
        (
            status,
            Json(serde_json::json!({"error": status.to_string()})),
        )
    };
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(bare(StatusCode::UNAUTHORIZED));
    }
    let cohort_id = id
        .parse::<u64>()
        .map_err(|_| bare(StatusCode::BAD_REQUEST))?;
    let request = membership_operation_body(&headers, &body).map_err(bare)?;
    let token = maintenance_token(&headers).ok_or_else(|| bare(StatusCode::UNAUTHORIZED))?;
    state
        .gateway
        .archive_cohort_with_maintenance(&request.operation_id, cohort_id, token)
        .await
        .map(Json)
        .map_err(|error| {
            let status = membership_status(error.clone());
            (
                status,
                Json(serde_json::json!({"error": error.to_string()})),
            )
        })
}

async fn membership_remove_route(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    RoutePath(id): RoutePath<String>,
    body: Bytes,
) -> Result<Json<MembershipSnapshot>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let request = membership_operation_body(&headers, &body)?;
    let token = maintenance_token(&headers).ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .gateway
        .remove_member_with_maintenance_and_archive_proof(
            &request.operation_id,
            &id,
            token,
            request.archive_proof.as_deref(),
        )
        .await
        .map(Json)
        .map_err(membership_status)
}

fn membership_operation_body(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<MembershipOperationRequest, StatusCode> {
    if !body.iter().all(u8::is_ascii_whitespace) {
        return serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST);
    }
    let operation_id = headers
        .get("idempotency-key")
        .or_else(|| headers.get("x-lakeday-replica-operation"))
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;
    Ok(MembershipOperationRequest {
        operation_id: operation_id.to_owned(),
        old_member_id: None,
        old_member_ids: Vec::new(),
        source_cohort_id: None,
        node: None,
        nodes: Vec::new(),
        name: None,
        machine_id: None,
        volume_id: None,
        ordinal: None,
        tier: None,
        max_append_bytes: None,
        expected_epoch: None,
        members: None,
        cohort_id: None,
        cohorts: None,
        stream_segments: None,
        archive_proof: None,
    })
}

async fn rebalance(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Json(request): Json<RebalanceRequest>,
) -> Result<Json<RebalanceReport>, StatusCode> {
    if !internal_authorized(
        &headers,
        &state.gateway.internal_token,
        &state.gateway.admin_token,
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let report = if let Some(token) = maintenance_token(&headers) {
        state
            .gateway
            .rebalance_with_maintenance(request.target.as_deref(), token)
            .await
    } else {
        state.gateway.rebalance(request.target.as_deref()).await
    }
    .map_err(membership_status)?;
    if report.failed > 0 {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Json(report))
}

fn membership_status(error: ReplicaError) -> StatusCode {
    match error {
        ReplicaError::LsnConflict => StatusCode::CONFLICT,
        ReplicaError::GatewayUnauthorized => StatusCode::UNAUTHORIZED,
        ReplicaError::CapacityExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        ReplicaError::NodeUnavailable | ReplicaError::QuorumUnavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::BAD_REQUEST,
    }
}

fn recovery_status(error: ReplicaError) -> StatusCode {
    match error {
        ReplicaError::GatewayUnauthorized | ReplicaError::CommitCertificateMissing => {
            StatusCode::UNAUTHORIZED
        }
        ReplicaError::RecoveryGap { .. }
        | ReplicaError::RecoveryIncomplete { .. }
        | ReplicaError::RecoveryAmbiguous { .. }
        | ReplicaError::NodeUnavailable
        | ReplicaError::QuorumUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::BAD_REQUEST,
    }
}

struct NodeClient<'a> {
    node: ReplicaNode,
    http: &'a reqwest::Client,
    internal_token: &'a str,
}

impl<'a> NodeClient<'a> {
    async fn supersede(
        &self,
        stream: &str,
        from_lsn: u64,
        writer_epoch: u64,
    ) -> Result<(), ReplicaError> {
        let response = self
            .http
            .post(format!("{}/internal/v1/supersede", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .json(&SupersedeRequest {
                stream: stream.to_owned(),
                from_lsn,
                writer_epoch,
            })
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            status if status.is_success() => Ok(()),
            StatusCode::CONFLICT => Err(ReplicaError::LsnConflict),
            StatusCode::UNAUTHORIZED => Err(ReplicaError::GatewayUnauthorized),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    fn new(node: ReplicaNode, http: &'a reqwest::Client, internal_token: &'a str) -> Self {
        Self {
            node,
            http,
            internal_token,
        }
    }

    async fn append_and_commit_many_with_placement(
        &self,
        records: &[EncryptedRecord],
        placement: Option<PlacementEpoch>,
    ) -> Result<(), ReplicaError> {
        let mut attempts = 0;
        loop {
            match self
                .append_and_commit_many_once(records, placement.as_ref())
                .await
            {
                Err(ReplicaError::NodeUnavailable) if attempts + 1 < NODE_OPERATION_ATTEMPTS => {
                    attempts += 1;
                }
                result => return result,
            }
        }
    }

    async fn append_and_commit_many_once(
        &self,
        records: &[EncryptedRecord],
        placement: Option<&PlacementEpoch>,
    ) -> Result<(), ReplicaError> {
        validate_append_batch(records)?;
        let body = EncryptedRecord::encode_binary_batch(records)?;
        let mut request = self
            .http
            .post(format!("{}/internal/v1/append-many", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE);
        if let Some(placement) = placement {
            request = request
                .header(PLACEMENT_EPOCH_HEADER, placement.epoch().to_string())
                .header(PLACEMENT_DIGEST_HEADER, placement.route_digest());
        }
        let response = request
            .body(body)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::CONFLICT
                if response
                    .headers()
                    .get(PLACEMENT_FENCED_HEADER)
                    .is_some_and(|value| value == "true") =>
            {
                Err(ReplicaError::WriterFenced)
            }
            StatusCode::CONFLICT => Err(ReplicaError::LsnConflict),
            StatusCode::UNAUTHORIZED => Err(ReplicaError::GatewayUnauthorized),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    async fn append_with_maintenance(
        &self,
        record: &EncryptedRecord,
        token: &str,
    ) -> Result<(), ReplicaError> {
        let body = record.encode_binary()?;
        let response = self
            .http
            .post(format!("{}/internal/v1/maintenance/append", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .header(MAINTENANCE_AUTH_HEADER, token)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(body)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::CONFLICT => Err(ReplicaError::WriterFenced),
            StatusCode::UNAUTHORIZED => Err(ReplicaError::GatewayUnauthorized),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    async fn compact_with_maintenance(
        &self,
        prefixes: &[TrimmedPrefix],
        token: &str,
    ) -> Result<(), ReplicaError> {
        let response = self
            .http
            .post(format!("{}/internal/v1/maintenance/compact", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .header(MAINTENANCE_AUTH_HEADER, token)
            .json(prefixes)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::CONFLICT => Err(ReplicaError::LsnConflict),
            StatusCode::UNAUTHORIZED => Err(ReplicaError::GatewayUnauthorized),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    async fn commit_with_maintenance(
        &self,
        record: &EncryptedRecord,
        token: &str,
    ) -> Result<(), ReplicaError> {
        let body = record.encode_binary()?;
        let response = self
            .http
            .post(format!("{}/internal/v1/maintenance/commit", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .header(MAINTENANCE_AUTH_HEADER, token)
            .header("content-type", ENCRYPTED_RECORD_CONTENT_TYPE)
            .body(body)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::CONFLICT => Err(ReplicaError::WriterFenced),
            StatusCode::UNAUTHORIZED => Err(ReplicaError::GatewayUnauthorized),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    async fn cas_membership(
        &self,
        request: &MembershipCasRequest,
        maintenance_token: &str,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let response = self
            .http
            .post(format!("{}/internal/v1/control/membership", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .header(MAINTENANCE_AUTH_HEADER, maintenance_token)
            .json(request)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if response.status() == StatusCode::CONFLICT {
            return Err(ReplicaError::LsnConflict);
        }
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        response
            .json::<MembershipSnapshot>()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)
    }

    async fn cas_membership_online(
        &self,
        request: &MembershipCasRequest,
    ) -> Result<MembershipSnapshot, ReplicaError> {
        let response = self
            .http
            .post(format!(
                "{}/internal/v1/control/membership/online",
                self.node.url
            ))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .json(request)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if response.status() == StatusCode::CONFLICT {
            return Err(ReplicaError::LsnConflict);
        }
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        response
            .json::<MembershipSnapshot>()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)
    }

    async fn install_placement_fence(
        &self,
        stream: &str,
        placement: &PlacementEpoch,
    ) -> Result<(), ReplicaError> {
        let response = self
            .http
            .post(format!(
                "{}/internal/v1/control/placement/fence",
                self.node.url
            ))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .json(&PlacementFenceRequest {
                stream: stream.to_owned(),
                epoch: placement.epoch(),
                route_digest: placement.route_digest().to_owned(),
            })
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::CONFLICT => Err(ReplicaError::WriterFenced),
            StatusCode::BAD_REQUEST => Err(ReplicaError::Protocol(
                "invalid placement fence request".to_owned(),
            )),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    async fn current_placement(&self, stream: &str) -> Result<PlacementEpoch, ReplicaError> {
        let response = self
            .http
            .get(format!("{}/internal/v1/control/placement", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .query(&[("stream", stream)])
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        response
            .json::<PlacementEpoch>()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)
    }

    async fn control_state(&self) -> Result<Option<DurableControlState>, ReplicaError> {
        let response = self
            .http
            .get(format!("{}/internal/v1/control", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        response
            .json::<DurableControlState>()
            .await
            .map(Some)
            .map_err(|_| ReplicaError::NodeUnavailable)
    }

    async fn cas_manifest(
        &self,
        request: &ManifestCasRequest,
    ) -> Result<Option<ReplicaManifest>, ReplicaError> {
        let response = self
            .http
            .post(format!("{}/internal/v1/control/manifest", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .json(request)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() == StatusCode::CONFLICT {
            return Err(ReplicaError::LsnConflict);
        }
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        response
            .json::<ReplicaManifest>()
            .await
            .map(Some)
            .map_err(|_| ReplicaError::NodeUnavailable)
    }

    async fn manifest(&self) -> Result<Option<ReplicaManifest>, ReplicaError> {
        let response = self
            .http
            .get(format!("{}/internal/v1/control/manifest", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        let manifest = response
            .json::<ReplicaManifest>()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        manifest.validate()?;
        Ok(Some(manifest))
    }

    async fn fence(&self, token: &str) -> Result<(), ReplicaError> {
        let response = self
            .http
            .post(format!("{}/internal/v1/maintenance/fence", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .header(MAINTENANCE_AUTH_HEADER, token)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::CONFLICT => Err(ReplicaError::WriterFenced),
            StatusCode::UNAUTHORIZED => Err(ReplicaError::GatewayUnauthorized),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    async fn release(&self, token: &str) -> Result<(), ReplicaError> {
        let response = self
            .http
            .delete(format!(
                "{}/internal/v1/maintenance/fence/{token}",
                self.node.url
            ))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .header(MAINTENANCE_AUTH_HEADER, token)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::UNAUTHORIZED => Err(ReplicaError::GatewayUnauthorized),
            StatusCode::CONFLICT => Err(ReplicaError::WriterFenced),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }

    async fn snapshot(&self, stream: Option<&str>) -> Result<NodeSnapshot, ReplicaError> {
        let request = self
            .http
            .get(format!("{}/internal/v1/records", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .timeout(SNAPSHOT_REQUEST_TIMEOUT)
            .query(&[("stream", stream.unwrap_or("*"))]);
        let response = request
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        response
            .json::<NodeSnapshot>()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)
    }

    async fn health(&self) -> Result<(), ReplicaError> {
        let response = self
            .http
            .get(format!("{}/internal/v1/healthz", self.node.url))
            .header(INTERNAL_AUTH_HEADER, self.internal_token)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(ReplicaError::NodeUnavailable)
        }
    }

    async fn storage_status(&self) -> Result<StorageNodeStatus, ReplicaError> {
        // `/metrics` is the canonical contract. Keep `/status` as a fallback
        // for older storage images during a roll-forward; any non-404 failure
        // is terminal so an auth or server error cannot be hidden.
        for path in ["/internal/v1/metrics", "/internal/v1/status"] {
            let response = self
                .http
                .get(format!("{}{}", self.node.url, path))
                .header(INTERNAL_AUTH_HEADER, self.internal_token)
                .send()
                .await
                .map_err(|_| ReplicaError::NodeUnavailable)?;
            if response.status() == StatusCode::NOT_FOUND {
                continue;
            }
            if !response.status().is_success() {
                return Err(ReplicaError::NodeUnavailable);
            }
            return response
                .json::<StorageNodeStatus>()
                .await
                .map_err(|_| ReplicaError::NodeUnavailable);
        }
        Err(ReplicaError::NodeUnavailable)
    }
}

fn validate_node(node: &ReplicaNode) -> Result<(), ReplicaError> {
    if node.id.trim().is_empty() {
        return Err(ReplicaError::NodeStorage(
            "replica node id must not be empty".to_owned(),
        ));
    }
    let url = reqwest::Url::parse(&node.url)
        .map_err(|_| ReplicaError::NodeStorage("replica node URL is invalid".to_owned()))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(ReplicaError::NodeStorage(
            "replica node URL must be http or https".to_owned(),
        ));
    }
    Ok(())
}

fn fresh_storage_status(status: &StorageNodeStatus, now_ms: u64, max_age: Duration) -> bool {
    if max_age.is_zero() {
        return false;
    }
    if status.boot_id.trim().is_empty()
        || status.node_name.trim().is_empty()
        || status.tier.trim().is_empty()
    {
        return false;
    }
    let filesystem = status.filesystem_capacity();
    let max_age_ms = max_age.as_millis().min(u128::from(u64::MAX)) as u64;
    let max_future_ms = METRICS_CLOCK_SKEW.as_millis().min(u128::from(u64::MAX)) as u64;
    let sample_timestamp_ms = status.sample_timestamp_ms();
    sample_timestamp_ms <= now_ms.saturating_add(max_future_ms)
        && now_ms.saturating_sub(sample_timestamp_ms) <= max_age_ms
        && filesystem.total_bytes >= filesystem.used_bytes
        && filesystem.free_bytes == filesystem.total_bytes.saturating_sub(filesystem.used_bytes)
        && filesystem.total_bytes > 0
}

fn aggregate_storage_capacity(statuses: &[StorageNodeStatus]) -> Result<(u64, u64), ReplicaError> {
    statuses
        .iter()
        .try_fold((0_u64, 0_u64), |(free, total), status| {
            let filesystem = status.filesystem_capacity();
            let free = free.checked_add(filesystem.free_bytes).ok_or_else(|| {
                ReplicaError::NodeStorage("storage free capacity overflow".to_owned())
            })?;
            let total = total.checked_add(filesystem.total_bytes).ok_or_else(|| {
                ReplicaError::NodeStorage("storage total capacity overflow".to_owned())
            })?;
            Ok((free, total))
        })
}

fn nonempty_env(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim().to_owned())
    })
}

fn default_node_name() -> String {
    nonempty_env(&[
        "LAKEDAY_REPLICA_NODE_NAME",
        "LAKEDAY_STORAGE_NODE_NAME",
        "HOSTNAME",
    ])
    .unwrap_or_else(|| "replica-node".to_owned())
}

fn default_storage_tier() -> String {
    nonempty_env(&["LAKEDAY_REPLICA_TIER", "LAKEDAY_REPLICA_STORAGE_TIER"])
        .unwrap_or_else(|| DEFAULT_STORAGE_TIER.to_owned())
}

fn default_data_dir() -> PathBuf {
    nonempty_env(&[
        "LAKEDAY_REPLICA_DATA_DIR",
        "LAKEDAY_REPLICA_DATA_PATH",
        "LAKEDAY_STORAGE_DATA_DIR",
    ])
    .map_or_else(|| PathBuf::from(DEFAULT_STORAGE_DATA_DIR), PathBuf::from)
}

#[allow(clippy::useless_conversion)]
fn filesystem_status(path: &Path) -> Result<FilesystemStatus, ReplicaError> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            ReplicaError::NodeStorage("data volume path contains a NUL byte".to_owned())
        })?;
        // SAFETY: `stats` is initialized by statvfs and `c_path` is a valid,
        // NUL-terminated path for the duration of the call.
        let mut stats = unsafe { std::mem::zeroed::<libc::statvfs>() };
        // SAFETY: libc::statvfs only writes to the provided initialized struct
        // and does not retain the path pointer.
        let result = unsafe { libc::statvfs(c_path.as_ptr(), &mut stats) };
        if result != 0 {
            return Err(ReplicaError::NodeStorage(
                std::io::Error::last_os_error().to_string(),
            ));
        }
        let block_size: u64 = if stats.f_frsize == 0 {
            stats.f_bsize
        } else {
            stats.f_frsize
        }
        .into();
        let total_blocks: u64 = stats.f_blocks.into();
        let free_blocks: u64 = stats.f_bavail.into();
        let total_bytes = total_blocks.checked_mul(block_size).ok_or_else(|| {
            ReplicaError::NodeStorage("statvfs total capacity overflow".to_owned())
        })?;
        let free_bytes = free_blocks.checked_mul(block_size).ok_or_else(|| {
            ReplicaError::NodeStorage("statvfs free capacity overflow".to_owned())
        })?;
        if free_bytes > total_bytes {
            return Err(ReplicaError::NodeStorage(
                "statvfs free capacity exceeds total capacity".to_owned(),
            ));
        }
        Ok(FilesystemStatus {
            total_bytes,
            free_bytes,
            used_bytes: total_bytes - free_bytes,
        })
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Err(ReplicaError::NodeStorage(
            "filesystem metrics unsupported on this platform".to_owned(),
        ))
    }
}

/// Parses JSON membership or a compact id=url,id=url environment value.
pub fn parse_nodes(value: &str) -> Result<Vec<ReplicaNode>, ReplicaError> {
    let value = value.trim();
    if value.starts_with('[') {
        return serde_json::from_str(value)
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()));
    }
    value
        .split(',')
        .filter(|item| !item.trim().is_empty())
        .enumerate()
        .map(|(index, item)| {
            let item = item.trim();
            let (id, url) = item.split_once('=').map_or_else(
                || (format!("node-{}", index + 1), item.to_owned()),
                |(id, url)| (id.to_owned(), url.to_owned()),
            );
            let node = ReplicaNode::new(id, url);
            validate_node(&node).map(|()| node)
        })
        .collect()
}

#[cfg(test)]
mod manifest_tests {
    use std::collections::BTreeMap;

    use super::{
        CohortStatus, DurableCohort, DurableControlState, DurableMember, StreamSegment,
        cohort_for_stream, may_adopt_authoritative_bootstrap, validate_manifest_cutover,
        validate_manifest_transition,
    };

    fn state_with_cohorts(ids: &[u64]) -> DurableControlState {
        let mut state = DurableControlState::default();
        for cohort_id in ids {
            let members = (0..3)
                .map(|ordinal| {
                    let id = format!("cohort-{cohort_id}-member-{ordinal}");
                    state.members.insert(
                        id.clone(),
                        DurableMember {
                            id: id.clone(),
                            url: format!("http://{id}"),
                            status: super::MemberStatus::Active,
                            cohort_id: *cohort_id,
                            ..DurableMember::default()
                        },
                    );
                    id
                })
                .collect::<Vec<_>>();
            state.cohorts.insert(
                *cohort_id,
                DurableCohort {
                    id: *cohort_id,
                    members,
                    status: CohortStatus::Active,
                    tier: String::new(),
                    max_append_bytes: 0,
                },
            );
        }
        state
    }

    #[test]
    fn only_empty_bootstrap_state_may_adopt_authoritative_cohort_ids() {
        let mut state = DurableControlState {
            membership_epoch: 1,
            ..DurableControlState::default()
        };
        assert!(may_adopt_authoritative_bootstrap(&state, true));
        assert!(!may_adopt_authoritative_bootstrap(&state, false));

        state.manifest_revision = 1;
        assert!(!may_adopt_authoritative_bootstrap(&state, true));
        state.manifest_revision = 0;
        state
            .operations
            .insert("committed-operation".to_owned(), "{}".to_owned());
        assert!(!may_adopt_authoritative_bootstrap(&state, true));
    }

    #[test]
    fn rendezvous_ring_distributes_streams_across_three_cohorts() {
        let state = state_with_cohorts(&[0, 1, 2]);
        let mut counts = [0_usize; 3];
        for stream_id in 0..4096 {
            let cohort = cohort_for_stream(&state, &format!("tenant/stream-{stream_id}"))
                .expect("complete active cohort");
            counts[cohort as usize] += 1;
        }
        assert!(counts.iter().all(|count| *count > 900), "counts={counts:?}");
    }

    #[test]
    fn adding_a_cohort_has_bounded_remap_and_ignores_member_order() {
        let two = state_with_cohorts(&[0, 1]);
        let three = state_with_cohorts(&[0, 1, 2]);
        let streams = (0..4096)
            .map(|stream_id| format!("tenant/stream-{stream_id}"))
            .collect::<Vec<_>>();
        let moved = streams
            .iter()
            .filter(|stream| cohort_for_stream(&two, stream) != cohort_for_stream(&three, stream))
            .count();
        // Rendezvous hashing moves only the keys won by the new cohort. Keep
        // a generous upper bound so this remains a deterministic safety test
        // rather than a statistical precision assertion.
        assert!(moved < streams.len() * 3 / 5, "moved={moved}");

        let mut reordered = three.clone();
        for cohort in reordered.cohorts.values_mut() {
            cohort.members.reverse();
        }
        for stream in streams {
            assert_eq!(
                cohort_for_stream(&three, &stream),
                cohort_for_stream(&reordered, &stream),
                "stream={stream}"
            );
        }
    }

    #[test]
    fn open_tail_closure_requires_the_durable_cutover_lsn() {
        let previous_segment = StreamSegment {
            start_lsn: 1,
            end_lsn: None,
            cohort_id: 0,
            member_ids: Vec::new(),
            member_hash: String::new(),
            writer_epoch: 0,
            manifest_revision: 1,
            placement_epoch: 0,
            operation_id: "open".to_owned(),
            tier: String::new(),
            max_append_bytes: 0,
        };
        let closed_segment = StreamSegment {
            end_lsn: Some(3),
            ..previous_segment.clone()
        };
        let next_segment = StreamSegment {
            start_lsn: 4,
            end_lsn: None,
            cohort_id: 1,
            operation_id: "cutover".to_owned(),
            ..previous_segment.clone()
        };
        let previous = BTreeMap::from([(String::from("stream"), vec![previous_segment])]);
        let next = BTreeMap::from([(String::from("stream"), vec![closed_segment, next_segment])]);
        assert!(validate_manifest_cutover(&previous, &next, None).is_err());
        assert!(validate_manifest_cutover(&previous, &next, Some(3)).is_ok());
    }

    #[test]
    fn an_empty_trailing_reservation_may_be_dropped_only_at_its_watermark() {
        let sealed = StreamSegment {
            start_lsn: 1,
            end_lsn: Some(3),
            cohort_id: 0,
            member_ids: Vec::new(),
            member_hash: String::new(),
            writer_epoch: 0,
            manifest_revision: 1,
            placement_epoch: 0,
            operation_id: "sealed".to_owned(),
            tier: String::new(),
            max_append_bytes: 0,
        };
        let reservation = StreamSegment {
            start_lsn: 4,
            end_lsn: None,
            cohort_id: 1,
            operation_id: "reservation".to_owned(),
            ..sealed.clone()
        };
        let previous = BTreeMap::from([(
            String::from("stream"),
            vec![sealed.clone(), reservation.clone()],
        )]);
        let dropped = BTreeMap::from([(String::from("stream"), vec![sealed.clone()])]);
        assert!(validate_manifest_transition(&previous, &dropped).is_ok());
        assert!(validate_manifest_cutover(&previous, &dropped, Some(3)).is_ok());
        assert!(validate_manifest_cutover(&previous, &dropped, None).is_err());
        assert!(validate_manifest_cutover(&previous, &dropped, Some(4)).is_err());

        // A sealed range can never be removed, and only the trailing open
        // range may go.
        let truncated = BTreeMap::from([(String::from("stream"), vec![reservation.clone()])]);
        assert!(validate_manifest_transition(&previous, &truncated).is_err());
        let gone: BTreeMap<String, Vec<StreamSegment>> = BTreeMap::new();
        assert!(validate_manifest_transition(&previous, &gone).is_err());

        // A stream whose whole history is one empty reservation disappears.
        let only_reservation = BTreeMap::from([(
            String::from("stream"),
            vec![StreamSegment {
                start_lsn: 1,
                ..reservation
            }],
        )]);
        assert!(validate_manifest_transition(&only_reservation, &gone).is_ok());
        assert!(validate_manifest_cutover(&only_reservation, &gone, Some(0)).is_ok());
        assert!(validate_manifest_cutover(&only_reservation, &gone, None).is_err());
    }
}

#[cfg(test)]
mod capability_tests {
    use super::{GatewayMetrics, StorageNodeStatus, online_reconfiguration_ready};

    fn status(protocol_version: u32) -> StorageNodeStatus {
        StorageNodeStatus {
            boot_id: "boot".to_owned(),
            online_protocol_version: protocol_version,
            node_name: "node".to_owned(),
            tier: "storage".to_owned(),
            log_bytes: 0,
            filesystem: super::FilesystemStatus {
                total_bytes: 1,
                free_bytes: 1,
                used_bytes: 0,
            },
            fs_total_bytes: 1,
            fs_free_bytes: 1,
            fs_used_bytes: 0,
            timestamp_ms: 1,
            observed_at_ms: 1,
            maintenance: false,
            maintenance_owner: None,
            maintenance_generation: 0,
        }
    }

    #[test]
    fn old_storage_status_defaults_to_legacy_protocol() {
        let status = serde_json::from_value::<StorageNodeStatus>(serde_json::json!({
            "boot_id": "boot",
            "node_name": "node",
            "tier": "storage",
            "log_bytes": 0
        }))
        .expect("legacy status should remain readable");
        assert_eq!(status.online_protocol_version, 0);
    }

    #[test]
    fn old_gateway_metrics_default_online_reconfiguration_to_false() {
        let metrics = serde_json::from_value::<GatewayMetrics>(serde_json::json!({
            "version": 1,
            "boot_id": "boot",
            "started_at_ms": 0,
            "timestamp_ms": 0,
            "observed_at_ms": 0,
            "updated_at_ms": 0,
            "append_attempts": 0,
            "append_acks": 0,
            "append_failures": 0,
            "acked_bytes": 0,
            "append_latency_nanos": 0,
            "append_attempts_total": 0,
            "append_acks_total": 0,
            "append_failures_total": 0,
            "acked_bytes_total": 0,
            "append_latency_nanos_total": 0,
            "cpu_utilization": 0.0,
            "memory_utilization": 0.0,
            "disk_free_bytes": 0,
            "disk_total_bytes": 0,
            "throughput_bytes_per_second": 0,
            "acked_bytes_per_second": 0,
            "latency_p95_ms": 0.0,
            "latency_p50_ms": 0.0,
            "window_ms": 0,
            "counters": {
                "append_attempts": 0,
                "append_acks": 0,
                "append_failures": 0,
                "acked_bytes": 0,
                "append_latency_nanos": 0,
                "append_attempts_total": 0,
                "append_acks_total": 0,
                "append_failures_total": 0,
                "acked_bytes_total": 0,
                "append_latency_nanos_total": 0
            },
            "storage_nodes": 0,
            "healthy_storage": 0,
            "active_requests": 0,
            "maintenance": false,
            "storage": []
        }))
        .expect("legacy metrics should remain readable");
        assert!(!metrics.online_reconfiguration);
    }

    #[test]
    fn mixed_active_member_versions_keep_online_reconfiguration_disabled() {
        let mixed = vec![
            status(1),
            status(0),
            status(1),
            status(1),
            status(1),
            status(1),
        ];
        assert!(!online_reconfiguration_ready(
            true,
            true,
            mixed.len(),
            &mixed
        ));

        let current = mixed.iter().map(|_| status(1)).collect::<Vec<_>>();
        assert!(online_reconfiguration_ready(
            true,
            true,
            current.len(),
            &current
        ));
        assert!(!online_reconfiguration_ready(
            false,
            true,
            current.len(),
            &current
        ));
        assert!(!online_reconfiguration_ready(
            true,
            false,
            current.len(),
            &current
        ));
        assert!(!online_reconfiguration_ready(
            true,
            true,
            current.len() + 1,
            &current
        ));
    }
}

mod cas_probe;
pub mod daemon;
