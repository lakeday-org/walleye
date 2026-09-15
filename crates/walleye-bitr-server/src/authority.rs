//! Authority handoff for replica control metadata.
//!
//! A placement cutover has two independent parts.  Records continue to be
//! admitted by the data-plane placement protocol, while the metadata writer
//! moves from the old control cohort to the object-store control head.  This
//! module covers only the latter transition:
//!
//! 1. certify one complete membership/cohort/manifest value from a quorum of
//!    the old metadata replicas;
//! 2. collect durable, metadata-only freeze acknowledgements from a quorum of
//!    those replicas; and
//! 3. construct the first (or next) immutable control head from that exact
//!    certified value.
//!
//! The freeze is deliberately separate from a data-plane writer fence.  It
//! prevents an old coordinator from publishing another metadata revision, but
//! it does not reject record appends or require a global maintenance window.
//! The caller must make the acknowledgement durable on the member and gate
//! every subsequent metadata CAS on it before reporting the acknowledgement.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::control_head::{
    ControlHead, ControlHeadError, ControlHeadSnapshot, ControlHeadStore, ControlMetadata,
    ControlSourceMarker,
};
use crate::{CohortStatus, DurableCohort, DurableMember, MembershipSnapshot};

/// The bootstrap authority name used when an existing control cohort is
/// migrated before it has an object-store head of its own.
pub const LEGACY_CONTROL_AUTHORITY: &str = "legacy-control-quorum";

/// The source marker and complete metadata certified by one old control
/// quorum.  The certificate is a read certificate; callers must obtain a
/// [`MetadataFreezeAck`] quorum before using it to publish a new head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceQuorumCertificate {
    /// Stable name for the old metadata authority.
    pub authority: String,
    /// Authority generation represented by the source state.
    pub authority_epoch: u64,
    /// Source control-head revision.  For a legacy state that has never
    /// adopted an object head this is the nonzero manifest revision, or one
    /// for an empty bootstrap state.
    pub revision: u64,
    /// Exact complete membership/cohort/manifest value certified by the
    /// source quorum.
    pub metadata: ControlMetadata,
    /// Cohort that supplied the source quorum and must be frozen before the
    /// new authority can publish.
    pub control_cohort_id: u64,
    /// All members in the source control cohort, in deterministic order.
    pub control_member_ids: Vec<String>,
    /// Source members whose snapshots agreed on this certificate.
    pub agreeing_member_ids: BTreeSet<String>,
}

impl SourceQuorumCertificate {
    /// Returns the exact source marker carried into the next control head.
    pub fn source_marker(&self) -> Result<ControlSourceMarker, AuthorityError> {
        ControlSourceMarker::new(
            self.authority.clone(),
            self.authority_epoch,
            self.revision,
            self.metadata_digest()?,
        )
        .map_err(AuthorityError::ControlHead)
    }

    /// Returns the self-authenticating digest of the certified metadata.
    pub fn metadata_digest(&self) -> Result<String, AuthorityError> {
        self.metadata.digest().map_err(AuthorityError::ControlHead)
    }

    /// Validates that the certificate can be used for an authority handoff.
    pub fn validate(&self, required_quorum: usize) -> Result<(), AuthorityError> {
        validate_quorum_size(required_quorum, self.control_member_ids.len())?;
        if self.authority.trim().is_empty() {
            return Err(AuthorityError::Invalid(
                "source authority must not be empty".to_owned(),
            ));
        }
        if self.authority_epoch == 0 || self.revision == 0 {
            return Err(AuthorityError::Invalid(
                "source authority epoch and revision must be nonzero".to_owned(),
            ));
        }
        let digest = self.metadata_digest()?;
        let cohorts = self
            .metadata
            .cohorts
            .get(&self.control_cohort_id)
            .ok_or_else(|| {
                AuthorityError::Invalid(
                    "source certificate names an unknown control cohort".to_owned(),
                )
            })?;
        if cohorts.members != self.control_member_ids
            || cohorts.members.len() != crate::REPLICATION_FACTOR
            || cohorts.members.len() < required_quorum
            || !matches!(
                cohorts.status,
                CohortStatus::Active | CohortStatus::Draining
            )
        {
            return Err(AuthorityError::Invalid(
                "source control cohort does not match the certified membership".to_owned(),
            ));
        }
        if self.agreeing_member_ids.len() < required_quorum
            || !self
                .agreeing_member_ids
                .iter()
                .all(|id| self.control_member_ids.binary_search(id).is_ok())
        {
            return Err(AuthorityError::InsufficientQuorum {
                required: required_quorum,
                actual: self.agreeing_member_ids.len(),
            });
        }
        if self.metadata.manifest.validate().is_err() {
            return Err(AuthorityError::Invalid(
                "source certificate contains an invalid manifest".to_owned(),
            ));
        }
        if digest.is_empty() {
            return Err(AuthorityError::Invalid(
                "source metadata digest must not be empty".to_owned(),
            ));
        }
        let marker = ControlSourceMarker::new(
            self.authority.clone(),
            self.authority_epoch,
            self.revision,
            digest,
        )
        .map_err(AuthorityError::ControlHead)?;
        ControlHead::new(
            self.authority_epoch,
            self.revision,
            "source-certificate-validation",
            marker,
            self.metadata.clone(),
        )
        .map_err(AuthorityError::ControlHead)?;
        Ok(())
    }

    /// Creates a metadata-only freeze request for the old source cohort.
    ///
    /// The request does not fence data appends.  Each old control member must
    /// persist the request and reject later metadata mutations whose source
    /// marker is older than this request before returning an acknowledgement.
    pub fn freeze_request(
        &self,
        operation_id: impl Into<String>,
        required_quorum: usize,
    ) -> Result<MetadataFreeze, AuthorityError> {
        self.validate(required_quorum)?;
        let operation_id = operation_id.into();
        validate_operation_id(&operation_id)?;
        Ok(MetadataFreeze {
            operation_id,
            source: self.source_marker()?,
            target_authority_epoch: self
                .authority_epoch
                .checked_add(1)
                .ok_or_else(|| AuthorityError::Invalid("authority epoch exhausted".to_owned()))?,
            control_cohort_id: self.control_cohort_id,
        })
    }

    /// Constructs the first object-store head after this source cohort has
    /// been durably frozen by quorum.
    pub fn initial_head(
        &self,
        frozen: &FrozenSourceQuorum,
        operation_id: impl Into<String>,
    ) -> Result<ControlHead, AuthorityError> {
        frozen.validate()?;
        if frozen.certificate != *self {
            return Err(AuthorityError::Invalid(
                "freeze certificate does not match the source metadata".to_owned(),
            ));
        }
        let marker = self.source_marker()?;
        ControlHead::new(
            frozen.freeze.target_authority_epoch,
            self.revision,
            operation_id,
            marker,
            self.metadata.clone(),
        )
        .map_err(AuthorityError::ControlHead)
    }

    /// Constructs the next object-store head when an already active
    /// object-store authority is handed to another authority.  The source
    /// quorum must prove the exact metadata represented by `current`; this
    /// prevents an old cohort from minting a head from a merely same-revision
    /// value.
    pub fn next_head(
        &self,
        frozen: &FrozenSourceQuorum,
        current: &ControlHeadSnapshot,
        operation_id: impl Into<String>,
    ) -> Result<ControlHead, AuthorityError> {
        frozen.validate()?;
        if frozen.certificate != *self {
            return Err(AuthorityError::Invalid(
                "freeze certificate does not match the source metadata".to_owned(),
            ));
        }
        let digest = self.metadata_digest()?;
        if current.head.metadata_digest != digest
            || current.head.metadata != self.metadata
            || self.authority_epoch != current.head.authority_epoch
            || self.revision != current.head.revision
        {
            return Err(AuthorityError::SourceDoesNotMatchHead);
        }
        let marker = self.source_marker()?;
        ControlHead::new(
            current
                .head
                .authority_epoch
                .checked_add(1)
                .ok_or_else(|| AuthorityError::Invalid("authority epoch exhausted".to_owned()))?,
            current
                .head
                .revision
                .checked_add(1)
                .ok_or_else(|| AuthorityError::Invalid("head revision exhausted".to_owned()))?,
            operation_id,
            marker,
            self.metadata.clone(),
        )
        .map_err(AuthorityError::ControlHead)
    }
}

/// One complete source response, identified by the member that served it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceReplicaSnapshot {
    pub member_id: String,
    pub snapshot: MembershipSnapshot,
}

impl SourceReplicaSnapshot {
    pub fn new(
        member_id: impl Into<String>,
        snapshot: MembershipSnapshot,
    ) -> Result<Self, AuthorityError> {
        let member_id = member_id.into();
        if member_id.trim().is_empty() {
            return Err(AuthorityError::Invalid(
                "source replica member id must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            member_id,
            snapshot,
        })
    }
}

/// A durable request that freezes only the old metadata authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetadataFreeze {
    pub operation_id: String,
    pub source: ControlSourceMarker,
    pub target_authority_epoch: u64,
    pub control_cohort_id: u64,
}

/// Acknowledgement returned only after one old metadata replica has persisted
/// the freeze and gates all future metadata writes on it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MetadataFreezeAck {
    pub member_id: String,
    pub operation_id: String,
    pub source: ControlSourceMarker,
    pub target_authority_epoch: u64,
    pub metadata_digest: String,
    pub control_cohort_id: u64,
}

/// A quorum of durable freeze acknowledgements.  Only this value may be used
/// to publish a new control head from a source certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenSourceQuorum {
    pub certificate: SourceQuorumCertificate,
    pub freeze: MetadataFreeze,
    pub acknowledged_member_ids: BTreeSet<String>,
    pub required_quorum: usize,
}

impl FrozenSourceQuorum {
    /// Validates the full freeze certificate and acknowledgement set.
    pub fn validate(&self) -> Result<(), AuthorityError> {
        self.certificate.validate(self.required_quorum)?;
        validate_operation_id(&self.freeze.operation_id)?;
        let source = self.certificate.source_marker()?;
        if self.freeze.source != source
            || self.freeze.control_cohort_id != self.certificate.control_cohort_id
            || self.freeze.target_authority_epoch
                != self
                    .certificate
                    .authority_epoch
                    .checked_add(1)
                    .ok_or_else(|| {
                        AuthorityError::Invalid("authority epoch exhausted".to_owned())
                    })?
        {
            return Err(AuthorityError::FreezeMismatch);
        }
        if self.acknowledged_member_ids.len() < self.required_quorum
            || !self.acknowledged_member_ids.iter().all(|id| {
                self.certificate
                    .control_member_ids
                    .binary_search(id)
                    .is_ok()
            })
        {
            return Err(AuthorityError::InsufficientQuorum {
                required: self.required_quorum,
                actual: self.acknowledged_member_ids.len(),
            });
        }
        Ok(())
    }

    /// Publishes the first head after source freeze.  A conditional create
    /// race remains an explicit result from [`ControlHeadStore::create`].
    pub async fn publish_initial(
        &self,
        store: &ControlHeadStore,
        operation_id: impl Into<String>,
    ) -> Result<ControlHeadSnapshot, AuthorityError> {
        let head = self.certificate.initial_head(self, operation_id)?;
        store
            .create(head)
            .await
            .map_err(AuthorityError::ControlHead)
    }

    /// Publishes the next head after an already active authority handoff.
    /// The store performs the final ETag CAS; this helper intentionally does
    /// not retry a failed serialization.
    pub async fn publish_next(
        &self,
        store: &ControlHeadStore,
        current: &ControlHeadSnapshot,
        operation_id: impl Into<String>,
    ) -> Result<ControlHeadSnapshot, AuthorityError> {
        let head = self.certificate.next_head(self, current, operation_id)?;
        store
            .update(current, head)
            .await
            .map_err(AuthorityError::ControlHead)
    }
}

/// Certifies one complete metadata value from a source control quorum.
///
/// Responses are grouped by the complete canonical metadata and source
/// revision.  A manifest-only match is insufficient: membership, cohorts,
/// and every immutable stream range must agree before a source can be frozen.
pub fn certify_source_quorum(
    authority: impl Into<String>,
    responses: &[SourceReplicaSnapshot],
    required_quorum: usize,
) -> Result<SourceQuorumCertificate, AuthorityError> {
    let authority = authority.into();
    if authority.trim().is_empty() {
        return Err(AuthorityError::Invalid(
            "source authority must not be empty".to_owned(),
        ));
    }
    validate_quorum_size(required_quorum, responses.len())?;

    let mut candidates = Vec::<(SourceQuorumCertificate, BTreeSet<String>)>::new();
    for response in responses {
        let candidate = candidate_from_snapshot(&authority, response)?;
        if let Some((existing, members)) = candidates
            .iter_mut()
            .find(|(existing, _)| same_source(existing, &candidate))
        {
            if !members.insert(response.member_id.clone()) {
                return Err(AuthorityError::Invalid(
                    "source quorum response contains a duplicate member".to_owned(),
                ));
            }
            existing.agreeing_member_ids = members.clone();
        } else {
            let mut members = BTreeSet::new();
            members.insert(response.member_id.clone());
            candidates.push((candidate, members));
        }
    }

    let winners = candidates
        .iter()
        .filter(|(_, members)| members.len() >= required_quorum)
        .collect::<Vec<_>>();
    if winners.len() != 1 {
        let actual = winners
            .iter()
            .map(|(_, members)| members.len())
            .max()
            .unwrap_or(0);
        return Err(AuthorityError::InsufficientQuorum {
            required: required_quorum,
            actual,
        });
    }
    let (mut certificate, members) = winners[0].clone();
    certificate.agreeing_member_ids = members.clone();
    certificate.validate(required_quorum)?;
    Ok(certificate)
}

/// Certifies that enough source replicas have durably installed the exact
/// metadata-only freeze.  An acknowledgement from an unlisted member or one
/// carrying another source digest is never counted.
pub fn certify_frozen_source(
    certificate: SourceQuorumCertificate,
    freeze: MetadataFreeze,
    acknowledgements: &[MetadataFreezeAck],
    required_quorum: usize,
) -> Result<FrozenSourceQuorum, AuthorityError> {
    certificate.validate(required_quorum)?;
    let expected_source = certificate.source_marker()?;
    if freeze.source != expected_source
        || freeze.control_cohort_id != certificate.control_cohort_id
        || freeze.target_authority_epoch
            != certificate
                .authority_epoch
                .checked_add(1)
                .ok_or_else(|| AuthorityError::Invalid("authority epoch exhausted".to_owned()))?
    {
        return Err(AuthorityError::FreezeMismatch);
    }
    validate_operation_id(&freeze.operation_id)?;
    let metadata_digest = certificate.metadata_digest()?;
    let mut acknowledged_member_ids = BTreeSet::new();
    for ack in acknowledgements {
        if ack.operation_id != freeze.operation_id
            || ack.source != freeze.source
            || ack.target_authority_epoch != freeze.target_authority_epoch
            || ack.metadata_digest != metadata_digest
            || ack.control_cohort_id != freeze.control_cohort_id
            || certificate
                .control_member_ids
                .binary_search(&ack.member_id)
                .is_err()
            || !acknowledged_member_ids.insert(ack.member_id.clone())
        {
            return Err(AuthorityError::FreezeMismatch);
        }
    }
    if acknowledged_member_ids.len() < required_quorum {
        return Err(AuthorityError::InsufficientQuorum {
            required: required_quorum,
            actual: acknowledged_member_ids.len(),
        });
    }
    let frozen = FrozenSourceQuorum {
        certificate,
        freeze,
        acknowledged_member_ids,
        required_quorum,
    };
    frozen.validate()?;
    Ok(frozen)
}

fn candidate_from_snapshot(
    authority: &str,
    response: &SourceReplicaSnapshot,
) -> Result<SourceQuorumCertificate, AuthorityError> {
    let snapshot = &response.snapshot;
    let members = snapshot
        .members
        .iter()
        .cloned()
        .map(|member| (member.id.clone(), member))
        .collect::<BTreeMap<String, DurableMember>>();
    if members.len() != snapshot.members.len() {
        return Err(AuthorityError::Invalid(
            "source membership contains duplicate member identities".to_owned(),
        ));
    }
    let cohorts = snapshot
        .cohorts
        .iter()
        .cloned()
        .map(|cohort| (cohort.id, cohort))
        .collect::<BTreeMap<u64, DurableCohort>>();
    if cohorts.len() != snapshot.cohorts.len() {
        return Err(AuthorityError::Invalid(
            "source membership contains duplicate cohort identities".to_owned(),
        ));
    }
    let manifest = snapshot.manifest.clone().ok_or_else(|| {
        AuthorityError::Invalid("source snapshot has no complete routing manifest".to_owned())
    })?;
    if manifest.revision != snapshot.manifest_revision
        || (manifest.revision > 0 && manifest.digest != snapshot.manifest_digest)
        || manifest.write_cohort_id != snapshot.write_cohort_id
        || manifest.stream_segments != snapshot.stream_segments
    {
        return Err(AuthorityError::Invalid(
            "source snapshot manifest fields disagree".to_owned(),
        ));
    }
    manifest
        .validate()
        .map_err(|error| AuthorityError::Invalid(error.to_string()))?;
    let metadata = ControlMetadata {
        membership_epoch: snapshot.membership_epoch,
        members,
        cohorts,
        manifest,
        pending_handoffs: BTreeMap::new(),
    };
    // ControlHead::new is also the single validation path for the complete
    // metadata graph, including cohort membership and segment ownership.
    let metadata_digest = metadata.digest().map_err(AuthorityError::ControlHead)?;
    let authority_epoch = if snapshot.control_authority_epoch == 0 {
        1
    } else {
        snapshot.control_authority_epoch
    };
    let revision = if snapshot.control_head_revision > 0 {
        if snapshot.control_head_digest.is_empty()
            || snapshot.control_head_digest != metadata_digest
        {
            return Err(AuthorityError::Invalid(
                "source control-head revision has no matching metadata digest".to_owned(),
            ));
        }
        snapshot.control_head_revision
    } else {
        snapshot.manifest_revision.max(1)
    };
    if !snapshot.control_head_digest.is_empty() && snapshot.control_head_digest != metadata_digest {
        return Err(AuthorityError::Invalid(
            "source control-head digest does not match complete metadata".to_owned(),
        ));
    }
    let cohort = metadata
        .cohorts
        .get(&snapshot.control_authority_cohort_id)
        .ok_or_else(|| {
            AuthorityError::Invalid("source control authority cohort is missing".to_owned())
        })?;
    if cohort.members.len() != crate::REPLICATION_FACTOR
        || cohort
            .members
            .iter()
            .any(|id| !metadata.members.contains_key(id))
    {
        return Err(AuthorityError::Invalid(
            "source control cohort is incomplete".to_owned(),
        ));
    }
    if cohort.members.binary_search(&response.member_id).is_err() {
        return Err(AuthorityError::Invalid(
            "source response did not come from its control cohort".to_owned(),
        ));
    }
    let control_member_ids = cohort.members.clone();
    let source = ControlSourceMarker::new(
        authority,
        authority_epoch,
        revision,
        metadata_digest.clone(),
    )
    .map_err(AuthorityError::ControlHead)?;
    ControlHead::new(
        authority_epoch,
        revision,
        "source-certificate-validation",
        source,
        metadata.clone(),
    )
    .map_err(AuthorityError::ControlHead)?;
    Ok(SourceQuorumCertificate {
        authority: authority.to_owned(),
        authority_epoch,
        revision,
        metadata,
        control_cohort_id: snapshot.control_authority_cohort_id,
        control_member_ids,
        agreeing_member_ids: BTreeSet::new(),
    })
}

fn same_source(left: &SourceQuorumCertificate, right: &SourceQuorumCertificate) -> bool {
    left.authority == right.authority
        && left.authority_epoch == right.authority_epoch
        && left.revision == right.revision
        && left.control_cohort_id == right.control_cohort_id
        && left.control_member_ids == right.control_member_ids
        && left.metadata == right.metadata
}

fn validate_quorum_size(required: usize, available: usize) -> Result<(), AuthorityError> {
    if required == 0 || required > available {
        return Err(AuthorityError::InsufficientQuorum {
            required,
            actual: available,
        });
    }
    Ok(())
}

fn validate_operation_id(operation_id: &str) -> Result<(), AuthorityError> {
    if operation_id.trim().is_empty() {
        return Err(AuthorityError::Invalid(
            "authority operation id must not be empty".to_owned(),
        ));
    }
    Ok(())
}

/// Errors returned while certifying or publishing an authority handoff.
#[derive(Debug, Error)]
pub enum AuthorityError {
    #[error("invalid replica authority handoff: {0}")]
    Invalid(String),
    #[error("source metadata quorum is insufficient: required {required}, got {actual}")]
    InsufficientQuorum { required: usize, actual: usize },
    #[error("metadata freeze does not match the certified source")]
    FreezeMismatch,
    #[error("source quorum does not match the active control head")]
    SourceDoesNotMatchHead,
    #[error("control-head operation failed: {0}")]
    ControlHead(#[from] ControlHeadError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReplicaManifest;
    use serde_json::json;

    fn manifest() -> ReplicaManifest {
        serde_json::from_value(json!({
            "version": 1,
            "revision": 1,
            "digest": "",
            "writer_epoch": 1,
            "cohort_id": 0,
            "member_set_hash": "",
            "write_cohort_id": 0,
            "stream_segments": {},
            "operation_id": "bootstrap",
            "tier": "hot",
            "max_append_bytes": 1024
        }))
        .expect("manifest")
    }

    fn snapshot(member_id: &str, digest: Option<String>) -> SourceReplicaSnapshot {
        let members = (0..3)
            .map(|index| DurableMember {
                id: format!("m{index}"),
                url: format!("http://m{index}"),
                cohort_id: 0,
                ..DurableMember::default()
            })
            .collect::<Vec<_>>();
        let cohorts = vec![DurableCohort {
            id: 0,
            members: members.iter().map(|member| member.id.clone()).collect(),
            status: CohortStatus::Active,
            tier: "hot".to_owned(),
            max_append_bytes: 1024,
        }];
        let mut manifest = manifest();
        manifest.digest = crate::manifest_digest(&manifest.stream_segments);
        manifest.member_set_hash = "members".to_owned();
        let snapshot = MembershipSnapshot {
            membership_epoch: 4,
            manifest_revision: manifest.revision,
            manifest_digest: digest.unwrap_or_else(|| manifest.digest.clone()),
            write_cohort_id: 0,
            control_authority_epoch: 1,
            control_head_revision: 0,
            control_head_digest: String::new(),
            control_authority_cohort_id: 0,
            metadata_freeze: None,
            manifest: Some(manifest),
            members,
            cohorts,
            stream_segments: BTreeMap::new(),
        };
        SourceReplicaSnapshot::new(member_id, snapshot).expect("source snapshot")
    }

    #[test]
    fn only_complete_metadata_quorum_is_certified() {
        let responses = vec![snapshot("m0", None), snapshot("m1", None), {
            let mut divergent = snapshot("m2", None);
            divergent.snapshot.members[0].url = "http://different".to_owned();
            divergent
        }];
        let certificate = certify_source_quorum("legacy", &responses, 2).expect("quorum");
        assert_eq!(certificate.agreeing_member_ids.len(), 2);
        assert_eq!(certificate.control_member_ids, vec!["m0", "m1", "m2"]);
    }

    #[test]
    fn freeze_requires_matching_durable_acknowledgements() {
        let responses = vec![snapshot("m0", None), snapshot("m1", None)];
        let certificate = certify_source_quorum("legacy", &responses, 2).expect("quorum");
        let freeze = certificate.freeze_request("freeze-1", 2).expect("freeze");
        let digest = certificate.metadata_digest().expect("digest");
        let freeze_for_ack = freeze.clone();
        let ack = |id: &str| MetadataFreezeAck {
            member_id: id.to_owned(),
            operation_id: freeze_for_ack.operation_id.clone(),
            source: freeze_for_ack.source.clone(),
            target_authority_epoch: freeze_for_ack.target_authority_epoch,
            metadata_digest: digest.clone(),
            control_cohort_id: freeze_for_ack.control_cohort_id,
        };
        let frozen = certify_frozen_source(certificate, freeze, &[ack("m0"), ack("m1")], 2)
            .expect("frozen quorum");
        assert_eq!(frozen.acknowledged_member_ids.len(), 2);
    }

    #[test]
    fn divergent_freeze_ack_is_never_counted() {
        let responses = vec![snapshot("m0", None), snapshot("m1", None)];
        let certificate = certify_source_quorum("legacy", &responses, 2).expect("quorum");
        let freeze = certificate.freeze_request("freeze-1", 2).expect("freeze");
        let ack = MetadataFreezeAck {
            member_id: "m0".to_owned(),
            operation_id: freeze.operation_id.clone(),
            source: freeze.source.clone(),
            target_authority_epoch: freeze.target_authority_epoch,
            metadata_digest: "wrong".to_owned(),
            control_cohort_id: freeze.control_cohort_id,
        };
        assert!(matches!(
            certify_frozen_source(certificate, freeze, &[ack], 2),
            Err(AuthorityError::FreezeMismatch)
        ));
    }
}
