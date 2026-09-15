//! Runtime integration for the object-store metadata authority.
//!
//! `authority.rs` contains the pure certificate state machine.  This module
//! binds it to the durable per-volume control document and the gateway's
//! private node protocol.  The old quorum remains the source of truth until a
//! quorum has persisted the metadata-only freeze; after that point its
//! membership and manifest CAS methods reject every new write.  The data
//! append path does not consult this fence.

use crate::authority::{
    LEGACY_CONTROL_AUTHORITY, MetadataFreeze, MetadataFreezeAck, SourceReplicaSnapshot,
    certify_frozen_source, certify_source_quorum,
};
use crate::control_head::{
    ControlHead, ControlHeadError, ControlHeadSnapshot, ControlHeadStore, ControlMetadata,
    ControlSourceMarker,
};
use crate::{
    AuthorityError, DurableControl, DurableControlState, MembershipSnapshot, NodeClient, NodeState,
    ReplicaError, ReplicaGateway, persist_control_state,
};
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use futures::future::join_all;

/// A public view of the currently loaded metadata authority.
#[derive(Clone, Debug)]
pub struct MetadataAuthorityState {
    pub head: Option<ControlHeadSnapshot>,
    pub freeze: Option<MetadataFreeze>,
}

/// A complete head candidate submitted to the object-store CAS.
#[derive(Clone, Debug)]
pub struct MetadataHeadUpdate {
    pub expected: Option<ControlHeadSnapshot>,
    pub next: ControlHead,
}

/// Errors raised by the runtime metadata authority integration.
#[derive(Debug, thiserror::Error)]
pub enum AuthoritativeHeadError {
    #[error("replica metadata authority error: {0}")]
    Replica(#[from] ReplicaError),
    #[error("replica authority certificate error: {0}")]
    Authority(#[from] AuthorityError),
    #[error("replica control-head error: {0}")]
    ControlHead(#[from] ControlHeadError),
}

/// Compatibility alias for callers that name the operation error explicitly.
pub type MetadataHeadUpdateError = AuthoritativeHeadError;

fn control_metadata(state: &DurableControlState) -> Result<ControlMetadata, ReplicaError> {
    let metadata = crate::control_metadata_from_state(state);
    metadata
        .digest()
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
    Ok(metadata)
}

fn source_marker_for_state(
    state: &DurableControlState,
    metadata_digest: &str,
) -> Result<ControlSourceMarker, ReplicaError> {
    let authority_epoch = state.control_authority_epoch.max(1);
    let revision = if state.control_head_revision == 0 {
        state.manifest_revision.max(1)
    } else {
        state.control_head_revision
    };
    if !state.control_head_digest.is_empty() && state.control_head_digest != metadata_digest {
        return Err(ReplicaError::LsnConflict);
    }
    ControlSourceMarker::new(
        LEGACY_CONTROL_AUTHORITY,
        authority_epoch,
        revision,
        metadata_digest,
    )
    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))
}

fn ensure_request_matches_state(
    state: &DurableControlState,
    request: &MetadataFreeze,
) -> Result<String, ReplicaError> {
    if request.operation_id.trim().is_empty() {
        return Err(ReplicaError::Protocol(
            "metadata freeze operation_id must not be empty".to_owned(),
        ));
    }
    let metadata = control_metadata(state)?;
    let digest = metadata
        .digest()
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
    let expected = source_marker_for_state(state, &digest)?;
    if request.source != expected
        || request.control_cohort_id != state.control_authority_cohort_id
        || request.target_authority_epoch
            != state
                .control_authority_epoch
                .max(1)
                .checked_add(1)
                .ok_or_else(|| ReplicaError::Protocol("authority epoch exhausted".to_owned()))?
    {
        return Err(ReplicaError::LsnConflict);
    }
    let cohort = state
        .cohorts
        .get(&state.control_authority_cohort_id)
        .ok_or(ReplicaError::QuorumUnavailable)?;
    if cohort.members.len() != crate::REPLICATION_FACTOR
        || cohort
            .members
            .iter()
            .any(|id| !state.members.contains_key(id))
    {
        return Err(ReplicaError::QuorumUnavailable);
    }
    Ok(digest)
}

impl DurableControl {
    /// Returns the durable source freeze, if this volume has acknowledged one.
    pub fn metadata_freeze(&self) -> Result<Option<MetadataFreeze>, ReplicaError> {
        self.with_state(|state| state.metadata_freeze.clone())
    }

    /// Persists a metadata-only freeze and returns the exact acknowledgement
    /// that may be counted by the source quorum.  This method does not touch
    /// the append log or the node's placement fences.
    pub fn freeze_metadata_authority(
        &self,
        member_id: &str,
        request: &MetadataFreeze,
    ) -> Result<MetadataFreezeAck, ReplicaError> {
        if member_id.trim().is_empty() {
            return Err(ReplicaError::Protocol(
                "metadata freeze member id must not be empty".to_owned(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicaError::NodeStorage("control lock is poisoned".to_owned()))?;
        let metadata_digest = ensure_request_matches_state(&state, request)?;
        let cohort = state
            .cohorts
            .get(&request.control_cohort_id)
            .ok_or(ReplicaError::QuorumUnavailable)?;
        if !cohort.members.iter().any(|id| id == member_id) {
            return Err(ReplicaError::LsnConflict);
        }
        if let Some(previous) = &state.metadata_freeze {
            if previous != request {
                return Err(ReplicaError::WriterFenced);
            }
        } else {
            let mut next = state.clone();
            next.metadata_freeze = Some(request.clone());
            persist_control_state(&self.path, &next)?;
            *state = next;
        }
        Ok(MetadataFreezeAck {
            member_id: member_id.to_owned(),
            operation_id: request.operation_id.clone(),
            source: request.source.clone(),
            target_authority_epoch: request.target_authority_epoch,
            metadata_digest,
            control_cohort_id: request.control_cohort_id,
        })
    }

    /// Installs an object-store head on a node after source freeze.  A frozen
    /// node accepts only a head at or beyond the freeze target and carrying the
    /// exact frozen source digest.
    pub fn adopt_authoritative_head(&self, head: &ControlHead) -> Result<(), ReplicaError> {
        if self.metadata_freeze()?.is_some_and(|freeze| {
            head.authority_epoch < freeze.target_authority_epoch
                || head.source_marker.digest != freeze.source.digest
        }) {
            return Err(ReplicaError::LsnConflict);
        }
        self.adopt_control_head(head)
    }
}

impl ReplicaGateway {
    pub(crate) fn control_head_store(&self) -> Result<&ControlHeadStore, ReplicaError> {
        self.control_head.as_ref().ok_or_else(|| {
            ReplicaError::Protocol(
                "object-store metadata authority requires direct mode".to_owned(),
            )
        })
    }

    /// Loads the current object-store metadata head without consulting the old
    /// control quorum.  A missing object is the pre-migration state.
    pub async fn load_authoritative_head(
        &self,
    ) -> Result<Option<ControlHeadSnapshot>, ReplicaError> {
        let loaded = self
            .control_head_store()?
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if loaded.is_none()
            && self
                .control()
                .map(|control| control.state())
                .transpose()?
                .is_some_and(|state| state.control_head_revision != 0)
        {
            return Err(ReplicaError::NodeStorage(
                "authoritative control head is missing after migration".to_owned(),
            ));
        }
        Ok(loaded)
    }

    /// Applies one complete metadata update through the object-store ETag CAS.
    /// This path intentionally has no retry loop; callers must reload and
    /// reconcile a concurrent writer's head.
    pub async fn cas_authoritative_head(
        &self,
        expected: &ControlHeadSnapshot,
        next: ControlHead,
    ) -> Result<ControlHeadSnapshot, ReplicaError> {
        let snapshot = self
            .control_head_store()?
            .update(expected, next)
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        if let Some(control) = self.control() {
            control.adopt_authoritative_head(&snapshot.head)?;
        }
        Ok(snapshot)
    }

    /// Freezes the current metadata quorum and creates (or advances) the
    /// object-store head from the quorum-certified complete value.  Record
    /// appends remain available throughout; only subsequent metadata CASes on
    /// the old control files are fenced.
    pub async fn migrate_control_authority(
        &self,
        operation_id: &str,
    ) -> Result<ControlHeadSnapshot, ReplicaError> {
        let head_store = self.control_head_store()?.clone();

        // The object head is the authority once it exists.  A restarted
        // coordinator must adopt that committed value before it contacts the
        // retired source cohort; otherwise an old quorum outage would make a
        // successful migration appear unavailable and could mint a second
        // head from stale local state.
        if let Some(existing) = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
        {
            if let Some(control) = self.control() {
                control.adopt_authoritative_head(&existing.head)?;
            }
            return Ok(existing);
        }
        if self
            .control()
            .map(|control| control.state())
            .transpose()?
            .is_some_and(|state| state.control_head_revision != 0)
        {
            return Err(ReplicaError::NodeStorage(
                "authoritative control head is missing after migration".to_owned(),
            ));
        }

        let peers = self.control_cohort_nodes()?;
        let responses = join_all(peers.iter().cloned().map(|node| {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            async move {
                let result = NodeClient::new(node.clone(), &client, &token)
                    .control_state()
                    .await;
                (node, result)
            }
        }))
        .await;
        let source_responses = responses
            .into_iter()
            .filter_map(|(node, result)| match result {
                Ok(Some(state)) => {
                    SourceReplicaSnapshot::new(node.id, MembershipSnapshot::from_state(&state)).ok()
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if source_responses
            .iter()
            .any(|response| response.snapshot.control_head_revision != 0)
        {
            return Err(ReplicaError::NodeStorage(
                "source cohort reports a migrated control head that is missing from object storage"
                    .to_owned(),
            ));
        }
        let certificate =
            certify_source_quorum(LEGACY_CONTROL_AUTHORITY, &source_responses, self.quorum)
                .map_err(|error| ReplicaError::Protocol(error.to_string()))?;

        // A crash after the source quorum persisted its freeze but before the
        // object Create must resume that exact operation.  Generating a new
        // operation id here would conflict with the durable freeze and could
        // never produce a quorum certificate after restart.  Ignore freezes
        // attached to divergent source snapshots; only a freeze for the
        // certified source metadata can be resumed.
        let expected_source = certificate
            .source_marker()
            .map_err(|error| ReplicaError::Protocol(error.to_string()))?;
        let mut durable_freeze: Option<MetadataFreeze> = None;
        let mut consider_freeze = |candidate: Option<&MetadataFreeze>| -> Result<(), ReplicaError> {
            let Some(candidate) = candidate else {
                return Ok(());
            };
            if candidate.source != expected_source
                || candidate.control_cohort_id != certificate.control_cohort_id
                || candidate.target_authority_epoch
                    != certificate.authority_epoch.checked_add(1).ok_or_else(|| {
                        ReplicaError::Protocol("authority epoch exhausted".to_owned())
                    })?
            {
                return Ok(());
            }
            if let Some(existing) = &durable_freeze {
                if existing != candidate {
                    return Err(ReplicaError::Protocol(
                        "source quorum contains conflicting durable metadata freezes".to_owned(),
                    ));
                }
            } else {
                durable_freeze = Some(candidate.clone());
            }
            Ok(())
        };
        if let Some(control) = self.control() {
            consider_freeze(
                control
                    .metadata_freeze()
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
                    .as_ref(),
            )?;
        }
        for response in &source_responses {
            consider_freeze(response.snapshot.metadata_freeze.as_ref())?;
        }
        let freeze = if let Some(freeze) = durable_freeze {
            freeze
        } else {
            certificate
                .freeze_request(operation_id.to_owned(), self.quorum)
                .map_err(|error| ReplicaError::Protocol(error.to_string()))?
        };
        let effective_operation_id = freeze.operation_id.clone();
        let acknowledgements = join_all(peers.iter().cloned().map(|node| {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            let freeze = freeze.clone();
            async move {
                NodeClient::new(node, &client, &token)
                    .freeze_metadata_authority(&freeze)
                    .await
                    .ok()
            }
        }))
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let frozen = certify_frozen_source(certificate, freeze, &acknowledgements, self.quorum)
            .map_err(|error| ReplicaError::Protocol(error.to_string()))?;

        // A concurrent migration may have won the Create while the source
        // quorum was being frozen.  Recheck the permanent authority before
        // constructing a head from the old certificate.
        let current = head_store
            .load()
            .await
            .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let snapshot = if let Some(existing) = current {
            existing
        } else {
            match frozen
                .publish_initial(&head_store, effective_operation_id)
                .await
            {
                Ok(snapshot) => snapshot,
                Err(AuthorityError::ControlHead(ControlHeadError::AlreadyExists)) => head_store
                    .load()
                    .await
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?
                    .ok_or_else(|| {
                        ReplicaError::Protocol(
                            "control-head Create lost a race but no head is readable".to_owned(),
                        )
                    })?,
                Err(error) => return Err(ReplicaError::Protocol(error.to_string())),
            }
        };
        if let Some(control) = self.control() {
            control.adopt_authoritative_head(&snapshot.head)?;
        }
        // Adoption is metadata-only. It can lag one node without blocking the
        // object-head commit; the next coordinator load repairs it from the
        // same immutable head.
        for node in peers {
            let client = self.client.clone();
            let token = self.internal_token.clone();
            let head = snapshot.head.clone();
            tokio::spawn(async move {
                let _ = NodeClient::new(node, &client, &token)
                    .adopt_control_head(&head)
                    .await;
            });
        }
        Ok(snapshot)
    }
}

impl<'a> NodeClient<'a> {
    async fn freeze_metadata_authority(
        &self,
        request: &MetadataFreeze,
    ) -> Result<MetadataFreezeAck, ReplicaError> {
        let response = self
            .http
            .post(format!(
                "{}/internal/v1/control/metadata/freeze",
                self.node.url
            ))
            .header(crate::INTERNAL_AUTH_HEADER, self.internal_token)
            .json(request)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        if response.status() == StatusCode::CONFLICT {
            return Err(ReplicaError::WriterFenced);
        }
        if !response.status().is_success() {
            return Err(ReplicaError::NodeUnavailable);
        }
        response
            .json::<MetadataFreezeAck>()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)
    }

    pub(crate) async fn adopt_control_head(&self, head: &ControlHead) -> Result<(), ReplicaError> {
        let response = self
            .http
            .post(format!(
                "{}/internal/v1/control/metadata/adopt",
                self.node.url
            ))
            .header(crate::INTERNAL_AUTH_HEADER, self.internal_token)
            .json(head)
            .send()
            .await
            .map_err(|_| ReplicaError::NodeUnavailable)?;
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::CONFLICT => Err(ReplicaError::LsnConflict),
            _ => Err(ReplicaError::NodeUnavailable),
        }
    }
}

/// Node endpoint that durably freezes metadata authority without fencing
/// appends.
pub(crate) async fn internal_metadata_freeze(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(request): Json<MetadataFreeze>,
) -> Result<Json<MetadataFreezeAck>, StatusCode> {
    if !crate::permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let control = state.control.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    control
        .freeze_metadata_authority(&state.node.node_name, &request)
        .map(Json)
        .map_err(crate::control_status)
}

/// Node endpoint that installs a complete object-store authority head into
/// the local cache after source freeze.
pub(crate) async fn internal_metadata_adopt(
    State(state): State<NodeState>,
    headers: HeaderMap,
    Json(head): Json<ControlHead>,
) -> Result<StatusCode, StatusCode> {
    if !crate::permits_internal(&headers, state.internal_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let control = state.control.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    control
        .adopt_authoritative_head(&head)
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(crate::control_status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DurableControl, ReplicaNode};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn nodes() -> Vec<ReplicaNode> {
        (0..3)
            .map(|index| {
                ReplicaNode::new(
                    format!("m{index}"),
                    format!("http://127.0.0.1:{}", 10000 + index),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn metadata_freeze_survives_restart_and_fences_both_legacy_cas_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let control = DurableControl::open(directory.path().join("control.json"), &nodes())?;
        let snapshot = control.membership()?;
        let responses = vec![
            SourceReplicaSnapshot::new("m0", snapshot.clone())?,
            SourceReplicaSnapshot::new("m1", snapshot.clone())?,
        ];
        let certificate = certify_source_quorum(LEGACY_CONTROL_AUTHORITY, &responses, 2)?;
        let freeze = certificate.freeze_request("freeze-1", 2)?;
        let first_ack = control.freeze_metadata_authority("m0", &freeze)?;
        let replay_ack = control.freeze_metadata_authority("m0", &freeze)?;
        assert_eq!(first_ack, replay_ack);
        assert_eq!(control.metadata_freeze()?, Some(freeze.clone()));

        let current = control.membership()?;
        assert!(matches!(
            control.cas_membership_document(
                current.membership_epoch,
                current.members,
                None,
                None,
                "blocked-membership",
            ),
            Err(ReplicaError::WriterFenced)
        ));
        assert!(matches!(
            control.cas_manifest(
                current.manifest_revision,
                &current.manifest_digest,
                BTreeMap::new(),
                "blocked-manifest",
                false,
            ),
            Err(ReplicaError::WriterFenced)
        ));

        let restarted = DurableControl::open(directory.path().join("control.json"), &nodes())?;
        assert_eq!(restarted.metadata_freeze()?, Some(freeze));
        Ok(())
    }
}
