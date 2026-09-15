//! Retirement receipts are derived from the permanent metadata authority and
//! durable archive heads. A membership response alone never permits Machine
//! destruction, even when it says that the old members have been removed.

use std::collections::BTreeMap;

use futures::future::join_all;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CohortStatus, ControlHead, MemberStatus, REPLICATION_FACTOR, ReplicaError, ReplicaGateway,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnlineCohortReplacementReceipt {
    pub operation_id: String,
    pub cohort_id: u64,
    pub old_members_removed: bool,
    pub all_ranges_archived: bool,
    pub hot_tail_archived: bool,
    pub metadata_authority_moved: bool,
    pub can_retire: bool,
    pub archive_proof: String,
    pub authority_marker: String,
}

fn invalid(reason: &str) -> ReplicaError {
    ReplicaError::Protocol(format!("cohort retirement is not proven: {reason}"))
}

/// Reads only one authoritative metadata value. In particular, an open range
/// is a failed proof, rather than a range to omit from the archive checks.
fn required_archive_prefixes(
    head: &ControlHead,
    operation_id: &str,
    source_cohort_id: u64,
    receipt_cohort_id: u64,
) -> Result<BTreeMap<String, u64>, ReplicaError> {
    if operation_id.trim().is_empty()
        || head.authority_epoch < 2
        || !head.completed_operations.contains_key(operation_id)
    {
        return Err(invalid(
            "the operation has no committed object-head receipt",
        ));
    }
    let source = head
        .metadata
        .cohorts
        .get(&source_cohort_id)
        .ok_or_else(|| invalid("source cohort is absent"))?;
    if source.status != CohortStatus::Retired || source.members.len() != REPLICATION_FACTOR {
        return Err(invalid("source cohort has not retired"));
    }
    for id in &source.members {
        let member = head
            .metadata
            .members
            .get(id)
            .ok_or_else(|| invalid("source member is absent"))?;
        if member.cohort_id != source_cohort_id || member.status != MemberStatus::Removed {
            return Err(invalid("a source member still participates in writes"));
        }
    }
    let eligible = head.metadata.cohorts.values().any(|cohort| {
        cohort.id != source_cohort_id
            && (receipt_cohort_id == source_cohort_id || cohort.id == receipt_cohort_id)
            && cohort.status == CohortStatus::Active
            && cohort.members.len() == REPLICATION_FACTOR
            && cohort.members.iter().all(|id| {
                head.metadata.members.get(id).is_some_and(|member| {
                    member.cohort_id == cohort.id && member.status == MemberStatus::Active
                })
            })
    });
    if !eligible {
        return Err(invalid("no complete successor cohort remains"));
    }
    let mut prefixes = BTreeMap::<String, u64>::new();
    for (stream, segments) in &head.metadata.manifest.stream_segments {
        for segment in segments
            .iter()
            .filter(|segment| segment.cohort_id == source_cohort_id)
        {
            let end = segment
                .end_lsn
                .ok_or_else(|| invalid("a source stream range is still open"))?;
            if segment.member_ids != source.members || end < segment.start_lsn {
                return Err(invalid("source range identity or boundary is inconsistent"));
            }
            prefixes
                .entry(stream.clone())
                .and_modify(|value| *value = (*value).max(end))
                .or_insert(end);
        }
    }
    Ok(prefixes)
}

impl ReplicaGateway {
    async fn verify_source_archive_ranges(
        &self,
        manifest: &crate::ReplicaManifest,
        source_cohort_id: u64,
    ) -> Result<(), ReplicaError> {
        for (stream, segments) in &manifest.stream_segments {
            for segment in segments
                .iter()
                .filter(|segment| segment.cohort_id == source_cohort_id)
            {
                let end = segment
                    .end_lsn
                    .ok_or_else(|| invalid("source archive proof contains an open range"))?;
                self.archive
                    .verify_range(stream, segment.start_lsn, end)
                    .await
                    .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            }
        }
        Ok(())
    }

    /// Publishes historical finite source ranges while the source members
    /// are still readable. Call with the proposed complete manifest before
    /// committing Removed membership; the later receipt independently checks
    /// the resulting archive heads against the committed manifest.
    pub(crate) async fn archive_source_finite_ranges(
        &self,
        manifest: &crate::ReplicaManifest,
        source_cohort_id: u64,
    ) -> Result<(), ReplicaError> {
        let mut prefixes = BTreeMap::<String, u64>::new();
        for (stream, segments) in &manifest.stream_segments {
            for segment in segments
                .iter()
                .filter(|segment| segment.cohort_id == source_cohort_id)
            {
                let end = segment
                    .end_lsn
                    .ok_or_else(|| invalid("a source range remains open before retirement"))?;
                prefixes
                    .entry(stream.clone())
                    .and_modify(|value| *value = (*value).max(end))
                    .or_insert(end);
            }
        }
        for (stream, end) in prefixes {
            let archived = self
                .archive
                .archived_lsn(&stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if archived >= end {
                continue;
            }
            let (records, _) = self.recover_hot_tail(&stream, archived, None, None).await?;
            let records = records
                .into_iter()
                .filter(|record| record.lsn() <= end)
                .collect::<Vec<_>>();
            self.archive
                .archive_committed(&records)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            let archived = self
                .archive
                .archived_lsn(&stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if archived < end {
                return Err(invalid("historical source records could not be archived"));
            }
        }
        self.verify_source_archive_ranges(manifest, source_cohort_id)
            .await
    }

    /// Produces the operator's permission to retire physical capacity only
    /// after every finite source range is covered by an actual archive head.
    /// Removed cohorts and their ranges are immutable, so unrelated later
    /// metadata commits cannot invalidate this proof while archive I/O runs.
    pub async fn verified_online_retirement_receipt(
        &self,
        operation_id: &str,
        source_cohort_id: u64,
        receipt_cohort_id: u64,
    ) -> Result<OnlineCohortReplacementReceipt, ReplicaError> {
        let snapshot = self
            .load_authoritative_head()
            .await?
            .ok_or_else(|| invalid("object-store metadata authority is absent"))?;
        let prefixes = required_archive_prefixes(
            &snapshot.head,
            operation_id,
            source_cohort_id,
            receipt_cohort_id,
        )?;
        self.verify_source_archive_ranges(&snapshot.head.metadata.manifest, source_cohort_id)
            .await?;
        let proofs = join_all(prefixes.into_iter().map(|(stream, required)| async move {
            let archived = self
                .archive
                .archived_lsn(&stream)
                .await
                .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
            if archived < required {
                return Err(invalid("an archive head is below a sealed source range"));
            }
            Ok((stream, required, archived))
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, ReplicaError>>()?;
        let proof_bytes = serde_json::to_vec(&(
            "lakeday/online-retirement/v1",
            operation_id,
            source_cohort_id,
            receipt_cohort_id,
            &snapshot.head.metadata_digest,
            &proofs,
        ))
        .map_err(|error| ReplicaError::NodeStorage(error.to_string()))?;
        let archive_proof = hex::encode(Sha256::digest(proof_bytes));
        let authority_marker = format!(
            "object-head:{}:{}:{}",
            snapshot.head.authority_epoch, snapshot.head.revision, snapshot.head.metadata_digest
        );
        Ok(OnlineCohortReplacementReceipt {
            operation_id: operation_id.to_owned(),
            cohort_id: receipt_cohort_id,
            old_members_removed: true,
            all_ranges_archived: true,
            hot_tail_archived: true,
            metadata_authority_moved: true,
            can_retire: true,
            archive_proof,
            authority_marker,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ControlMetadata, ControlSourceMarker, DurableCohort, DurableMember, OpaqueArchive,
        ReplicaManifest, ReplicaNode, StreamSegment,
    };
    use std::sync::Arc;
    use walleye_bitr::EncryptedRecord;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn fixture(end: Option<u64>) -> TestResult<ControlHead> {
        let mut members = BTreeMap::new();
        let mut cohorts = BTreeMap::new();
        for cohort_id in 0..2 {
            let ids = (0..3)
                .map(|index| format!("c{cohort_id}-{index}"))
                .collect::<Vec<_>>();
            for (index, id) in ids.iter().enumerate() {
                members.insert(
                    id.clone(),
                    DurableMember {
                        id: id.clone(),
                        url: format!("http://127.0.0.1:{}", 12000 + cohort_id * 3 + index as u64),
                        status: if cohort_id == 0 {
                            MemberStatus::Removed
                        } else {
                            MemberStatus::Active
                        },
                        cohort_id,
                        ..DurableMember::default()
                    },
                );
            }
            cohorts.insert(
                cohort_id,
                DurableCohort {
                    id: cohort_id,
                    members: ids,
                    status: if cohort_id == 0 {
                        CohortStatus::Retired
                    } else {
                        CohortStatus::Active
                    },
                    tier: "small".to_owned(),
                    max_append_bytes: 0,
                },
            );
        }
        let segments = BTreeMap::from([(
            "tenant/a".to_owned(),
            vec![StreamSegment {
                start_lsn: 1,
                end_lsn: end,
                cohort_id: 0,
                member_ids: cohorts[&0].members.clone(),
                member_hash: "source-hash".to_owned(),
                writer_epoch: 1,
                manifest_revision: 1,
                placement_epoch: 1,
                operation_id: "range".to_owned(),
                tier: "small".to_owned(),
                max_append_bytes: 0,
            }],
        )]);
        let manifest = ReplicaManifest {
            version: 1,
            revision: 1,
            digest: crate::manifest_digest(&segments),
            writer_epoch: 1,
            cohort_id: 1,
            member_set_hash: "target-hash".to_owned(),
            write_cohort_id: 0,
            stream_segments: segments,
            operation_id: "retire".to_owned(),
            tier: "small".to_owned(),
            max_append_bytes: 0,
        };
        Ok(ControlHead::new(
            2,
            1,
            "retire",
            ControlSourceMarker::new("legacy", 1, 1, "source")?,
            ControlMetadata {
                membership_epoch: 1,
                members,
                cohorts,
                manifest,
                pending_handoffs: BTreeMap::new(),
            },
        )?)
    }

    #[test]
    fn retirement_never_omits_an_open_source_range() -> TestResult {
        let head = fixture(None)?;
        assert!(required_archive_prefixes(&head, "retire", 0, 1).is_err());
        let head = fixture(Some(3))?;
        assert_eq!(
            required_archive_prefixes(&head, "retire", 0, 1)?,
            BTreeMap::from([("tenant/a".to_owned(), 3)])
        );
        assert!(required_archive_prefixes(&head, "different-operation", 0, 1).is_err());
        assert!(required_archive_prefixes(&head, "retire", 0, 7).is_err());
        Ok(())
    }

    #[test]
    fn every_stream_is_validated_after_an_open_range() -> TestResult {
        let mut segments = fixture(None)?.metadata.manifest.stream_segments;
        let mut invalid = segments["tenant/a"].clone();
        invalid[0].start_lsn = 4;
        segments.insert("tenant/z".to_owned(), invalid);
        assert!(crate::validate_stream_segments(&segments).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn removed_members_are_insufficient_until_the_archive_covers_the_hot_tail() -> TestResult
    {
        let directory = tempfile::tempdir()?;
        let head = fixture(Some(3))?;
        let nodes = head
            .metadata
            .members
            .values()
            .filter(|member| member.cohort_id == 0)
            .map(|member| ReplicaNode::new(member.id.clone(), member.url.clone()))
            .collect();
        let archive = Arc::new(OpaqueArchive::new(
            Arc::new(object_store::memory::InMemory::new()),
            "receipt-test",
            16,
        )?);
        let gateway = ReplicaGateway::new_direct(
            nodes,
            2,
            "y8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8vLy8s=",
            "internal",
            directory.path().join("control.json"),
            Arc::clone(&archive),
        )?;
        gateway
            .control_head
            .as_ref()
            .ok_or("missing head store")?
            .create(head)
            .await?;
        assert!(
            gateway
                .verified_online_retirement_receipt("retire", 0, 1)
                .await
                .is_err()
        );
        let records = (1..=3).map(|lsn| serde_json::from_value::<EncryptedRecord>(serde_json::json!({
            "stream":"tenant/a", "lsn":lsn, "committed_lsn":lsn-1, "writer_epoch":1,
            "nonce":vec![0;24], "ciphertext":vec![lsn as u8;32], "authentication":vec![0;32],
        }))).collect::<Result<Vec<_>, _>>()?;
        archive.archive_committed(&records[..2]).await?;
        assert!(
            gateway
                .verified_online_retirement_receipt("retire", 0, 1)
                .await
                .is_err()
        );
        archive.archive_committed(&records).await?;
        let receipt = gateway
            .verified_online_retirement_receipt("retire", 0, 1)
            .await?;
        assert!(
            receipt.can_retire && receipt.hot_tail_archived && receipt.metadata_authority_moved
        );
        assert_eq!(receipt.cohort_id, 1);
        assert!(!receipt.archive_proof.is_empty());
        let scale_in = gateway
            .verified_online_retirement_receipt("retire", 0, 0)
            .await?;
        assert_eq!(scale_in.cohort_id, 0);
        Ok(())
    }
}
