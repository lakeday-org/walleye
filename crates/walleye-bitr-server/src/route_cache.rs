//! Bounded process-local cache for durable route-repair proofs.
//!
//! A route-repair proof says that a quorum of the exact members named by one
//! immutable segment persisted the current placement manifest.  The proof is
//! useful for avoiding the same manifest repair before every append.  It is
//! never a write-authority token: every append still carries the placement
//! token and the storage member validates it against the durable, stream-local
//! placement sidecar before admitting the write.
//!
//! The cache has no expiry timer and performs no background refresh.  A head,
//! authority, route, or member-identity change produces a different key and
//! therefore cannot hit an older entry.  A process restart simply starts with
//! an empty cache;
//! the durable stream-local placement sidecar on a member remains the source
//! of truth for append admission.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{DurableMember, StreamSegment};

/// Default bound for one gateway's process-local route proof cache.
pub const DEFAULT_ROUTE_REPAIR_CACHE_CAPACITY: usize = 256;

/// The immutable metadata that identifies one route repair obligation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteRepairContext {
    /// Object-store/control authority generation.
    pub authority_epoch: u64,
    /// Revision of the complete control head that names this route.
    pub head_revision: u64,
    /// Digest of the complete control head/manifest.
    pub head_digest: String,
    /// The exact immutable stream range being repaired.
    pub route: StreamSegment,
    /// Exact durable member identities for `route.member_ids`.
    pub members: Vec<DurableMember>,
}

impl RouteRepairContext {
    /// Builds a context and validates that the member identities match the
    /// immutable segment.  The caller supplies the member list from the same
    /// authoritative head used to construct the route token.
    pub fn new(
        authority_epoch: u64,
        head_revision: u64,
        head_digest: impl Into<String>,
        route: StreamSegment,
        members: Vec<DurableMember>,
    ) -> Result<Self, RouteRepairCacheError> {
        let context = Self {
            authority_epoch,
            head_revision,
            head_digest: head_digest.into(),
            route,
            members,
        };
        context.validate()?;
        Ok(context)
    }

    /// Returns the deterministic digest used as the bounded cache key.
    pub fn key_digest(&self) -> Result<String, RouteRepairCacheError> {
        self.validate()?;
        // Callers normally obtain members from a BTreeMap, but the cache key
        // must not depend on an equivalent caller-provided vector order.
        // Keep the route's canonical member order and serialize identities in
        // that same order.
        let by_id = self
            .members
            .iter()
            .map(|member| (member.id.as_str(), member))
            .collect::<BTreeMap<_, _>>();
        let canonical_members = self
            .route
            .member_ids
            .iter()
            .map(|id| {
                by_id.get(id.as_str()).copied().ok_or_else(|| {
                    RouteRepairCacheError::Invalid(
                        "route repair context is missing a route member identity".to_owned(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let encoded = serde_json::to_vec(&(
            self.authority_epoch,
            self.head_revision,
            &self.head_digest,
            &self.route,
            canonical_members,
        ))?;
        Ok(hex::encode(Sha256::digest(encoded)))
    }

    fn validate(&self) -> Result<(), RouteRepairCacheError> {
        if self.authority_epoch == 0 || self.head_revision == 0 {
            return Err(RouteRepairCacheError::Invalid(
                "route repair context requires nonzero authority and head revisions".to_owned(),
            ));
        }
        if self.head_digest.trim().is_empty() {
            return Err(RouteRepairCacheError::Invalid(
                "route repair context is missing the head digest".to_owned(),
            ));
        }
        if self.route.start_lsn == 0
            || self.route.member_ids.is_empty()
            || self.route.member_ids.iter().any(|id| id.trim().is_empty())
            || self
                .route
                .member_ids
                .windows(2)
                .any(|window| window[0] >= window[1])
        {
            return Err(RouteRepairCacheError::Invalid(
                "route repair context has an invalid member set".to_owned(),
            ));
        }
        if self.route.member_hash.trim().is_empty()
            || self.route.operation_id.trim().is_empty()
            || self.route.manifest_revision == 0
        {
            return Err(RouteRepairCacheError::Invalid(
                "route repair context is missing immutable route binding".to_owned(),
            ));
        }
        let mut members = BTreeMap::new();
        for member in &self.members {
            if member.id.trim().is_empty() || members.insert(member.id.clone(), member).is_some() {
                return Err(RouteRepairCacheError::Invalid(
                    "route repair context has duplicate or empty member identities".to_owned(),
                ));
            }
        }
        if members.len() != self.route.member_ids.len()
            || self
                .route
                .member_ids
                .iter()
                .any(|id| !members.contains_key(id))
        {
            return Err(RouteRepairCacheError::Invalid(
                "route repair context members do not match the route".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Evidence supplied by the caller after a route repair fan-out has reached
/// its required quorum.  The cache accepts no entry without this proof.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteRepairProof {
    pub context: RouteRepairContext,
    /// Member IDs whose durable manifest CAS returned the exact route/head.
    pub acknowledged_member_ids: Vec<String>,
    /// Required durable acknowledgements for this route.
    pub quorum: usize,
}

impl RouteRepairProof {
    fn validate(&self) -> Result<BTreeSet<String>, RouteRepairCacheError> {
        self.context.validate()?;
        if self.quorum == 0 || self.quorum > self.context.route.member_ids.len() {
            return Err(RouteRepairCacheError::Invalid(
                "route repair proof has an invalid quorum".to_owned(),
            ));
        }
        let route_members = self
            .context
            .route
            .member_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let acknowledged = self
            .acknowledged_member_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if acknowledged.len() != self.acknowledged_member_ids.len()
            || acknowledged.iter().any(|id| !route_members.contains(id))
            || acknowledged.len() < self.quorum
        {
            return Err(RouteRepairCacheError::Invalid(
                "route repair proof does not contain an exact member quorum".to_owned(),
            ));
        }
        Ok(acknowledged)
    }
}

/// Errors returned while constructing or inserting route proof metadata.
#[derive(Debug, Error)]
pub enum RouteRepairCacheError {
    #[error("invalid route repair cache metadata: {0}")]
    Invalid(String),
    #[error("route repair cache metadata serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone, Debug)]
struct CachedRoute {
    context: RouteRepairContext,
    last_used: u64,
}

/// A bounded cache of route-repair quorum proofs.
#[derive(Debug)]
pub struct RouteRepairCache {
    capacity: usize,
    clock: u64,
    entries: BTreeMap<String, CachedRoute>,
}

impl Default for RouteRepairCache {
    fn default() -> Self {
        Self::new(DEFAULT_ROUTE_REPAIR_CACHE_CAPACITY)
    }
}

impl RouteRepairCache {
    /// Creates a cache with a fixed maximum number of route entries.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            clock: 0,
            entries: BTreeMap::new(),
        }
    }

    /// Records a route only after the caller supplies a quorum proof.
    pub fn record_quorum_repair(
        &mut self,
        proof: RouteRepairProof,
    ) -> Result<String, RouteRepairCacheError> {
        let _acknowledged = proof.validate()?;
        if self.capacity == 0 {
            return Err(RouteRepairCacheError::Invalid(
                "route repair cache capacity must be positive".to_owned(),
            ));
        }
        let key = proof.context.key_digest()?;
        self.clock = self.clock.saturating_add(1);
        self.entries.insert(
            key.clone(),
            CachedRoute {
                context: proof.context,
                last_used: self.clock,
            },
        );
        while self.entries.len() > self.capacity {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        Ok(key)
    }

    /// Tests a context and updates its recency.  A hit only permits the
    /// caller to skip its manifest-repair RPC; node-side placement validation
    /// remains mandatory for the append itself.
    pub fn contains(
        &mut self,
        context: &RouteRepairContext,
    ) -> Result<bool, RouteRepairCacheError> {
        context.validate()?;
        let key = context.key_digest()?;
        let Some(entry) = self.entries.get_mut(&key) else {
            return Ok(false);
        };
        if entry.context != *context {
            self.entries.remove(&key);
            return Ok(false);
        }
        self.clock = self.clock.saturating_add(1);
        entry.last_used = self.clock;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn member(id: &str, url: &str, volume_id: &str) -> DurableMember {
        serde_json::from_value(json!({
            "id": id,
            "url": url,
            "status": "active",
            "cohort_id": 1,
            "machine_id": format!("machine-{id}"),
            "volume_id": volume_id,
            "ordinal": 0,
            "tier": "small",
            "max_append_bytes": 0
        }))
        .expect("member fixture")
    }

    fn context(head_digest: &str, route_member: &str) -> RouteRepairContext {
        let members = vec![
            member("a", "http://a", "volume-a"),
            member("b", "http://b", "volume-b"),
            member("c", "http://c", "volume-c"),
        ];
        let mut route_ids = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        if route_member != "a" {
            route_ids[0] = route_member.to_owned();
        }
        let route = StreamSegment {
            start_lsn: 1,
            end_lsn: None,
            cohort_id: 1,
            member_ids: route_ids,
            member_hash: "member-hash".to_owned(),
            writer_epoch: 1,
            manifest_revision: 2,
            placement_epoch: 1,
            operation_id: "route-operation".to_owned(),
            tier: "small".to_owned(),
            max_append_bytes: 0,
        };
        RouteRepairContext::new(3, 7, head_digest, route, members).expect("context")
    }

    fn proof(context: RouteRepairContext, acknowledgements: &[&str]) -> RouteRepairProof {
        RouteRepairProof {
            context,
            acknowledged_member_ids: acknowledgements.iter().map(|id| (*id).to_owned()).collect(),
            quorum: 2,
        }
    }

    #[test]
    fn only_an_exact_quorum_proof_populates_the_cache() {
        let proof_context = context("head-a", "a");
        let mut cache = RouteRepairCache::new(4);
        assert!(
            cache
                .record_quorum_repair(proof(proof_context.clone(), &["a"]))
                .is_err()
        );
        assert!(
            !cache
                .contains(&proof_context)
                .expect("missing proof lookup")
        );
        cache
            .record_quorum_repair(proof(proof_context.clone(), &["a", "b"]))
            .expect("quorum proof");
        assert!(cache.contains(&proof_context).expect("lookup"));
        assert!(
            !cache
                .contains(&context("head-b", "a"))
                .expect("head mismatch lookup")
        );
    }

    #[test]
    fn member_identity_changes_do_not_hit_an_old_proof() {
        let context = context("head-a", "a");
        let mut cache = RouteRepairCache::default();
        cache
            .record_quorum_repair(proof(context.clone(), &["a", "b"]))
            .expect("quorum proof");
        let mut changed = context.clone();
        changed.members[0].volume_id = "replacement-volume".to_owned();
        assert!(!cache.contains(&changed).expect("identity lookup"));
        assert!(cache.contains(&context).expect("original proof"));
    }

    #[test]
    fn authority_and_head_revision_changes_do_not_hit_an_old_proof() {
        let context = context("head-a", "a");
        let mut cache = RouteRepairCache::default();
        cache
            .record_quorum_repair(proof(context.clone(), &["a", "b"]))
            .expect("quorum proof");

        let mut authority_changed = context.clone();
        authority_changed.authority_epoch += 1;
        assert!(
            !cache
                .contains(&authority_changed)
                .expect("authority lookup")
        );

        let mut revision_changed = context.clone();
        revision_changed.head_revision += 1;
        assert!(!cache.contains(&revision_changed).expect("revision lookup"));
        assert!(cache.contains(&context).expect("original proof"));
    }

    #[test]
    fn equivalent_member_vector_order_has_one_deterministic_key() {
        let context = context("head-a", "a");
        let mut reordered = context.clone();
        reordered.members.reverse();
        assert_eq!(
            context.key_digest().expect("canonical key"),
            reordered.key_digest().expect("reordered canonical key")
        );
    }

    #[test]
    fn cache_is_bounded_and_evicts_the_least_recently_used_entry() {
        let mut cache = RouteRepairCache::new(2);
        let first = context("head-a", "a");
        let second = context("head-b", "a");
        let third = context("head-c", "a");
        cache
            .record_quorum_repair(proof(first.clone(), &["a", "b"]))
            .expect("first proof");
        cache
            .record_quorum_repair(proof(second.clone(), &["a", "b"]))
            .expect("second proof");
        assert!(cache.contains(&first).expect("first hit"));
        cache
            .record_quorum_repair(proof(third.clone(), &["a", "b"]))
            .expect("third proof");
        assert!(cache.contains(&first).expect("retained first"));
        assert!(!cache.contains(&second).expect("evicted second"));
        assert!(cache.contains(&third).expect("retained third"));
    }
}
