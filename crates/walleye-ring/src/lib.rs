//! Weighted rendezvous placement with immutable membership snapshots and bounded donors.
//! Reconstructed from the surviving Verglas contracts; this does not claim wire
//! compatibility with the unavailable historical hash framing or gossip implementation.
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};
use xxhash_rust::xxh3::Xxh3;

/// A cache member's stable identity, private endpoint, and provisioned capacity weight.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub endpoint: String,
    pub weight: f64,
}
impl Node {
    /// Reject unusable weights and ambiguous identities before changing membership.
    pub fn new(
        id: impl Into<String>,
        endpoint: impl Into<String>,
        weight: f64,
    ) -> Result<Self, RingError> {
        let node = Self {
            id: id.into(),
            endpoint: endpoint.into(),
            weight,
        };
        node.validate()?;
        Ok(node)
    }
    fn validate(&self) -> Result<(), RingError> {
        if self.id.is_empty()
            || !self.weight.is_finite()
            || self.weight <= 0.0
            || !(self.endpoint.starts_with("http://") || self.endpoint.starts_with("https://"))
        {
            return Err(RingError::InvalidMember);
        }
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
pub enum RingError {
    #[error("ring requires at least one member")]
    Empty,
    #[error("invalid member identity, endpoint, or capacity weight")]
    InvalidMember,
    #[error("ring member identities must be unique")]
    Duplicate,
}

/// One stable ownership generation, plus the previous generation during handoff.
#[derive(Clone, Debug)]
pub struct Ring {
    members: Vec<Node>,
    previous: Option<(Vec<Node>, Instant)>,
    epoch: u64,
}
impl Ring {
    /// Construct deterministic ownership independent of membership input order.
    pub fn new(members: Vec<Node>) -> Result<Self, RingError> {
        validate(&members)?;
        Ok(Self {
            members,
            previous: None,
            epoch: 0,
        })
    }
    /// Install an explicit membership change. Query-driven cache shrinking never changes weights.
    pub fn replace(
        &mut self,
        members: Vec<Node>,
        now: Instant,
        warming: Duration,
    ) -> Result<(), RingError> {
        validate(&members)?;
        let old = std::mem::replace(&mut self.members, members);
        self.previous = Some((old, now + warming));
        self.epoch += 1;
        Ok(())
    }
    /// Select one owner from stable cache-key coordinates.
    pub fn owner(&self, key: &[u8]) -> &Node {
        choose(&self.members, key)
    }
    /// Find the single former owner while a join or drain warming window remains open.
    pub fn donor(&self, key: &[u8], now: Instant) -> Option<&Node> {
        let (previous, deadline) = self.previous.as_ref()?;
        if now >= *deadline {
            return None;
        }
        let donor = choose(previous, key);
        (donor.id != self.owner(key).id).then_some(donor)
    }
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    pub fn members(&self) -> &[Node] {
        &self.members
    }
}
fn validate(members: &[Node]) -> Result<(), RingError> {
    if members.is_empty() {
        return Err(RingError::Empty);
    }
    let mut seen = HashSet::new();
    for n in members {
        n.validate()?;
        if !seen.insert(&n.id) {
            return Err(RingError::Duplicate);
        }
    }
    Ok(())
}
fn choose<'a>(members: &'a [Node], key: &[u8]) -> &'a Node {
    members
        .iter()
        .min_by(|a, b| {
            score(a, key)
                .total_cmp(&score(b, key))
                .then(a.id.cmp(&b.id))
        })
        .expect("validated nonempty membership")
}
fn score(node: &Node, key: &[u8]) -> f64 {
    let mut hash = Xxh3::new();
    for part in [key, node.id.as_bytes()] {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    // Map 53 random bits into (0,1); weighted exponential races implement HRW.
    let unit = ((hash.digest() >> 11) as f64 + 1.0) / ((1u64 << 53) as f64 + 2.0);
    -unit.ln() / node.weight
}

/// Live cache membership. Readers retain one coherent snapshot across peer I/O.
#[derive(Debug)]
pub struct Membership(std::sync::RwLock<std::sync::Arc<Ring>>);
impl Membership {
    pub fn new(members: Vec<Node>) -> Result<Self, RingError> {
        Ok(Self(std::sync::RwLock::new(std::sync::Arc::new(
            Ring::new(members)?,
        ))))
    }
    pub fn snapshot(&self) -> std::sync::Arc<Ring> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// Ignore reordered/unchanged discovery responses so donor deadlines do not reset.
    pub fn update(
        &self,
        mut members: Vec<Node>,
        now: Instant,
        warming: Duration,
    ) -> Result<bool, RingError> {
        validate(&members)?;
        members.sort_by(|a, b| a.id.cmp(&b.id));
        let mut guard = self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut current = guard.members().to_vec();
        current.sort_by(|a, b| a.id.cmp(&b.id));
        if current == members {
            return Ok(false);
        }
        let mut next = (**guard).clone();
        next.replace(members, now, warming)?;
        *guard = std::sync::Arc::new(next);
        Ok(true)
    }
}
