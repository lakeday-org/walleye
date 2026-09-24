//! Self-managed table ownership, with the bucket as the only coordinator.
//!
//! Modelled on Deno's celld (<https://github.com/denoland/celld>, Apache-2.0):
//! each process publishes a lease in the bucket and renews it, each table has
//! one ownership record that names the owning process and a fencing epoch,
//! and a record changes hands only by a conditional write that exactly one
//! contender can win. There is no membership protocol, failure detector or
//! consensus service. No code is copied from celld; the protocol is adapted
//! to Walleye's writer, whose fencing epoch is the Lance MemWAL writer epoch.
//!
//! Layout under the engine root:
//!
//! - `_walleye/nodes/<node>.json` is one process's lease:
//!   `{"node","addr","renewal","ttl_ms","renewed_at_ms","draining"}`. `node`
//!   is the process, not the host: `<WALLEYE_NODE_ID>.<12 hex>`, fresh on
//!   every start, so a replacement beside the original is a different node.
//!   Only the process that owns a lease writes it.
//! - `_walleye/own/<key>/<seq>.json` is an ownership record,
//!   `{"node","epoch","alarms"}`, one object per version. The newest `seq` is
//!   the record; writing the next `seq` with create-if-absent is the
//!   compare-and-swap. An empty `node` is a released record. A key is a table
//!   name or `view.<name>` for a view with no source table; `alarms` are the
//!   key's pending alarms (see [`crate::alarms`]), which travel with it.
//!
//! Liveness is judged on the observer's monotonic clock and never on a wall
//! clock. A lease is dead once the observer has seen the same version of it
//! for `ttl + skew`, or once it is gone. The holder counts a renewal only when
//! the write was issued while it was still authoritative and completed within
//! `skew`, and it is authoritative for `ttl` after the issue of the last
//! renewal it counted. So by the time a peer may call it dead, it has already
//! stopped answering as an owner.
use crate::alarms::Alarm;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// How long a lease stands, how much slack a peer allows before it believes
/// the lease has lapsed, and how often the bucket is sampled.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseConfig {
    /// A process is an owner for this long after the last renewal it counted.
    /// Renewals go out every third of it.
    pub ttl_ms: u64,
    /// What a peer waits beyond `ttl_ms` before it calls a lease dead, and the
    /// longest a renewal may take to land and still count.
    pub skew_ms: u64,
    /// How often the leases and ownership records are read.
    pub sample_ms: u64,
}
impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            ttl_ms: 10_000,
            skew_ms: 2_000,
            sample_ms: 2_000,
        }
    }
}
impl LeaseConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.ttl_ms < 300 {
            return Err("lease ttl_ms must be at least 300".into());
        }
        // A renewal that may take `skew` to land has to land before the next
        // one is due, or two in flight could count out of order.
        if self.skew_ms == 0 || self.skew_ms >= self.ttl_ms / 3 {
            return Err("lease skew_ms must be positive and below a third of ttl_ms".into());
        }
        if self.sample_ms == 0 || self.sample_ms > self.ttl_ms {
            return Err("lease sample_ms must be positive and no more than ttl_ms".into());
        }
        Ok(())
    }
    fn ttl(&self) -> Duration {
        Duration::from_millis(self.ttl_ms)
    }
    fn skew(&self) -> Duration {
        Duration::from_millis(self.skew_ms)
    }
    pub fn sample(&self) -> Duration {
        Duration::from_millis(self.sample_ms)
    }
    pub fn renew_every(&self) -> Duration {
        Duration::from_millis(self.ttl_ms / 3)
    }
    /// How long a peer must see one version of a lease before it is dead.
    pub fn verdict(&self) -> Duration {
        self.ttl() + self.skew()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct LeaseRecord {
    node: String,
    addr: String,
    renewal: u64,
    ttl_ms: u64,
    /// Wall clock at issue, for people reading the bucket. Nothing decides on it.
    renewed_at_ms: u64,
    draining: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct OwnerRecord {
    /// The owning process, or empty for a released record.
    pub node: String,
    /// The Lance MemWAL writer epoch the owner's writer claims. There is one
    /// fencing epoch per table, and this is it.
    pub epoch: u64,
    /// Pending alarms by name. Only the owner changes them, by writing a new
    /// version of this record, and a new owner inherits them with it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub alarms: BTreeMap<String, Alarm>,
}

/// What an alarm change came to.
#[derive(Debug, PartialEq, Eq)]
pub enum AlarmUpdate<T> {
    /// Written to the record.
    Applied(T),
    /// The change asked for nothing, so nothing was written.
    Skipped,
    /// This process does not own the key, or lost it to the write.
    NotOwner,
}

/// A live process another node can forward to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub node: String,
    pub addr: String,
}

/// Where a table's requests go, as this process sees it now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    /// This process owns the table at this epoch.
    Local { epoch: u64 },
    /// A process with a lease this node has not seen lapse owns it.
    /// `verdict_in` is how long until this node could call that lease dead if
    /// it stopped renewing now.
    Remote { peer: Peer, verdict_in: Duration },
    /// Nobody owns it: never claimed, released, or its owner's lease is dead.
    Unowned,
}

/// What a fresh read of a lease says about its holder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Liveness {
    /// Renewed recently enough; `verdict_in` until it could be called dead.
    Live { peer: Peer, verdict_in: Duration },
    /// This process has seen the same version for a whole verdict.
    Lapsed,
    /// There is no lease: its holder retired it, or it was collected after
    /// lapsing.
    Gone,
}

/// Why a claim did not make this process the owner.
#[derive(Debug)]
pub enum Refusal {
    /// A live process owns the table.
    Owned { peer: Peer, verdict_in: Duration },
    /// This process may not claim right now: it has no lease yet, or it is
    /// draining, or another claimant won the same version.
    NotNow { retry_after: Duration },
}

#[derive(Debug)]
struct Session {
    node: String,
    renewal: u64,
    /// The issue instant of the last renewal that counted.
    anchor: Option<Instant>,
    draining: bool,
    retired: bool,
}

#[derive(Clone, Debug)]
struct Held {
    seq: u64,
    epoch: u64,
    alarms: BTreeMap<String, Alarm>,
}

#[derive(Debug)]
struct Seen {
    version: String,
    first_seen: Instant,
    lease: Option<LeaseRecord>,
}

struct State {
    session: Session,
    held: BTreeMap<String, Held>,
    leases: HashMap<String, Seen>,
    records: HashMap<String, (u64, OwnerRecord)>,
    /// Sessions of this process that stood down or retired. They are dead the
    /// moment this process says so; peers find out from their own clocks.
    dead: HashSet<String>,
    /// Tables this process held when it stood down, for the engine to close.
    lost: Vec<String>,
    /// Leases of stood-down sessions still to be deleted.
    abandoned: Vec<String>,
    /// Where each known key belongs, as of the last sweep: see [`place`].
    placement: HashMap<String, Peer>,
    /// The most keys one live process should hold as of that sweep.
    fair_share: usize,
    started: Instant,
}

pub struct Ownership {
    store: Arc<dyn ObjectStore>,
    root: Path,
    node_id: String,
    addr: String,
    config: LeaseConfig,
    state: Mutex<State>,
    /// Wakes the renewer at once when a session stands down.
    renew_now: tokio::sync::Notify,
    /// Wakes whatever fires alarms: a key was claimed, released or lost, or
    /// an alarm changed.
    pub(crate) alarms_changed: tokio::sync::Notify,
    /// One writer of a table's record at a time within this process, so two
    /// of its own requests never race each other for the same version.
    writing: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

fn session_name(node_id: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{node_id}.{}", &id[..12])
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Rendezvous score of `node` for `table`. The live process with the highest
/// score is the one that claims an orphaned table, so a dead owner's tables
/// spread across the survivors instead of all landing on one.
fn score(table: &str, node: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(format!("{table}\0{node}").as_bytes())
}

/// Where each key belongs: rendezvous hashing with bounded load. Keys are
/// taken in a fixed order and each goes to the highest-scoring process that
/// has fewer than its fair share, `ceil(keys / processes)`. Every process that
/// sees the same keys and the same live leases works out the same answer, no
/// process is given more than its fair share, and a key keeps its place while
/// the membership does.
pub fn place(keys: &[String], nodes: &[String]) -> HashMap<String, String> {
    let mut placed = HashMap::new();
    if nodes.is_empty() {
        return placed;
    }
    let mut keys: Vec<&String> = keys.iter().collect();
    keys.sort_by_key(|key| (xxhash_rust::xxh3::xxh3_64(key.as_bytes()), (*key).clone()));
    keys.dedup();
    let cap = keys.len().div_ceil(nodes.len());
    let mut load: HashMap<&str, usize> = nodes.iter().map(|n| (n.as_str(), 0)).collect();
    for key in keys {
        let mut ranked: Vec<&String> = nodes.iter().collect();
        ranked.sort_by_key(|node| std::cmp::Reverse((score(key, node), (*node).clone())));
        let chosen = ranked
            .into_iter()
            .find(|node| load[node.as_str()] < cap)
            .expect("the shares add up to at least every key");
        *load.get_mut(chosen.as_str()).expect("a counted node") += 1;
        placed.insert(key.clone(), chosen.clone());
    }
    placed
}

fn version_of(meta: &object_store::ObjectMeta) -> String {
    match &meta.e_tag {
        Some(tag) => tag.clone(),
        None => format!("{}:{}", meta.last_modified.timestamp_micros(), meta.size),
    }
}

fn seq_name(seq: u64) -> String {
    format!("{seq:020}.json")
}

impl Ownership {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        root: Path,
        node_id: &str,
        addr: String,
        config: LeaseConfig,
    ) -> Self {
        Self {
            store,
            root: root.clone().join("_walleye"),
            node_id: node_id.to_owned(),
            addr,
            config,
            state: Mutex::new(State {
                session: Session {
                    node: session_name(node_id),
                    renewal: 0,
                    anchor: None,
                    draining: false,
                    retired: false,
                },
                held: BTreeMap::new(),
                leases: HashMap::new(),
                records: HashMap::new(),
                dead: HashSet::new(),
                lost: Vec::new(),
                abandoned: Vec::new(),
                placement: HashMap::new(),
                fair_share: usize::MAX,
                started: Instant::now(),
            }),
            renew_now: tokio::sync::Notify::new(),
            alarms_changed: tokio::sync::Notify::new(),
            writing: Mutex::new(HashMap::new()),
        }
    }

    async fn writing(&self, table: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = self
            .writing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(table.to_owned())
            .or_default()
            .clone();
        lock.lock_owned().await
    }

    pub fn config(&self) -> &LeaseConfig {
        &self.config
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lease_path(&self, node: &str) -> Path {
        self.root.clone().join("nodes").join(format!("{node}.json"))
    }
    fn table_dir(&self, table: &str) -> Path {
        self.root.clone().join("own").join(table)
    }

    /// This process's current session name.
    pub fn node(&self) -> String {
        self.state().session.node.clone()
    }

    /// Authority, evaluated now on the monotonic clock. A lapse found here
    /// stands the session down on the spot, whether or not the renewer has
    /// noticed yet: a process paused past its lease must not act as an owner
    /// for even one request after it resumes.
    fn authoritative_locked(&self, state: &mut State) -> bool {
        if state.session.retired {
            return false;
        }
        match state.session.anchor {
            Some(anchor) if anchor.elapsed() < self.config.ttl() => true,
            Some(_) => {
                self.stand_down_locked(state);
                false
            }
            None => false,
        }
    }

    pub fn authoritative(&self) -> bool {
        let mut state = self.state();
        self.authoritative_locked(&mut state)
    }

    /// Give up everything this session held and start a new one. The engine
    /// closes the writers of the tables in `lost`; peers take them over once
    /// they see the old lease lapse.
    fn stand_down_locked(&self, state: &mut State) {
        let old = std::mem::replace(&mut state.session.node, session_name(&self.node_id));
        let held = std::mem::take(&mut state.held);
        eprintln!(
            "walleye.ownership stand_down node={old} next={} tables={}",
            state.session.node,
            held.len()
        );
        state.lost.extend(held.into_keys());
        state.abandoned.push(old.clone());
        state.dead.insert(old);
        state.session.renewal = 0;
        state.session.anchor = None;
        self.renew_now.notify_one();
        self.alarms_changed.notify_waiters();
    }

    /// Tables this process stopped owning without releasing them, since the
    /// last call.
    pub fn take_lost(&self) -> Vec<String> {
        std::mem::take(&mut self.state().lost)
    }

    /// Publish or renew this process's lease once. Returns whether the
    /// renewal counted.
    pub async fn renew(&self) -> bool {
        let issued = Instant::now();
        let (node, path, body, abandoned) = {
            let mut state = self.state();
            if state.session.retired {
                return false;
            }
            // Issued after a lapse: that session is over, and this write must
            // not revive it. Checking authority stands it down, and the write
            // goes out for the new session instead.
            self.authoritative_locked(&mut state);
            let abandoned = std::mem::take(&mut state.abandoned);
            state.session.renewal += 1;
            let record = LeaseRecord {
                node: state.session.node.clone(),
                addr: self.addr.clone(),
                renewal: state.session.renewal,
                ttl_ms: self.config.ttl_ms,
                renewed_at_ms: wall_ms(),
                draining: state.session.draining,
            };
            (
                record.node.clone(),
                self.lease_path(&record.node),
                serde_json::to_vec(&record).expect("a lease serializes"),
                abandoned,
            )
        };
        for old in abandoned {
            let _ = self.store.delete(&self.lease_path(&old)).await;
        }
        let written =
            tokio::time::timeout(self.config.skew(), self.store.put(&path, body.into())).await;
        let mut state = self.state();
        match written {
            Ok(Ok(_)) if state.session.node == node && !state.session.retired => {
                // A renewal counts only if it went out while the session was
                // still authoritative (or is the session's first), and landed
                // inside the skew the peers allow for.
                let first = state.session.anchor.is_none();
                let in_time = state
                    .session
                    .anchor
                    .is_some_and(|anchor| issued.duration_since(anchor) < self.config.ttl());
                if first || in_time {
                    state.session.anchor = Some(issued);
                    true
                } else {
                    false
                }
            }
            Ok(Ok(_)) => false,
            Ok(Err(error)) => {
                eprintln!("walleye.ownership renew node={node} outcome=error error={error}");
                false
            }
            Err(_) => {
                eprintln!(
                    "walleye.ownership renew node={node} outcome=late skew_ms={}",
                    self.config.skew_ms
                );
                false
            }
        }
    }

    /// Renew for as long as the process runs.
    pub async fn renew_forever(&self) {
        loop {
            self.renew().await;
            tokio::select! {
                _ = tokio::time::sleep(self.config.renew_every()) => {}
                _ = self.renew_now.notified() => {}
            }
        }
    }

    /// Read every lease and every ownership record once, and forget leases
    /// that are dead.
    pub async fn sample(&self) -> Result<(), Error> {
        // Expiry is judged against the instant before the listing and a new
        // version is dated from the instant after it. Both err late: a
        // renewal that counted landed before `before`, so the listing shows
        // it, and a version first seen is never dated before it was written.
        let before = Instant::now();
        let listed: Vec<_> = self
            .store
            .list(Some(&self.root.clone().join("nodes")))
            .try_collect()
            .await?;
        let listed_at = Instant::now();
        let mut present = HashMap::new();
        for meta in listed {
            let Some(node) = meta
                .location
                .filename()
                .and_then(|name| name.strip_suffix(".json"))
            else {
                continue;
            };
            present.insert(node.to_owned(), version_of(&meta));
        }
        // Bodies only for leases whose version moved or was never read.
        let changed: Vec<String> = {
            let state = self.state();
            present
                .iter()
                .filter(|(node, version)| {
                    state
                        .leases
                        .get(*node)
                        .is_none_or(|seen| &seen.version != *version || seen.lease.is_none())
                })
                .map(|(node, _)| node.clone())
                .collect()
        };
        let mut bodies = HashMap::new();
        for node in changed {
            if let Ok(got) = self.store.get(&self.lease_path(&node)).await
                && let Ok(bytes) = got.bytes().await
            {
                bodies.insert(node, serde_json::from_slice::<LeaseRecord>(&bytes).ok());
            }
        }
        let mut expired = Vec::new();
        {
            let mut state = self.state();
            state.leases.retain(|node, _| present.contains_key(node));
            for (node, version) in present {
                let body = bodies.remove(&node).flatten();
                match state.leases.get_mut(&node) {
                    Some(seen) if seen.version == version => {
                        if seen.lease.is_none() {
                            seen.lease = body;
                        }
                    }
                    _ => {
                        state.leases.insert(
                            node,
                            Seen {
                                version,
                                first_seen: listed_at,
                                lease: body,
                            },
                        );
                    }
                }
            }
            let mine = state.session.node.clone();
            for (node, seen) in &state.leases {
                if *node != mine
                    && before.saturating_duration_since(seen.first_seen) >= self.config.verdict()
                {
                    expired.push(node.clone());
                }
            }
        }
        for node in expired {
            // Dead for good: its holder counted no renewal it could still be
            // acting on, and a stood-down session never renews again.
            let _ = self.store.delete(&self.lease_path(&node)).await;
        }

        let records: Vec<_> = self
            .store
            .list(Some(&self.root.clone().join("own")))
            .try_collect()
            .await?;
        let mut newest: HashMap<String, u64> = HashMap::new();
        for meta in records {
            let parts: Vec<_> = meta.location.parts().collect();
            let [.., table, file] = parts.as_slice() else {
                continue;
            };
            let Some(seq) = file
                .as_ref()
                .strip_suffix(".json")
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            let entry = newest.entry(table.as_ref().to_owned()).or_insert(0);
            *entry = (*entry).max(seq);
        }
        let stale: Vec<(String, u64)> = {
            let state = self.state();
            newest
                .iter()
                .filter(|(table, seq)| state.records.get(*table).is_none_or(|(s, _)| s != *seq))
                .map(|(table, seq)| (table.clone(), *seq))
                .collect()
        };
        for (table, seq) in stale {
            if let Some(record) = self.read_version(&table, seq).await? {
                self.state().records.insert(table, (seq, record));
            }
        }
        self.state()
            .records
            .retain(|table, _| newest.contains_key(table));
        Ok(())
    }

    /// Sample for as long as the process runs.
    pub async fn sample_forever(&self) {
        loop {
            if let Err(error) = self.sample().await {
                eprintln!("walleye.ownership sample outcome=error error={error}");
            }
            tokio::time::sleep(self.config.sample()).await;
        }
    }

    async fn read_version(&self, table: &str, seq: u64) -> Result<Option<OwnerRecord>, Error> {
        match self
            .store
            .get(&self.table_dir(table).join(seq_name(seq)))
            .await
        {
            Ok(got) => Ok(Some(serde_json::from_slice(&got.bytes().await?)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// The newest version of a table's record, read from the bucket.
    async fn read_latest(&self, table: &str) -> Result<(u64, Option<OwnerRecord>), Error> {
        // A version can be collected between the listing and the read; the
        // listing after that shows its successor.
        for _ in 0..4 {
            let listed: Vec<_> = self
                .store
                .list(Some(&self.table_dir(table)))
                .try_collect()
                .await?;
            let seq = listed
                .iter()
                .filter_map(|meta| {
                    meta.location
                        .filename()
                        .and_then(|name| name.strip_suffix(".json"))
                        .and_then(|seq| seq.parse::<u64>().ok())
                })
                .max()
                .unwrap_or(0);
            if seq == 0 {
                self.state().records.remove(table);
                return Ok((0, None));
            }
            if let Some(record) = self.read_version(table, seq).await? {
                self.state()
                    .records
                    .insert(table.to_owned(), (seq, record.clone()));
                return Ok((seq, Some(record)));
            }
        }
        Err(format!("the ownership record of {table} kept moving while it was read").into())
    }

    /// Write version `seq` of a table's record if nobody has. `Ok(false)` is a
    /// lost race. A version number below one that was collected could be
    /// written again by a contender with a stale view, so a write only counts
    /// once a listing shows it is the newest.
    async fn write_version(
        &self,
        table: &str,
        seq: u64,
        record: &OwnerRecord,
    ) -> Result<bool, Error> {
        let dir = self.table_dir(table);
        let put = self
            .store
            .put_opts(
                &dir.clone().join(seq_name(seq)),
                serde_json::to_vec(record)?.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        match put {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        let listed: Vec<_> = self.store.list(Some(&dir)).try_collect().await?;
        let versions: Vec<u64> = listed
            .iter()
            .filter_map(|meta| {
                meta.location
                    .filename()
                    .and_then(|name| name.strip_suffix(".json"))
                    .and_then(|seq| seq.parse::<u64>().ok())
            })
            .collect();
        if versions.iter().copied().max() != Some(seq) {
            return Ok(false);
        }
        // Keep the newest two, so a reader that listed a moment ago still
        // finds what it listed or its successor.
        for old in versions.into_iter().filter(|v| *v + 1 < seq) {
            let _ = self.store.delete(&dir.clone().join(seq_name(old))).await;
        }
        self.state()
            .records
            .insert(table.to_owned(), (seq, record.clone()));
        Ok(true)
    }

    /// How long until this process could call `node` dead, or `None` if it is
    /// dead already. `fresh` reads the lease from the bucket first, and only a
    /// fresh verdict may justify a claim: the last sample can be a renewal
    /// behind.
    async fn lease_verdict(
        &self,
        node: &str,
        fresh: bool,
    ) -> Result<Option<(Peer, Duration)>, Error> {
        Ok(match self.liveness(node, fresh).await? {
            Liveness::Live { peer, verdict_in } => Some((peer, verdict_in)),
            Liveness::Lapsed | Liveness::Gone => None,
        })
    }

    /// What `node`'s lease says about it. `fresh` reads it from the bucket.
    pub async fn liveness(&self, node: &str, fresh: bool) -> Result<Liveness, Error> {
        if self.state().dead.contains(node) {
            return Ok(Liveness::Lapsed);
        }
        let known = self
            .state()
            .leases
            .get(node)
            .is_some_and(|seen| seen.lease.is_some());
        let mut now = Instant::now();
        if fresh || !known {
            let before = Instant::now();
            match self.store.get(&self.lease_path(node)).await {
                Ok(got) => {
                    let version = version_of(&got.meta);
                    let lease: Option<LeaseRecord> =
                        serde_json::from_slice(&got.bytes().await?).ok();
                    let mut state = self.state();
                    match state.leases.get_mut(node) {
                        Some(seen) if seen.version == version => {
                            if seen.lease.is_none() {
                                seen.lease = lease;
                            }
                            now = before;
                        }
                        _ => {
                            state.leases.insert(
                                node.to_owned(),
                                Seen {
                                    version,
                                    first_seen: Instant::now(),
                                    lease,
                                },
                            );
                        }
                    }
                }
                Err(object_store::Error::NotFound { .. }) => {
                    self.state().leases.remove(node);
                    return Ok(Liveness::Gone);
                }
                Err(error) => return Err(error.into()),
            }
        }
        let state = self.state();
        let Some(seen) = state.leases.get(node) else {
            return Ok(Liveness::Gone);
        };
        let elapsed = now.saturating_duration_since(seen.first_seen);
        if elapsed >= self.config.verdict() {
            return Ok(Liveness::Lapsed);
        }
        let addr = seen
            .lease
            .as_ref()
            .map(|lease| lease.addr.clone())
            .unwrap_or_default();
        Ok(Liveness::Live {
            peer: Peer {
                node: node.to_owned(),
                addr,
            },
            verdict_in: self.config.verdict() - elapsed,
        })
    }

    /// How long until `node` could be judged dead, read from the bucket now,
    /// if it is live and taking keys: not draining, not gone.
    pub async fn accepting(&self, node: &str) -> Result<Option<Duration>, Error> {
        let Liveness::Live { verdict_in, .. } = self.liveness(node, true).await? else {
            return Ok(None);
        };
        let draining = self
            .state()
            .leases
            .get(node)
            .and_then(|seen| seen.lease.as_ref())
            .is_some_and(|lease| lease.draining);
        Ok((!draining).then_some(verdict_in))
    }

    /// The epoch this process holds `table` at, if it owns it now.
    pub fn holds(&self, table: &str) -> Option<u64> {
        let mut state = self.state();
        if !self.authoritative_locked(&mut state) {
            return None;
        }
        state.held.get(table).map(|held| held.epoch)
    }

    /// The acknowledgement check: this process still owns `table`, at the
    /// epoch its writer holds.
    pub fn confirm(&self, table: &str, epoch: u64) -> bool {
        self.holds(table) == Some(epoch)
    }

    pub fn held_tables(&self) -> Vec<String> {
        self.state().held.keys().cloned().collect()
    }

    pub fn draining(&self) -> bool {
        self.state().session.draining
    }

    /// Who owns `table`. `fresh` reads the record and the owner's lease from
    /// the bucket rather than trusting the last sample.
    pub async fn resolve(&self, table: &str, fresh: bool) -> Result<Route, Error> {
        if let Some(epoch) = self.holds(table) {
            return Ok(Route::Local { epoch });
        }
        let cached = self.state().records.get(table).cloned();
        let record = match cached {
            Some((_, record)) if !fresh => Some(record),
            _ => self.read_latest(table).await?.1,
        };
        let Some(record) = record else {
            return Ok(Route::Unowned);
        };
        if record.node.is_empty() {
            return Ok(Route::Unowned);
        }
        if record.node == self.node() {
            // The bucket says this session owns it and the local map lost
            // track, which only a sample racing a claim can do.
            return Ok(match self.holds(table) {
                Some(epoch) => Route::Local { epoch },
                None => Route::Unowned,
            });
        }
        Ok(match self.lease_verdict(&record.node, fresh).await? {
            Some((peer, verdict_in)) => Route::Remote { peer, verdict_in },
            None => Route::Unowned,
        })
    }

    /// Take `table` if nobody live owns it. The record keeps the epoch it
    /// had; the writer's open moves it to the epoch the writer claims.
    pub async fn claim(&self, table: &str) -> Result<Result<u64, Refusal>, Error> {
        let retry = Duration::from_millis(self.config.sample_ms.min(1_000));
        if let Some(epoch) = self.holds(table) {
            return Ok(Ok(epoch));
        }
        let _writing = self.writing(table).await;
        if let Some(epoch) = self.holds(table) {
            return Ok(Ok(epoch));
        }
        // Who owns it now, from the bucket, before whether this process may
        // claim: a process that cannot claim still learns where to send the
        // request.
        let (seq, record) = self.read_latest(table).await?;
        let me = self.node();
        if let Some(record) = &record
            && !record.node.is_empty()
            && record.node != me
            && let Some((peer, verdict_in)) = self.lease_verdict(&record.node, true).await?
        {
            return Ok(Err(Refusal::Owned { peer, verdict_in }));
        }
        {
            let mut state = self.state();
            if !self.authoritative_locked(&mut state) || state.session.draining {
                return Ok(Err(Refusal::NotNow { retry_after: retry }));
            }
        }
        let epoch = record.as_ref().map_or(0, |record| record.epoch);
        let alarms = record
            .as_ref()
            .map(|record| record.alarms.clone())
            .unwrap_or_default();
        let claim = OwnerRecord {
            node: me.clone(),
            epoch,
            alarms: alarms.clone(),
        };
        if !self.write_version(table, seq + 1, &claim).await? {
            return Ok(Err(Refusal::NotNow { retry_after: retry }));
        }
        let mut state = self.state();
        if state.session.node != me {
            // Stood down while the claim was in flight. The record names a
            // session that is now dead, which peers will see for themselves.
            return Ok(Err(Refusal::NotNow { retry_after: retry }));
        }
        let pending = alarms.len();
        state.held.insert(
            table.to_owned(),
            Held {
                seq: seq + 1,
                epoch,
                alarms,
            },
        );
        drop(state);
        self.alarms_changed.notify_waiters();
        let prior = record.map(|record| record.node).unwrap_or_default();
        eprintln!(
            "walleye.ownership claim table={table} node={me} epoch={epoch} alarms={pending} prior={}",
            if prior.is_empty() { "-" } else { &prior }
        );
        Ok(Ok(epoch))
    }

    /// Record the epoch this owner's writer is about to claim, before it
    /// claims it. Fails when this process no longer owns the table.
    pub async fn set_epoch(&self, table: &str, epoch: u64) -> Result<bool, Error> {
        let _writing = self.writing(table).await;
        let (held, me) = {
            let mut state = self.state();
            if !self.authoritative_locked(&mut state) {
                return Ok(false);
            }
            match state.held.get(table) {
                Some(held) => (held.clone(), state.session.node.clone()),
                None => return Ok(false),
            }
        };
        if held.epoch == epoch {
            return Ok(true);
        }
        let record = OwnerRecord {
            node: me.clone(),
            epoch,
            alarms: held.alarms.clone(),
        };
        let written = self.write_version(table, held.seq + 1, &record).await?;
        let mut state = self.state();
        if !written || state.session.node != me {
            state.held.remove(table);
            return Ok(false);
        }
        state.held.insert(
            table.to_owned(),
            Held {
                seq: held.seq + 1,
                epoch,
                alarms: held.alarms,
            },
        );
        Ok(true)
    }

    /// Hand `table` back: the record names nobody, at the same epoch, so the
    /// next claimant can take it at once rather than after a lease lapses.
    pub async fn release(&self, table: &str) -> Result<(), Error> {
        let _writing = self.writing(table).await;
        let held = self.state().held.remove(table);
        let Some(held) = held else { return Ok(()) };
        self.alarms_changed.notify_waiters();
        let record = OwnerRecord {
            node: String::new(),
            epoch: held.epoch,
            alarms: held.alarms,
        };
        if self.write_version(table, held.seq + 1, &record).await? {
            eprintln!(
                "walleye.ownership release table={table} epoch={}",
                held.epoch
            );
        }
        Ok(())
    }

    /// Change the alarms of a key this process owns, as one compare-and-swap
    /// on its record. `change` returns `None` to leave the record alone. The
    /// write is the proof of ownership: a process that lost the key cannot
    /// make it, so no two processes both begin one firing.
    pub async fn update_alarms<T>(
        &self,
        key: &str,
        change: impl FnOnce(&mut BTreeMap<String, Alarm>) -> Option<T>,
    ) -> Result<AlarmUpdate<T>, Error> {
        let _writing = self.writing(key).await;
        let (held, me) = {
            let mut state = self.state();
            if !self.authoritative_locked(&mut state) {
                return Ok(AlarmUpdate::NotOwner);
            }
            match state.held.get(key) {
                Some(held) => (held.clone(), state.session.node.clone()),
                None => return Ok(AlarmUpdate::NotOwner),
            }
        };
        let mut alarms = held.alarms.clone();
        let Some(out) = change(&mut alarms) else {
            return Ok(AlarmUpdate::Skipped);
        };
        let record = OwnerRecord {
            node: me.clone(),
            epoch: held.epoch,
            alarms: alarms.clone(),
        };
        let written = self.write_version(key, held.seq + 1, &record).await?;
        let mut state = self.state();
        if !written || state.session.node != me {
            state.held.remove(key);
            state.lost.push(key.to_owned());
            return Ok(AlarmUpdate::NotOwner);
        }
        state.held.insert(
            key.to_owned(),
            Held {
                seq: held.seq + 1,
                epoch: held.epoch,
                alarms,
            },
        );
        drop(state);
        self.alarms_changed.notify_waiters();
        Ok(AlarmUpdate::Applied(out))
    }

    /// Every alarm on the keys this process owns now, as (key, name, alarm).
    pub fn held_alarms(&self) -> Vec<(String, String, Alarm)> {
        let mut state = self.state();
        if !self.authoritative_locked(&mut state) {
            return Vec::new();
        }
        state
            .held
            .iter()
            .flat_map(|(key, held)| {
                held.alarms
                    .iter()
                    .map(|(name, alarm)| (key.clone(), name.clone(), alarm.clone()))
            })
            .collect()
    }

    /// The alarms of any key, read from the bucket: what whoever owns it has
    /// pending.
    pub async fn read_alarms(&self, key: &str) -> Result<BTreeMap<String, Alarm>, Error> {
        if let Some(held) = self.state().held.get(key) {
            return Ok(held.alarms.clone());
        }
        Ok(self
            .read_latest(key)
            .await?
            .1
            .map(|record| record.alarms)
            .unwrap_or_default())
    }

    /// Stop claiming, and say so in the lease so peers stop sending this
    /// process tables to claim.
    pub async fn drain(&self) {
        self.state().session.draining = true;
        self.renew().await;
    }

    /// End the session: nothing further is claimed or acknowledged, and the
    /// lease is removed so peers need not wait for it to lapse.
    pub async fn retire(&self) {
        let node = {
            let mut state = self.state();
            state.session.retired = true;
            state.session.anchor = None;
            let node = state.session.node.clone();
            state.dead.insert(node.clone());
            node
        };
        let _ = self.store.delete(&self.lease_path(&node)).await;
        eprintln!("walleye.ownership retire node={node}");
    }

    /// Whether this process has watched the bucket long enough to judge a
    /// lease it found at start: until then every lease looks alive.
    pub fn settled(&self) -> bool {
        self.state().started.elapsed() >= self.config.verdict()
    }

    /// The live process that should claim `table` if it is orphaned: the
    /// rendezvous winner among live, non-draining leases, this one included.
    pub fn preferred(&self, table: &str) -> Option<Peer> {
        let live = self.live_peers();
        let state = self.state();
        if let Some(placed) = state.placement.get(table)
            && live.contains(placed)
        {
            return Some(placed.clone());
        }
        drop(state);
        // A key the last sweep did not know goes to its rendezvous winner.
        live.into_iter().max_by_key(|peer| score(table, &peer.node))
    }

    /// The live, non-draining processes, this one included when it is an
    /// owner, as this process sees them now.
    pub fn live_peers(&self) -> Vec<Peer> {
        let mut state = self.state();
        let authoritative = self.authoritative_locked(&mut state);
        let me = state.session.node.clone();
        let mut live = Vec::new();
        let mut consider = |node: &str, addr: &str| {
            live.push(Peer {
                node: node.to_owned(),
                addr: addr.to_owned(),
            });
        };
        if authoritative && !state.session.draining {
            consider(&me, &self.addr);
        }
        for (node, seen) in &state.leases {
            if *node == me || state.dead.contains(node) {
                continue;
            }
            let Some(lease) = &seen.lease else { continue };
            if lease.draining || seen.first_seen.elapsed() >= self.config.verdict() {
                continue;
            }
            consider(node, &lease.addr);
        }
        live.sort_by(|a, b| a.node.cmp(&b.node));
        live
    }

    /// Work out where every known key belongs among the live processes, for
    /// claims and hand-backs until the next sweep.
    pub fn plan(&self, keys: &[String]) {
        let live = self.live_peers();
        let nodes: Vec<String> = live.iter().map(|peer| peer.node.clone()).collect();
        let placed = place(keys, &nodes);
        let fair_share = keys.len().div_ceil(nodes.len().max(1));
        let by_node: HashMap<&str, &Peer> = live.iter().map(|p| (p.node.as_str(), p)).collect();
        let mut state = self.state();
        state.placement = placed
            .into_iter()
            .filter_map(|(key, node)| by_node.get(node.as_str()).map(|p| (key, (*p).clone())))
            .collect();
        state.fair_share = if nodes.is_empty() {
            usize::MAX
        } else {
            fair_share
        };
    }

    /// A key this process should hand back: it holds more than its fair share
    /// of the known keys, and the key belongs to another live process. One
    /// at a time, the one this process has least claim to.
    pub fn surplus(&self, known: &[String]) -> Option<String> {
        let me = self.node();
        let state = self.state();
        let held: Vec<&String> = state
            .held
            .keys()
            .filter(|key| known.contains(key))
            .collect();
        if held.len() <= state.fair_share {
            return None;
        }
        held.into_iter()
            .filter(|key| {
                state
                    .placement
                    .get(key.as_str())
                    .is_some_and(|peer| peer.node != me)
            })
            .min_by_key(|key| score(key, &me))
            .cloned()
    }

    /// Of `tables`, those the last sample shows nobody live owns and this
    /// process should claim.
    pub fn orphaned(&self, tables: &[String]) -> Vec<String> {
        let me = self.node();
        tables
            .iter()
            .filter(|table| {
                let state = self.state();
                if state.held.contains_key(*table) {
                    return false;
                }
                let orphan = match state.records.get(*table) {
                    None => true,
                    Some((_, record)) if record.node.is_empty() => true,
                    Some((_, record)) => {
                        state.dead.contains(&record.node)
                            || state.leases.get(&record.node).is_none_or(|seen| {
                                seen.first_seen.elapsed() >= self.config.verdict()
                            })
                    }
                };
                drop(state);
                orphan && self.preferred(table).is_some_and(|peer| peer.node == me)
            })
            .cloned()
            .collect()
    }

    /// What this process believes, for `/internal/ownership`.
    pub fn status(&self) -> serde_json::Value {
        let settled = self.settled();
        let mut state = self.state();
        let authoritative = self.authoritative_locked(&mut state);
        let held: serde_json::Map<String, serde_json::Value> = state
            .held
            .iter()
            .map(|(table, held)| (table.clone(), serde_json::json!(held.epoch)))
            .collect();
        let leases: Vec<serde_json::Value> = state
            .leases
            .iter()
            .map(|(node, seen)| {
                serde_json::json!({
                    "node": node,
                    "addr": seen.lease.as_ref().map(|lease| lease.addr.clone()),
                    "draining": seen.lease.as_ref().is_some_and(|lease| lease.draining),
                    "unchanged_ms": seen.first_seen.elapsed().as_millis() as u64,
                    "dead": seen.first_seen.elapsed() >= self.config.verdict(),
                })
            })
            .collect();
        serde_json::json!({
            "node": state.session.node,
            "authoritative": authoritative,
            "draining": state.session.draining,
            // Whether it sweeps yet: claims orphans and hands back surplus.
            "settled": settled,
            "held": held,
            "leases": leases,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> LeaseConfig {
        LeaseConfig {
            ttl_ms: 600,
            skew_ms: 100,
            sample_ms: 100,
        }
    }

    async fn node(store: &Arc<dyn ObjectStore>, id: &str) -> Arc<Ownership> {
        let owners = Arc::new(Ownership::new(
            store.clone(),
            Path::from("root"),
            id,
            format!("http://{id}"),
            config(),
        ));
        assert!(owners.renew().await, "the first lease lands");
        owners
    }

    fn local_store(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    /// Many claimants, one record version: exactly one wins, on a store whose
    /// only conditional write is create-if-absent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn racing_claims_have_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path());
        for round in 0..20 {
            let table = format!("t{round}");
            let mut nodes = Vec::new();
            for index in 0..6 {
                nodes.push(node(&store, &format!("n{index}")).await);
            }
            let claims = futures::future::join_all(nodes.iter().map(|owners| {
                let table = table.clone();
                async move { owners.claim(&table).await.unwrap().is_ok() }
            }))
            .await;
            assert_eq!(
                claims.iter().filter(|won| **won).count(),
                1,
                "round {round}: {claims:?}"
            );
            let winners: Vec<_> = nodes.iter().filter(|n| n.holds(&table).is_some()).collect();
            assert_eq!(winners.len(), 1);
            let (_, record) = nodes[0].read_latest(&table).await.unwrap();
            assert_eq!(record.unwrap().node, winners[0].node());
        }
    }

    /// A live owner is never displaced; a released record is taken at once;
    /// a lapsed owner is taken only after the verdict, and itself stops
    /// answering as an owner before that.
    #[tokio::test]
    async fn live_owners_keep_tables_and_lapsed_ones_lose_them() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path());
        let a = node(&store, "a").await;
        let b = node(&store, "b").await;
        assert!(a.claim("t").await.unwrap().is_ok());
        assert!(matches!(
            b.claim("t").await.unwrap(),
            Err(Refusal::Owned { .. })
        ));
        assert!(a.set_epoch("t", 4).await.unwrap());

        // Released: the next claimant takes it at the same epoch.
        a.release("t").await.unwrap();
        assert_eq!(b.claim("t").await.unwrap().unwrap(), 4);

        // B stops renewing. It stops answering as an owner after ttl on its
        // own clock, and A can claim only after ttl + skew on A's, so there is
        // never a moment when both hold the table.
        let before = b.node();
        let started = Instant::now();
        loop {
            a.renew().await;
            if let Ok(epoch) = a.claim("t").await.unwrap() {
                assert_eq!(epoch, 4, "the epoch carries over to the new owner");
                assert_eq!(b.holds("t"), None, "two owners at once");
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(started.elapsed() >= Duration::from_millis(700));
        assert_eq!(b.take_lost(), vec!["t".to_owned()]);
        assert_ne!(b.node(), before, "a lapsed owner starts a new session");
        assert!(b.renew().await, "which may publish a lease and claim again");
        assert!(matches!(
            b.claim("t").await.unwrap(),
            Err(Refusal::Owned { .. })
        ));
    }

    /// Placement is balanced within one, the same wherever it is worked out,
    /// and stable: a key keeps its process while the membership does.
    #[test]
    fn placement_is_balanced_and_deterministic() {
        for (keys, nodes) in [(12, 3), (13, 3), (7, 4), (100, 5), (3, 5)] {
            let keys: Vec<String> = (0..keys).map(|i| format!("t{i}")).collect();
            let nodes: Vec<String> = (0..nodes).map(|i| format!("n{i}.x")).collect();
            let placed = place(&keys, &nodes);
            assert_eq!(placed.len(), keys.len());
            let mut load: HashMap<&String, usize> = HashMap::new();
            for node in placed.values() {
                *load.entry(node).or_default() += 1;
            }
            let cap = keys.len().div_ceil(nodes.len());
            assert!(load.values().all(|n| *n <= cap), "{load:?}");
            let mut reversed = nodes.clone();
            reversed.reverse();
            assert_eq!(
                place(&keys, &reversed),
                placed,
                "order of discovery is irrelevant"
            );
        }
    }

    /// The hand-back rule reaches a fixed point from any starting spread: a
    /// process over its share gives one key to where it belongs, the process
    /// it belongs to takes it, and nothing moves once no process is over.
    /// Keys only ever move to their place, so the number of moves is bounded
    /// by the number of keys.
    #[test]
    fn hand_backs_reach_a_fixed_point() {
        let nodes: Vec<String> = (0..3).map(|i| format!("n{i}.x")).collect();
        for seed in 0..200_u64 {
            let keys: Vec<String> = (0..12).map(|i| format!("k{seed}-{i}")).collect();
            let placed = place(&keys, &nodes);
            let cap = keys.len().div_ceil(nodes.len());
            // Any starting spread, skewed by the seed.
            let mut owner: HashMap<String, String> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let n = ((seed as usize) * 7 + i * i) % 3;
                    (
                        k.clone(),
                        nodes[if i % 4 == 0 { n } else { seed as usize % 3 }].clone(),
                    )
                })
                .collect();
            let mut moves = 0;
            loop {
                let mut moved = false;
                for node in &nodes {
                    let mine: Vec<&String> = keys.iter().filter(|k| owner[*k] == *node).collect();
                    if mine.len() <= cap {
                        continue;
                    }
                    if let Some(key) = mine
                        .into_iter()
                        .filter(|k| placed[*k] != *node)
                        .min_by_key(|k| score(k, node))
                    {
                        owner.insert(key.clone(), placed[key].clone());
                        moved = true;
                        moves += 1;
                    }
                }
                if !moved {
                    break;
                }
                assert!(moves <= keys.len(), "seed {seed}: moves did not stop");
            }
            let mut load: HashMap<&String, usize> = HashMap::new();
            for node in owner.values() {
                *load.entry(node).or_default() += 1;
            }
            assert!(
                nodes.iter().all(|n| load.get(n).copied().unwrap_or(0) == 4),
                "seed {seed}: {load:?}"
            );
        }
    }

    /// A contender that read an old version and writes it after it was
    /// collected must not believe it won.
    #[tokio::test]
    async fn a_stale_version_written_after_collection_does_not_win() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path());
        let a = node(&store, "a").await;
        assert!(a.claim("t").await.unwrap().is_ok());
        for epoch in 1..6 {
            assert!(a.set_epoch("t", epoch).await.unwrap());
        }
        let (seq, _) = a.read_latest("t").await.unwrap();
        assert!(seq >= 6);
        let ghost = OwnerRecord {
            node: "ghost".into(),
            epoch: 99,
            alarms: BTreeMap::new(),
        };
        assert!(
            !a.write_version("t", 2, &ghost).await.unwrap(),
            "version 2 was collected, and writing it again wins nothing"
        );
    }
}
