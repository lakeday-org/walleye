//! Durable, stream-local placement admission fences.
//!
//! A placement epoch is host metadata.  It is deliberately separate from an
//! [`EncryptedRecord`](walleye_bitr::EncryptedRecord)'s authenticated
//! `writer_epoch`: moving a stream between cohorts must never rewrite an
//! existing ciphertext or its authentication material.
//!
//! [`PlacementStore::admit`] and [`PlacementStore::install_fence`] serialize
//! through the same stream gate.  A lease admitted before a fence is allowed
//! to finish, while the fence marks the stream as transitioning before it
//! waits for those leases to drain.  A delayed request carrying the old
//! placement therefore cannot enter after the fence has started.  Other
//! streams continue to admit while this happens; there is no cell-wide lock.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use serde::{Deserialize, Serialize};

/// Version of the sidecar placement document.
pub const PLACEMENT_STATE_VERSION: u8 = 1;

/// Name used when a replica places the sidecar beside its append log.
pub const PLACEMENT_STATE_FILENAME: &str = "replica-placement.json";

/// Returns the default placement sidecar path for a mounted replica volume.
#[must_use]
pub fn placement_state_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir.as_ref().join(PLACEMENT_STATE_FILENAME)
}

/// Host-owned identity for one stream placement.
///
/// `epoch` is monotonic for a stream.  `route_digest` binds that epoch to the
/// complete route/head value that authorized it, so two different routes
/// cannot both be accepted at one epoch.  The digest is opaque to this
/// module; the control-head implementation computes and validates it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlacementEpoch {
    pub epoch: u64,
    #[serde(default)]
    pub route_digest: String,
}

impl PlacementEpoch {
    /// The bootstrap route used before a stream has had a live handoff.
    #[must_use]
    pub fn bootstrap() -> Self {
        Self::default()
    }

    /// Creates a placement value bound to a control-head route digest.
    #[must_use]
    pub fn new(epoch: u64, route_digest: impl Into<String>) -> Self {
        Self {
            epoch,
            route_digest: route_digest.into(),
        }
    }

    /// Returns the monotonic host epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Returns the opaque control-head route digest.
    #[must_use]
    pub fn route_digest(&self) -> &str {
        &self.route_digest
    }
}

/// Errors returned before a caller can mutate a node log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlacementError {
    /// A stream name is required because the gate is stream scoped.
    InvalidStream,
    /// Placement epoch zero is the bootstrap value and higher epochs must be
    /// bound to a non-empty control-head route digest.
    InvalidEpoch,
    /// A persisted placement document could not be read or written.
    Storage(String),
    /// A request arrived while this stream's fence was draining admitted
    /// writes. The caller must obtain the route published after the fence.
    Transitioning,
    /// The request carries an older placement than the durable stream gate.
    Stale {
        current: PlacementEpoch,
        received: PlacementEpoch,
    },
    /// The request carries an epoch that the durable control head has not yet
    /// published to this node.
    Future {
        current: PlacementEpoch,
        received: PlacementEpoch,
    },
    /// The epoch matches but its route digest does not. This is a split-brain
    /// route and must never be treated as an idempotent retry.
    Conflict {
        current: PlacementEpoch,
        received: PlacementEpoch,
    },
    /// A fence attempted to move the stream backwards.
    Decreasing {
        current: PlacementEpoch,
        requested: PlacementEpoch,
    },
    /// The requested fence has the same epoch but a different route identity.
    FenceConflict {
        current: PlacementEpoch,
        requested: PlacementEpoch,
    },
}

impl fmt::Display for PlacementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStream => formatter.write_str("placement stream must not be empty"),
            Self::InvalidEpoch => formatter.write_str(
                "placement epoch zero must have no digest and higher epochs require a route digest",
            ),
            Self::Storage(reason) => write!(formatter, "placement storage failed: {reason}"),
            Self::Transitioning => formatter.write_str("stream placement is transitioning"),
            Self::Stale { current, received } => write!(
                formatter,
                "stale stream placement epoch {} (current {})",
                received.epoch, current.epoch
            ),
            Self::Future { current, received } => write!(
                formatter,
                "stream placement epoch {} is ahead of current {}",
                received.epoch, current.epoch
            ),
            Self::Conflict { current, received } => write!(
                formatter,
                "stream placement epoch {} has a conflicting route digest (current {})",
                received.epoch, current.epoch
            ),
            Self::Decreasing { current, requested } => write!(
                formatter,
                "stream placement cannot move from epoch {} backwards to {}",
                current.epoch, requested.epoch
            ),
            Self::FenceConflict { current, requested } => write!(
                formatter,
                "stream placement epoch {} already names another route (requested {})",
                requested.epoch, current.epoch
            ),
        }
    }
}

impl std::error::Error for PlacementError {}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PersistedPlacementState {
    version: u8,
    #[serde(default)]
    streams: BTreeMap<String, PlacementEpoch>,
}

struct StreamGate {
    state: Mutex<StreamGateState>,
    drained: Condvar,
}

struct StreamGateState {
    placement: PlacementEpoch,
    admitted: usize,
    transitioning: bool,
}

impl StreamGate {
    fn new(placement: PlacementEpoch) -> Self {
        Self {
            state: Mutex::new(StreamGateState {
                placement,
                admitted: 0,
                transitioning: false,
            }),
            drained: Condvar::new(),
        }
    }
}

struct PlacementStoreInner {
    path: PathBuf,
    /// Stream gates are independent. This map lock is held only long enough
    /// to find/create one gate; it is never held while an append or fsync is
    /// in progress.
    gates: Mutex<BTreeMap<String, Arc<StreamGate>>>,
    /// Last document successfully written to disk. It is separate from gate
    /// locks so admissions on unrelated streams never wait for a sidecar
    /// fsync.
    persisted: Mutex<BTreeMap<String, PlacementEpoch>>,
    /// Fence publication serializes sidecar snapshots across streams. It is
    /// only acquired by a fence installation, never by append admission.
    persist_serial: Mutex<()>,
    /// A sidecar rename followed by a failed directory fsync leaves the
    /// durable outcome uncertain. Stop this process from accepting appends or
    /// publishing another snapshot; a restart reloads the sidecar atomically.
    failed_closed: AtomicBool,
}

/// Durable stream-local placement admission state.
#[derive(Clone)]
pub struct PlacementStore {
    inner: Arc<PlacementStoreInner>,
}

impl PlacementStore {
    /// Opens a placement sidecar. A missing sidecar means every stream is at
    /// the bootstrap placement epoch. Callers that have already persisted the
    /// control-state migration marker must use [`Self::open_existing`], which
    /// fails closed when the sidecar is absent.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PlacementError> {
        let path = path.as_ref().to_owned();
        let persisted = read_state(&path)?;
        Ok(Self::from_persisted(path, persisted))
    }

    /// Opens a sidecar that was previously initialized. This is deliberately
    /// stricter than [`Self::open`]: after the control state says placement
    /// migration completed, an absent sidecar is a storage failure rather
    /// than an invitation to recreate epoch-zero gates.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, PlacementError> {
        let path = path.as_ref().to_owned();
        let persisted = read_state_required(&path)?;
        Ok(Self::from_persisted(path, persisted))
    }

    /// Initializes a missing sidecar from the certified legacy route map.
    ///
    /// The caller must only pass routes read from the durable control state
    /// while the control-state migration marker is still false. Existing
    /// sidecars are never overwritten: a valid sidecar is accepted only when
    /// it exactly matches the route map (the crash-recovery case after the
    /// sidecar rename but before the control marker write), while malformed or
    /// divergent state fails closed.
    pub fn bootstrap_legacy(
        path: impl AsRef<Path>,
        legacy_routes: BTreeMap<String, PlacementEpoch>,
    ) -> Result<Self, PlacementError> {
        for (stream, placement) in &legacy_routes {
            if stream.trim().is_empty() {
                return Err(PlacementError::Storage(
                    "legacy placement state contains an empty stream".to_owned(),
                ));
            }
            validate_epoch(placement).map_err(|_| {
                PlacementError::Storage("invalid legacy placement epoch".to_owned())
            })?;
        }
        let path = path.as_ref().to_owned();
        if let Some(persisted) = read_state_optional(&path)? {
            if persisted != legacy_routes {
                return Err(PlacementError::Storage(
                    "existing placement sidecar disagrees with legacy control routes".to_owned(),
                ));
            }
            return Ok(Self::from_persisted(path, persisted));
        }
        persist_state(&path, &legacy_routes)?;
        Ok(Self::from_persisted(path, legacy_routes))
    }

    fn from_persisted(path: PathBuf, persisted: BTreeMap<String, PlacementEpoch>) -> Self {
        Self {
            inner: Arc::new(PlacementStoreInner {
                path,
                gates: Mutex::new(BTreeMap::new()),
                persisted: Mutex::new(persisted),
                persist_serial: Mutex::new(()),
                failed_closed: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the backing sidecar path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Returns the current durable placement for a stream.
    pub fn current(&self, stream: &str) -> Result<PlacementEpoch, PlacementError> {
        self.ensure_healthy()?;
        let gate = self.gate(stream)?;
        let state = gate
            .state
            .lock()
            .map_err(|_| PlacementError::Storage("placement gate is poisoned".to_owned()))?;
        Ok(state.placement.clone())
    }

    /// Admits one append at the exact current placement and returns a lease.
    ///
    /// The caller must hold the lease through the complete node append and
    /// acknowledgement decision. Dropping it signals a waiting fence that
    /// this append has drained.
    pub fn admit(
        &self,
        stream: &str,
        placement: &PlacementEpoch,
    ) -> Result<PlacementLease, PlacementError> {
        self.ensure_healthy()?;
        validate_epoch(placement)?;
        let gate = self.gate(stream)?;
        let mut state = gate
            .state
            .lock()
            .map_err(|_| PlacementError::Storage("placement gate is poisoned".to_owned()))?;
        if state.transitioning {
            return Err(PlacementError::Transitioning);
        }
        compare_admission(&state.placement, placement)?;
        state.admitted = state.admitted.saturating_add(1);
        drop(state);
        Ok(PlacementLease {
            gate,
            released: false,
            placement: placement.clone(),
        })
    }

    /// Installs a higher placement epoch after all admissions accepted under
    /// the old epoch drain. The sidecar is synced before this returns, so a
    /// restart cannot forget the fence. Reapplying the exact current value is
    /// idempotent; decreasing or same-epoch conflicting values are rejected.
    pub fn install_fence(
        &self,
        stream: &str,
        requested: PlacementEpoch,
    ) -> Result<(), PlacementError> {
        self.ensure_healthy()?;
        validate_epoch(&requested)?;
        let gate = self.gate(stream)?;
        let mut state = gate
            .state
            .lock()
            .map_err(|_| PlacementError::Storage("placement gate is poisoned".to_owned()))?;
        // Only one fence may own the stream transition. A second request
        // waits for that publication and then re-evaluates its monotonicity;
        // checking only before the wait would let epoch N+1 publish before a
        // concurrent epoch N and then allow the older request to regress the
        // route.
        while state.transitioning {
            state = gate
                .drained
                .wait(state)
                .map_err(|_| PlacementError::Storage("placement gate is poisoned".to_owned()))?;
            self.ensure_healthy()?;
        }
        match requested.epoch.cmp(&state.placement.epoch) {
            std::cmp::Ordering::Less => {
                return Err(PlacementError::Decreasing {
                    current: state.placement.clone(),
                    requested,
                });
            }
            std::cmp::Ordering::Equal => {
                if requested == state.placement {
                    return Ok(());
                }
                return Err(PlacementError::FenceConflict {
                    current: state.placement.clone(),
                    requested,
                });
            }
            std::cmp::Ordering::Greater => {}
        }

        // Set the transition bit before waiting. New old-route requests now
        // fail at admission while leases already accepted before this point
        // are allowed to complete.
        state.transitioning = true;
        while state.admitted != 0 {
            state = gate
                .drained
                .wait(state)
                .map_err(|_| PlacementError::Storage("placement gate is poisoned".to_owned()))?;
        }

        // Only fence publication serializes across streams. Admissions on
        // other streams never acquire this mutex.
        let _serial = match self.inner.persist_serial.lock() {
            Ok(serial) => serial,
            Err(_) => {
                self.inner.failed_closed.store(true, Ordering::Release);
                state.transitioning = false;
                gate.drained.notify_all();
                return Err(PlacementError::Storage(
                    "placement persistence is poisoned".to_owned(),
                ));
            }
        };
        if self.inner.failed_closed.load(Ordering::Acquire) {
            state.transitioning = false;
            gate.drained.notify_all();
            return Err(PlacementError::Storage(
                "placement store is fail-closed after an uncertain sidecar write".to_owned(),
            ));
        }
        let mut persisted = match self.inner.persisted.lock() {
            Ok(persisted) => persisted,
            Err(_) => {
                self.inner.failed_closed.store(true, Ordering::Release);
                state.transitioning = false;
                gate.drained.notify_all();
                return Err(PlacementError::Storage(
                    "placement state is poisoned".to_owned(),
                ));
            }
        };
        let mut next = persisted.clone();
        if let Some(persisted) = next.get(stream) {
            // A stream gate is the admission serialization point, but this
            // check also guards against accidental future alternate gate
            // implementations publishing a stale sidecar snapshot.
            if persisted != &state.placement {
                self.inner.failed_closed.store(true, Ordering::Release);
                state.transitioning = false;
                gate.drained.notify_all();
                return Err(PlacementError::Storage(
                    "placement gate disagrees with durable sidecar".to_owned(),
                ));
            }
        }
        next.insert(stream.to_owned(), requested.clone());
        if let Err(error) = persist_state(&self.inner.path, &next) {
            self.inner.failed_closed.store(true, Ordering::Release);
            state.transitioning = false;
            gate.drained.notify_all();
            return Err(error);
        }
        *persisted = next;
        state.placement = requested;
        state.transitioning = false;
        gate.drained.notify_all();
        Ok(())
    }

    /// Returns a snapshot of the sidecar values. This is intended for status
    /// and tests; append admission should use [`Self::admit`].
    pub fn snapshot(&self) -> Result<BTreeMap<String, PlacementEpoch>, PlacementError> {
        self.ensure_healthy()?;
        self.inner
            .persisted
            .lock()
            .map(|state| state.clone())
            .map_err(|_| PlacementError::Storage("placement state is poisoned".to_owned()))
    }

    /// Returns whether a stream has an explicit durable placement entry.
    /// Unseen streams may still be opened by the route-creation path; a
    /// stream already present in the sidecar must always carry its token.
    pub fn contains_stream(&self, stream: &str) -> Result<bool, PlacementError> {
        self.ensure_healthy()?;
        if stream.trim().is_empty() {
            return Err(PlacementError::InvalidStream);
        }
        self.inner
            .persisted
            .lock()
            .map(|state| state.contains_key(stream))
            .map_err(|_| PlacementError::Storage("placement state is poisoned".to_owned()))
    }

    /// Returns whether a stream is currently draining admitted appends.
    pub fn is_transitioning(&self, stream: &str) -> Result<bool, PlacementError> {
        let gate = self.gate(stream)?;
        gate.state
            .lock()
            .map(|state| state.transitioning)
            .map_err(|_| PlacementError::Storage("placement gate is poisoned".to_owned()))
    }

    fn gate(&self, stream: &str) -> Result<Arc<StreamGate>, PlacementError> {
        if stream.trim().is_empty() {
            return Err(PlacementError::InvalidStream);
        }
        let mut gates = self
            .inner
            .gates
            .lock()
            .map_err(|_| PlacementError::Storage("placement gates are poisoned".to_owned()))?;
        if let Some(gate) = gates.get(stream) {
            return Ok(Arc::clone(gate));
        }
        let placement = self
            .inner
            .persisted
            .lock()
            .map_err(|_| PlacementError::Storage("placement state is poisoned".to_owned()))?
            .get(stream)
            .cloned()
            .unwrap_or_default();
        let gate = Arc::new(StreamGate::new(placement));
        gates.insert(stream.to_owned(), Arc::clone(&gate));
        Ok(gate)
    }

    fn ensure_healthy(&self) -> Result<(), PlacementError> {
        if self.inner.failed_closed.load(Ordering::Acquire) {
            return Err(PlacementError::Storage(
                "placement store is fail-closed after an uncertain sidecar write".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A stream append admitted under one exact placement.
pub struct PlacementLease {
    gate: Arc<StreamGate>,
    released: bool,
    placement: PlacementEpoch,
}

impl PlacementLease {
    /// Returns the placement that was checked at admission.
    #[must_use]
    pub fn placement(&self) -> &PlacementEpoch {
        &self.placement
    }

    /// Explicitly releases the lease. Drop performs the same operation.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        if let Ok(mut state) = self.gate.state.lock() {
            debug_assert!(state.admitted > 0);
            state.admitted = state.admitted.saturating_sub(1);
            if state.admitted == 0 {
                self.gate.drained.notify_all();
            }
        }
    }
}

impl Drop for PlacementLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

fn compare_admission(
    current: &PlacementEpoch,
    received: &PlacementEpoch,
) -> Result<(), PlacementError> {
    match received.epoch.cmp(&current.epoch) {
        std::cmp::Ordering::Less => Err(PlacementError::Stale {
            current: current.clone(),
            received: received.clone(),
        }),
        std::cmp::Ordering::Greater => Err(PlacementError::Future {
            current: current.clone(),
            received: received.clone(),
        }),
        std::cmp::Ordering::Equal if received.route_digest == current.route_digest => Ok(()),
        std::cmp::Ordering::Equal => Err(PlacementError::Conflict {
            current: current.clone(),
            received: received.clone(),
        }),
    }
}

fn validate_epoch(placement: &PlacementEpoch) -> Result<(), PlacementError> {
    if (placement.epoch == 0 && !placement.route_digest.is_empty())
        || (placement.epoch > 0 && placement.route_digest.trim().is_empty())
    {
        return Err(PlacementError::InvalidEpoch);
    }
    Ok(())
}

fn read_state(path: &Path) -> Result<BTreeMap<String, PlacementEpoch>, PlacementError> {
    Ok(read_state_optional(path)?.unwrap_or_default())
}

fn read_state_required(path: &Path) -> Result<BTreeMap<String, PlacementEpoch>, PlacementError> {
    read_state_optional(path)?.ok_or_else(|| {
        PlacementError::Storage("placement sidecar is missing after initialization".to_owned())
    })
}

fn read_state_optional(
    path: &Path,
) -> Result<Option<BTreeMap<String, PlacementEpoch>>, PlacementError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(PlacementError::Storage(error.to_string())),
    };
    let document = serde_json::from_slice::<PersistedPlacementState>(&bytes)
        .map_err(|error| PlacementError::Storage(format!("invalid placement state: {error}")))?;
    if document.version != PLACEMENT_STATE_VERSION {
        return Err(PlacementError::Storage(
            "unsupported placement state version".to_owned(),
        ));
    }
    for (stream, placement) in &document.streams {
        if stream.trim().is_empty() {
            return Err(PlacementError::Storage(
                "placement state contains an empty stream".to_owned(),
            ));
        }
        validate_epoch(placement)
            .map_err(|_| PlacementError::Storage("invalid placement epoch".to_owned()))?;
    }
    Ok(Some(document.streams))
}

fn persist_state(
    path: &Path,
    streams: &BTreeMap<String, PlacementEpoch>,
) -> Result<(), PlacementError> {
    let parent = path.parent().ok_or_else(|| {
        PlacementError::Storage("placement state has no parent directory".to_owned())
    })?;
    std::fs::create_dir_all(parent).map_err(|error| PlacementError::Storage(error.to_string()))?;
    let document = PersistedPlacementState {
        version: PLACEMENT_STATE_VERSION,
        streams: streams.clone(),
    };
    let encoded = serde_json::to_vec(&document)
        .map_err(|error| PlacementError::Storage(error.to_string()))?;
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    let mut temp_os = path.as_os_str().to_owned();
    temp_os.push(format!(".tmp-{}-{sequence}", std::process::id()));
    let temp = PathBuf::from(temp_os);
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(|error| PlacementError::Storage(error.to_string()))?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(|error| PlacementError::Storage(error.to_string()))?;
        std::fs::rename(&temp, path).map_err(|error| PlacementError::Storage(error.to_string()))?;
        let directory =
            File::open(parent).map_err(|error| PlacementError::Storage(error.to_string()))?;
        directory
            .sync_all()
            .map_err(|error| PlacementError::Storage(error.to_string()))
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    write_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    fn store() -> (tempfile::TempDir, PlacementStore) {
        let directory = tempfile::tempdir().expect("temporary placement directory");
        let path = directory.path().join(PLACEMENT_STATE_FILENAME);
        let store = PlacementStore::open(path).expect("open placement store");
        (directory, store)
    }

    #[test]
    fn stale_append_is_rejected_before_a_mutation_can_run() {
        let (_directory, store) = store();
        let current = PlacementEpoch::bootstrap();
        let lease = store
            .admit("tenant/stream", &current)
            .expect("bootstrap admit");
        drop(lease);
        store
            .install_fence("tenant/stream", PlacementEpoch::new(1, "route-1"))
            .expect("install fence");

        let stale = store.admit("tenant/stream", &current);
        assert!(matches!(stale, Err(PlacementError::Stale { .. })));
        assert_eq!(
            store.current("tenant/stream").expect("current placement"),
            PlacementEpoch::new(1, "route-1")
        );
    }

    #[test]
    fn fence_waits_for_old_lease_and_then_rejects_delayed_old_route() {
        let (_directory, store) = store();
        let old = PlacementEpoch::bootstrap();
        let lease = store.admit("tenant/stream", &old).expect("old admit");
        let fencing = store.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_thread = Arc::clone(&finished);
        let handle = thread::spawn(move || {
            fencing
                .install_fence("tenant/stream", PlacementEpoch::new(1, "route-1"))
                .expect("fence install");
            finished_thread.store(true, Ordering::Release);
        });

        for _ in 0..100 {
            if store
                .is_transitioning("tenant/stream")
                .expect("transition status")
            {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            store
                .is_transitioning("tenant/stream")
                .expect("transition status")
        );
        assert!(matches!(
            store.admit("tenant/stream", &old),
            Err(PlacementError::Transitioning)
        ));
        assert!(!finished.load(Ordering::Acquire));

        drop(lease);
        handle.join().expect("fence thread");
        let fresh = PlacementEpoch::new(1, "route-1");
        let lease = store.admit("tenant/stream", &fresh).expect("new admit");
        drop(lease);
    }

    #[test]
    fn unrelated_streams_do_not_wait_for_each_other() {
        let (_directory, store) = store();
        let old = PlacementEpoch::bootstrap();
        let held = store.admit("tenant/a", &old).expect("stream a admit");
        store
            .install_fence("tenant/b", PlacementEpoch::new(1, "route-b"))
            .expect("stream b fence");
        let b = PlacementEpoch::new(1, "route-b");
        let b_lease = store.admit("tenant/b", &b).expect("stream b admit");
        drop(b_lease);
        assert!(!store.is_transitioning("tenant/a").expect("stream a status"));
        drop(held);
    }

    #[test]
    fn exact_reapply_persists_and_reopen_keeps_fence() {
        let directory = tempfile::tempdir().expect("temporary placement directory");
        let path = directory.path().join(PLACEMENT_STATE_FILENAME);
        let first = PlacementStore::open(&path).expect("open placement store");
        let placement = PlacementEpoch::new(3, "route-3");
        first
            .install_fence("tenant/stream", placement.clone())
            .expect("install fence");
        first
            .install_fence("tenant/stream", placement.clone())
            .expect("idempotent reapply");
        assert!(matches!(
            first.install_fence("tenant/stream", PlacementEpoch::new(2, "route-2")),
            Err(PlacementError::Decreasing { .. })
        ));
        assert!(matches!(
            first.install_fence("tenant/stream", PlacementEpoch::new(3, "other-route")),
            Err(PlacementError::FenceConflict { .. })
        ));
        drop(first);

        let reopened = PlacementStore::open(&path).expect("reopen placement store");
        assert_eq!(
            reopened
                .current("tenant/stream")
                .expect("reopened placement"),
            placement
        );
        assert!(matches!(
            reopened.admit("tenant/stream", &PlacementEpoch::bootstrap()),
            Err(PlacementError::Stale { .. })
        ));
    }

    #[test]
    fn concurrent_fences_cannot_regress_the_epoch() {
        // Keep both requests behind one already-admitted append so they are
        // forced to contend at the same transition boundary. Whichever
        // request wins must leave the larger epoch installed; the loser must
        // re-check after the winner publishes and return Decreasing.
        for iteration in 0..64_u64 {
            let (_directory, store) = store();
            let stream = format!("tenant/concurrent-{iteration}");
            let old = PlacementEpoch::bootstrap();
            let held = store.admit(&stream, &old).expect("old admit");
            let start = Arc::new(std::sync::Barrier::new(3));
            let lower_store = store.clone();
            let lower_start = Arc::clone(&start);
            let lower_stream = stream.clone();
            let lower = thread::spawn(move || {
                lower_start.wait();
                lower_store.install_fence(&lower_stream, PlacementEpoch::new(1, "route-1"))
            });
            let higher_store = store.clone();
            let higher_start = Arc::clone(&start);
            let higher_stream = stream.clone();
            let higher = thread::spawn(move || {
                higher_start.wait();
                higher_store.install_fence(&higher_stream, PlacementEpoch::new(2, "route-2"))
            });
            start.wait();
            while !store.is_transitioning(&stream).expect("transition status") {
                thread::yield_now();
            }
            drop(held);
            let lower_result = lower.join().expect("lower fence thread");
            let higher_result = higher.join().expect("higher fence thread");
            let final_placement = store.current(&stream).expect("final placement");
            assert_eq!(final_placement, PlacementEpoch::new(2, "route-2"));
            assert!(
                higher_result.is_ok(),
                "the larger concurrent fence must publish: lower={lower_result:?} higher={higher_result:?}"
            );
            assert!(
                lower_result.is_ok()
                    || matches!(lower_result, Err(PlacementError::Decreasing { .. })),
                "the lower concurrent fence may publish first or be rejected after recheck: {lower_result:?}"
            );
        }
    }

    #[test]
    fn higher_epochs_require_a_route_digest() {
        let (_directory, store) = store();
        assert!(matches!(
            store.install_fence("tenant/stream", PlacementEpoch::new(1, "")),
            Err(PlacementError::InvalidEpoch)
        ));
    }

    #[test]
    fn bootstrap_rejects_future_route_until_explicit_fence() {
        let (_directory, store) = store();
        let future = PlacementEpoch::new(1, "route-1");

        assert!(matches!(
            store.admit("tenant/stream", &future),
            Err(PlacementError::Future { .. })
        ));
        store
            .install_fence("tenant/stream", future.clone())
            .expect("explicit bootstrap fence");
        assert!(store.admit("tenant/stream", &future).is_ok());
    }

    #[test]
    fn legacy_bootstrap_is_durable_and_missing_reopen_fails_closed() {
        let directory = tempfile::tempdir().expect("temporary placement directory");
        let path = directory.path().join(PLACEMENT_STATE_FILENAME);
        let routes = BTreeMap::from([(
            "tenant/stream".to_owned(),
            PlacementEpoch::new(1, "legacy-route"),
        )]);
        let store = PlacementStore::bootstrap_legacy(&path, routes.clone())
            .expect("bootstrap legacy placement");
        assert_eq!(store.snapshot().expect("snapshot"), routes);
        assert_eq!(
            PlacementStore::open_existing(&path)
                .expect("reopen initialized placement")
                .snapshot()
                .expect("reopened snapshot"),
            routes
        );

        std::fs::remove_file(&path).expect("remove sidecar");
        assert!(matches!(
            PlacementStore::open_existing(&path),
            Err(PlacementError::Storage(message))
                if message.contains("missing after initialization")
        ));
    }

    #[test]
    fn legacy_bootstrap_never_overwrites_corrupt_or_divergent_sidecar() {
        let directory = tempfile::tempdir().expect("temporary placement directory");
        let path = directory.path().join(PLACEMENT_STATE_FILENAME);
        std::fs::write(&path, b"not-json").expect("write corrupt sidecar");
        let route = BTreeMap::from([(
            "tenant/stream".to_owned(),
            PlacementEpoch::new(1, "legacy-route"),
        )]);
        assert!(matches!(
            PlacementStore::bootstrap_legacy(&path, route.clone()),
            Err(PlacementError::Storage(message))
                if message.contains("invalid placement state")
        ));

        let valid = PlacementStore::open(&path);
        assert!(valid.is_err(), "corrupt sidecar must remain corrupt");

        let path = directory.path().join("divergent-placement.json");
        let other = BTreeMap::from([(
            "tenant/stream".to_owned(),
            PlacementEpoch::new(1, "other-route"),
        )]);
        PlacementStore::bootstrap_legacy(&path, other).expect("write divergent sidecar");
        assert!(matches!(
            PlacementStore::bootstrap_legacy(&path, route),
            Err(PlacementError::Storage(message))
                if message.contains("disagrees with legacy control routes")
        ));
    }
}
