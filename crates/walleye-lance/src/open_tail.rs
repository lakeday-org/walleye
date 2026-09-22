//! Saying, in shared storage, that a stream's tail is only in a private log.
//!
//! The shard manifest cannot answer this. It learns WAL positions at flush
//! time, so between a checkpoint and the next one it records nothing about the
//! rows a writer has acknowledged, and it has no field naming the log they are
//! in. Two Bitr clusters, or a Bitr cluster and a node writing the object-store
//! WAL, are identical to it.
//!
//! That gap is not academic: a writer on a second cluster claims the epoch in
//! good order, replays a log that has never heard of the stream, and opens an
//! empty table over rows another quorum is still holding. Nothing refuses it
//! and nothing reports it.
//!
//! So a writer leaves a note where everyone can see it. The note says only
//! that entries exist after some position; it does not say whose they are,
//! because identity is the harder question and the useful one is answerable
//! directly. A claimant reads the note and asks its own log whether it can
//! produce those entries. If it can, the two share a log and the handover is
//! ordinary. If it cannot, the tail is somewhere it cannot reach, and it
//! refuses rather than opening a stream with a hole in it.
//!
//! The note is written once per checkpoint cycle rather than once per append -
//! on the transition from drained to dirty - so it costs a put where a flush
//! already costs far more, and nothing on the hot path.
use lance::dataset::mem_wal::util::shard_wal_path;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// What a writer leaves behind while it holds rows nobody else can read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenTail {
    /// The flush position the tail begins after. Entries at
    /// `after + 1` and beyond are acknowledged and unflushed.
    pub after: u64,
    /// Which stream, for an operator reading this by hand.
    pub stream: String,
    /// Where to reach the writer holding it, when it is reachable at all.
    ///
    /// A claimant that cannot read the tail does not have to give up: it can
    /// ask this address to flush, which puts the rows in shared storage and
    /// makes the stream takeable by anyone. Absent when the writer had no
    /// address to advertise, in which case the only remedy is manual.
    #[serde(default)]
    pub holder: Option<String>,
}

/// Where the note lives: beside the shard's WAL rather than inside it, so the
/// WAL's own filename parser never sees it.
fn marker_path(base: &ObjectPath, shard: Uuid) -> ObjectPath {
    let wal = shard_wal_path(base, &shard);
    let name = wal
        .filename()
        .map(|f| format!("{f}.walleye_open_tail"))
        .unwrap_or_else(|| format!("{shard}.walleye_open_tail"));
    wal.parts()
        .take(wal.parts().count().saturating_sub(1))
        .fold(ObjectPath::default(), |path, part| path.join(part.as_ref()))
        .join(name.as_str())
}

/// Read the note, if there is one. A note that cannot be parsed is treated as
/// present rather than absent: the safe reading of a damaged warning is that
/// the thing it warned about is still true.
pub async fn read(
    store: &lance::io::ObjectStore,
    base: &ObjectPath,
    shard: Uuid,
    stream: &str,
) -> lance::Result<Option<OpenTail>> {
    let path = marker_path(base, shard);
    match store.read_one_all(&path).await {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).unwrap_or(OpenTail {
            after: 0,
            stream: stream.to_owned(),
            holder: None,
        }))),
        Err(lance::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Leave the note. Called on the first append after a drain, not on every one.
pub async fn write(
    store: &lance::io::ObjectStore,
    base: &ObjectPath,
    shard: Uuid,
    tail: &OpenTail,
) -> lance::Result<()> {
    let body = serde_json::to_vec(tail)
        .map_err(|e| lance::Error::io(format!("writing the open-tail note: {e}")))?;
    store.put(&marker_path(base, shard), &body).await?;
    Ok(())
}

/// Take the note down, because the tail is in shared storage now and anyone
/// may have it.
pub async fn clear(
    store: &lance::io::ObjectStore,
    base: &ObjectPath,
    shard: Uuid,
) -> lance::Result<()> {
    match store.delete(&marker_path(base, shard)).await {
        Ok(()) => Ok(()),
        // Already gone is the state we wanted.
        Err(lance::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}
