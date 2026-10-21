//! Immutable object-store archival for committed encrypted replica records.
//!
//! The archive never decrypts a payload. A conditional per-stream head names
//! content-addressed batches and is the only authority for `archived_lsn`.

use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use walleye_bitr::EncryptedRecord;

const HEAD_UPDATE_ATTEMPTS: usize = 8;

/// Segments read at once. A range of the archive is many small objects, and
/// on an object store each read costs a round trip: read one at a time, a
/// recovery or catch-up waits for as many round trips as the range has
/// segments, which grows with the history since the last flush.
const SEGMENT_READS_IN_FLIGHT: usize = 32;

/// Segment references the head holds before it folds the oldest into an index
/// page, and how many of the newest it keeps when it does. The head is read
/// and rewritten on every archive pass by every member, so it has to stay the
/// same size however long the stream lives; the newest references stay in it
/// because recovery and catch-up almost always start there.
const HEAD_SEGMENTS_MAX: usize = 64;
const HEAD_SEGMENTS_KEPT: usize = 16;

/// Failure to publish or verify an opaque archive stream.
#[derive(Debug, Error)]
pub enum ArchiveError {
    /// The archive route or batch policy is malformed.
    #[error("invalid replica archive configuration: {0}")]
    Configuration(String),
    /// Object storage could not complete the requested operation.
    #[error("replica archive object-store operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// An archive object could not be encoded or decoded.
    #[error("replica archive JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
    /// Records or archive metadata do not form one exact contiguous stream.
    #[error("replica archive is not contiguous: {0}")]
    Contiguity(String),
    /// Content-addressed bytes do not match their published digest.
    #[error("replica archive checksum mismatch for {0}")]
    Checksum(String),
    /// Concurrent publishers prevented bounded head advancement.
    #[error("replica archive head remained contended")]
    Contended,
    /// A read asked for records a durable checkpoint already released.
    #[error(
        "replica archive released through LSN {released_lsn}; a read after {after_lsn} was asked"
    )]
    Released { after_lsn: u64, released_lsn: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Segment {
    stream: String,
    first_lsn: u64,
    last_lsn: u64,
    records: Vec<EncryptedRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SegmentRef {
    first_lsn: u64,
    last_lsn: u64,
    key: String,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ArchiveHead {
    stream: String,
    archived_lsn: u64,
    writer_epoch: u64,
    /// Everything through here was covered by the table's durable checkpoint
    /// and its segments deleted. No read may start below it.
    #[serde(default)]
    released_lsn: u64,
    /// The newest index page: the references to every segment before
    /// `segments`, folded out of the head so it stays small.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page: Option<PageRef>,
    segments: Vec<SegmentRef>,
}

/// An immutable, content-addressed page of segment references, and the page
/// before it. Pages chain back to the stream's first segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IndexPage {
    stream: String,
    previous: Option<PageRef>,
    segments: Vec<SegmentRef>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PageRef {
    first_lsn: u64,
    last_lsn: u64,
    key: String,
    digest: String,
}

struct LoadedHead {
    head: ArchiveHead,
    update: Option<UpdateVersion>,
}

/// One payload-blind archive namespace shared by replica nodes and gateways.
#[derive(Clone)]
pub struct OpaqueArchive {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    max_records_per_segment: usize,
}

impl OpaqueArchive {
    /// Where the archive is: its store and prefix, the same on every node
    /// archiving to it.
    pub fn location(&self) -> String {
        format!("{}/{}", self.store, self.prefix)
    }

    /// Creates an archive under one non-empty object prefix and hard batch bound.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        max_records_per_segment: usize,
    ) -> Result<Self, ArchiveError> {
        let prefix = prefix.into().trim_matches('/').to_owned();
        if prefix.is_empty() || max_records_per_segment == 0 {
            return Err(ArchiveError::Configuration(
                "prefix and max_records_per_segment must be nonzero".to_owned(),
            ));
        }
        Ok(Self {
            store,
            prefix,
            max_records_per_segment,
        })
    }

    /// Returns the object store used by this archive.
    ///
    /// The control-head store is deliberately a separate namespace, but it
    /// must use the same configured object-store client so archive and
    /// metadata durability have one explicit provider boundary.  The caller
    /// remains responsible for choosing a distinct control-head prefix.
    #[must_use]
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Returns the normalized archive prefix.  A control-head namespace
    /// should be derived from this value rather than writing beside stream
    /// objects by accident.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Publishes every new contiguous committed record in bounded immutable batches.
    pub async fn archive_committed(
        &self,
        records: &[EncryptedRecord],
    ) -> Result<u64, ArchiveError> {
        if records.is_empty() {
            return Ok(0);
        }
        let stream = records[0].stream();
        if stream.is_empty() || records.iter().any(|record| record.stream() != stream) {
            return Err(ArchiveError::Contiguity(
                "one archive call must contain exactly one non-empty stream".to_owned(),
            ));
        }
        for window in records.windows(2) {
            if window[1].lsn() != window[0].lsn().saturating_add(1)
                || window[1].committed_lsn() != window[0].lsn()
                || window[1].writer_epoch() < window[0].writer_epoch()
            {
                return Err(ArchiveError::Contiguity(format!(
                    "expected LSN {} after {}",
                    window[0].lsn().saturating_add(1),
                    window[0].lsn()
                )));
            }
        }

        // The contention budget applies to one conditional head update, not
        // to the number of immutable segments in a history. A large stream
        // may legitimately need more than `HEAD_UPDATE_ATTEMPTS` batches;
        // reset the budget after each successful publication.
        let mut contention_attempts = 0_usize;
        loop {
            let loaded = self.load_head(stream).await?;
            let expected = loaded.head.archived_lsn.saturating_add(1);
            let remaining = records
                .iter()
                .filter(|record| record.lsn() >= expected)
                .cloned()
                .collect::<Vec<_>>();
            if remaining.is_empty() {
                return Ok(loaded.head.archived_lsn);
            }
            if remaining[0].lsn() != expected
                || remaining[0].committed_lsn() != expected.saturating_sub(1)
                || remaining[0].writer_epoch() < loaded.head.writer_epoch
            {
                return Err(ArchiveError::Contiguity(format!(
                    "expected LSN {expected}, received {}",
                    remaining[0].lsn()
                )));
            }

            let batch = &remaining[..remaining.len().min(self.max_records_per_segment)];
            let segment_ref = self.put_segment(stream, batch).await?;
            let mut next = loaded.head;
            next.archived_lsn = segment_ref.last_lsn;
            next.writer_epoch = batch
                .last()
                .map_or(next.writer_epoch, EncryptedRecord::writer_epoch);
            next.segments.push(segment_ref);
            if next.segments.len() > HEAD_SEGMENTS_MAX {
                self.fold(&mut next).await?;
            }
            match self.put_head(&next, loaded.update).await {
                Ok(()) => {
                    contention_attempts = 0;
                    if next.archived_lsn == records.last().map_or(0, EncryptedRecord::lsn) {
                        return Ok(next.archived_lsn);
                    }
                }
                Err(ArchiveError::ObjectStore(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                )) => {
                    contention_attempts = contention_attempts.saturating_add(1);
                    if contention_attempts >= HEAD_UPDATE_ATTEMPTS {
                        return Err(ArchiveError::Contended);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Returns the highest contiguous record named by the authoritative head.
    /// Every stream with an archived prefix: `(stream, archived_lsn,
    /// writer_epoch)`. A member booting on an empty volume uses this to
    /// learn the prefixes it must treat as compacted before it accepts an
    /// append at `archived_lsn + 1`.
    pub async fn stream_heads(&self) -> Result<Vec<(String, u64, u64)>, ArchiveError> {
        use futures::TryStreamExt;
        let prefix = Path::from(format!("{}/streams", self.prefix));
        let mut listing = self.store.list(Some(&prefix));
        let mut heads = Vec::new();
        while let Some(object) = listing.try_next().await? {
            if object.location.filename() != Some("head.json") {
                continue;
            }
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let head: ArchiveHead = serde_json::from_slice(&bytes)?;
            if head.archived_lsn == 0 {
                continue;
            }
            self.validate_head_chain(&head.stream, &head)?;
            heads.push((head.stream, head.archived_lsn, head.writer_epoch));
        }
        heads.sort();
        Ok(heads)
    }

    pub async fn archived_lsn(&self, stream: &str) -> Result<u64, ArchiveError> {
        let head = self.load_head(stream).await?.head;
        self.validate_head_chain(stream, &head)?;
        Ok(head.archived_lsn)
    }

    /// Verifies one finite archive range without downloading unrelated
    /// history. The head references are checked in full first, so a forged
    /// watermark or a gap before the requested range cannot make an archive
    /// proof appear complete. Only segment payloads that overlap the range
    /// are fetched and checked against their content digests.
    pub(crate) async fn verify_range(
        &self,
        stream: &str,
        start_lsn: u64,
        end_lsn: u64,
    ) -> Result<(), ArchiveError> {
        if stream.is_empty() || start_lsn == 0 || start_lsn > end_lsn {
            return Err(ArchiveError::Contiguity(
                "archive verification range must be non-empty and start at LSN 1 or later"
                    .to_owned(),
            ));
        }
        let head = self.load_head(stream).await?.head;
        self.validate_head_chain(stream, &head)?;
        if end_lsn > head.archived_lsn {
            return Err(ArchiveError::Contiguity(format!(
                "archive watermark {} trails requested range through LSN {end_lsn}",
                head.archived_lsn
            )));
        }

        let mut expected_lsn = start_lsn;
        let mut range_complete = false;
        let mut previous_writer_epoch = None;
        let references = self
            .references_after(stream, &head, start_lsn.saturating_sub(1))
            .await?;
        let wanted: Vec<&SegmentRef> = references
            .iter()
            .filter(|reference| reference.first_lsn <= end_lsn)
            .collect();
        for segment in self.load_segments(stream, &wanted).await? {
            for record in &segment.records {
                if record.lsn() < start_lsn {
                    continue;
                }
                if record.lsn() > end_lsn {
                    break;
                }
                if record.lsn() != expected_lsn {
                    return Err(ArchiveError::Contiguity(format!(
                        "expected LSN {expected_lsn} in requested archive range, received {}",
                        record.lsn()
                    )));
                }
                if previous_writer_epoch.is_some_and(|epoch| record.writer_epoch() < epoch) {
                    return Err(ArchiveError::Contiguity(
                        "writer epoch moved backward in requested archive range".to_owned(),
                    ));
                }
                previous_writer_epoch = Some(record.writer_epoch());
                if record.lsn() == end_lsn {
                    range_complete = true;
                    break;
                }
                expected_lsn = record.lsn().saturating_add(1);
            }
            if range_complete {
                break;
            }
        }
        if !range_complete {
            return Err(ArchiveError::Contiguity(format!(
                "archive range {}-{} is not fully verified",
                start_lsn, end_lsn
            )));
        }
        if end_lsn == head.archived_lsn && previous_writer_epoch != Some(head.writer_epoch) {
            return Err(ArchiveError::Contiguity(
                "archive head writer epoch does not match its verified tail".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns the published archive watermark and the tail's writer epoch.
    pub(crate) async fn archived_tail(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Option<(u64, u64)>, ArchiveError> {
        let head = self.load_head(stream).await?.head;
        if head.archived_lsn < after_lsn {
            return Err(ArchiveError::Contiguity(format!(
                "archive watermark {} trails compacted LSN {after_lsn}",
                head.archived_lsn
            )));
        }
        if head.archived_lsn == after_lsn {
            return Ok(None);
        }
        self.validate_head_chain(stream, &head)?;
        // A released prefix is proven by the checkpoint that released it,
        // not by segments, which are gone: verify from there.
        let after_lsn = after_lsn.max(head.released_lsn);
        if after_lsn == head.archived_lsn {
            return Ok(Some((head.archived_lsn, head.writer_epoch)));
        }
        let references = self.references_after(stream, &head, after_lsn).await?;
        let wanted: Vec<&SegmentRef> = references.iter().collect();
        let mut verified_epoch = 0_u64;
        for segment in self.load_segments(stream, &wanted).await? {
            if segment
                .records
                .first()
                .is_some_and(|record| record.writer_epoch() < verified_epoch)
            {
                return Err(ArchiveError::Contiguity(
                    "writer epoch moved backward between archive segments".to_owned(),
                ));
            }
            verified_epoch = segment
                .records
                .last()
                .map_or(verified_epoch, EncryptedRecord::writer_epoch);
        }
        if verified_epoch != head.writer_epoch {
            return Err(ArchiveError::Contiguity(
                "archive head does not match its verified tail".to_owned(),
            ));
        }
        Ok(Some((head.archived_lsn, head.writer_epoch)))
    }

    /// Reads and verifies archived records strictly after one caller watermark.
    pub async fn recover(
        &self,
        stream: &str,
        after_lsn: u64,
    ) -> Result<Vec<EncryptedRecord>, ArchiveError> {
        let head = self.load_head(stream).await?.head;
        self.validate_head_chain(stream, &head)?;
        // A segment wholly below the requested tail contributes no record.
        // Its place in the chain is proven by the references; fetching and
        // hashing it would make every open of a long-lived stream walk its
        // entire history for nothing.
        let references = self.references_after(stream, &head, after_lsn).await?;
        let wanted: Vec<&SegmentRef> = references.iter().collect();
        let mut tail_writer_epoch = 0_u64;
        let mut recovered = Vec::new();
        for segment in self.load_segments(stream, &wanted).await? {
            if segment
                .records
                .first()
                .is_some_and(|record| record.writer_epoch() < tail_writer_epoch)
            {
                return Err(ArchiveError::Contiguity(
                    "writer epoch moved backward between archive segments".to_owned(),
                ));
            }
            tail_writer_epoch = segment
                .records
                .last()
                .map_or(tail_writer_epoch, EncryptedRecord::writer_epoch);
            recovered.extend(
                segment
                    .records
                    .into_iter()
                    .filter(|record| record.lsn() > after_lsn),
            );
        }
        // The epoch chain is checked across the segments that were read. When
        // the requested tail lies at or beyond the archived head, none were,
        // and the head's own epoch is the only evidence there is.
        if after_lsn < head.archived_lsn && head.writer_epoch != tail_writer_epoch {
            return Err(ArchiveError::Contiguity(
                "head writer epoch does not match its archived tail".to_owned(),
            ));
        }
        Ok(recovered)
    }

    /// Every segment reference with records after `after_lsn`, in order:
    /// the head's own, and as many index pages back as the range reaches.
    /// Each page is checked against its digest, and the chain must run
    /// contiguously from the first reference returned to the archived tail.
    async fn references_after(
        &self,
        stream: &str,
        head: &ArchiveHead,
        after_lsn: u64,
    ) -> Result<Vec<SegmentRef>, ArchiveError> {
        if after_lsn < head.released_lsn {
            return Err(ArchiveError::Released {
                after_lsn,
                released_lsn: head.released_lsn,
            });
        }
        let (mut references, _) = self.walk(stream, head, after_lsn).await?;
        references.retain(|reference| reference.last_lsn > after_lsn);
        Ok(references)
    }

    /// The segment references from the one holding `after_lsn + 1` to the
    /// archived tail, checked contiguous, and the index pages read to reach
    /// them.
    async fn walk(
        &self,
        stream: &str,
        head: &ArchiveHead,
        after_lsn: u64,
    ) -> Result<(Vec<SegmentRef>, Vec<PageRef>), ArchiveError> {
        if after_lsn >= head.archived_lsn {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut references = head.segments.clone();
        let mut pages = Vec::new();
        let mut next_page = head.page.clone();
        while references
            .first()
            .is_none_or(|first| first.first_lsn > after_lsn.saturating_add(1))
        {
            let Some(reference) = next_page else {
                break;
            };
            let page = self.read_page(stream, &reference).await?;
            if page.segments.last().map(|last| last.last_lsn)
                != references
                    .first()
                    .map(|first| first.first_lsn.saturating_sub(1))
                    .or(Some(reference.last_lsn))
            {
                return Err(ArchiveError::Contiguity(format!(
                    "index page {} does not meet the references after it",
                    reference.key
                )));
            }
            next_page = page.previous;
            pages.push(reference);
            let mut older = page.segments;
            older.append(&mut references);
            references = older;
        }
        let mut expected = references.first().map_or(1, |first| first.first_lsn);
        if expected > after_lsn.saturating_add(1) {
            return Err(ArchiveError::Contiguity(format!(
                "archive references start at LSN {expected}, after {after_lsn} was requested"
            )));
        }
        for reference in &references {
            if reference.first_lsn != expected || reference.last_lsn < reference.first_lsn {
                return Err(ArchiveError::Contiguity(format!(
                    "expected segment at LSN {expected}, received {}",
                    reference.first_lsn
                )));
            }
            expected = reference.last_lsn.saturating_add(1);
        }
        if expected.saturating_sub(1) != head.archived_lsn {
            return Err(ArchiveError::Contiguity(format!(
                "head {} does not match segment tail {}",
                head.archived_lsn,
                expected.saturating_sub(1)
            )));
        }
        Ok((references, pages))
    }

    /// Where the stream's archive begins and ends: `(released_lsn,
    /// archived_lsn)`.
    pub async fn extent(&self, stream: &str) -> Result<(u64, u64), ArchiveError> {
        let head = self.load_head(stream).await?.head;
        self.validate_head_chain(stream, &head)?;
        Ok((head.released_lsn, head.archived_lsn))
    }

    /// Lets go of the stream's archive through `through_lsn`, as far as it is
    /// archived: the head records the release first, so no read that loads
    /// it afterwards asks for what follows, and then every segment wholly
    /// inside the released prefix is deleted, with every index page that
    /// names only such segments. A read that loaded the head before the
    /// release and wanted part of that prefix fails and is retried; nothing
    /// that reads from the table's checkpoint onwards ever wanted it.
    /// Returns the released watermark.
    pub async fn release(&self, stream: &str, through_lsn: u64) -> Result<u64, ArchiveError> {
        let mut contention_attempts = 0_usize;
        loop {
            let loaded = self.load_head(stream).await?;
            let head = loaded.head;
            self.validate_head_chain(stream, &head)?;
            let target = through_lsn.min(head.archived_lsn);
            if target <= head.released_lsn {
                return Ok(head.released_lsn);
            }
            let (references, pages) = self.walk(stream, &head, head.released_lsn).await?;
            // The head names nothing released: those references, and the
            // newest page once all it names is released, go with the objects.
            let mut next = head.clone();
            next.released_lsn = target;
            next.segments
                .retain(|reference| reference.last_lsn > target);
            if next
                .page
                .as_ref()
                .is_some_and(|page| page.last_lsn <= target)
            {
                next.page = None;
            }
            match self.put_head(&next, loaded.update).await {
                Ok(()) => {}
                Err(ArchiveError::ObjectStore(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                )) => {
                    contention_attempts = contention_attempts.saturating_add(1);
                    if contention_attempts >= HEAD_UPDATE_ATTEMPTS {
                        return Err(ArchiveError::Contended);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
            let doomed = references
                .iter()
                .filter(|reference| reference.last_lsn <= target)
                .map(|reference| reference.key.clone())
                .chain(
                    pages
                        .iter()
                        .filter(|page| page.last_lsn <= target)
                        .map(|page| page.key.clone()),
                );
            for key in doomed {
                match self.store.delete(&Path::from(key.as_str())).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        eprintln!(
                            "lakeday.replica archive release could not delete {key}: {error}"
                        );
                    }
                }
            }
            return Ok(target);
        }
    }

    /// Moves all but the newest references out of `head` into a new index
    /// page, chained to the page before it.
    async fn fold(&self, head: &mut ArchiveHead) -> Result<(), ArchiveError> {
        let kept = head
            .segments
            .split_off(head.segments.len() - HEAD_SEGMENTS_KEPT);
        let folded = std::mem::replace(&mut head.segments, kept);
        let (Some(first), Some(last)) = (folded.first(), folded.last()) else {
            return Ok(());
        };
        let (first_lsn, last_lsn) = (first.first_lsn, last.last_lsn);
        let page = IndexPage {
            stream: head.stream.clone(),
            previous: head.page.take(),
            segments: folded,
        };
        let bytes = serde_json::to_vec(&page)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        let key = format!(
            "{}/streams/{}/index/{first_lsn:020}-{last_lsn:020}-{digest}.json",
            self.prefix,
            stream_digest(&head.stream)
        );
        self.put_immutable(&key, bytes).await?;
        head.page = Some(PageRef {
            first_lsn,
            last_lsn,
            key,
            digest,
        });
        Ok(())
    }

    /// Reads one index page and checks it against the reference to it.
    async fn read_page(
        &self,
        stream: &str,
        reference: &PageRef,
    ) -> Result<IndexPage, ArchiveError> {
        let path = Path::from(reference.key.as_str());
        let bytes = self.store.get(&path).await?.bytes().await?;
        if hex::encode(Sha256::digest(&bytes)) != reference.digest {
            return Err(ArchiveError::Checksum(reference.key.clone()));
        }
        let page: IndexPage = serde_json::from_slice(&bytes)?;
        if page.stream != stream
            || page.segments.first().map(|first| first.first_lsn) != Some(reference.first_lsn)
            || page.segments.last().map(|last| last.last_lsn) != Some(reference.last_lsn)
            || page
                .previous
                .as_ref()
                .is_some_and(|previous| previous.last_lsn.saturating_add(1) != reference.first_lsn)
        {
            return Err(ArchiveError::Contiguity(format!(
                "index page {} does not match the reference to it",
                reference.key
            )));
        }
        Ok(page)
    }

    /// Reads the segments `references` name, several at a time, each checked
    /// against its digest and its head entry, and returns them in order.
    async fn load_segments(
        &self,
        stream: &str,
        references: &[&SegmentRef],
    ) -> Result<Vec<Segment>, ArchiveError> {
        let mut segments = Vec::with_capacity(references.len());
        for chunk in references.chunks(SEGMENT_READS_IN_FLIGHT) {
            let reads: Vec<_> = chunk
                .iter()
                .map(|reference| {
                    tokio::spawn(read_segment(Arc::clone(&self.store), (*reference).clone()))
                })
                .collect();
            for read in futures::future::join_all(reads).await {
                segments.push(read.map_err(|source| object_store::Error::JoinError { source })??);
            }
        }
        for (segment, reference) in segments.iter().zip(references) {
            self.verify_segment(segment, reference, stream)?;
        }
        Ok(segments)
    }

    /// Writes one deterministic content-addressed segment, accepting exact retries.
    async fn put_segment(
        &self,
        stream: &str,
        records: &[EncryptedRecord],
    ) -> Result<SegmentRef, ArchiveError> {
        let first_lsn = records[0].lsn();
        let last_lsn = records.last().map_or(first_lsn, EncryptedRecord::lsn);
        let segment = Segment {
            stream: stream.to_owned(),
            first_lsn,
            last_lsn,
            records: records.to_vec(),
        };
        let bytes = serde_json::to_vec(&segment)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        let key = format!(
            "{}/streams/{}/segments/{first_lsn:020}-{last_lsn:020}-{digest}.json",
            self.prefix,
            stream_digest(stream)
        );
        self.put_immutable(&key, bytes).await?;
        Ok(SegmentRef {
            first_lsn,
            last_lsn,
            key,
            digest,
        })
    }

    /// Writes one content-addressed object, accepting an exact retry.
    async fn put_immutable(&self, key: &str, bytes: Vec<u8>) -> Result<(), ArchiveError> {
        let path = Path::from(key);
        let result = self
            .store
            .put_opts(
                &path,
                Bytes::from(bytes.clone()).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await;
        if let Err(object_store::Error::AlreadyExists { .. }) = result {
            let existing = self.store.get(&path).await?.bytes().await?;
            if existing.as_ref() != bytes.as_slice() {
                return Err(ArchiveError::Checksum(key.to_owned()));
            }
        } else {
            result?;
        }
        Ok(())
    }

    /// Loads one head with its conditional-update identity or an empty stream head.
    async fn load_head(&self, stream: &str) -> Result<LoadedHead, ArchiveError> {
        let path = self.head_path(stream);
        match self.store.get(&path).await {
            Ok(result) => {
                let update = Some(UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                });
                let bytes = result.bytes().await?;
                let head: ArchiveHead = serde_json::from_slice(&bytes)?;
                if head.stream != stream {
                    return Err(ArchiveError::Checksum(path.to_string()));
                }
                Ok(LoadedHead { head, update })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(LoadedHead {
                head: ArchiveHead {
                    stream: stream.to_owned(),
                    archived_lsn: 0,
                    writer_epoch: 0,
                    released_lsn: 0,
                    page: None,
                    segments: Vec::new(),
                },
                update: None,
            }),
            Err(error) => Err(error.into()),
        }
    }

    /// Advances one stream head with create-or-exact-update semantics.
    async fn put_head(
        &self,
        head: &ArchiveHead,
        update: Option<UpdateVersion>,
    ) -> Result<(), ArchiveError> {
        let mode = update.map_or(PutMode::Create, PutMode::Update);
        self.store
            .put_opts(
                &self.head_path(&head.stream),
                Bytes::from(serde_json::to_vec(head)?).into(),
                PutOptions {
                    mode,
                    ..PutOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Verifies that one immutable object matches the head entry that names it.
    fn verify_segment(
        &self,
        segment: &Segment,
        reference: &SegmentRef,
        stream: &str,
    ) -> Result<(), ArchiveError> {
        if segment.stream != stream
            || segment.first_lsn != reference.first_lsn
            || segment.last_lsn != reference.last_lsn
            || segment.records.first().map(EncryptedRecord::lsn) != Some(reference.first_lsn)
            || segment.records.last().map(EncryptedRecord::lsn) != Some(reference.last_lsn)
        {
            return Err(ArchiveError::Contiguity(format!(
                "segment {} metadata does not match its head entry",
                reference.key
            )));
        }
        let mut expected = reference.first_lsn;
        let mut writer_epoch = 0_u64;
        for record in &segment.records {
            if record.stream() != stream
                || record.lsn() != expected
                || record.committed_lsn() != expected.saturating_sub(1)
                || record.writer_epoch() < writer_epoch
            {
                return Err(ArchiveError::Contiguity(format!(
                    "expected LSN {expected} in {}",
                    reference.key
                )));
            }
            writer_epoch = record.writer_epoch();
            expected = expected.saturating_add(1);
        }
        Ok(())
    }

    /// Checks the complete in-head reference chain without fetching segment
    /// payloads. This is intentionally the cheap path used by every
    /// watermark lookup; callers that need data safety for a finite range
    /// must additionally call [`Self::verify_range`].
    fn validate_head_chain(&self, stream: &str, head: &ArchiveHead) -> Result<(), ArchiveError> {
        if head.stream != stream {
            return Err(ArchiveError::Checksum(self.head_path(stream).to_string()));
        }
        let segment_prefix = format!(
            "{}/streams/{}/segments/",
            self.prefix,
            stream_digest(stream)
        );
        let mut expected = 1_u64;
        if let Some(page) = &head.page {
            let index_prefix = format!("{}/streams/{}/index/", self.prefix, stream_digest(stream));
            if !page.key.starts_with(&index_prefix)
                || page.digest.len() != 64
                || !page.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                || page.first_lsn == 0
                || page.last_lsn < page.first_lsn
            {
                return Err(ArchiveError::Checksum(page.key.clone()));
            }
            expected = page.last_lsn.checked_add(1).ok_or_else(|| {
                ArchiveError::Contiguity("archive LSN range exhausted".to_owned())
            })?;
        } else if head.released_lsn > 0 {
            // Everything before the released prefix is gone from the head:
            // the references start inside it, or right after it, or there
            // are none and the archive ends where the release did.
            expected = match head.segments.first() {
                Some(first) if first.first_lsn <= head.released_lsn.saturating_add(1) => {
                    first.first_lsn
                }
                Some(first) => {
                    return Err(ArchiveError::Contiguity(format!(
                        "released through {}, references start at {}",
                        head.released_lsn, first.first_lsn
                    )));
                }
                None => head.released_lsn.saturating_add(1),
            };
        }
        for reference in &head.segments {
            if reference.first_lsn != expected || reference.last_lsn < reference.first_lsn {
                return Err(ArchiveError::Contiguity(format!(
                    "expected segment at LSN {expected}, received {}",
                    reference.first_lsn
                )));
            }
            if !reference.key.starts_with(&segment_prefix)
                || reference.digest.len() != 64
                || !reference
                    .digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(ArchiveError::Checksum(reference.key.clone()));
            }
            expected = reference.last_lsn.checked_add(1).ok_or_else(|| {
                ArchiveError::Contiguity("archive LSN range exhausted".to_owned())
            })?;
        }
        let archived_lsn = expected.saturating_sub(1);
        if head.archived_lsn != archived_lsn {
            return Err(ArchiveError::Contiguity(format!(
                "head watermark {} does not match segment tail {archived_lsn}",
                head.archived_lsn
            )));
        }
        if head.released_lsn > head.archived_lsn {
            return Err(ArchiveError::Contiguity(format!(
                "head released {} past its archived tail {}",
                head.released_lsn, head.archived_lsn
            )));
        }
        if head.archived_lsn == 0 && head.writer_epoch != 0 {
            return Err(ArchiveError::Contiguity(
                "empty archive head has a nonzero writer epoch".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns the metadata path for one stream without exposing its identity in the key.
    fn head_path(&self, stream: &str) -> Path {
        Path::from(format!(
            "{}/streams/{}/head.json",
            self.prefix,
            stream_digest(stream)
        ))
    }
}

/// Reads one segment and checks it against the digest its head entry names.
async fn read_segment(
    store: Arc<dyn ObjectStore>,
    reference: SegmentRef,
) -> Result<Segment, ArchiveError> {
    let path = Path::from(reference.key.as_str());
    let bytes = store.get(&path).await?.bytes().await?;
    if hex::encode(Sha256::digest(&bytes)) != reference.digest {
        return Err(ArchiveError::Checksum(reference.key));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn stream_digest(stream: &str) -> String {
    hex::encode(Sha256::digest(stream.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use serde_json::json;

    fn opaque(stream: &str, lsn: u64) -> EncryptedRecord {
        serde_json::from_value(json!({
            "stream": stream,
            "writer_epoch": 7,
            "lsn": lsn,
            "committed_lsn": lsn - 1,
            "nonce": vec![lsn as u8; 24],
            "ciphertext": vec![lsn as u8; 37],
            "authentication": vec![lsn as u8; 32],
        }))
        .expect("opaque test envelope")
    }

    #[tokio::test]
    async fn archived_lsn_rejects_a_malformed_head_claim() -> Result<(), ArchiveError> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let archive = OpaqueArchive::new(Arc::clone(&store), "replica", 16)?;
        let stream = "tenant-a/catalog";
        let head = ArchiveHead {
            stream: stream.to_owned(),
            archived_lsn: 9,
            writer_epoch: 7,
            released_lsn: 0,
            page: None,
            segments: Vec::new(),
        };
        store
            .put(
                &archive.head_path(stream),
                serde_json::to_vec(&head)?.into(),
            )
            .await?;

        assert!(matches!(
            archive.archived_lsn(stream).await,
            Err(ArchiveError::Contiguity(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn verify_range_rejects_a_missing_referenced_segment() -> Result<(), ArchiveError> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let archive = OpaqueArchive::new(Arc::clone(&store), "replica", 16)?;
        let stream = "tenant-a/catalog";
        let key = format!(
            "{}/streams/{}/segments/{:020}-{:020}-{}.json",
            archive.prefix,
            stream_digest(stream),
            1,
            3,
            "0".repeat(64),
        );
        let head = ArchiveHead {
            stream: stream.to_owned(),
            archived_lsn: 3,
            writer_epoch: 7,
            released_lsn: 0,
            page: None,
            segments: vec![SegmentRef {
                first_lsn: 1,
                last_lsn: 3,
                key,
                digest: "0".repeat(64),
            }],
        };
        store
            .put(
                &archive.head_path(stream),
                serde_json::to_vec(&head)?.into(),
            )
            .await?;

        assert!(matches!(
            archive.verify_range(stream, 1, 3).await,
            Err(ArchiveError::ObjectStore(
                object_store::Error::NotFound { .. }
            ))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn verify_range_rejects_a_corrupt_referenced_segment() -> Result<(), ArchiveError> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let archive = OpaqueArchive::new(Arc::clone(&store), "replica", 16)?;
        let stream = "tenant-a/catalog";
        archive
            .archive_committed(&(1..=3).map(|lsn| opaque(stream, lsn)).collect::<Vec<_>>())
            .await?;

        let segment = store
            .list(Some(&Path::from("replica")))
            .filter_map(|entry| async move { entry.ok() })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .find(|entry| !entry.location.as_ref().ends_with("head.json"))
            .expect("archived segment");
        let original = store.get(&segment.location).await?.bytes().await?;
        let mut corrupted = original.to_vec();
        corrupted[original.len() / 2] ^= 0x55;
        store.put(&segment.location, corrupted.into()).await?;

        assert!(matches!(
            archive.verify_range(stream, 1, 3).await,
            Err(ArchiveError::Checksum(_))
        ));
        Ok(())
    }
}
