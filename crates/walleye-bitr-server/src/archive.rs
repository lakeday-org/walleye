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
    segments: Vec<SegmentRef>,
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
        for reference in &head.segments {
            if reference.last_lsn < start_lsn || reference.first_lsn > end_lsn {
                continue;
            }
            let path = Path::from(reference.key.as_str());
            let bytes = self.store.get(&path).await?.bytes().await?;
            let digest = hex::encode(Sha256::digest(&bytes));
            if digest != reference.digest {
                return Err(ArchiveError::Checksum(reference.key.clone()));
            }
            let segment: Segment = serde_json::from_slice(&bytes)?;
            self.verify_segment(&segment, reference, stream)?;

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
        let mut expected = 1_u64;
        let mut verified_epoch = 0_u64;
        for reference in &head.segments {
            if reference.first_lsn != expected || reference.last_lsn < reference.first_lsn {
                return Err(ArchiveError::Contiguity(format!(
                    "expected segment at LSN {expected}, received {}",
                    reference.first_lsn
                )));
            }
            if reference.last_lsn > after_lsn {
                let path = Path::from(reference.key.as_str());
                let bytes = self.store.get(&path).await?.bytes().await?;
                let digest = hex::encode(Sha256::digest(&bytes));
                if digest != reference.digest {
                    return Err(ArchiveError::Checksum(reference.key.clone()));
                }
                let segment: Segment = serde_json::from_slice(&bytes)?;
                self.verify_segment(&segment, reference, stream)?;
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
            expected = reference.last_lsn.saturating_add(1);
        }
        if head.archived_lsn != expected.saturating_sub(1) || verified_epoch != head.writer_epoch {
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
        let mut expected = 1_u64;
        let mut tail_writer_epoch = 0_u64;
        let mut recovered = Vec::new();
        for reference in &head.segments {
            if reference.first_lsn != expected || reference.last_lsn < reference.first_lsn {
                return Err(ArchiveError::Contiguity(format!(
                    "expected segment at LSN {expected}, received {}",
                    reference.first_lsn
                )));
            }
            // A segment wholly below the requested tail contributes no
            // record. Its place in the chain is proven by the head's
            // references; fetching and hashing it would make every open of
            // a long-lived stream walk its entire history, one object at a
            // time, for nothing.
            if reference.last_lsn <= after_lsn {
                expected = reference.last_lsn.saturating_add(1);
                continue;
            }
            let path = Path::from(reference.key.as_str());
            let bytes = self.store.get(&path).await?.bytes().await?;
            let digest = hex::encode(Sha256::digest(&bytes));
            if digest != reference.digest {
                return Err(ArchiveError::Checksum(reference.key.clone()));
            }
            let segment: Segment = serde_json::from_slice(&bytes)?;
            self.verify_segment(&segment, reference, stream)?;
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
            expected = reference.last_lsn.saturating_add(1);
        }
        if head.archived_lsn != expected.saturating_sub(1) {
            return Err(ArchiveError::Contiguity(format!(
                "head {} does not match segment tail {}",
                head.archived_lsn,
                expected.saturating_sub(1)
            )));
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
        let path = Path::from(key.as_str());
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
                return Err(ArchiveError::Checksum(key));
            }
        } else {
            result?;
        }
        Ok(SegmentRef {
            first_lsn,
            last_lsn,
            key,
            digest,
        })
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
