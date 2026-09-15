//! Arrow IPC encoding, immutable owner validation, and fencing sentinels.
use crate::{WalBackendError, WalResult};
use arrow_array::RecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use std::{io::Cursor, sync::Arc};
pub const OWNER_DO_ID_KEY: &str = "owner_do_id";
pub const FENCE_SENTINEL_KEY: &str = "fence_sentinel";
pub const WRITER_EPOCH_KEY: &str = "writer_epoch";
/// Adds immutable owner metadata to an Arrow batch before it enters Lance.
///
/// Existing owner metadata is never overwritten.  A caller attempting to
/// mutate a batch already attributed to another DO receives an error before
/// it can consume a WAL position.
pub fn with_owner(batch: RecordBatch, owner_do_id: &str) -> WalResult<RecordBatch> {
    if owner_do_id.trim().is_empty() {
        return Err(WalBackendError::InvalidIdentity {
            field: OWNER_DO_ID_KEY.to_owned(),
            reason: "owner identity must not be empty".to_owned(),
        });
    }
    let mut metadata = batch.schema().metadata().clone();
    if let Some(existing) = metadata.get(OWNER_DO_ID_KEY)
        && existing != owner_do_id
    {
        return Err(WalBackendError::OwnerMismatch {
            expected: owner_do_id.to_owned(),
            received: existing.clone(),
        });
    }
    metadata.insert(OWNER_DO_ID_KEY.to_owned(), owner_do_id.to_owned());
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        batch.schema().fields().to_vec(),
        metadata,
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))
}

/// A decoded Lance WAL entry.  One entry contains one or more Arrow batches;
/// the entry itself consumes exactly one Bitr LSN.
#[derive(Debug)]
pub struct IpcEntry {
    pub(crate) writer_epoch: u64,
    pub(crate) fence_sentinel: bool,
    pub(crate) batches: Vec<RecordBatch>,
}

impl IpcEntry {
    /// Returns the epoch embedded by Lance in the IPC schema metadata.
    #[must_use]
    pub fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns whether this entry is the data-less predecessor fence marker.
    #[must_use]
    pub fn is_fence_sentinel(&self) -> bool {
        self.fence_sentinel
    }

    /// Returns all Arrow batches carried by this one WAL entry.
    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

/// Encodes multiple Arrow batches as one Lance-compatible IPC WAL entry.
pub fn encode_ipc_batches(writer_epoch: u64, batches: &[RecordBatch]) -> WalResult<Bytes> {
    if writer_epoch == 0 {
        return Err(WalBackendError::InvalidIdentity {
            field: WRITER_EPOCH_KEY.to_owned(),
            reason: "writer epoch must be positive".to_owned(),
        });
    }
    let schema = batches
        .first()
        .ok_or_else(|| WalBackendError::CorruptIpc("WAL entry has no batches".to_owned()))?
        .schema();
    validate_batch_shapes(batches, schema.as_ref())?;
    let mut metadata = schema.metadata().clone();
    metadata.insert(WRITER_EPOCH_KEY.to_owned(), writer_epoch.to_string());
    metadata.remove(FENCE_SENTINEL_KEY);
    let ipc_schema = Arc::new(ArrowSchema::new_with_metadata(
        schema.fields().to_vec(),
        metadata,
    ));
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &ipc_schema)
            .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
        }
        writer
            .finish()
            .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
    }
    Ok(Bytes::from(buffer))
}

/// Encodes Lance's empty-schema predecessor fence marker.
pub fn encode_fence_sentinel(writer_epoch: u64) -> WalResult<Bytes> {
    if writer_epoch == 0 {
        return Err(WalBackendError::InvalidIdentity {
            field: WRITER_EPOCH_KEY.to_owned(),
            reason: "writer epoch must be positive".to_owned(),
        });
    }
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(WRITER_EPOCH_KEY.to_owned(), writer_epoch.to_string());
    metadata.insert(FENCE_SENTINEL_KEY.to_owned(), "true".to_owned());
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        arrow_schema::Fields::empty(),
        metadata,
    ));
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
    }
    Ok(Bytes::from(buffer))
}

/// Decodes and strictly validates one Lance Arrow IPC WAL entry.
pub fn decode_ipc_entry(bytes: &[u8]) -> WalResult<IpcEntry> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
    let schema = reader.schema();
    let writer_epoch = schema
        .metadata()
        .get(WRITER_EPOCH_KEY)
        .ok_or_else(|| WalBackendError::CorruptIpc("missing writer epoch metadata".to_owned()))?
        .parse::<u64>()
        .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
    if writer_epoch == 0 {
        return Err(WalBackendError::CorruptIpc(
            "writer epoch metadata must be positive".to_owned(),
        ));
    }
    let fence_sentinel = schema
        .metadata()
        .get(FENCE_SENTINEL_KEY)
        .is_some_and(|value| value == "true");
    let mut clean_metadata = schema.metadata().clone();
    clean_metadata.remove(WRITER_EPOCH_KEY);
    clean_metadata.remove(FENCE_SENTINEL_KEY);
    let logical_schema = Arc::new(ArrowSchema::new_with_metadata(
        schema.fields().to_vec(),
        clean_metadata,
    ));
    let mut batches = Vec::new();
    for batch in &mut reader {
        let batch = batch.map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
        let clean = RecordBatch::try_new(logical_schema.clone(), batch.columns().to_vec())
            .map_err(|error| WalBackendError::CorruptIpc(error.to_string()))?;
        if clean.num_rows() == 0 {
            return Err(WalBackendError::CorruptIpc(
                "WAL entry contains an empty batch".to_owned(),
            ));
        }
        batches.push(clean);
    }
    if fence_sentinel {
        if !logical_schema.fields().is_empty() || !batches.is_empty() {
            return Err(WalBackendError::CorruptIpc(
                "fence sentinel must have an empty schema and no batches".to_owned(),
            ));
        }
    } else if batches.is_empty() {
        return Err(WalBackendError::CorruptIpc(
            "data WAL entry must contain at least one batch".to_owned(),
        ));
    }
    Ok(IpcEntry {
        writer_epoch,
        fence_sentinel,
        batches,
    })
}

/// Checks every batch in one IPC entry carries this backend's owner identity.
pub(crate) fn validate_owner(batches: &[RecordBatch], expected: &str) -> WalResult<()> {
    for batch in batches {
        let schema = batch.schema();
        let received = schema
            .metadata()
            .get(OWNER_DO_ID_KEY)
            .ok_or(WalBackendError::OwnerMissing)?;
        if received != expected {
            return Err(WalBackendError::OwnerMismatch {
                expected: expected.to_owned(),
                received: received.clone(),
            });
        }
    }
    Ok(())
}

/// Validates non-empty, shape-compatible Arrow batches before IPC encoding.
fn validate_batch_shapes(batches: &[RecordBatch], schema: &ArrowSchema) -> WalResult<()> {
    for (index, batch) in batches.iter().enumerate() {
        if batch.num_rows() == 0 {
            return Err(WalBackendError::CorruptIpc(format!(
                "batch {index} has no rows"
            )));
        }
        if batch.schema().fields() != schema.fields() {
            return Err(WalBackendError::CorruptIpc(format!(
                "batch {index} has a different Arrow schema"
            )));
        }
        if batch.schema().metadata().get(OWNER_DO_ID_KEY) != schema.metadata().get(OWNER_DO_ID_KEY)
        {
            return Err(WalBackendError::OwnerMissing);
        }
    }
    Ok(())
}
