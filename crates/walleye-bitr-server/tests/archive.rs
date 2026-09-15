//! Acceptance tests for opaque object-store archival and cold recovery.

use std::sync::Arc;

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt as _;
use object_store::memory::InMemory;
use serde_json::json;
use walleye_bitr::EncryptedRecord;
use walleye_bitr_server::OpaqueArchive;

fn opaque(stream: &str, lsn: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": 7,
        "lsn": lsn,
        "committed_lsn": lsn - 1,
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 37],
        "authentication": vec![marker; 32],
    }))
    .expect("opaque test envelope")
}

fn opaque_at_epoch(stream: &str, lsn: u64, epoch: u64) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": epoch,
        "lsn": lsn,
        "committed_lsn": lsn - 1,
        "nonce": vec![lsn as u8; 24],
        "ciphertext": vec![lsn as u8; 37],
        "authentication": vec![lsn as u8; 32],
    }))
    .expect("opaque test envelope")
}

/// Committed records are sealed into bounded immutable batches without
/// exposing their ciphertext as object names or requiring a tenant key.
#[tokio::test]
async fn archives_contiguous_opaque_batches_idempotently() -> Result<(), Box<dyn std::error::Error>>
{
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive = OpaqueArchive::new(Arc::clone(&store), "replica", 2)?;
    let records = vec![
        opaque("tenant-a/catalog", 1, 11),
        opaque("tenant-a/catalog", 2, 12),
        opaque("tenant-a/catalog", 3, 13),
    ];

    assert_eq!(archive.archive_committed(&records).await?, 3);
    assert_eq!(archive.archive_committed(&records).await?, 3);
    assert_eq!(archive.archived_lsn("tenant-a/catalog").await?, 3);
    assert_eq!(archive.recover("tenant-a/catalog", 0).await?, records);

    let objects = store
        .list(Some(&object_store::path::Path::from("replica")))
        .collect::<Vec<_>>()
        .await;
    assert_eq!(objects.iter().filter(|entry| entry.is_ok()).count(), 3);
    Ok(())
}

/// Publication refuses a gap instead of advancing an archive watermark past
/// a record that exists only on the hot replica mesh.
#[tokio::test]
async fn refuses_to_archive_a_noncontiguous_prefix() -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive = OpaqueArchive::new(store, "replica", 16)?;

    let error = archive
        .archive_committed(&[opaque("tenant-a/catalog", 2, 22)])
        .await
        .expect_err("archive gap");
    assert!(error.to_string().contains("expected LSN 1"));
    assert_eq!(archive.archived_lsn("tenant-a/catalog").await?, 0);
    Ok(())
}

/// An archive head also preserves the monotonic writer fence required after
/// the hot copies have been compacted.
#[tokio::test]
async fn refuses_a_writer_epoch_regression() -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive = OpaqueArchive::new(store, "replica", 16)?;
    let records = [
        opaque_at_epoch("tenant-a/catalog", 1, 9),
        opaque_at_epoch("tenant-a/catalog", 2, 8),
    ];

    assert!(archive.archive_committed(&records).await.is_err());
    assert_eq!(archive.archived_lsn("tenant-a/catalog").await?, 0);
    Ok(())
}

/// Cold recovery starts after the caller's exact watermark and returns each
/// archived encrypted record once across batch boundaries.
#[tokio::test]
async fn cold_recovery_resumes_inside_an_archived_batch() -> Result<(), Box<dyn std::error::Error>>
{
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive = OpaqueArchive::new(store, "replica", 2)?;
    let records = vec![
        opaque("tenant-a/catalog", 1, 31),
        opaque("tenant-a/catalog", 2, 32),
        opaque("tenant-a/catalog", 3, 33),
    ];
    archive.archive_committed(&records).await?;

    assert_eq!(archive.recover("tenant-a/catalog", 1).await?, records[1..]);
    Ok(())
}

/// Recovering a tail reads only the segments that hold it. Every open of a
/// long-lived stream asks for the records past its checkpoint; walking the
/// whole history one object at a time made that cost minutes on staging.
#[tokio::test]
async fn tail_recovery_reads_only_the_segments_it_needs() -> Result<(), Box<dyn std::error::Error>>
{
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive = OpaqueArchive::new(Arc::clone(&store), "replica", 2)?;
    let records = vec![
        opaque("tenant-a/catalog", 1, 41),
        opaque("tenant-a/catalog", 2, 42),
        opaque("tenant-a/catalog", 3, 43),
    ];
    archive.archive_committed(&records).await?;
    // Two segments: [1, 2] and [3]. Corrupt the first one in place.
    let mut segments = store
        .list(Some(&object_store::path::Path::from("replica")))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| !entry.location.as_ref().ends_with("head.json"))
        .map(|entry| entry.location)
        .collect::<Vec<_>>();
    segments.sort();
    assert_eq!(segments.len(), 2, "{segments:?}");
    let first = segments[0].clone();
    let original = store.get(&first).await?.bytes().await?;
    let mut corrupted = original.to_vec();
    corrupted[original.len() / 2] ^= 0x55;
    store.put(&first, corrupted.into()).await?;

    // The tail past the first segment never touches it.
    assert_eq!(archive.recover("tenant-a/catalog", 2).await?, records[2..]);
    assert!(archive.recover("tenant-a/catalog", 3).await?.is_empty());
    // A recovery that needs the first segment still detects the damage.
    assert!(matches!(
        archive.recover("tenant-a/catalog", 0).await,
        Err(walleye_bitr_server::ArchiveError::Checksum(_))
    ));
    Ok(())
}
