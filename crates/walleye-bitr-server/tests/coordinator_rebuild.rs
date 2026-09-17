//! Stateless-coordinator rebuild across a writer failover.
//!
//! A coordinator with an empty writer cache reconstructs a stream by merging
//! the immutable archive with the quorum-certified hot candidates. Writer
//! epochs only ever rise along a stream, so the archived prefix is written at
//! an older epoch than the stream's current one. The merge must accept that
//! prefix; rejecting it destroys the reconstruction it has already started and
//! leaves the stream unappendable until the process restarts.
use std::sync::Arc;

use object_store::ObjectStore;
use serde_json::json;
use tokio::net::TcpListener;
use walleye_bitr::{EncryptedRecord, ReplicaError};
use walleye_bitr_server::{DiskReplica, OpaqueArchive, ReplicaGateway, ReplicaNode, node_router};

const INTERNAL_TOKEN: &str = "coordinator-rebuild-internal-token";
const ROOT: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct RunningNode {
    member: ReplicaNode,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

fn record(stream: &str, lsn: u64, epoch: u64) -> EncryptedRecord {
    serde_json::from_value(json!({
        "stream": stream,
        "writer_epoch": epoch,
        "lsn": lsn,
        "committed_lsn": lsn.saturating_sub(1),
        "nonce": vec![lsn as u8; 24],
        "ciphertext": vec![lsn as u8; 64],
        "authentication": vec![lsn as u8; 32],
    }))
    .expect("record")
}

fn records(stream: &str, range: std::ops::RangeInclusive<u64>, epoch: u64) -> Vec<EncryptedRecord> {
    range.map(|lsn| record(stream, lsn, epoch)).collect()
}

fn lsns(records: &[EncryptedRecord]) -> Vec<u64> {
    records.iter().map(EncryptedRecord::lsn).collect()
}

async fn group(directory: &tempfile::TempDir, names: &[&str]) -> TestResult<Vec<RunningNode>> {
    let mut bound = Vec::new();
    for name in names {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        bound.push((
            *name,
            ReplicaNode::new(*name, format!("http://{}", listener.local_addr()?)),
            listener,
        ));
    }
    let members = bound
        .iter()
        .map(|(_, member, _)| member.clone())
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for (name, member, listener) in bound {
        let data = directory.path().join(name);
        let disk = Arc::new(DiskReplica::open_with_control(
            data.join("replica.log"),
            name,
            "hot",
            &data,
            data.join("control.json"),
            &members,
        )?);
        let app = node_router(Arc::clone(&disk), ROOT, Some(INTERNAL_TOKEN))?;
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        nodes.push(RunningNode { member, task });
    }
    Ok(nodes)
}

/// A coordinator over the cohort with its own empty writer cache, exactly
/// like a Flycast peer that has never served this stream.
fn coordinator(
    directory: &tempfile::TempDir,
    nodes: &[RunningNode],
    archive: &Arc<OpaqueArchive>,
    name: &str,
) -> TestResult<ReplicaGateway> {
    Ok(ReplicaGateway::new_direct(
        nodes.iter().map(|node| node.member.clone()).collect(),
        2,
        ROOT,
        INTERNAL_TOKEN,
        directory.path().join(format!("{name}-control.json")),
        Arc::clone(archive),
    )?)
}

/// The archived prefix predates the last writer failover, so its records
/// carry a lower writer epoch than the stream's current one. A fresh
/// coordinator that merges the archive with the hot candidates must rebuild
/// the whole committed prefix and accept the writer's next append. Seeding
/// the merge with the stream's writer epoch instead rejected the first
/// archived record, abandoned the cleared prefix, and wedged the stream at
/// committed LSN 0 with `LsnConflict` on every retry.
#[tokio::test]
async fn rebuild_merges_an_archived_prefix_written_before_a_writer_failover() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let archive = Arc::new(OpaqueArchive::new(Arc::clone(&store), "bitr", 2)?);
    let stream = "walleye/failover";

    // The original writer commits a prefix, fails over, and the successor
    // writer continues the same stream at a higher epoch.
    let nodes = group(&directory, &["a", "b", "c"]).await?;
    let writer = coordinator(&directory, &nodes, &archive, "writer")?;
    let old_epoch = records(stream, 1..=4, 3);
    let new_epoch = records(stream, 5..=6, 5);
    assert!(writer.append_many(old_epoch.clone()).await? >= 2);
    assert!(writer.append_many(new_epoch.clone()).await? >= 2);

    // Only the pre-failover prefix reaches the archive. The hot cohort still
    // holds every record, so nothing has been trimmed and the merge floor is
    // the bottom of the stream.
    assert_eq!(archive.archive_committed(&old_epoch).await?, 4);
    assert_eq!(archive.archived_lsn(stream).await?, 4);
    let hot = writer.recover(stream, 0).await?;
    assert_eq!(lsns(&hot), (1..=6).collect::<Vec<_>>());

    // A stateless coordinator that has never served the stream initializes
    // through committed LSN 6, which spans both writer epochs. Accepting LSN 7
    // is only possible if the merged rebuild recovered the true tail: the
    // append is refused unless the reconstructed committed LSN is exactly 6.
    let stateless = coordinator(&directory, &nodes, &archive, "stateless")?;
    let seventh = record(stream, 7, 5);
    let acknowledgements = stateless
        .append_many(vec![seventh.clone()])
        .await
        .map_err(|error| match error {
            ReplicaError::LsnConflict => {
                "rebuilt prefix was abandoned: the coordinator reported a stale committed LSN"
                    .to_owned()
            }
            other => format!("append after rebuild failed: {other:?}"),
        })?;
    assert!(acknowledgements >= 2);

    // The stream is extendable, not wedged: the tail reads back whole and the
    // next append continues from it.
    let recovered = stateless.recover(stream, 0).await?;
    assert_eq!(lsns(&recovered), (1..=7).collect::<Vec<_>>());
    assert_eq!(recovered.last(), Some(&seventh));
    assert!(stateless.append_many(vec![record(stream, 8, 5)]).await? >= 2);

    for node in nodes {
        node.task.abort();
    }
    Ok(())
}
