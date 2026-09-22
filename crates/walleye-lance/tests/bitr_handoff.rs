//! Passing the writer between processes when the WAL is a Bitr quorum.
//!
//! Over the object-store WAL this works: the claim is a compare-and-swap on
//! the shard manifest, and a writer that takes it replays the log the last one
//! left, so nothing acknowledged is lost. Bitr changes where the log lives,
//! not who arbitrates the claim — the manifest in the object store still does
//! that — so the question is whether the replay still finds the rows.
//!
//! It depends entirely on whether the two writers are talking to the same Bitr
//! cluster, and the difference is not one the manifest can see.
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use walleye_bitr::{MemoryReplica, QuorumWriter};
use walleye_lance::{
    BitrWalBackend, LanceDurability, LanceStorageOptions, Table, TableConfig, next_writer_epoch,
    prepare_bitr_takeover,
};

const ROUNDS: i64 = 6;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn row(id: i64, value: &str) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .unwrap()
}

/// One Bitr cluster. Two `QuorumWriter`s over one of these are two processes
/// against the same quorum; two of these are two clusters.
fn cluster() -> Arc<MemoryReplica> {
    Arc::new(MemoryReplica::healthy())
}

fn writer(on: &Arc<MemoryReplica>) -> Arc<QuorumWriter> {
    Arc::new(QuorumWriter::new(on.clone(), [7; 32]))
}

/// Claim the writer for this stream and open it, exactly as the daemon does:
/// mint the epoch from the manifest, build a Bitr identity fenced to it, open.
async fn claim(uri: &str, on: &Arc<QuorumWriter>) -> lance::Result<(Table, u64, bool)> {
    let config = TableConfig::new("events", uri.to_owned(), schema(), vec!["id".into()]).unwrap();
    let storage = LanceStorageOptions::default();
    let moved = prepare_bitr_takeover(&storage, uri, config.shard_id, &config.stream, on).await?;
    let epoch = next_writer_epoch(&storage, uri, config.shard_id).await?;
    let backend = Arc::new(
        BitrWalBackend::new(on.clone(), &config.stream, config.shard_id, epoch)
            .map_err(|e| lance::Error::io(format!("Bitr identity for {}: {e}", config.stream)))?,
    );
    let table = Table::open(config, storage, LanceDurability::Bitr(backend)).await?;
    let claimed = table.writer_epoch();
    Ok((table, claimed, moved))
}

async fn ids(table: &mut Table) -> Vec<i64> {
    let scanned = table.scan(None, 10_000).await.expect("scan");
    let mut found: Vec<i64> = scanned
        .iter()
        .flat_map(|batch: &RecordBatch| {
            let column = batch.column_by_name("id").expect("id column");
            let values = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is int64");
            (0..values.len()).map(|i| values.value(i)).collect::<Vec<_>>()
        })
        .collect();
    found.sort_unstable();
    found
}

/// Two processes, one Bitr cluster: the same rota the object-store WAL allows.
///
/// The claim is still the manifest's, and the log both of them append to is
/// the quorum's, so a writer that takes over replays what the last one wrote
/// and no acknowledged row goes missing. Nothing is checkpointed between
/// rounds, so every round is replaying a live tail rather than reading flushed
/// files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_writers_on_one_cluster_hand_the_stream_back_and_forth() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}/events", dir.path().display());
    let shared = cluster();
    let (left, right) = (writer(&shared), writer(&shared));

    let mut epochs = Vec::new();
    for round in 0..ROUNDS {
        let on = if round % 2 == 0 { &left } else { &right };
        let (mut table, epoch, _) = claim(&uri, on)
            .await
            .unwrap_or_else(|e| panic!("round {round} could not claim the writer: {e}"));
        epochs.push(epoch);
        // Everything the earlier rounds acknowledged is visible to this one,
        // because it replayed their entries out of the quorum.
        assert_eq!(
            ids(&mut table).await,
            (0..round).collect::<Vec<_>>(),
            "round {round} replayed every earlier row"
        );
        table
            .append(vec![row(round, "written")])
            .await
            .unwrap_or_else(|e| panic!("round {round} held the writer but could not write: {e}"));
        table.close().await.expect("close");
    }
    for pair in epochs.windows(2) {
        assert!(
            pair[1] > pair[0],
            "each claim takes a higher epoch than the last, got {epochs:?}"
        );
    }
}

/// Two processes, two Bitr clusters, one stream. This is the shape that does
/// not work, and the point of writing it down is that nothing refuses it.
///
/// The manifest arbitrates the claim, so the handover looks orderly: the
/// second writer takes the next epoch and the first is fenced, exactly as
/// before. But the log it replays is its own cluster's, and the rows the first
/// one acknowledged are in the other cluster's quorum. The manifest cannot see
/// the difference, because which Bitr cluster a writer talks to is not
/// something it records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clusters_on_one_stream_lose_what_the_other_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file://{}/events", dir.path().display());
    let (first, second) = (cluster(), cluster());

    let (mut one, first_epoch, _) = claim(&uri, &writer(&first)).await.unwrap();
    one.append(vec![row(1, "acknowledged by the first cluster")])
        .await
        .expect("the first writer stores a row");
    assert_eq!(ids(&mut one).await, vec![1]);
    // Deliberately no checkpoint: the row is acknowledged and durable in the
    // first quorum, and nowhere else yet. That is the state a live handover
    // happens in.

    let (mut two, second_epoch, moved) = claim(&uri, &writer(&second))
        .await
        .expect("a writer on another cluster claims the stream regardless");
    // Not a bad reset: the one guard there is declined to treat this as a
    // stream moving off the object-store WAL, which is the only mismatch it
    // knows how to look for. There is no guard for the mismatch that is
    // happening, so the open simply finds an empty log and believes it.
    assert!(
        !moved,
        "the second cluster did not reset anything; it had nothing to replay"
    );
    assert!(
        second_epoch > first_epoch,
        "the manifest hands over as usual: {first_epoch} then {second_epoch}"
    );

    let after = ids(&mut two).await;
    assert_eq!(
        after,
        Vec::<i64>::new(),
        "the row the first cluster acknowledged is not in the second's log"
    );

    two.append(vec![row(2, "acknowledged by the second cluster")])
        .await
        .expect("the second writer stores a row");
    assert_eq!(
        ids(&mut two).await,
        vec![2],
        "two writers, two logs, one stream: each sees only its own"
    );
    let _ = one.close().await;
    let _ = two.close().await;
}
