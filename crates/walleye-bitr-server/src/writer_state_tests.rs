use super::*;

fn record(stream: &str, lsn: u64, marker: u8) -> EncryptedRecord {
    serde_json::from_value(serde_json::json!({
        "stream": stream,
        "writer_epoch": 1,
        "lsn": lsn,
        "committed_lsn": lsn - 1,
        "nonce": vec![marker; 24],
        "ciphertext": vec![marker; 32],
        "authentication": vec![marker; 32],
    }))
    .expect("opaque test record")
}

fn acknowledged(records: Vec<EncryptedRecord>) -> GatewayWriterState {
    let mut state = GatewayWriterState::default();
    for record in records {
        let stream = state.streams.entry(record.stream().to_owned()).or_default();
        stream.committed_lsn = record.lsn();
        stream.writer_epoch = record.writer_epoch();
        stream.records.insert(record.lsn(), record);
    }
    state
}

#[test]
fn snapshot_publication_preserves_concurrent_acknowledgements() {
    let first = record("tenant/first", 1, 1);
    let second = record("tenant/first", 2, 2);
    let other = record("tenant/other", 1, 3);
    let cached = acknowledged(vec![first.clone(), second.clone(), other.clone()]);
    let mut rebuilt = acknowledged(vec![first]);
    preserve_acknowledged_suffixes(&cached, &mut rebuilt).expect("merge acknowledged suffix");
    assert_eq!(rebuilt.streams["tenant/first"].committed_lsn, 2);
    assert_eq!(rebuilt.streams["tenant/first"].records[&2], second);
    assert_eq!(rebuilt.streams["tenant/other"].records[&1], other);
}

#[test]
fn snapshot_conflicting_with_acknowledged_bytes_is_rejected() {
    let cached = acknowledged(vec![record("tenant/first", 1, 1)]);
    let mut rebuilt = acknowledged(vec![record("tenant/first", 1, 9)]);
    assert!(matches!(
        preserve_acknowledged_suffixes(&cached, &mut rebuilt),
        Err(ReplicaError::LsnConflict)
    ));
}

#[test]
fn snapshot_merge_does_not_resurrect_trimmed_records() {
    let cached = acknowledged(vec![record("tenant/first", 1, 1)]);
    let mut rebuilt = GatewayWriterState::default();
    rebuilt.streams.insert(
        "tenant/first".to_owned(),
        StreamWriterState {
            committed_lsn: 1,
            writer_epoch: 1,
            records: BTreeMap::new(),
        },
    );
    preserve_acknowledged_suffixes(&cached, &mut rebuilt).expect("preserve trim checkpoint");
    assert!(rebuilt.streams["tenant/first"].records.is_empty());
    assert_eq!(rebuilt.streams["tenant/first"].committed_lsn, 1);
}
