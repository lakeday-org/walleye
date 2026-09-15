//! Startup compatibility checks for the durable placement sidecar.

use std::fs;

use sha2::{Digest, Sha256};
use walleye_bitr_server::{
    DiskReplica, DurableControl, DurableControlState, ReplicaNode, StreamSegment,
    placement_state_path,
};

fn members() -> Vec<ReplicaNode> {
    (0..3)
        .map(|index| {
            ReplicaNode::new(
                format!("node-{index}"),
                format!("http://127.0.0.1:{}", 21_000 + index),
            )
        })
        .collect()
}

fn legacy_control_state(control: &DurableControl) -> DurableControlState {
    let mut state = control.state().expect("control state");
    let member_ids = state
        .cohorts
        .get(&0)
        .expect("bootstrap cohort")
        .members
        .clone();
    let mut member_hash = Sha256::new();
    member_hash.update(b"lakeday-cloud/cohort-members/v1\0");
    for member_id in &member_ids {
        let member = state.members.get(member_id).expect("cohort member");
        member_hash.update(member.id.as_bytes());
        member_hash.update([0]);
        member_hash.update(member.url.as_bytes());
        member_hash.update([0]);
    }
    let member_hash = hex::encode(member_hash.finalize());
    let route = StreamSegment {
        start_lsn: 1,
        end_lsn: None,
        cohort_id: 0,
        member_ids,
        member_hash: member_hash.clone(),
        writer_epoch: 1,
        manifest_revision: 1,
        placement_epoch: 0,
        operation_id: "legacy-route".to_owned(),
        tier: String::new(),
        max_append_bytes: 0,
    };
    state.manifest_revision = 1;
    state.manifest_operation_id = "legacy-route".to_owned();
    state.manifest_writer_epoch = 1;
    state.manifest_cohort_id = 0;
    state.manifest_member_hash = member_hash;
    state.placement_initialized = false;
    state
        .stream_segments
        .insert("tenant/legacy".to_owned(), vec![route]);
    let mut manifest_hash = Sha256::new();
    manifest_hash.update(b"lakeday-cloud/replica-manifest/v1\0");
    manifest_hash.update(serde_json::to_vec(&state.stream_segments).expect("manifest json"));
    state.manifest_digest = hex::encode(manifest_hash.finalize());
    state
}

#[test]
fn legacy_routes_seed_the_sidecar_before_control_marker() -> Result<(), Box<dyn std::error::Error>>
{
    let volume = tempfile::tempdir()?;
    let control_path = volume.path().join("replica-control.json");
    let log_path = volume.path().join("replica.log");
    let data_dir = volume.path().join("data");
    let members = members();

    // Write the shape emitted by an old image: a persisted route with no host
    // placement epoch and no migration marker. DurableControl::open performs
    // its existing control-file normalization before DiskReplica bootstraps
    // placement from the resulting certified route.
    let control = DurableControl::open(&control_path, &members)?;
    let legacy = legacy_control_state(&control);
    drop(control);
    fs::write(&control_path, serde_json::to_vec(&legacy)?)?;

    // The old image has a valid manifest but no placement host metadata.
    // Reopening it must preserve the route and only add the sidecar marker.

    let replica = DiskReplica::open_with_control(
        &log_path,
        "node-0",
        "hot",
        &data_dir,
        &control_path,
        &members,
    )
    .map_err(|error| format!("legacy disk open failed: {error:?}"))?;
    let placement = replica.current_placement("tenant/legacy")?;
    assert_eq!(placement.epoch(), 1);
    assert!(!placement.route_digest().is_empty());
    assert!(
        replica
            .control()
            .expect("direct control")
            .state()?
            .placement_initialized
    );
    assert!(placement_state_path(&data_dir).is_file());

    // The control marker is outside the sidecar. Once it is set, losing the
    // sidecar must fail closed instead of silently reopening epoch-zero gates.
    drop(replica);
    fs::remove_file(placement_state_path(&data_dir))?;
    let reopened = DiskReplica::open_with_control(
        &log_path,
        "node-0",
        "hot",
        &data_dir,
        &control_path,
        &members,
    );
    let error = match reopened {
        Ok(_) => return Err("missing initialized sidecar unexpectedly reopened".into()),
        Err(error) => error,
    };
    assert!(error.to_string().contains("missing after initialization"));
    Ok(())
}
