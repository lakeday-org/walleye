//! The replica reads its mode from the command line it is given. A hardcoded
//! empty list made every flag look like no flag at all, so the control-head
//! probe could never run and a typo booted a live replica instead.
use walleye_bitr_server::daemon;

/// An argument the replica does not support must stop it, not fall through
/// to the boot path. Falling through binds the listeners and seeds from the
/// archive, which is the opposite of what an operator passing a flag wants.
#[tokio::test]
async fn an_unsupported_argument_is_rejected_before_anything_boots() {
    let error = daemon::run_with_arguments(vec!["--bogus".to_owned()])
        .await
        .expect_err("an unsupported argument must not start a replica");
    assert!(
        error.to_string().contains("unsupported replica arguments"),
        "names the reason, got {error}"
    );
}

/// Recognising the probe flag is what makes it reachable at all. It is
/// rejected here only because this test supplies no archive environment,
/// which proves it took the probe branch rather than the boot branch.
#[tokio::test]
async fn the_control_head_probe_flag_is_recognised() {
    let error = daemon::run_with_arguments(vec!["--verify-control-head-cas".to_owned()])
        .await
        .expect_err("no archive is configured in this test process");
    let error = error.to_string();
    assert!(
        !error.contains("unsupported replica arguments"),
        "the probe flag is a supported mode, got {error}"
    );
    assert!(
        error.contains("LAKEDAY_REPLICA_ARCHIVE"),
        "it reached the archive it probes, got {error}"
    );
}
