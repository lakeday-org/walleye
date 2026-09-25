//! Tables change hands by themselves: a dead owner's tables are taken over by
//! the live processes, a paused owner that comes back finds it owns nothing,
//! two claimants leave one owner, and a replacement beside the original takes
//! over when the original leaves.
mod common;

use common::*;
use serde_json::json;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use walleye_node::LeaseConfig;

/// Kill an owner of a third of the tables, without a release. The survivors
/// take its tables over within a lease verdict plus the sampling that notices
/// it, end up sharing all of them evenly, and every acknowledged write is
/// there exactly once.
async fn a_killed_owner_is_replaced(bitr_url: Option<&str>) {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let lease = fast();
    let mut procs = vec![
        Proc::start("a", &root, cache.path(), bitr_url, lease.clone()).await,
        Proc::start("b", &root, cache.path(), bitr_url, lease.clone()).await,
        Proc::start("c", &root, cache.path(), bitr_url, lease.clone()).await,
    ];

    // Twelve tables, created through A and placed evenly.
    let tables: Vec<String> = (0..12).map(|i| format!("t{i}")).collect();
    for table in &tables {
        define(&procs[0].base, table).await;
    }
    let bases: Vec<&str> = procs.iter().map(|p| p.base.as_str()).collect();
    let spread = settled_spread(&bases, &tables, Duration::from_secs(30)).await;
    assert_eq!(spread, vec![4, 4, 4], "placed evenly before the kill");

    // Rows through every process, acknowledged and never flushed: they exist
    // only in the write-ahead log when their owner dies.
    let mut acked: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    let doomed = session(&procs[0].base).await;
    let mut doomed_tables = Vec::new();
    for (index, table) in tables.iter().enumerate() {
        for id in 0..6 {
            let via = bases[(index + id as usize) % 3];
            let answer = write(via, table, id).await;
            assert_eq!(answer.status, 200, "write {table}/{id}: {}", answer.body);
            if answer.owner == doomed && id == 0 {
                doomed_tables.push(table.clone());
            }
            acked.entry(table.clone()).or_default().push(id);
        }
    }
    assert_eq!(doomed_tables.len(), 4, "{doomed_tables:?}");
    let a = procs.remove(0);
    let (b, c) = (procs.remove(0), procs.remove(0));

    a.kill().await;
    let killed = Instant::now();

    // Nobody asks for anything: the survivors notice on their own.
    let (b_held, c_held) = loop {
        let (b_held, c_held) = (held(&b.base).await, held(&c.base).await);
        if b_held.len() + c_held.len() == tables.len() {
            break (b_held, c_held);
        }
        assert!(
            killed.elapsed() < Duration::from_secs(20),
            "the survivors never took everything: b {b_held:?} c {c_held:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let failover = killed.elapsed();
    let bound = Duration::from_millis(lease.ttl_ms + lease.skew_ms + 2 * lease.sample_ms + 2_000);
    println!(
        "  failover of {} tables: {:.0} ms (ttl {} + skew {} + sample {} ms); b took {}, c took {}",
        tables.len(),
        failover.as_secs_f64() * 1000.0,
        lease.ttl_ms,
        lease.skew_ms,
        lease.sample_ms,
        b_held.len(),
        c_held.len()
    );
    // The owner's last renewal can precede the kill by a renewal interval,
    // and a peer can have seen it up to a sample before that.
    let earliest = Duration::from_millis(lease.ttl_ms * 2 / 3 + lease.skew_ms - lease.sample_ms);
    assert!(
        failover >= earliest,
        "a table moved before its owner's lease could have lapsed: {failover:?}"
    );
    assert!(
        failover < bound,
        "failover took {failover:?}, bound {bound:?}"
    );
    let spread = settled_spread(&[&b.base, &c.base], &tables, Duration::from_secs(30)).await;
    assert_eq!(spread, vec![6, 6], "the survivors share everything evenly");

    // Every acknowledged row is served, once, through either survivor, and
    // new writes land.
    for (index, table) in tables.iter().enumerate() {
        let via = if index % 2 == 0 { &b.base } else { &c.base };
        exactly_once(via, table, &acked[table]).await;
        let answer = write(via, table, 100).await;
        assert_eq!(answer.status, 200, "write after failover: {}", answer.body);
        acked.get_mut(table).unwrap().push(100);
        exactly_once(via, table, &acked[table]).await;
    }
    b.stop().await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_owner_is_replaced_on_the_object_store_log() {
    a_killed_owner_is_replaced(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_owner_is_replaced_on_the_bitr_log() {
    let logs = tempfile::tempdir().unwrap();
    let (gateway, tasks) = bitr(logs.path()).await;
    a_killed_owner_is_replaced(Some(&gateway)).await;
    for task in tasks {
        task.abort();
    }
}

/// An owner paused past its lease loses its tables to a peer, and when it
/// resumes it acts as an owner for nothing: its writer's epoch is behind the
/// new owner's, its next write goes to the new owner, and it has stood down
/// to a new session holding no tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_owner_is_fenced_and_stands_down() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let lease = fast();
    let first = Proc::start("a", &root, cache.path(), None, lease.clone()).await;
    let second = Proc::start("b", &root, cache.path(), None, lease.clone()).await;
    define(&first.base, "p").await;
    // A is whichever owns the table.
    let (a, b) = match holder_of(&[&first.base, &second.base], "p").await {
        0 => (first, second),
        _ => (second, first),
    };
    for id in 0..5 {
        assert_eq!(write(&a.base, "p", id).await.status, 200);
    }
    let a_before = session(&a.base).await;
    let a_epoch = held(&a.base).await["p"];

    // Paused well past the verdict and the sampling that acts on it.
    let pause = Duration::from_millis(lease.ttl_ms + lease.skew_ms + 4 * lease.sample_ms + 1_000);
    a.pause(pause);
    let paused = Instant::now();
    // The claim takes the record at the old epoch; opening the writer then
    // claims the next one, which is what fences the paused owner's writer.
    let b_epoch = loop {
        if let Some(epoch) = held(&b.base).await.get("p").copied()
            && epoch > a_epoch
        {
            break epoch;
        }
        assert!(
            paused.elapsed() < pause,
            "b never took the paused owner's table"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    println!(
        "  a paused owner's table moved after {:.0} ms; writer epoch {a_epoch} -> {b_epoch}",
        paused.elapsed().as_secs_f64() * 1000.0
    );
    // The paused process's own writer is still open inside it. Its first act
    // on resuming is this write, sent straight to it.
    let b_session = session(&b.base).await;
    let answer = write(&a.base, "p", 5).await;
    assert_eq!(answer.status, 200, "{}", answer.body);
    assert_eq!(
        answer.owner, b_session,
        "the resumed process sent the write to the new owner rather than taking it"
    );
    let after = status(&a.base).await;
    assert_ne!(after["node"], a_before, "the resumed process stood down");
    assert_eq!(after["held"], json!({}), "and holds nothing");
    // A forward that still believes the old owner holds the table is refused
    // rather than applied.
    let refused = post(
        &a.base,
        "/v1/streams/p/events",
        json!({"rows": [{"id": 6, "at": 6}]}),
        true,
    )
    .await;
    assert_eq!(refused.status, 409, "{}", refused.body);
    assert_eq!(refused.route_error.as_deref(), Some("stale-owner"));
    exactly_once(&b.base, "p", &[0, 1, 2, 3, 4, 5]).await;
    let (ids, _) = ids(&b.base, "p").await;
    assert!(!ids.contains(&6), "the refused write was not applied");
    a.stop().await;
    b.stop().await;
}

/// Two processes claiming the same table at the same moment: exactly one
/// owns it, and writes through both land there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_racing_for_a_table_leave_one_owner() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let a = Proc::start("a", &root, cache.path(), None, fast()).await;
    let b = Proc::start("b", &root, cache.path(), None, fast()).await;
    for round in 0..10 {
        let table = format!("r{round}");
        let (left, right) = tokio::join!(define(&a.base, &table), define(&b.base, &table));
        let _ = (left, right);
        let (x, y) = tokio::join!(write(&a.base, &table, 1), write(&b.base, &table, 2));
        assert_eq!((x.status, y.status), (200, 200), "{} {}", x.body, y.body);
        assert_eq!(x.owner, y.owner, "round {round}: one owner");
        let (a_held, b_held) = (held(&a.base).await, held(&b.base).await);
        // Never both. Neither is possible for a moment, while one hands the
        // table to the other as the tables are balanced between them.
        assert!(
            !(a_held.contains_key(&table) && b_held.contains_key(&table)),
            "round {round}: at most one process holds {table}"
        );
        exactly_once(&a.base, &table, &[1, 2]).await;
    }
    a.stop().await;
    b.stop().await;
}

/// The ramp upgrade: a replacement starts beside the original under the same
/// configured id. The original hands it nothing while both run - they are one
/// node, not two, and the replacement is not taking traffic yet - so the
/// replacement sends every request to the original, and once the original
/// releases it takes everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_takes_over_when_the_original_leaves() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let lease = fast();
    let original = Proc::start("single", &root, cache.path(), None, lease.clone()).await;
    let tables: Vec<String> = (0..5).map(|i| format!("u{i}")).collect();
    let mut acked: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for table in &tables {
        define(&original.base, table).await;
        for id in 0..3 {
            assert_eq!(write(&original.base, table, id).await.status, 200);
            acked.entry(table.clone()).or_default().push(id);
        }
    }
    let original_session = session(&original.base).await;

    let replacement = Proc::start("single", &root, cache.path(), None, lease.clone()).await;
    let replacement_session = session(&replacement.base).await;
    assert_ne!(original_session, replacement_session);
    // Once both have swept, the original still holds every table: a
    // successor on its own node is never handed a key.
    let spread = settled_spread(
        &[&original.base, &replacement.base],
        &tables,
        Duration::from_secs(30),
    )
    .await;
    assert_eq!(
        spread,
        vec![tables.len(), 0],
        "nothing moved to the successor"
    );
    // A write through the replacement lands on the original, which holds it.
    for table in &tables {
        let answer = write(&replacement.base, table, 10).await;
        assert_eq!(answer.status, 200, "{}", answer.body);
        assert_eq!(
            answer.owner, original_session,
            "{table} is served by the original until it leaves"
        );
        acked.get_mut(table).unwrap().push(10);
    }

    let leaving = Instant::now();
    original.stop().await;
    let released = leaving.elapsed();
    let taken = loop {
        if held(&replacement.base).await.len() == tables.len() {
            break leaving.elapsed();
        }
        assert!(
            leaving.elapsed() < Duration::from_secs(10),
            "never took over"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    println!(
        "  replacement: the original released in {:.0} ms, the replacement held every table \
         {:.0} ms after the stop began, with no request asking (sample interval {} ms)",
        released.as_secs_f64() * 1000.0,
        taken.as_secs_f64() * 1000.0,
        lease.sample_ms
    );
    for table in &tables {
        let answer = write(&replacement.base, table, 20).await;
        assert_eq!(answer.status, 200, "{}", answer.body);
        assert_eq!(answer.owner, replacement_session);
        acked.get_mut(table).unwrap().push(20);
        exactly_once(&replacement.base, table, &acked[table]).await;
    }
    replacement.stop().await;
}

/// Routing, end to end: a non-owner forwards to the owner; while the owner is
/// dead and its lease has not yet lapsed nobody may claim, so the answer is
/// 503 with `Retry-After`; and a request forwarded to a process that does not
/// own the table is refused with 409 and a route error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_are_forwarded_held_off_or_refused() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    // A lease long enough that the refusal is certainly asked for before it
    // could lapse, however loaded the machine running this is.
    let lease = LeaseConfig {
        ttl_ms: 6_000,
        skew_ms: 1_000,
        sample_ms: 250,
    };
    let first = Proc::start("a", &root, cache.path(), None, lease.clone()).await;
    let second = Proc::start("b", &root, cache.path(), None, lease.clone()).await;
    define(&first.base, "q").await;
    // A is whichever owns the table.
    let (a, b) = match holder_of(&[&first.base, &second.base], "q").await {
        0 => (first, second),
        _ => (second, first),
    };
    let a_session = session(&a.base).await;

    let forwarded = write(&b.base, "q", 1).await;
    assert_eq!(forwarded.status, 200);
    assert_eq!(forwarded.owner, a_session, "B forwarded to the owner");

    let refused = post(
        &b.base,
        "/v1/streams/q/events",
        json!({"rows": [{"id": 2, "at": 2}]}),
        true,
    )
    .await;
    assert_eq!(refused.status, 409);
    assert_eq!(refused.route_error.as_deref(), Some("stale-owner"));

    a.kill().await;
    let killed = Instant::now();
    let held_off = write(&b.base, "q", 3).await;
    let answered = killed.elapsed();
    assert!(
        answered < Duration::from_millis(lease.ttl_ms * 2 / 3),
        "answered before the lease could lapse, took {answered:?}"
    );
    assert_eq!(held_off.status, 503, "{}", held_off.body);
    assert!(held_off.retry_after.is_some(), "503 carries Retry-After");
    assert_eq!(held_off.route_error.as_deref(), Some("owner-unreachable"));

    // A client that honours Retry-After gets through once the lease lapses.
    let landed = loop {
        let answer = write(&b.base, "q", 3).await;
        if answer.status == 200 {
            break killed.elapsed();
        }
        assert_eq!(answer.status, 503, "{}", answer.body);
        let wait: u64 = answer.retry_after.unwrap().parse().unwrap();
        assert!(wait >= 1);
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    println!(
        "  a write through a peer of a killed owner landed after {:.0} ms",
        landed.as_secs_f64() * 1000.0
    );
    exactly_once(&b.base, "q", &[1, 3]).await;
    b.stop().await;
}
