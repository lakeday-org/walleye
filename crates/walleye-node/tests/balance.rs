//! Keys spread evenly and stay put. A process holding more than its share of
//! the known keys hands one back per sweep to the process it belongs to, so
//! rolling restarts end evenly spread; writes and schedules carry on through
//! the hand-backs; and a cluster that is not changing moves nothing.
mod common;

use common::*;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// Write to every table through random processes until told to stop, and
/// report what was acknowledged. A refusal is a retry for the caller, not an
/// acknowledgement, so it is only counted.
fn writers(
    bases: Arc<std::sync::Mutex<Vec<String>>>,
    tables: &[String],
    stop: Arc<AtomicBool>,
) -> Vec<tokio::task::JoinHandle<(String, Vec<i64>, usize)>> {
    tables
        .iter()
        .cloned()
        .map(|table| {
            let bases = bases.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut acked = Vec::new();
                let mut refused = 0;
                let mut id = 0_i64;
                while !stop.load(Ordering::Acquire) {
                    let base = {
                        let bases = bases.lock().unwrap();
                        bases[id as usize % bases.len()].clone()
                    };
                    // A process may be stopping as it is picked; a request
                    // that never got an answer is a refusal like any other.
                    let answered = client()
                        .post(format!("{base}/v1/streams/{table}/events"))
                        .header("authorization", format!("Bearer {TOKEN}"))
                        .json(&json!({"rows": [{"id": id, "at": id}]}))
                        .send()
                        .await;
                    let ok = matches!(&answered, Ok(r) if r.status() == 200);
                    if let Ok(response) = answered {
                        let _ = response.bytes().await;
                    }
                    if ok {
                        acked.push(id);
                    } else {
                        refused += 1;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    id += 1;
                }
                (table, acked, refused)
            })
        })
        .collect()
}

/// Three rounds of graceful rolling restarts of three processes, with twelve
/// tables written to throughout: after each round the spread is within one of
/// even, and no acknowledged write is lost or doubled.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rolling_restarts_end_evenly_spread() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let ids = ["a", "b", "c"];
    let mut procs = Vec::new();
    for id in ids {
        procs.push(Proc::start(id, &root, cache.path(), None, fast()).await);
    }
    let tables: Vec<String> = (0..12).map(|i| format!("r{i}")).collect();
    for table in &tables {
        define(&procs[0].base, table).await;
    }
    let bases = Arc::new(std::sync::Mutex::new(
        procs.iter().map(|p| p.base.clone()).collect::<Vec<_>>(),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let running = writers(bases.clone(), &tables, stop.clone());

    let mut spreads = Vec::new();
    for round in 0..3 {
        for (slot, id) in ids.iter().enumerate() {
            let leaving = procs.remove(slot);
            let departed = leaving.base.clone();
            bases.lock().unwrap().retain(|b| *b != departed);
            leaving.stop().await;
            let replacement = Proc::start(id, &root, cache.path(), None, fast()).await;
            bases.lock().unwrap().push(replacement.base.clone());
            procs.insert(slot, replacement);
        }
        let current: Vec<&str> = procs.iter().map(|p| p.base.as_str()).collect();
        let spread = settled_spread(&current, &tables, Duration::from_secs(60)).await;
        println!("  after round {round}: {spread:?}");
        assert!(
            spread.iter().all(|n| (3..=5).contains(n)),
            "round {round}: within one of four each: {spread:?}"
        );
        spreads.push(spread);
    }
    stop.store(true, Ordering::Release);
    let mut refusals = 0;
    for writer in running {
        let (table, acked, refused) = writer.await.unwrap();
        refusals += refused;
        exactly_once(&procs[0].base, &table, &acked).await;
    }
    println!("  spreads {spreads:?}; {refusals} refused writes, all retryable");
    for p in procs {
        p.stop().await;
    }
}

/// A cluster that is not changing reaches a fixed point and stays there: no
/// key moves and no epoch changes over many sweeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_steady_cluster_moves_nothing() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let lease = fast();
    let mut procs = Vec::new();
    for id in ["a", "b", "c"] {
        procs.push(Proc::start(id, &root, cache.path(), None, lease.clone()).await);
    }
    let tables: Vec<String> = (0..12).map(|i| format!("s{i}")).collect();
    for table in &tables {
        define(&procs[0].base, table).await;
    }
    let bases: Vec<&str> = procs.iter().map(|p| p.base.as_str()).collect();
    let spread = settled_spread(&bases, &tables, Duration::from_secs(30)).await;
    assert_eq!(spread, vec![4, 4, 4]);
    let snapshot = |bases: Vec<String>| async move {
        let mut all = BTreeMap::new();
        for base in bases {
            for (key, epoch) in held(&base).await {
                all.insert(key, (base.clone(), epoch));
            }
        }
        all
    };
    let owned: Vec<String> = bases.iter().map(|b| b.to_string()).collect();
    let before = snapshot(owned.clone()).await;
    // Forty sweeps.
    tokio::time::sleep(Duration::from_millis(40 * lease.sample_ms)).await;
    let after = snapshot(owned).await;
    assert_eq!(before, after, "nothing moved and no writer reopened");
    for p in procs {
        p.stop().await;
    }
}

/// The write gap a hand-back causes on the key it moves: a second process
/// joins beside one holding every table, the first hands half of them back
/// one sweep at a time, and a writer on each table measures the longest time
/// between acknowledgements.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_hand_back_costs_its_key_a_short_gap() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let a = Proc::start("a", &root, cache.path(), None, fast()).await;
    let tables: Vec<String> = (0..6).map(|i| format!("g{i}")).collect();
    for table in &tables {
        define(&a.base, table).await;
    }
    assert_eq!(held(&a.base).await.len(), tables.len());
    let a_session = session(&a.base).await;

    let stop = Arc::new(AtomicBool::new(false));
    let mut running = Vec::new();
    for table in tables.clone() {
        let base = a.base.clone();
        let stop = stop.clone();
        running.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            let mut last: Option<(Instant, String)> = None;
            let mut gap_at_move = Duration::ZERO;
            let mut widest_steady = Duration::ZERO;
            let mut id = 0_i64;
            while !stop.load(Ordering::Acquire) {
                let answer = write(&base, &table, id).await;
                if answer.status == 200 {
                    if let Some((at, owner)) = &last {
                        if *owner != answer.owner {
                            gap_at_move = gap_at_move.max(at.elapsed());
                        } else {
                            widest_steady = widest_steady.max(at.elapsed());
                        }
                    }
                    last = Some((Instant::now(), answer.owner.clone()));
                    acked.push(id);
                }
                id += 1;
            }
            (
                table,
                acked,
                gap_at_move,
                widest_steady,
                last.map(|(_, o)| o),
            )
        }));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let b = Proc::start("b", &root, cache.path(), None, fast()).await;
    let spread = settled_spread(&[&a.base, &b.base], &tables, Duration::from_secs(30)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Release);
    assert_eq!(spread, vec![3, 3], "half handed back");

    let mut moved_gaps = Vec::new();
    let mut steady = Duration::ZERO;
    for writer in running {
        let (table, acked, gap, widest, final_owner) = writer.await.unwrap();
        exactly_once(&b.base, &table, &acked).await;
        steady = steady.max(widest);
        if final_owner.as_deref() != Some(a_session.as_str()) {
            moved_gaps.push(gap);
        }
    }
    assert_eq!(moved_gaps.len(), 3, "three tables moved");
    let widest = moved_gaps.iter().max().copied().unwrap_or_default();
    println!(
        "  hand-back write gap on the moved keys: {:?} ms; widest gap elsewhere {} ms",
        moved_gaps.iter().map(|g| g.as_millis()).collect::<Vec<_>>(),
        steady.as_millis()
    );
    assert!(
        widest < Duration::from_secs(2),
        "a hand-back costs its key well under a lease: {widest:?}"
    );
    a.stop().await;
    b.stop().await;
}

/// Schedules keep running once per occurrence while their keys are handed
/// back: one process runs six one-second schedules, a second joins, and the
/// first hands half of its keys - some of the schedules among them - back.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn schedules_fire_once_per_occurrence_across_hand_backs() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let a = Proc::start("a", &root, cache.path(), None, fast()).await;
    let names: Vec<String> = (0..6).map(|i| format!("tick{i}")).collect();
    for name in &names {
        view(&a.base, name, ticker_into(&format!("{name}_runs"))).await;
    }
    for name in &names {
        let table = format!("{name}_runs");
        wait_for("each schedule ran twice", Duration::from_secs(30), || {
            let (base, table) = (a.base.clone(), table.clone());
            async move { ticks_in(&base, &table).await.len() >= 2 }
        })
        .await;
    }
    let b = Proc::start("b", &root, cache.path(), None, fast()).await;
    let keys: Vec<String> = names.iter().map(|n| format!("view.{n}")).collect();
    let mut moved = 0;
    let started = Instant::now();
    while moved == 0 {
        moved = keys.len() - {
            let held = held(&a.base).await;
            keys.iter().filter(|k| held.contains_key(*k)).count()
        };
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "no schedule was handed back"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Let everything settle and run a few more times on its new owner.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut total = 0;
    for name in &names {
        let runs = ticks_in(&b.base, &format!("{name}_runs")).await;
        each_occurrence_once(&runs);
        total += runs.len();
    }
    let on_b = {
        let held = held(&b.base).await;
        keys.iter().filter(|k| held.contains_key(*k)).count()
    };
    println!("  {on_b} of 6 schedules handed back; {total} runs, each occurrence once");
    a.stop().await;
    b.stop().await;
}
