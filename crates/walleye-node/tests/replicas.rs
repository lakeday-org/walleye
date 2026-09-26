//! Launch nodes that restart catch their replicas up from their peers.
//!
//! Each process here is one launch Machine: the engine and its own Bitr
//! replica and coordinator, sharing a runtime, so killing it kills both. A
//! replica that comes back after missing writes must become complete, and it
//! must not stand between the cluster and a quorum while it does: a second
//! failure afterwards, or during, costs a failover and nothing more.
mod common;

use common::*;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

const TABLES: usize = 6;

fn tables() -> Vec<String> {
    (0..TABLES).map(|i| format!("w{i}")).collect()
}

/// Every table written to through whichever nodes are up, as fast as each is
/// answered, recording what was acknowledged and when each table last was.
struct Writers {
    stop: Arc<AtomicBool>,
    bases: Arc<Mutex<Vec<Option<String>>>>,
    last_ack: Arc<Mutex<BTreeMap<String, Instant>>>,
    tasks: Vec<tokio::task::JoinHandle<(String, Vec<i64>)>>,
}

impl Writers {
    fn start(bases: Vec<Option<String>>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let bases = Arc::new(Mutex::new(bases));
        let last_ack = Arc::new(Mutex::new(BTreeMap::new()));
        let tasks = tables()
            .into_iter()
            .map(|table| {
                let (stop, bases, last_ack) = (stop.clone(), bases.clone(), last_ack.clone());
                tokio::spawn(async move {
                    let mut acked = Vec::new();
                    let mut id = 0_i64;
                    while !stop.load(Ordering::Acquire) {
                        let base = {
                            let bases = bases.lock().unwrap();
                            let up: Vec<&String> = bases.iter().flatten().collect();
                            up[id as usize % up.len()].clone()
                        };
                        let answered = client()
                            .post(format!("{base}/v1/streams/{table}/events"))
                            .header("authorization", format!("Bearer {TOKEN}"))
                            .json(&serde_json::json!({"rows": [{"id": id, "at": id}]}))
                            .send()
                            .await;
                        let ok = matches!(&answered, Ok(r) if r.status() == 200);
                        if let Ok(response) = answered {
                            let _ = response.bytes().await;
                        }
                        if ok {
                            acked.push(id);
                            last_ack
                                .lock()
                                .unwrap()
                                .insert(table.clone(), Instant::now());
                        } else {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        id += 1;
                    }
                    (table, acked)
                })
            })
            .collect();
        Self {
            stop,
            bases,
            last_ack,
            tasks,
        }
    }
    fn set(&self, index: usize, base: Option<String>) {
        self.bases.lock().unwrap()[index] = base;
    }
    /// How long after `since` every table had a write acknowledged again.
    async fn all_writing_after(&self, since: Instant, limit: Duration) -> Duration {
        loop {
            let latest = self.last_ack.lock().unwrap().clone();
            if tables()
                .iter()
                .all(|t| latest.get(t).is_some_and(|at| *at > since))
            {
                return latest.values().max().unwrap().duration_since(since);
            }
            assert!(
                since.elapsed() < limit,
                "writes did not resume within {limit:?}: {latest:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    async fn finish(self) -> BTreeMap<String, Vec<i64>> {
        self.stop.store(true, Ordering::Release);
        let mut acked = BTreeMap::new();
        for task in self.tasks {
            let (table, ids) = task.await.unwrap();
            acked.insert(table, ids);
        }
        acked
    }
}

/// Until node `index`'s replica holds every stream as far as the most
/// complete answering replica does. Returns how long that took.
async fn complete(launch: &Launch, index: usize, up: &[usize], limit: Duration) -> Duration {
    let started = Instant::now();
    loop {
        let mut furthest: BTreeMap<String, u64> = BTreeMap::new();
        for peer in up {
            if let Some(positions) = launch.positions(*peer).await {
                for (stream, lsn) in positions {
                    let entry = furthest.entry(stream).or_insert(0);
                    *entry = (*entry).max(lsn);
                }
            }
        }
        if let Some(own) = launch.positions(index).await
            && !furthest.is_empty()
            && furthest
                .iter()
                .all(|(stream, lsn)| own.get(stream).is_some_and(|mine| mine >= lsn))
        {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < limit,
            "node-{index} never caught up: {:?}",
            launch.positions(index).await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn launch_cluster(launch: &Launch, cache: &std::path::Path) -> Vec<Option<Proc>> {
    let mut procs = Vec::new();
    for index in 0..3 {
        procs.push(Some(launch.start(index, cache, fast()).await));
    }
    for table in tables() {
        define(&procs[0].as_ref().unwrap().base, &table).await;
    }
    procs
}

fn bases(procs: &[Option<Proc>]) -> Vec<Option<String>> {
    procs
        .iter()
        .map(|p| p.as_ref().map(|p| p.base.clone()))
        .collect()
}

/// Until the node at `base` holds at least one of the tables.
async fn holding_tables(base: &str) {
    let started = Instant::now();
    loop {
        let holding = held(base).await;
        if tables().iter().any(|table| holding.contains_key(table)) {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{base} never took a table"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn check_exactly_once(base: &str, acked: &BTreeMap<String, Vec<i64>>) {
    for (table, ids) in acked {
        exactly_once(base, table, ids).await;
    }
}

/// A node killed while the others write comes back behind, catches up from
/// its peers, and then carries the cluster through the loss of a second
/// node: writes resume after a normal failover, and nothing acknowledged is
/// lost or doubled.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_restarted_replica_catches_up_and_carries_a_second_failure() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let launch = Launch::new(dir.path());
    let mut procs = launch_cluster(&launch, cache.path()).await;
    let writers = Writers::start(bases(&procs));
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Node 2 dies; the others fail its tables over and keep writing.
    holding_tables(&procs[2].as_ref().unwrap().base).await;
    writers.set(2, None);
    procs[2].take().unwrap().kill().await;
    let first_kill = Instant::now();
    let first_failover = writers
        .all_writing_after(first_kill, Duration::from_secs(30))
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // It comes back missing everything written meanwhile, and catches up.
    procs[2] = Some(launch.start(2, cache.path(), fast()).await);
    writers.set(2, Some(procs[2].as_ref().unwrap().base.clone()));
    let caught_up = complete(&launch, 2, &[0, 1], Duration::from_secs(30)).await;

    // Now a second node dies. Node 2's replica is one of the two left.
    holding_tables(&procs[0].as_ref().unwrap().base).await;
    writers.set(0, None);
    procs[0].take().unwrap().kill().await;
    let second_kill = Instant::now();
    let second_failover = writers
        .all_writing_after(second_kill, Duration::from_secs(30))
        .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let acked = writers.finish().await;
    println!(
        "  first failover {:.0} ms; node-2 caught up {:.0} ms after restarting; \
         second failover {:.0} ms",
        first_failover.as_secs_f64() * 1000.0,
        caught_up.as_secs_f64() * 1000.0,
        second_failover.as_secs_f64() * 1000.0,
    );
    check_exactly_once(&procs[1].as_ref().unwrap().base, &acked).await;
    assert!(
        second_failover < first_failover * 2 + Duration::from_secs(2),
        "the second failover costs about what the first did: {first_failover:?} then \
         {second_failover:?}"
    );
    for p in procs.into_iter().flatten() {
        p.stop().await;
    }
}

/// A second node dies while the restarted one is still catching up. The
/// cluster is short a complete replica until the catch-up finishes, so
/// writes may wait; they must not be lost, doubled or fenced in a loop, and
/// they resume once the lagging replica holds what it missed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_second_failure_during_catch_up_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let launch = Launch::new(dir.path());
    let mut procs = launch_cluster(&launch, cache.path()).await;
    let writers = Writers::start(bases(&procs));
    tokio::time::sleep(Duration::from_secs(1)).await;
    writers.set(2, None);
    procs[2].take().unwrap().kill().await;
    // Long enough that it has a good deal to catch up on.
    tokio::time::sleep(Duration::from_secs(6)).await;
    procs[2] = Some(launch.start(2, cache.path(), fast()).await);
    // Straight away: node 1 dies while node 2 is catching up.
    writers.set(1, None);
    procs[1].take().unwrap().kill().await;
    let killed = Instant::now();
    writers.set(2, Some(procs[2].as_ref().unwrap().base.clone()));
    let resumed = writers
        .all_writing_after(killed, Duration::from_secs(45))
        .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let acked = writers.finish().await;
    println!(
        "  a failure during catch-up: writes resumed on every table {:.0} ms later",
        resumed.as_secs_f64() * 1000.0
    );
    check_exactly_once(&procs[0].as_ref().unwrap().base, &acked).await;
    for p in procs.into_iter().flatten() {
        p.stop().await;
    }
}

/// Kill, restart and catch up the same cluster's nodes one after another:
/// each failover costs about what the first did, rather than growing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn successive_failovers_do_not_get_slower() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let launch = Launch::new(dir.path());
    let mut procs = launch_cluster(&launch, cache.path()).await;
    let writers = Writers::start(bases(&procs));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut failovers = Vec::new();
    for round in 0..4 {
        let victim = round % 3;
        // Each round kills a node that holds tables, so each is a failover
        // and not the loss of a node nobody needed.
        holding_tables(&procs[victim].as_ref().unwrap().base).await;
        writers.set(victim, None);
        procs[victim].take().unwrap().kill().await;
        let killed = Instant::now();
        failovers.push(
            writers
                .all_writing_after(killed, Duration::from_secs(45))
                .await,
        );
        procs[victim] = Some(launch.start(victim, cache.path(), fast()).await);
        writers.set(victim, Some(procs[victim].as_ref().unwrap().base.clone()));
        let others: Vec<usize> = (0..3).filter(|i| *i != victim).collect();
        complete(&launch, victim, &others, Duration::from_secs(30)).await;
    }
    let acked = writers.finish().await;
    println!(
        "  successive failovers: {:?} ms",
        failovers.iter().map(|f| f.as_millis()).collect::<Vec<_>>()
    );
    check_exactly_once(&procs[0].as_ref().unwrap().base, &acked).await;
    let first = failovers[0];
    assert!(
        failovers
            .iter()
            .all(|f| *f < first * 2 + Duration::from_secs(2)),
        "failovers grew: {failovers:?}"
    );
    for p in procs.into_iter().flatten() {
        p.stop().await;
    }
}

/// Successive failovers over what a real launch cluster runs on: an archive
/// with an object store's latency, replicas a network apart, and a history
/// that has built up before the first failure. Neither the failover nor the
/// catch-up after each restart may grow from one round to the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn successive_failovers_at_scale_do_not_get_slower() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let launch = Launch::under(
        dir.path(),
        Conditions {
            archive: Duration::from_millis(60),
            peer: Duration::from_millis(2),
        },
    );
    let mut procs = launch_cluster(&launch, cache.path()).await;
    let writers = Writers::start(bases(&procs));
    tokio::time::sleep(Duration::from_secs(20)).await;
    let mut failovers = Vec::new();
    let mut catch_ups = Vec::new();
    for victim in [0, 1, 0, 2] {
        holding_tables(&procs[victim].as_ref().unwrap().base).await;
        writers.set(victim, None);
        procs[victim].take().unwrap().kill().await;
        let killed = Instant::now();
        failovers.push(
            writers
                .all_writing_after(killed, Duration::from_secs(60))
                .await,
        );
        // A Machine takes a few seconds to come back.
        tokio::time::sleep(Duration::from_secs(4)).await;
        procs[victim] = Some(launch.start(victim, cache.path(), fast()).await);
        writers.set(victim, Some(procs[victim].as_ref().unwrap().base.clone()));
        let others: Vec<usize> = (0..3).filter(|i| *i != victim).collect();
        catch_ups.push(complete(&launch, victim, &others, Duration::from_secs(60)).await);
        eprintln!(
            "  round: node-{victim} failover {} ms, caught up {} ms",
            failovers.last().unwrap().as_millis(),
            catch_ups.last().unwrap().as_millis()
        );
    }
    let acked = writers.finish().await;
    println!(
        "  at scale: failovers {:?} ms; catch-ups {:?} ms",
        failovers.iter().map(|f| f.as_millis()).collect::<Vec<_>>(),
        catch_ups.iter().map(|f| f.as_millis()).collect::<Vec<_>>()
    );
    check_exactly_once(&procs[1].as_ref().unwrap().base, &acked).await;
    let (first_failover, first_catch_up) = (failovers[0], catch_ups[0]);
    assert!(
        failovers
            .iter()
            .all(|f| *f < first_failover * 2 + Duration::from_secs(2)),
        "failovers grew: {failovers:?}"
    );
    assert!(
        catch_ups
            .iter()
            .all(|c| *c < first_catch_up * 2 + Duration::from_secs(2)),
        "catch-ups grew: {catch_ups:?}"
    );
    for p in procs.into_iter().flatten() {
        p.stop().await;
    }
}
