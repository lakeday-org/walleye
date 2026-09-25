//! A graceful restart and a roll refuse nothing.
//!
//! Each process is one launch Machine: engine and replica together. Writers
//! go to every node in turn, so most writes are forwarded to their table's
//! owner. A node that is stopped hands its tables over while it still
//! answers; one that starts joins only once it accepts connections. Neither
//! may leave a peer forwarding to an address that refuses it, so every write
//! is answered 200, and every acknowledged row is there exactly once.
mod common;

use common::*;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

const TABLES: usize = 6;

fn tables() -> Vec<String> {
    (0..TABLES).map(|i| format!("g{i}")).collect()
}

/// One writer per table, sending each write to the next node that is up and
/// keeping every answer that was not a 200.
struct Writers {
    stop: Arc<AtomicBool>,
    bases: Arc<Mutex<Vec<Option<String>>>>,
    refused: Arc<Mutex<Vec<String>>>,
    tasks: Vec<tokio::task::JoinHandle<(String, Vec<i64>)>>,
}

impl Writers {
    fn start(bases: Vec<Option<String>>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let bases = Arc::new(Mutex::new(bases));
        let refused = Arc::new(Mutex::new(Vec::new()));
        let tasks = tables()
            .into_iter()
            .map(|table| {
                let (stop, bases, refused) = (stop.clone(), bases.clone(), refused.clone());
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
                        match answered {
                            Ok(response) if response.status() == 200 => {
                                let _ = response.bytes().await;
                                acked.push(id);
                            }
                            Ok(response) => {
                                let route = response
                                    .headers()
                                    .get("x-walleye-route-error")
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("-")
                                    .to_owned();
                                let status = response.status();
                                let body = response.text().await.unwrap_or_default();
                                refused.lock().unwrap().push(format!(
                                    "{table} {id} via {base}: {status} {route} {}",
                                    body.chars().take(200).collect::<String>()
                                ));
                            }
                            Err(error) => {
                                refused
                                    .lock()
                                    .unwrap()
                                    .push(format!("{table} {id} via {base}: {error}"));
                            }
                        }
                        id += 1;
                        tokio::time::sleep(Duration::from_millis(40)).await;
                    }
                    (table, acked)
                })
            })
            .collect();
        Self {
            stop,
            bases,
            refused,
            tasks,
        }
    }
    fn set(&self, index: usize, base: Option<String>) {
        self.bases.lock().unwrap()[index] = base;
    }
    async fn finish(self) -> (BTreeMap<String, Vec<i64>>, Vec<String>) {
        self.stop.store(true, Ordering::Release);
        let mut acked = BTreeMap::new();
        for task in self.tasks {
            let (table, ids) = task.await.unwrap();
            acked.insert(table, ids);
        }
        let refused = self.refused.lock().unwrap().clone();
        (acked, refused)
    }
}

async fn cluster(launch: &Launch, cache: &std::path::Path) -> Vec<Option<Proc>> {
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

/// Until the node at `base` holds one of the tables.
async fn holding_tables(base: &str) {
    wait_for(
        "the node to hold a table",
        Duration::from_secs(30),
        || async {
            let holding = held(base).await;
            tables().iter().any(|table| holding.contains_key(table))
        },
    )
    .await;
}

/// Restart `victim` gracefully: clients stop being sent to it, as a proxy
/// stops routing to a Machine that is stopping, then it hands over and
/// stops, and a new process on the same volume starts and is routed to
/// again. Peers go on forwarding to whichever owner they see throughout.
async fn restart(
    launch: &Launch,
    procs: &mut [Option<Proc>],
    writers: &Writers,
    victim: usize,
    cache: &std::path::Path,
) {
    holding_tables(&procs[victim].as_ref().unwrap().base).await;
    writers.set(victim, None);
    procs[victim].take().unwrap().stop().await;
    procs[victim] = Some(launch.start(victim, cache, fast()).await);
    writers.set(victim, Some(procs[victim].as_ref().unwrap().base.clone()));
    // Long enough for the peers to hand the new process its share.
    tokio::time::sleep(Duration::from_secs(4)).await;
}

async fn check(procs: &[Option<Proc>], writers: Writers) {
    let (acked, refused) = writers.finish().await;
    assert!(
        refused.is_empty(),
        "{} writes were refused: {:#?}",
        refused.len(),
        &refused[..refused.len().min(10)]
    );
    let base = &procs[0].as_ref().unwrap().base;
    for (table, ids) in &acked {
        exactly_once(base, table, ids).await;
    }
}

/// The same node restarted gracefully twice, writes forwarded throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_graceful_restart_refuses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let launch = Launch::under(
        dir.path(),
        Conditions {
            archive: Duration::from_millis(20),
            peer: Duration::from_millis(2),
        },
    );
    let mut procs = cluster(&launch, cache.path()).await;
    let writers = Writers::start(bases(&procs));
    tokio::time::sleep(Duration::from_secs(2)).await;
    for _ in 0..2 {
        restart(&launch, &mut procs, &writers, 1, cache.path()).await;
    }
    check(&procs, writers).await;
    for p in procs.into_iter().flatten() {
        p.stop().await;
    }
}

/// Every node restarted gracefully in turn, as an upgrade rolls a cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_roll_refuses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let launch = Launch::under(
        dir.path(),
        Conditions {
            archive: Duration::from_millis(20),
            peer: Duration::from_millis(2),
        },
    );
    let mut procs = cluster(&launch, cache.path()).await;
    let writers = Writers::start(bases(&procs));
    tokio::time::sleep(Duration::from_secs(2)).await;
    for victim in 0..3 {
        restart(&launch, &mut procs, &writers, victim, cache.path()).await;
    }
    check(&procs, writers).await;
    for p in procs.into_iter().flatten() {
        p.stop().await;
    }
}
