//! A stopping owner hands its tables to a peer, measured from the side of a
//! client that keeps writing through the peer. Its own binary, so the gap it
//! measures is the handover's and not the machine's: cargo runs test binaries
//! one after another.
mod common;

use common::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use walleye_node::LeaseConfig;

/// A stopping owner flushes, releases and leaves, and a writer that keeps
/// going through its peer sees the table move with no gap longer than one
/// sampling interval and no write lost.
async fn a_stopping_owner_hands_over(bitr_url: Option<&str>) {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let lease = LeaseConfig::default();
    let a = Proc::start("a", &root, cache.path(), bitr_url, lease.clone()).await;
    let b = Proc::start("b", &root, cache.path(), bitr_url, lease.clone()).await;
    let tables: Vec<String> = (0..4).map(|i| format!("h{i}")).collect();
    for table in &tables {
        define(&a.base, table).await;
    }
    let a_session = session(&a.base).await;
    let b_session = session(&b.base).await;

    // One writer per table, through B, as fast as it is answered.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut writers = Vec::new();
    for table in tables.clone() {
        let base = b.base.clone();
        let stop = stop.clone();
        let started = started.clone();
        writers.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            let mut owners = Vec::new();
            let mut last: Option<(Instant, String)> = None;
            let mut widest = (Duration::ZERO, String::new());
            let mut refused = Vec::new();
            let mut id = 0_i64;
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                let answer = write(&base, &table, id).await;
                if answer.status == 200 {
                    // From the first acknowledgement: the first write pays
                    // for the table's open, which is not a handover.
                    if let Some((at, owner)) = &last
                        && at.elapsed() > widest.0
                    {
                        widest = (at.elapsed(), format!("{owner} -> {}", answer.owner));
                    }
                    last = Some((Instant::now(), answer.owner.clone()));
                    acked.push(id);
                    if owners.is_empty() {
                        started.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    }
                    if owners.last() != Some(&answer.owner) {
                        owners.push(answer.owner.clone());
                    }
                } else {
                    refused.push((id, answer.status, answer.route_error, answer.body));
                }
                id += 1;
            }
            (table, acked, owners, widest, refused)
        }));
    }
    // Every writer is landing rows on A before A is told to stop.
    let waiting = Instant::now();
    while started.load(std::sync::atomic::Ordering::Acquire) < tables.len() {
        assert!(
            waiting.elapsed() < Duration::from_secs(30),
            "the writers never started"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let stopping = Instant::now();
    a.stop().await;
    let released = stopping.elapsed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, std::sync::atomic::Ordering::Release);

    let mut widest_overall = Duration::ZERO;
    for writer in writers {
        let (table, acked, owners, widest, refused) = writer.await.unwrap();
        println!(
            "  {table}: {} writes, widest gap {:.0} ms ({})",
            acked.len(),
            widest.0.as_secs_f64() * 1000.0,
            widest.1
        );
        let widest = widest.0;
        widest_overall = widest_overall.max(widest);
        assert!(refused.is_empty(), "{table} refused writes: {refused:?}");
        assert_eq!(
            owners,
            vec![a_session.clone(), b_session.clone()],
            "{table} moved from A to B once"
        );
        exactly_once(&b.base, &table, &acked).await;
    }
    println!(
        "  graceful handover of {} tables: release took {:.0} ms; widest gap between \
         acknowledged writes {:.0} ms (sample interval {} ms)",
        tables.len(),
        released.as_secs_f64() * 1000.0,
        widest_overall.as_secs_f64() * 1000.0,
        lease.sample_ms
    );
    assert!(
        widest_overall < Duration::from_millis(lease.sample_ms),
        "a gap of {widest_overall:?} is longer than one sampling interval"
    );
    assert_eq!(held(&b.base).await.len(), tables.len());
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopping_owner_hands_over_on_the_object_store_log() {
    a_stopping_owner_hands_over(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopping_owner_hands_over_on_the_bitr_log() {
    let logs = tempfile::tempdir().unwrap();
    let (gateway, tasks) = bitr(logs.path()).await;
    a_stopping_owner_hands_over(Some(&gateway)).await;
    for task in tasks {
        task.abort();
    }
}
