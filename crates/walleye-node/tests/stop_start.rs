//! A launch instance stopped and started again, over and over, with tables
//! handed between its nodes as they come and go, serves every table after
//! every start, with every acknowledged row.
//!
//! Stopping an instance stops all three Machines at once and then deletes
//! their volumes; starting it makes fresh ones, whose replicas seed from the
//! archive. While the three drain together each one's tables are handed to
//! the others, which claim them - writing to the log as they do - just before
//! they stop too. Whatever the log committed after the last archive pass was
//! lost with the volumes, while the tables' own manifests remembered the
//! positions, and a table whose manifest had seen a position the log no
//! longer held could never be opened again.
mod common;

use common::*;
use std::time::{Duration, Instant};

const TABLES: usize = 6;
const CYCLES: usize = 6;

fn tables() -> Vec<String> {
    (0..TABLES).map(|i| format!("t{i}")).collect()
}

/// Every table answers a read with exactly the rows acknowledged into it,
/// through some node, within `limit`.
async fn every_table_serves(bases: &[String], acknowledged: &[Vec<i64>], limit: Duration) {
    for (table, ids) in tables().iter().zip(acknowledged) {
        let started = Instant::now();
        let mut last = String::new();
        loop {
            let base = &bases[started.elapsed().as_millis() as usize % bases.len()];
            let answer = post(
                base,
                "/v1/query",
                serde_json::json!({"sql": format!("SELECT id FROM {table} ORDER BY id")}),
                false,
            )
            .await;
            if answer.status == 200 {
                let found: Vec<i64> = answer
                    .body
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["id"].as_i64().unwrap())
                    .collect();
                assert_eq!(&found, ids, "{table} does not hold exactly its rows");
                break;
            }
            last = format!("{} {}", answer.status, answer.body);
            assert!(
                started.elapsed() < limit,
                "{table} never served again: {last}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let _ = last;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stopping_and_starting_an_instance_never_leaves_a_table_unopenable() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let launch = Launch::under(
        dir.path(),
        Conditions {
            archive: Duration::from_millis(40),
            peer: Duration::from_millis(2),
        },
    );
    let mut procs = Vec::new();
    for index in 0..3 {
        procs.push(launch.start(index, cache.path(), fast()).await);
    }
    for table in tables() {
        define(&procs[0].base, &table).await;
    }
    let mut acknowledged: Vec<Vec<i64>> = vec![Vec::new(); TABLES];
    let mut next = 0_i64;
    for cycle in 1..=CYCLES {
        // A write to every table, as the instance was used between stops.
        for (index, table) in tables().iter().enumerate() {
            let base = &procs[index % procs.len()].base;
            for _ in 0..40 {
                let answer = write(base, table, next).await;
                if answer.status == 200 {
                    acknowledged[index].push(next);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            next += 1;
        }
        // Stop: all three at once, then their volumes go.
        let stopping: Vec<_> = procs
            .drain(..)
            .map(|proc| tokio::spawn(proc.stop()))
            .collect();
        for stop in stopping {
            stop.await.unwrap();
        }
        for index in 0..3 {
            launch.wipe(index);
        }
        // Start: fresh volumes, one node after another as Machines boot.
        for index in 0..3 {
            procs.push(launch.start(index, cache.path(), fast()).await);
        }
        let bases: Vec<String> = procs.iter().map(|proc| proc.base.clone()).collect();
        every_table_serves(&bases, &acknowledged, Duration::from_secs(45)).await;
        println!("  cycle {cycle}: every table served with all its rows");
    }
    for proc in procs {
        proc.stop().await;
    }
}
