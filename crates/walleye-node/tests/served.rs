//! A cluster that says every table is served must serve every table on the
//! first request. After a clean stop and a start of every process, ownership
//! moves for a while: nobody sweeps until it has judged the leases it found,
//! the first sweeps claim, and a process that claimed more than its share
//! hands keys back one sweep at a time. `/readyz?require=all&tables=served`
//! answers ready only once that has stopped, so a caller that waits for it on
//! every node can call the cluster running and mean it.
mod common;

use common::*;
use std::time::{Duration, Instant};

async fn served(base: &str) -> (u16, serde_json::Value) {
    let answer = client()
        .get(format!("{base}/readyz?require=all&tables=served"))
        .send()
        .await
        .unwrap();
    let status = answer.status().as_u16();
    (status, answer.json().await.unwrap_or_default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_restarted_cluster_serves_every_table_once_every_node_says_so() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let lease = fast();
    let tables: Vec<String> = (0..9).map(|i| format!("s{i}")).collect();

    let mut procs = Vec::new();
    for id in ["a", "b", "c"] {
        procs.push(Proc::start(id, &root, cache.path(), None, lease.clone()).await);
    }
    for table in &tables {
        define(&procs[0].base, table).await;
    }
    for (index, table) in tables.iter().enumerate() {
        assert_eq!(write(&procs[index % 3].base, table, 1).await.status, 200);
    }
    let bases: Vec<&str> = procs.iter().map(|p| p.base.as_str()).collect();
    settled_spread(&bases, &tables, Duration::from_secs(60)).await;

    // A clean stop of every process: each releases its tables and retires its
    // lease, which is what a drained stop of a cluster does.
    for p in procs {
        p.stop().await;
    }

    for round in 0..3 {
        let mut procs = Vec::new();
        for id in ["a", "b", "c"] {
            procs.push(Proc::start(id, &root, cache.path(), None, lease.clone()).await);
        }
        let bases: Vec<String> = procs.iter().map(|p| p.base.clone()).collect();

        // Straight after the start nobody has judged the bucket yet, and says
        // so rather than claiming to serve.
        let (status, body) = served(&bases[0]).await;
        assert_eq!(
            status, 503,
            "round {round}: ready before ownership settled: {body}"
        );
        assert!(
            body["reason"]
                .as_str()
                .is_some_and(|reason| !reason.is_empty()),
            "{body}"
        );

        // Wait until every node says every table is served.
        let started = Instant::now();
        let mut reasons = std::collections::BTreeSet::new();
        loop {
            let mut all = true;
            for base in &bases {
                let (status, body) = served(base).await;
                if status != 200 {
                    all = false;
                    if let Some(reason) = body["reason"].as_str() {
                        reasons.insert(reason.split(':').next().unwrap_or(reason).to_owned());
                    }
                }
            }
            if all {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "round {round}: never served: {reasons:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Then the first request for every table, through every node, is
        // answered: no owner still moving, no table unowned, no writer shut.
        for (index, table) in tables.iter().enumerate() {
            for base in &bases {
                let answer = post(
                    base,
                    "/v1/query",
                    serde_json::json!({"sql": format!("SELECT count(*) AS n FROM {table}")}),
                    false,
                )
                .await;
                assert_eq!(
                    answer.status, 200,
                    "round {round}: first query for {table} via {base} (index {index}): {}",
                    answer.body
                );
            }
        }
        for p in procs {
            p.stop().await;
        }
    }
}
