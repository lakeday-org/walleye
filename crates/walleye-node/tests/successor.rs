//! A process replacing another of the same node - a ramp upgrade - takes each
//! table over from it the moment the original releases it, against a bucket
//! with an object store's round trips rather than a local disk's.
//!
//! The original owns every table and keeps serving them while its successor
//! starts beside it. Writers go through the successor, which forwards to the
//! original, as the proxy does once the successor is routed. Then the
//! original stops and releases everything at once. What a write waits for
//! from there is the successor's open of each table, so that is measured
//! per table: from the moment the bucket shows the table released to the
//! first write the successor acknowledges on it.
//!
//! Needs `minio` on the PATH:
//! `cargo test -p walleye-node --test successor -- --ignored --nocapture`.
mod common;

use common::*;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
};
use object_store::{ObjectStoreExt, path::Path};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const BUCKET: &str = "walleye-test";
/// One way, so a round trip costs twice this: an in-region object store.
const ONE_WAY: Duration = Duration::from_millis(5);
const TABLES: usize = 6;
/// Measured at 0.86 to 1.06 s in a debug build over the relay, where it was
/// 1.37 to 1.95 s before a successor prepared its tables and inherited the
/// arrival number; this leaves room for a busy machine and none for a
/// return to opening from nothing.
const TAKEOVER_OVER_A_BUCKET: Duration = Duration::from_millis(1_500);

/// Every write each table acknowledged: when, by which process, which id.
type Acks = Arc<Mutex<BTreeMap<String, Vec<(Instant, String, i64)>>>>;

/// A MinIO server over a fresh directory with the bucket already in it.
struct Minio {
    child: std::process::Child,
    address: std::net::SocketAddr,
    _dir: tempfile::TempDir,
}
impl Minio {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(BUCKET)).unwrap();
        let address = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let child = std::process::Command::new("minio")
            .args(["server", "--quiet", "--address", &address.to_string()])
            .arg(dir.path())
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("minio on the PATH");
        wait_for("minio ready", Duration::from_secs(30), || async {
            client()
                .get(format!("http://{address}/minio/health/ready"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
        })
        .await;
        Self {
            child,
            address,
            _dir: dir,
        }
    }
}
impl Drop for Minio {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

/// A TCP relay that holds every chunk for `one_way` in each direction before
/// passing it on, without limiting throughput: a delay line, not a throttle.
async fn delayed(upstream: std::net::SocketAddr, one_way: Duration) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((inbound, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(outbound) = TcpStream::connect(upstream).await else {
                    return;
                };
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (in_read, in_write) = inbound.into_split();
                let (out_read, out_write) = outbound.into_split();
                tokio::join!(
                    pump(in_read, out_write, one_way),
                    pump(out_read, in_write, one_way)
                );
            });
        }
    });
    address
}
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    delay: Duration,
) {
    let (chunks, mut queued) = tokio::sync::mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    let writer = tokio::spawn(async move {
        while let Some((at, bytes)) = queued.recv().await {
            tokio::time::sleep_until((at + delay).into()).await;
            if to.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = to.shutdown().await;
    });
    let mut buffer = vec![0; 64 * 1024];
    while let Ok(n) = from.read(&mut buffer).await {
        if n == 0 || chunks.send((Instant::now(), buffer[..n].to_vec())).is_err() {
            break;
        }
    }
    drop(chunks);
    let _ = writer.await;
}

fn storage(endpoint: &str) -> ObjectStoreParams {
    let options: HashMap<String, String> = [
        ("aws_access_key_id", "minioadmin"),
        ("aws_secret_access_key", "minioadmin"),
        ("aws_region", "us-east-1"),
        ("aws_endpoint", endpoint),
        ("allow_http", "true"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();
    ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            options,
        ))),
        ..Default::default()
    }
}

/// The node an ownership record names, empty once released.
async fn record_holder(store: &ObjectStore, root: &Path, table: &str) -> Option<String> {
    use futures::TryStreamExt;
    let dir = root.clone().join("_walleye").join("own").join(table);
    let listed: Vec<_> = store.inner.list(Some(&dir)).try_collect().await.ok()?;
    let newest = listed
        .iter()
        .filter_map(|meta| {
            let name = meta.location.filename()?.strip_suffix(".json")?;
            Some((name.parse::<u64>().ok()?, meta.location.clone()))
        })
        .max_by_key(|(seq, _)| *seq)?;
    let bytes = store.inner.get(&newest.1).await.ok()?.bytes().await.ok()?;
    let record: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    record["node"].as_str().map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs minio on the PATH"]
async fn a_successor_takes_each_table_over_as_soon_as_it_is_released() {
    let minio = Minio::start().await;
    let relay = delayed(minio.address, ONE_WAY).await;
    // The processes read their bucket credentials from the environment, as
    // the binary does; nothing else in this test binary reads it.
    for (name, value) in [
        ("AWS_ACCESS_KEY_ID", "minioadmin".to_owned()),
        ("AWS_SECRET_ACCESS_KEY", "minioadmin".to_owned()),
        ("AWS_REGION", "us-east-1".to_owned()),
        ("AWS_ENDPOINT", format!("http://{relay}")),
        ("AWS_ALLOW_HTTP", "true".to_owned()),
    ] {
        // SAFETY: set before any process starts, while this test is the only
        // thread that reads the environment.
        unsafe { std::env::set_var(name, value) };
    }
    let root = format!("s3://{BUCKET}/store");
    let direct = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        &root,
        &storage(&format!("http://{}", minio.address)),
    )
    .await
    .unwrap();
    let worst = replace_the_only_node(&root, direct).await;
    assert!(
        worst < TAKEOVER_OVER_A_BUCKET,
        "a table waited {worst:?} for its successor after it was released"
    );
}

/// The same on a local disk, where nothing but the takeover's own work is
/// left to wait for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_successor_takes_each_table_over_on_a_local_disk() {
    let store = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let direct = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        &root,
        &ObjectStoreParams::default(),
    )
    .await
    .unwrap();
    let worst = replace_the_only_node(&root, direct).await;
    assert!(
        worst < Duration::from_secs(1),
        "a table waited {worst:?} for its successor after it was released"
    );
}

/// Start a node's replacement beside it, keep writing through the
/// replacement, stop the original, and return the longest any table waited
/// from its release to the replacement's first acknowledged write on it.
/// `direct` reads the bucket without any added delay.
async fn replace_the_only_node(root: &str, (direct, prefix): (Arc<ObjectStore>, Path)) -> Duration {
    let cache = tempfile::tempdir().unwrap();
    let lease = fast();

    let original = Proc::start("single", root, cache.path(), None, lease.clone()).await;
    let tables: Vec<String> = (0..TABLES).map(|i| format!("t{i}")).collect();
    for table in &tables {
        define(&original.base, table).await;
    }
    // History worth opening: rows in the base and in flushed generations.
    for round in 0..3 {
        for table in &tables {
            for n in 0..20 {
                let id = -(round * 1000 + n) - 1;
                assert_eq!(write(&original.base, table, id).await.status, 200);
            }
            let flushed = post(
                &original.base,
                &format!("/v1/table/{table}/flush_lsm/"),
                serde_json::json!({}),
                false,
            )
            .await;
            assert_eq!(flushed.status, 200, "flush {table}: {}", flushed.body);
        }
    }
    let original_session = session(&original.base).await;

    // The replacement starts beside it, under the same node id.
    let successor = Proc::start("single", root, cache.path(), None, lease.clone()).await;
    let successor_session = session(&successor.base).await;
    assert!(held(&successor.base).await.is_empty());
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        held(&successor.base).await.is_empty(),
        "the successor took a key before the original stopped"
    );

    // One writer per table through the successor, which forwards.
    let stop = Arc::new(AtomicBool::new(false));
    let acks: Acks = Default::default();
    let mut writers = Vec::new();
    for table in tables.clone() {
        let (base, stop, acks) = (successor.base.clone(), stop.clone(), acks.clone());
        writers.push(tokio::spawn(async move {
            let mut refused = Vec::new();
            let mut id = 0_i64;
            while !stop.load(Ordering::Acquire) {
                let answer = write(&base, &table, id).await;
                if answer.status == 200 {
                    acks.lock()
                        .unwrap()
                        .entry(table.clone())
                        .or_default()
                        .push((Instant::now(), answer.owner, id));
                } else {
                    refused.push((id, answer.status, answer.body));
                }
                id += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            (table, refused)
        }));
    }
    // Every release as the bucket shows it, read without the relay's delay.
    let released: Arc<Mutex<HashMap<String, Instant>>> = Default::default();
    let watching = Arc::new(AtomicBool::new(true));
    let watcher = {
        let (released, watching, tables) = (released.clone(), watching.clone(), tables.clone());
        let original = original_session.clone();
        tokio::spawn(async move {
            while watching.load(Ordering::Acquire) {
                for table in &tables {
                    if released.lock().unwrap().contains_key(table) {
                        continue;
                    }
                    // Released, or already taken by the time it was read.
                    let holder = record_holder(&direct, &prefix, table).await;
                    if holder.is_some_and(|holder| holder != original) {
                        released
                            .lock()
                            .unwrap()
                            .insert(table.clone(), Instant::now());
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    let stopping = Instant::now();
    original.stop().await;
    let stop_took = stopping.elapsed();
    tokio::time::sleep(Duration::from_secs(4)).await;
    stop.store(true, Ordering::Release);
    watching.store(false, Ordering::Release);
    watcher.await.unwrap();

    let acks = acks.lock().unwrap().clone();
    let released = released.lock().unwrap().clone();
    let mut worst = Duration::ZERO;
    for writer in writers {
        let (table, refused) = writer.await.unwrap();
        assert!(refused.is_empty(), "{table} refused writes: {refused:?}");
        let written = &acks[&table];
        let owners: Vec<&String> = written.iter().fold(Vec::new(), |mut seen, (_, o, _)| {
            if seen.last() != Some(&o) {
                seen.push(o);
            }
            seen
        });
        assert_eq!(
            owners,
            vec![&original_session, &successor_session],
            "{table} moved once"
        );
        let at = released[&table];
        let first = written
            .iter()
            .find(|(_, owner, _)| *owner == successor_session)
            .map(|(when, _, _)| *when)
            .unwrap();
        let takeover = first.saturating_duration_since(at);
        let gap = written
            .windows(2)
            .map(|pair| pair[1].0 - pair[0].0)
            .max()
            .unwrap();
        println!(
            "  {table}: released {:.0} ms into the stop; first write on the successor {:.0} ms \
             after; widest gap between acknowledged writes {:.0} ms",
            at.saturating_duration_since(stopping).as_secs_f64() * 1000.0,
            takeover.as_secs_f64() * 1000.0,
            gap.as_secs_f64() * 1000.0,
        );
        worst = worst.max(takeover);
        let ids: Vec<i64> = written.iter().map(|(_, _, id)| *id).collect();
        exactly_once(&successor.base, &table, &ids).await;
    }
    println!(
        "  the original's stop took {:.0} ms; the slowest takeover after release {:.0} ms",
        stop_took.as_secs_f64() * 1000.0,
        worst.as_secs_f64() * 1000.0
    );
    successor.stop().await;
    worst
}
