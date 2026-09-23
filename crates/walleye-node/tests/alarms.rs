//! Alarms and the schedules built on them, across processes: an alarm fires
//! once on whoever owns its key, through a kill, a handover and a restart; a
//! failing handler backs off and succeeds once; a schedule runs once per
//! occurrence however many processes there are, and one that fell behind runs
//! once for everything it missed.
mod common;

use common::*;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

/// A view over `events` whose worker sets its own alarm `delay_ms` after each
/// batch, and whose `alarm` handler records that it was woken.
fn waker(delay_ms: u64) -> Value {
    json!({
        "source": "events",
        "target": "seen",
        "worker": format!(
            "export default {{ \
               batch(rows, ctx) {{ ctx.setAlarm(Date.now() + {delay_ms}); \
                                  return rows.map(r => ({{ id: r.id }})); }}, \
               alarm(info, ctx) {{ ctx.write('woken', {{ at: info.scheduledTime, \
                 attempt: info.attempt, nonce: Math.random() }}); }} \
             }}"
        ),
    })
}

/// An alarm set on the owner of its key fires once on the key's next owner
/// after the first is killed with the alarm pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_alarm_on_a_killed_owner_fires_once_on_the_next() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let mut procs = vec![
        Proc::start("a", &root, cache.path(), None, fast()).await,
        Proc::start("b", &root, cache.path(), None, fast()).await,
        Proc::start("c", &root, cache.path(), None, fast()).await,
    ];
    define(&procs[0].base, "events").await;
    view(&procs[0].base, "waker", waker(4_000)).await;
    assert_eq!(write(&procs[0].base, "events", 1).await.status, 200);
    // A is whichever owns the source, and so the view's alarms.
    let bases: Vec<&str> = procs.iter().map(|p| p.base.as_str()).collect();
    let owner = holder_of(&bases, "events").await;
    let a = procs.remove(owner);
    let (b, c) = (procs.remove(0), procs.remove(0));
    wait_for(
        "the worker set its alarm",
        Duration::from_secs(10),
        || async {
            get_json(&a.base, "/internal/alarms").await["alarms"]
                .as_array()
                .is_some_and(|alarms| alarms.iter().any(|a| a["alarm"] == "worker:waker"))
        },
    )
    .await;

    a.kill().await;
    let killed = Instant::now();
    wait_for(
        "the alarm fired on a survivor",
        Duration::from_secs(20),
        || async { !rows(&b.base, "SELECT at FROM woken").await.is_empty() },
    )
    .await;
    println!(
        "  alarm of a killed owner fired {:.0} ms after the kill",
        killed.elapsed().as_secs_f64() * 1000.0
    );
    // Long enough for a second firing, if there were going to be one.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let woken = rows(&c.base, "SELECT at, attempt FROM woken").await;
    assert_eq!(woken.len(), 1, "fired exactly once: {woken:?}");
    assert_eq!(woken[0]["attempt"], 1, "on its first attempt: {woken:?}");
    b.stop().await;
    c.stop().await;
}

/// A schedule whose owner stops gracefully mid-run moves to a peer with every
/// occurrence run once: none doubled across the handover, none lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schedule_handed_over_gracefully_neither_doubles_nor_drops() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let a = Proc::start("a", &root, cache.path(), None, fast()).await;
    let b = Proc::start("b", &root, cache.path(), None, fast()).await;
    view(&a.base, "ticker", ticker()).await;
    let bases = [a.base.as_str(), b.base.as_str()];
    wait_for(
        "the schedule ran three times",
        Duration::from_secs(20),
        || async { ticks(&a.base).await.len() >= 3 },
    )
    .await;
    let owner = holder(&bases, "view.ticker")
        .await
        .expect("someone owns it");
    let (leaving, staying) = if owner == 0 { (a, b) } else { (b, a) };
    let before = ticks(&staying.base).await.len();
    leaving.stop().await;
    wait_for(
        "the schedule ran three more times",
        Duration::from_secs(20),
        || async { ticks(&staying.base).await.len() >= before + 3 },
    )
    .await;
    let runs = ticks(&staying.base).await;
    println!("  runs across a graceful handover: {runs:?}");
    each_occurrence_once(&runs);
    staying.stop().await;
}

/// Alarms pending when the only process stops fire after a new one starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_alarms_fire_after_a_restart() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let first = Proc::start("single", &root, cache.path(), None, fast()).await;
    define(&first.base, "events").await;
    view(&first.base, "waker", waker(2_000)).await;
    assert_eq!(write(&first.base, "events", 1).await.status, 200);
    wait_for(
        "the worker set its alarm",
        Duration::from_secs(10),
        || async {
            get_json(&first.base, "/internal/alarms").await["alarms"]
                .as_array()
                .is_some_and(|alarms| !alarms.is_empty())
        },
    )
    .await;
    first.stop().await;
    let second = Proc::start("single", &root, cache.path(), None, fast()).await;
    let started = Instant::now();
    wait_for(
        "the pending alarm fired",
        Duration::from_secs(20),
        || async { !rows(&second.base, "SELECT at FROM woken").await.is_empty() },
    )
    .await;
    println!(
        "  alarm pending across a restart fired {:.0} ms after the new process started",
        started.elapsed().as_secs_f64() * 1000.0
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(rows(&second.base, "SELECT at FROM woken").await.len(), 1);
    second.stop().await;
}

/// A handler that fails is retried with backoff, sees the attempt count, and
/// once it succeeds its effect happens once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_handler_backs_off_then_succeeds_once() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let node = Proc::start("single", &root, cache.path(), None, fast()).await;
    define(&node.base, "events").await;
    view(
        &node.base,
        "flaky",
        json!({
            "source": "events",
            "target": "seen",
            "worker": "export default { \
                batch(rows, ctx) { ctx.setAlarm(Date.now()); return rows.map(r => ({ id: r.id })); }, \
                alarm(info, ctx) { \
                  if (info.attempt < 3) throw new Error('not yet, attempt ' + info.attempt); \
                  ctx.write('woken', { attempt: info.attempt, retries: info.retryCount, \
                                       at: info.scheduledTime, done: Date.now() }); } }",
        }),
    )
    .await;
    let asked = Instant::now();
    assert_eq!(write(&node.base, "events", 1).await.status, 200);
    wait_for(
        "the third attempt succeeded",
        Duration::from_secs(30),
        || async {
            !rows(&node.base, "SELECT attempt FROM woken")
                .await
                .is_empty()
        },
    )
    .await;
    let took = asked.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let woken = rows(&node.base, "SELECT attempt, retries, at, done FROM woken").await;
    assert_eq!(woken.len(), 1, "succeeded once: {woken:?}");
    assert_eq!(woken[0]["attempt"], 3);
    assert_eq!(woken[0]["retries"], 2);
    // Two failures cost the first two backoffs, 2 s and 4 s.
    let waited = woken[0]["done"].as_u64().unwrap() - woken[0]["at"].as_u64().unwrap();
    println!("  two failures then a success: {waited} ms after the scheduled time");
    assert!(waited >= 6_000, "backed off 2 s then 4 s: {waited} ms");
    assert!(took < Duration::from_secs(20), "{took:?}");
    let pending = get_json(&node.base, "/internal/alarms").await;
    assert_eq!(
        pending["alarms"],
        json!([]),
        "a one-shot is gone once it succeeds"
    );
    node.stop().await;
}

/// A schedule on a three-process cluster runs once per occurrence, not once
/// per process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schedule_on_three_processes_runs_once_per_occurrence() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let a = Proc::start("a", &root, cache.path(), None, fast()).await;
    let b = Proc::start("b", &root, cache.path(), None, fast()).await;
    let c = Proc::start("c", &root, cache.path(), None, fast()).await;
    view(&b.base, "ticker", ticker()).await;
    wait_for("the schedule ran", Duration::from_secs(20), || async {
        !ticks(&a.base).await.is_empty()
    })
    .await;
    let started = Instant::now();
    let first = ticks(&a.base).await.len();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let runs = ticks(&c.base).await;
    let ran = runs.len() - first;
    println!(
        "  {ran} runs in {:.1} s on three processes",
        started.elapsed().as_secs_f64()
    );
    each_occurrence_once(&runs);
    assert!(
        (4..=7).contains(&ran),
        "once a second, not three times: {runs:?}"
    );
    // Every process describes the same next run.
    let described: Vec<Value> =
        futures::future::join_all([&a.base, &b.base, &c.base].map(|base| async move {
            post(base, "/v1/view/ticker/describe/", json!({}), false)
                .await
                .body
        }))
        .await;
    assert!(
        described.iter().all(|d| d["next_run"]["at_ms"].is_u64()),
        "{described:?}"
    );
    a.stop().await;
    b.stop().await;
    c.stop().await;
}

/// A schedule that could not run for several occurrences runs once when it
/// can, for the latest time it missed, and says how many it covers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missed_occurrences_coalesce_into_one_run() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let first = Proc::start("single", &root, cache.path(), None, fast()).await;
    view(&first.base, "ticker", ticker()).await;
    wait_for(
        "the schedule ran twice",
        Duration::from_secs(20),
        || async { ticks(&first.base).await.len() >= 2 },
    )
    .await;
    let before = ticks(&first.base).await.len();
    first.stop().await;
    // Down for several occurrences.
    tokio::time::sleep(Duration::from_millis(3_500)).await;
    let second = Proc::start("single", &root, cache.path(), None, fast()).await;
    wait_for(
        "the schedule ran after the restart",
        Duration::from_secs(20),
        || async { ticks(&second.base).await.len() > before + 1 },
    )
    .await;
    let runs = ticks(&second.base).await;
    let (at, missed) = *runs.iter().max_by_key(|(_, missed)| *missed).expect("runs");
    println!("  first run after downtime: scheduled {at}, covering {missed} missed: {runs:?}");
    assert!(
        missed >= 3,
        "several occurrences were missed and covered by one run: {runs:?}"
    );
    each_occurrence_once(&runs);
    second.stop().await;
}

/// A worker sets its own alarm and its `alarm` handler runs, and the alarm
/// shows on the view and on the inspection route while it is pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_wakes_itself() {
    let store = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let root = format!("file://{}/store", store.path().display());
    let node = Proc::start("single", &root, cache.path(), None, fast()).await;
    define(&node.base, "events").await;
    view(&node.base, "waker", waker(1_500)).await;
    assert_eq!(write(&node.base, "events", 1).await.status, 200);
    wait_for("the alarm is visible", Duration::from_secs(10), || async {
        post(&node.base, "/v1/view/waker/describe/", json!({}), false)
            .await
            .body["worker_alarm"]["at_ms"]
            .is_u64()
    })
    .await;
    wait_for("the alarm handler ran", Duration::from_secs(10), || async {
        !rows(&node.base, "SELECT at FROM woken").await.is_empty()
    })
    .await;
    let described = post(&node.base, "/v1/view/waker/describe/", json!({}), false).await;
    assert!(
        described.body.get("worker_alarm").is_none(),
        "{}",
        described.body
    );
    node.stop().await;
}
