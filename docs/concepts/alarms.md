# Alarms and schedules

Everything in Walleye that runs at a time rather than when a request arrives
runs on an alarm: a view on a clock, a worker that wakes itself, and the retry
of a view pass that failed. There is no other scheduler.

## Where an alarm lives

An alarm belongs to a key: a table, or a view that has no source table. A
view's alarms belong to its source table, or to the view itself when it reads
nothing. Each key has one owner at a time (see
[a cluster](../self-hosting/cluster.md#ownership-in-the-bucket)), and its
alarms are stored in its ownership record in the bucket. So:

- only the owner fires them;
- a process that takes the key over - after a stop, a crash or a restart -
  finds them in the record it claims and fires them;
- nothing about an alarm is held only in memory.

## Firing

A firing is two writes to the key's record around the handler. The first
records the attempt and moves the alarm to the time it should be retried if
nothing reports back; only the current owner can make that write, so two
processes never begin the same firing. The second clears a one-shot alarm, or
moves a schedule to its next occurrence.

Delivery is at least once. If the owner dies after the handler ran and before
the second write, the next owner fires it again, and the attempt count says
so. Write handlers so a repeat is harmless: rows with the same content, or the
same primary key, are stored once.

A handler that fails is retried after 2 seconds, then 4, doubling to at most
128 seconds. After 7 failed attempts a one-shot alarm is dropped and a
schedule moves on to its next occurrence. A retry never delays a schedule's
next occurrence. An alarm set for a time already past fires at once.

## Schedules

A view with no source runs on `every_seconds` or on a five-field `cron` (UTC,
minute resolution). An interval runs as soon as the view is defined and then
on its interval; a cron runs at its next named time.

If a schedule could not run for a while - its owner was down, or it was
between owners - it runs once when it can, for the most recent time it
missed, rather than once for every time. The worker sees which time it
stands for and how many it covered:

```js
export default (rows, ctx) => [{
  at: ctx.scheduled.scheduledTime, // ms since the epoch
  covered: ctx.scheduled.missed,   // earlier occurrences this run stands for
}]
```

`POST /v1/view/<name>/refresh/` on a view with no source runs it only if its
schedule is due.

## A worker's own alarm

A worker can wake itself, as a Durable Object does:

```js
export default {
  batch(rows, ctx) {
    ctx.setAlarm(Date.now() + 60_000); // or a Date
    return rows;
  },
  alarm(info, ctx) {
    // info.scheduledTime, info.attempt (1 on the first try), info.retryCount
    ctx.write('reminders', { at: info.scheduledTime });
  },
};
```

`ctx.getAlarm()` answers when it is set to fire, or `null`, and
`ctx.deleteAlarm()` cancels it. A worker has one alarm; setting it again
replaces it. The change takes effect only if the turn that made it finishes
without throwing. An `alarm` handler that sets no new alarm leaves none.

## Seeing them

`POST /v1/view/<name>/describe/` includes, when they are pending,
`next_run` (a schedule's next occurrence), `worker_alarm` and `retry`, each
with `at_ms`, `scheduled_ms` and `attempt`. It answers the same on every node.

`GET /internal/alarms`, with the deployment token, lists every alarm pending
on the keys the node owns.
