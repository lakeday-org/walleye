# Lifecycle

Two clocks run in a Walleye deployment: a table's, and a node's. Neither one
needs managing, and knowing what each does explains most of what you will
otherwise read as a surprise.

## A table opens, closes, and reopens

A table opens when something touches it, and stays open while it is being used.
Open costs memory — see [cache tiers](cache-tiers.md) — so a table that has
gone five minutes without a query or an insert is checkpointed to object
storage, closed, and its memory returned. The next use reopens it. A sweeper
does the closing once a minute and skips any table that is busy, so the idle
timeout is five minutes and the close lands five to six minutes after the last
use.

Nothing is lost in that. A checkpoint is durable before the close, and a reopen
reads it back. The only cost is the reopen itself, paid by whichever request
arrives first.

The consequence worth planning for is the one in the other direction: a
workload that touches every table every minute never lets one go idle, so
nothing is ever released.

## Flushes and compaction

Rows arrive into a memtable and are flushed as a generation. Every append
checks, and once eight generations exist they are merged in the background —
keeping the newest row per key and rebuilding the indexes — so query fan-out
stays bounded. The merge runs detached from the append that noticed it, and at
most once every ten seconds per table.

That is the whole schedule. There is no compaction you have to run and no
maintenance window. You can force a flush now, and you can force a merge now,
but forcing a merge is not the same operation the background schedule runs:
`compact_lsm/` merges from two generations, not eight, so it will do work the
schedule would have left alone.

```sh
curl -sX POST localhost:8080/v1/table/clicks/flush_lsm/ \
  -H "authorization: Bearer $TOKEN"
curl -sX POST localhost:8080/v1/table/clicks/compact_lsm/ \
  -H "authorization: Bearer $TOKEN"
curl -sX POST localhost:8080/v1/table/clicks/get_lsm_stats/ \
  -H "authorization: Bearer $TOKEN"
```

`get_lsm_stats/` lists the generations with their row counts and index names,
which is the honest way to find out whether compaction is keeping up.

## A node stops and starts

Stopping a node cleanly finishes what is in flight, checkpoints every table it
owns and hands each one over before the process exits, so another node can
serve it at once. Starting it again brings the tables back from
object storage and rebuilds the cache as queries arrive: correct immediately,
warm shortly after.

Because the cache is disposable and every acknowledged write is already
durable, a stop followed by a start on a different machine size loses nothing.
That is all a resize is.

In a cluster, roll one node at a time and watch `/readyz?require=all`. A quorum
of two of three means the other two go on acknowledging writes while one is
away, and the node being restarted hands its tables to them as it stops, so a
client sees at most a retried request. The restarted node comes back owning
nothing and takes tables only as other nodes leave. Waiting for every member
to be serving again before taking the next node out keeps two replicas
answering throughout. [Losing a node](shapes.md#losing-a-node) is the same
handover without the plan.

On one node, an upgrade can start the replacement beside the original. The
replacement forwards everything to the original until the original is stopped
and hands its tables over, then serves them itself. Set
`WALLEYE_ADVERTISE_URL` so the replacement can reach the original.

## Tokens

Clients use [access tokens](../sdk/http.md#access-tokens), which come and go
without touching the node: revoking one takes effect within a few seconds and
restarts nothing. Give each client its own, with only the scopes it uses, and
revoke the one you no longer trust rather than all of them.

The deployment token is the node's own. Changing it is a restart, and on more
than one node the cutover is not instantaneous: for a short window which token
a request is accepted with depends on which node served it.

## Deleting

Dropping a table deletes its catalog entry and its Lance data. It is not
recoverable and there is no undo, because there is no backup that Walleye keeps
for you — [what you own](../self-hosting/operating.md) says so plainly.

In a cluster the replication log's archive is a separate prefix in its own
bucket, and a drop does not touch it. Those segments are yours to age out with
the bucket's own lifecycle rules.

That archive is the **only** thing a lifecycle rule may touch. The tables'
own prefix contains the write-ahead log, and expiring any of it destroys
acknowledged rows without reporting anything.
[What you own](../self-hosting/operating.md) explains what happens and why the
rule admits no exceptions.

## On the managed service

The same clocks, with the node half driven through an API instead of a process
manager: `stop` checkpoints every table and releases the nodes, `start` brings
them back on the current shape and cache, `resize` is accepted while stopped,
and `delete` removes the instance and its storage. A stopped instance keeps its
data and runs no nodes. Access tokens are made and revoked separately, in any
state, and none of that restarts anything.

What a table does is identical, because it is the same engine.
