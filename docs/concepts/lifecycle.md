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

Stopping a node cleanly finishes what is in flight and checkpoints every open
table before the process exits. Starting it again brings the tables back from
object storage and rebuilds the cache as queries arrive: correct immediately,
warm shortly after.

Because the cache is disposable and every acknowledged write is already
durable, a stop followed by a start on a different machine size loses nothing.
That is all a resize is.

In a cluster, roll one node at a time and watch `/readyz?require=all`. A quorum
of two of three means the other two go on acknowledging writes while one is
away — but ownership does not move, so the node you are restarting stops
answering for its own tables until it is back. A restart inside the
fifteen-second forward-retry budget is invisible to a client; one slower than
that is a 502 on that node's tables. Waiting for every member to be serving
again before taking the next node out is what keeps that to one node's share at
a time. [Losing a node](shapes.md#losing-a-node) is the same mechanism without
the plan.

## Rotating the token

The token is the deployment's, not a user's. Changing it revokes every client at
once, which is the point of it, and the cutover is not instantaneous on more
than one node: for a short window which token a request is accepted with
depends on which node served it, so a client should retry a 401 for that long
rather than treat it as a bad token.

## Deleting

Dropping a table deletes its catalog entry and its Lance data. It is not
recoverable and there is no undo, because there is no backup that Walleye keeps
for you — [what you own](../self-hosting/operating.md) says so plainly.

In a cluster the replication log's archive is a separate prefix in its own
bucket, and a drop does not touch it. Those segments are yours to age out with
the bucket's own lifecycle rules.

## On the managed service

The same clocks, with the node half driven through an API instead of a process
manager: `stop` checkpoints every table and releases the nodes, `start` brings
them back on the current shape and cache, `resize` is accepted while stopped,
`token` mints a new one, and `delete` removes the instance and its storage. A
stopped instance keeps its data and runs no nodes.

What a table does is identical, because it is the same engine.
