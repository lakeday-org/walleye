# Lifecycle

Two clocks run in a Walleye deployment: a table's, and a node's. Neither one
needs managing, and knowing what each does explains most of what you will
otherwise read as a surprise.

## A table opens, closes, and reopens

A table opens when something touches it, and stays open while it is being used.
Open costs memory — see [cache tiers](cache-tiers.md) — so a table that has
gone five minutes without a query or an insert is checkpointed to object
storage, closed, and its memory returned. The next use reopens it.

Nothing is lost in that. A checkpoint is durable before the close, and a reopen
reads it back. The only cost is the reopen itself, paid by whichever request
arrives first.

The consequence worth planning for is the one in the other direction: a
workload that touches every table every minute never lets one go idle, so
nothing is ever released.

## Flushes and compaction

Rows arrive into a memtable and are flushed as a generation. Once eight
generations exist they are merged in the background, keeping the newest row per
key and rebuilding the indexes, so query fan-out stays bounded.

That is the whole schedule. There is no compaction you have to run and no
maintenance window, although you can force either now:

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

In a cluster, roll one node at a time and watch `/readyz`. A quorum of two of
three means the other two keep serving while one is away, and the tables it
owned move to their next winner and move back when it returns.

## Rotating the token

The token is the deployment's, not a user's. Changing it revokes every client at
once, which is the point of it, and the cutover is not instantaneous on more
than one node: for a short window which token a request is accepted with
depends on which node served it, so a client should retry a 401 for that long
rather than treat it as a bad token.

## Deleting

Dropping a table deletes the table and everything it owned in object storage.
It is not recoverable and there is no undo, because there is no backup that
Walleye keeps for you — [what you own](../self-hosting/operating.md) says so
plainly.

## On the managed service

The same clocks, with the node half driven through an API instead of a process
manager: `stop` checkpoints every table and releases the nodes, `start` brings
them back on the current shape and cache, `resize` is accepted while stopped,
`token` mints a new one, and `delete` removes the instance and its storage. A
stopped instance keeps its data and runs no nodes.

What a table does is identical, because it is the same engine.
