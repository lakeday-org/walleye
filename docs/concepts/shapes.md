# Shapes

A Walleye deployment is one node or three. That is the shape, and it is the
only structural choice: the binary, the protocol and the guarantees do not
change with it.

| | One node | Three nodes |
|---|---|---|
| A write is durable when | the log reaches object storage | a quorum of replicas holds it |
| One node is lost | nothing answers until it is back | no acknowledged write is lost; the tables it owned move to the survivors |
| Ownership | everything | each table belongs to one live node |
| Turned on by | nothing, it is the default | `WALLEYE_MEMBERS` and `WALLEYE_BITR_URL` |

[A single node](../self-hosting/single-node.md) and
[a cluster](../self-hosting/cluster.md) are how to run each. This page is what
changes between them.

## Any node, any request

Every member serves the whole API, so a client needs no knowledge of the
topology and nothing pins it to a node.

Each table is owned by exactly one live node, recorded in the bucket. The
first node to use a table claims it, and it keeps it until it stops or dies.
Only the owner holds the writer, so a request that arrives anywhere else is
forwarded to the owner and answered from there. Reads are forwarded too, which
is why a read always includes rows that are still in memory.

## Losing a node

**Nothing committed is lost.** A write is acknowledged only once a quorum of
two replicas holds it, so the two survivors hold everything that was
acknowledged, and the archive receives it behind them.

**Its tables move to the survivors.** Every node renews a lease in the bucket.
When a node's lease has gone unrenewed for the lease time plus a skew
allowance, the survivors claim its tables, spread between them, and replay the
log before serving. With the defaults that is about fifteen seconds. Until
then a request for one of those tables is answered 503 with `Retry-After`.

A node that stops cleanly hands its tables over instead, and a survivor takes
each one at once. [A cluster](../self-hosting/cluster.md#ownership-in-the-bucket)
has the details.

## Fencing

Two writers for one table would be a lost-update machine, so every handover is
fenced by one epoch: the Lance writer epoch, which the owner writes into the
table's ownership record before its writer claims it through a
compare-and-swap on the manifest. From that moment the previous writer's
appends and commits fail. In a cluster the replicas are fenced from the same
claim, so a process that was slow rather than dead cannot write behind the new
owner's back.

A process also stops acting as an owner the moment its own lease lapses on its
own clock, which is before any peer may take its tables, and it acknowledges a
write only if it still owns the table after the rows are durable. A forward
that lands on a non-owner is refused with 409 rather than forwarded onward.

## SQL across owners

A `SELECT` whose tables all share one owner is forwarded there and runs there.
A `SELECT` that spans owners works too: the node that received it gathers the
rows it does not own from their owners and runs the query locally. What it
gathers is that owner's own snapshot, so it carries unflushed rows and the
answer is not stale.

Gathering has a ceiling. A table with more than a million rows is refused
rather than shipped across the network:

```
stream <name> has more than 1000000 rows; query it on its owner rather than
joining it across members
```

The limit is on how many rows one table has to move — not on how large a table
may be, and not on how many a statement may name. The fix is to stop moving
them: query that table on its own, or alongside only tables its own node owns,
and the whole statement is forwarded instead of gathered. If you need the join,
narrow the large side first.

## Readiness

Two probes, because a cluster makes two different questions out of one.

`/healthz` reports unavailable until a node can make writes durable — a
reachable quorum, and its own tables opened. Point a load balancer at it and
traffic never reaches a node that would refuse a write. Once a node has served
it stays healthy even if the quorum is later lost, because reads stay correct
without one.

`/readyz` is the strict question: can this node make a write durable. It names
the members serving and the members it cannot reach, so a caller that reaches
the cluster through one address can tell a whole cluster from a quorum of one.
`/readyz?require=all` is stricter still and reports ready only when every
configured member is serving, which is what "the cluster is up" means to a
client about to write to a table the missing member owns.

It is not asked at the moment you ask it. A background poll refreshes it every
two seconds, and write readiness is withdrawn only after three consecutive
failed polls, so `/readyz` can answer 200 for about six seconds after a quorum
is gone — longer if the polls are timing out rather than failing fast. That
hysteresis is deliberate: a probe timing out under load is not the same as a
quorum being gone. Treat a ready answer as "ready a moment ago", and do not
build a fence out of it that assumes otherwise.

Both the member names and `?require=all` come from the replica gateway's own
answer. A node running without `WALLEYE_BITR_URL` — a single node — answers a
literal `{"ready": true}`: it names nobody, and `require=all` is a silent
no-op rather than an error.

A write that arrives while the quorum is unreachable gets a 503. On the LanceDB
routes it carries `Retry-After`; the native write routes return a bare 503 with
the reason in the body. A forward to a peer that is still starting is retried
for up to fifteen seconds. A member that takes longer than that to boot is
better gated on with `require=all` than waited out.

## On the managed service

`ramp` is one node and `launch` is three, provisioned with the replica daemon
already wired up. Moving between them is a resize, and it is one way: an
instance that has run as `launch` does not go back to `ramp`.
