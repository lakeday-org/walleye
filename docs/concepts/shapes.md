# Shapes

A Walleye deployment is one node or three. That is the shape, and it is the
only structural choice: the binary, the protocol and the guarantees do not
change with it.

| | One node | Three nodes |
|---|---|---|
| A write is durable when | the log reaches object storage | a quorum of replicas holds it |
| One node is lost | nothing answers until it is back | no acknowledged write is lost; the tables it owns stop answering until it is back |
| Ownership | everything | each table belongs to one node |
| Turned on by | nothing, it is the default | `WALLEYE_MEMBERS` and `WALLEYE_BITR_URL` |

[A single node](../self-hosting/single-node.md) and
[a cluster](../self-hosting/cluster.md) are how to run each. This page is what
changes between them.

## Any node, any request

Every member serves the whole API, so a client needs no knowledge of the
topology and nothing pins it to a node.

Each table is owned by exactly one member, picked by weighted rendezvous
hashing over the member list. Only the owner holds the writer, so a request
that arrives anywhere else is forwarded to the owner and answered from there.
Reads are forwarded too, which is why a read always includes rows that are
still in memory.

Membership is static and identical on every node, so ownership is the same
everywhere and survives a restart.

## Losing a node

Read this part before you size a cluster on it, because durability and
availability come apart here.

**Nothing committed is lost.** A write is acknowledged only once a quorum of
two replicas holds it, so the two survivors hold everything that was
acknowledged, and the archive receives it behind them.

**The tables that node owns stop answering.** Ownership is a pure function of
the table name over the static member list, with no liveness in it, so a node
that is away stays the owner of its tables. Requests for them are forwarded to
it, retried for fifteen seconds, and then answered 502. Tables owned by the
other two carry on untouched.

So a three-node cluster survives a node loss without losing data and without
losing service to roughly two thirds of its tables. It is not a failover
cluster for the remaining third. Bring the node back rather than waiting for
the cluster to route around it.

Ownership does move when membership itself changes, which today means
Kubernetes endpoint discovery rather than the static list, with a warming
window during which the previous owner still serves. `WALLEYE_MEMBERS` does not
turn that on. [A cluster](../self-hosting/cluster.md) is where that lives.

## Fencing

Ownership does not move under a node loss, so fencing is not about failover. It
is about the paths that do hand a table's writer from one process to another —
a restart, a reopen, an ownership change under endpoint discovery — because two
writers for one table would be a lost-update machine. A new owner claims the
next writer epoch through a compare-and-swap on the manifest, and from that
moment the previous owner's appends and commits fail. In a cluster the replicas
are fenced from the same claim, so a process that was slow rather than dead
cannot write behind the new owner's back.

Forwarded requests carry the sender's view of the membership and are refused if
the receiver's differs, and a forward that lands on a non-owner is refused
rather than forwarded onward.

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
