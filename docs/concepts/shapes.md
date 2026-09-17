# Shapes

A Walleye deployment is one node or three. That is the shape, and it is the
only structural choice: the binary, the protocol and the guarantees do not
change with it.

| | One node | Three nodes |
|---|---|---|
| A write is durable when | the log reaches object storage | a quorum of replicas holds it |
| One node is lost | nothing answers until it is back | the other two keep serving |
| Ownership | everything | each table belongs to one node |
| Turned on by | nothing, it is the default | `WALLEYE_MEMBERS` and `WALLEYE_BITR_URL` |

[A single node](../self-hosting/single-node.md) and
[a cluster](../self-hosting/cluster.md) are how to run each. This page is what
changes between them.

## Any node, any request

Every member serves the whole API, so a client needs no knowledge of the
topology and nothing pins it to a node.

Each table is owned by exactly one member, chosen by hashing the table's name
over the member list. Only the owner holds the writer, so a request that
arrives anywhere else is forwarded to the owner and answered from there. Reads
are forwarded too, which is why a read always includes rows that are still in
memory.

Membership is static and identical on every node, so ownership is the same
everywhere and survives a restart. Lose a node and the tables it owned move to
their next winner for as long as it is away, then move back, because the list
did not change.

## Fencing

Ownership moving is the interesting case, because two writers for one table
would be a lost-update machine. A new owner claims the next writer epoch
through a compare-and-swap on the manifest, and from that moment the previous
owner's appends and commits fail. In a cluster the replicas are fenced from the
same claim, so a node that was slow rather than dead cannot write behind the
new owner's back.

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

`/readyz` is the strict question, asked now: can this node make a write durable
at this moment. It names the members serving and the members it cannot reach,
so a caller that reaches the cluster through one address can tell a whole
cluster from a quorum of one. `/readyz?require=all` is stricter still and
reports ready only when every configured member is serving, which is what "the
cluster is up" means to a client about to write to a table the missing member
owns.

A write that arrives while the quorum is unreachable gets a 503 with
`Retry-After` rather than failing somewhere deep, and a forward to a peer that
is still starting is retried for up to fifteen seconds. A member that takes
longer than that to boot is better gated on with `require=all` than waited out.

## On the managed service

`ramp` is one node and `launch` is three, provisioned with the replica daemon
already wired up. Moving between them is a resize, and it is one way: an
instance that has run as `launch` does not go back to `ramp`.
