# Cache tiers

A node has memory and a local disk, and the cache lives in both. What
sets them is a tier: two numbers on a node you run yourself, a named size on
the managed service. They buy the same thing.

```
WALLEYE_RAM_GB    memory for the whole process
WALLEYE_NVME_GB   disk for the cache and the replication log together
```

Both describe the machine, not the cache. The node holds fixed floors out of
them and everything that allocates borrows the rest through one governor —
memtables, request bodies, compactions, queries, worker heaps and the log.
[Budgets](../self-hosting/budgets.md) is the arithmetic; this page is what it
means for a workload.

## What the cache holds

Recently written fragments, indexes, and whatever queries touched. It is not a
storage limit: a table can be far larger than the disk, and colder data is read
from object storage on demand. A bigger tier means fewer of those reads, not
more room.

It is also disposable. Losing it costs time and never data, which is why a
restart, a replacement or a resize is safe and merely cold.

## The real limit is how many tables stay open

For most workloads the binding constraint is not how much data a tier holds. It
is how many tables it can keep open at once, because an open table leases
memory for the whole time it stays open:

```
48 MiB  +  100,000 × (dimensions × 4 + 128) bytes  per vector column
```

The 48 MiB is the memtable and its unflushed bound. The rest is the in-memory
graph, sized for the memtable's row capacity of 100,000 rows, and it is charged
once per vector column. A table with no vector column costs only the 48 MiB.

A 768-dimension table is the common case:

```
100,000 × (768 × 4 + 128) = 320,000,000 bytes = 305 MiB
305 MiB + 48 MiB = 353 MiB
```

So a node with 2 GiB, after its floors and with room left for memtables,
inserts and compaction, keeps roughly four of those open at a time.

Opening one more than fits is refused, and the error names the table, what it
needed, what was left and what was already held. Nothing is evicted to make
room and the cache is not quietly degraded to fit it — the open is simply
refused:

```
Resources exhausted: table audit needs 48 MiB but only 16 MiB of the 256 MiB
memory budget can be held (16 MiB is the cache's working floor, 224 MiB is
already held)
```

## The refusal is usually temporary

A table nobody has touched for five minutes is checkpointed, closed, and its
memory returned. The next use reopens it. That costs a reopen and never data,
so an open refused now generally succeeds on a retry a few minutes later.

When it does not clear, the remedies are:

- keep fewer tables open at a time;
- move up a tier;
- drop a table you no longer need, which frees its memory at once rather than
  in five minutes.

Any query or insert resets a table's idle clock. A workload that touches every
table every minute never lets one go idle, so nothing is ever released and
waiting will not help. That one has to be fixed by touching fewer tables or by
giving the node more memory.

The same arithmetic catches a fan-out: a pipeline writing into three tables
needs room for three writers, not one.

## On three nodes

A node opens only the tables it owns. Three nodes each lease against their own
memory, so a cluster keeps roughly three times as many tables open as one node
of the same size — but a single very busy table is still one node's problem,
because it has one owner.

## On the managed service

The tier is a name — `small` through `xlarge` — and it sets the same two
numbers, with the shape deciding how many nodes there are and what CPU they
run on. The formula above is what you are buying, and it is the reason to move
up a tier.
