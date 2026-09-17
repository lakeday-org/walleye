# Durability

A write is acknowledged only once it is durable. That is the whole promise,
and everything else on this page is what it costs to keep it.

## On one node

Every write is appended to a write-ahead log in object storage before the
client is told it succeeded. The append is a conditional put, so two writers
cannot both believe they extended the log, and the acknowledgement is the
bucket's own acknowledgement rather than a local one.

Nothing durable lives on the node. The local disk holds cache, and losing the
node loses no acknowledged write — it loses the thing that was serving, which
is a different problem and the reason to run more than one.

## On three nodes

Each node runs a replica of the log on its own disk. A write is acknowledged
once a quorum of replicas holds it, and committed segments are archived to
object storage behind that. Two of three is the default quorum, so one node can
be down without stopping writes or losing anything.

The trade is latency for availability: a local quorum acknowledges faster than
a bucket does, and it keeps serving through a node loss that would take a
single node offline.

Both arrangements end in the same place. Object storage is the durable record
either way; the cluster puts a replicated log in front of it.

## The cache is disposable

Everything on local disk and in memory is cache: hot fragments, indexes, the
rows a query touched. It is rebuilt from object storage whenever a node is
replaced, restarted or resized. Nothing you have to back up lives there, and
nothing acknowledged is lost by throwing it away.

That is why a stop, a restart and a resize are all safe, and why a cold node is
slow rather than wrong.

## A retry is not a duplicate

A write that timed out may or may not have landed, so clients retry, and a
retry that lands twice is a bug you find weeks later in a count.

Walleye keys every row: your own key if the schema names one, otherwise a hash
of the row's content. Writing the same row again replaces it. So a retried
insert is a no-op rather than a second copy, and a source you re-read from a
cursor writes nothing for the rows you already have.

## What it does not cover

Durability is not backup. The bucket holds everything needed to rebuild a
node, and nothing holds the bucket: lifecycle rules, versioning and retention
are yours, and dropping a table deletes what it owned. See
[what you own](../self-hosting/operating.md).

Durability is also not availability. On one node, a write in flight while the
node is replaced fails and should be retried; everything already acknowledged
is safe. [Shapes](shapes.md) is that choice.

## On the managed service

The same two arrangements under different names. A `ramp` instance is one node
committing to object storage. A `launch` instance is three nodes committing at
a quorum. The guarantee is identical because the code is.
