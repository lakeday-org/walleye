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
be down without losing anything acknowledged. What it costs is a short pause
for the tables that node owns, which is the paragraph after next.

The trade is latency: a local quorum acknowledges faster than a bucket does.

A node loss costs no acknowledged data — the two survivors are the quorum —
and each table is owned by one node, so the tables it owned stop answering
until a survivor has seen its lease lapse and taken them over, about fifteen
seconds by default. On one node there is no survivor, so nothing answers until
it is back. [Losing a node](shapes.md#losing-a-node) is the mechanism.

Both arrangements end in the same place. Object storage is the durable record
either way; the cluster puts a replicated log in front of it.

## The cache is disposable

The cache is hot fragments, indexes and the rows a query touched. It lives in
memory and on local disk, it is rebuilt from object storage whenever a node is
replaced, restarted or resized, and nothing acknowledged is lost by throwing it
away.

On one node that is everything the local disk holds. In a cluster it is not:
the replication log above lives on that same disk and is durable state, not
cache. `WALLEYE_NVME_GB` sizes the two together, and the node charges the log's
actual bytes against the same ceiling as it grows, so a disk sized for the
cache alone will squeeze one of them.

Losing that disk is still not losing data — a quorum of two held every
acknowledged write, and the replica re-seeds from the archive and catches up
from its peers when it comes back. It is slower than losing a pure cache.

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
node, and nothing holds the bucket: versioning and access are yours, and
dropping a table deletes what it owned.

One rule has no exceptions, because breaking it loses acknowledged rows with
no error: never expire or age out anything under the tables' prefix. The
write-ahead log lives there, and it is the log rather than old copies of it.
[What you own](../self-hosting/operating.md) explains what happens if you do.

Durability is also not availability, on either shape. A write in flight while
its owner changes can be refused - 409 or 503 with an `x-walleye-route-error`
header saying which - and should be retried; everything already acknowledged
is safe, and a retried row with a primary key is not stored twice.
[Shapes](shapes.md) is that choice.

## On the managed service

The same two arrangements under different names. A `ramp` instance is one node
committing to object storage. A `launch` instance is three nodes committing at
a quorum. The guarantee is identical because the code is.
