# Budgets

`WALLEYE_RAM_GB` and `WALLEYE_NVME_GB` describe the machine, not the cache.
Everything that allocates borrows from one governor: memtables, request
bodies, compactions, queries, worker heaps and the replication log. Anything
that cannot be afforded is refused rather than attempted.

## What is held back

| Reserve | Size | Why |
|---|---|---|
| Runtime memory floor | 256 MiB | The process itself, outside the cache. |
| Replica buffers | 128 MiB | Only in cluster mode, for in-flight replication. |
| Filesystem floor | 512 MiB | Metadata, journal, and the cache's own bookkeeping. |

Below about 512 MiB of memory the floors scale down rather than swallowing the
whole budget, so a small test configuration still starts. That is a
convenience for tests, not a supported production size.

The cache gets whatever is left, and gives it back elastically: when a query
needs memory the cache shrinks, and when the query finishes it grows again.
There is a floor under that too, so a long-lived borrower cannot hold the
cache at zero.

## Reading it

```sh
curl -s localhost:8080/internal/cache/stats -H "authorization: Bearer $TOKEN"
```

The `budget` object reports the whole budget, what is reserved, and what is
available. When a reservation fails the error names the borrower and the
arithmetic:

```
Resources exhausted: table audit needs 48 MiB but only 16 MiB of the 256 MiB
memory budget can be held (16 MiB is the cache's working floor, 224 MiB is
already held)
```

## Sizing

An open stream holds a writer against the budget for as long as it stays open,
roughly a memtable plus an unflushed bound, plus a vector graph sized for the
memtable when the table has vector columns. Streams that nobody touches are
closed by the idle sweeper and their memory returns.

The consequence worth planning for: a worker that fans out into several
streams needs room for a writer in each. Three target streams means three
writers, not one.
