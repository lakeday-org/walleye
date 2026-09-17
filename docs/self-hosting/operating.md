# What you own

Running Walleye yourself means the engine is yours and so is everything
around it. This page is the honest list.

## Yours

**The bucket.** Versioning, access, and the bill. Walleye writes its log and
its data there and does not clean up after you beyond deleting what a dropped
table owned.

> **Never apply a lifecycle, expiration or retention rule to the table
> prefix.** It will destroy acknowledged data, silently.
>
> The write-ahead log lives inside each table's own prefix, at
> `<root>/data/<table>/_mem_wal/<shard>/wal/`. Those objects are not old
> copies or garbage awaiting collection: they are the log, and nothing ever
> deletes one. A rule that expires objects by age deletes the oldest, which
> are the lowest positions in the log.
>
> Recovery reads the log forward and stops at the first position that is
> missing. Everything above a deleted range is therefore dropped, with no
> error and no warning. Those rows are unreachable at first and then genuinely
> lost, because the next flush records that recovery may start above them. The
> vacated positions are then reused, so a later restart can replay new entries
> interleaved with surviving old ones and bring deleted rows back.
>
> What an operator sees is nothing: no failure, no log line, no metric. Just a
> row count that drops after a restart, and possibly old rows returning after
> a later one. A partially completed bucket restore, or a cross-region
> replication that catches up out of order, does the same thing.
>
> The engine does not currently detect this, which is why the rule is absolute
> rather than a matter of tuning. The only prefix a retention rule belongs on
> is the replication log's archive, which lives in its own bucket and prefix.

Versioning and deletion protection are worth having. They do not interfere
with anything Walleye writes, and they are what turns an accidental rule into
something recoverable.

**Backups.** There is no backup command. The bucket holds everything needed to
rebuild a node, so backing up Walleye means backing up that prefix, with
whatever consistency your storage gives you.

**Upgrades.** Roll one node at a time and watch `/readyz`. Rolling back to an
image older than the LanceDB protocol work cannot open tables created through
it, because the older catalog parser rejects fields it does not know. The
catalog is tolerant now, so upgrades from here are reversible.

**Secrets.** The deployment token, the data key, the replica root key, and any
key a worker names. Generated tokens are printed to standard error, which is
fine locally and wrong in production.

**Capacity.** The budgets are declared, not discovered. A node given eight
gigabytes will refuse work that needs more rather than swapping or dying, and
[budgets](budgets.md) explains what it holds back.

## Not implemented, here or anywhere

Saying this plainly is more useful than leaving it out. None of these exist on
the managed side either:

- `update`, `delete` and `merge_insert` on the LanceDB surface
- full-text search, and full-text indexes
- namespaces, which are accepted and ignored
- vector index types other than the one layout, an in-memory graph flushed per
  generation
- a compaction schedule you can set; merging is automatic once eight
  generations exist, and `compact_lsm/` forces one, but there is no window or
  cron to configure

## Different on the managed side

The managed offering is the same binary. What it adds is operational rather
than functional: provisioned clusters with the replica daemon already wired,
durability defaults chosen for you, upgrades and rolling restarts, metrics and
alerting, and support.

There is no capability in the managed product that this binary does not have.
If that changes, this page should say so, and a page that quietly stopped
being true would be worse than no page.
