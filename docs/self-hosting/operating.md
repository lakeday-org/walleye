# What you own

Running Walleye yourself means the engine is yours and so is everything
around it. This page is the honest list.

## Yours

**The bucket.** Lifecycle rules, versioning, retention, and the bill. Walleye
writes its log and its data there and does not clean up after you beyond
deleting what a dropped table owned.

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
