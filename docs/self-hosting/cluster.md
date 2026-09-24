# A cluster

Three nodes, each running the API and an embedded replica daemon. A write is
acknowledged once a quorum of replicas holds it, so losing one node loses
nothing that was acknowledged, and the tables that node owned move to the
other two by themselves. [Losing a node](#losing-a-node) is how.

## What each node runs

Setting `WALLEYE_BITR_URL` turns on quorum durability and starts the replica
daemon inside the same process. The daemon holds the write-ahead log on local
disk, replicates it to its peers, and archives committed segments to object
storage.

```sh
export WALLEYE_BUCKET=my-bucket
export WALLEYE_MEMBERS='a=http://node-a:8080,b=http://node-b:8080,c=http://node-c:8080'
export WALLEYE_NODE_ID=a
export WALLEYE_BITR_URL=http://127.0.0.1:30080
export WALLEYE_TOKEN=... # the same on every node
```

The member list is where the replicas are and where each node says peers can
reach it. It does not decide who owns a table: the bucket does, as described
[below](#ownership-in-the-bucket). A request that arrives at a non-owner is
forwarded to the owner.

## The replica daemon

It reads its own environment, and those names still carry a `LAKEDAY_` prefix
from where the code came from. That is a wart, not a hint that something
managed is required.

| Variable | Meaning |
|---|---|
| `LAKEDAY_DATAPLANE_ROOT_KEY` | Root key the replicas authenticate with. The same on every node. |
| `LAKEDAY_REPLICA_NODE_NAME` | This replica's name. |
| `LAKEDAY_REPLICA_MEMBERS` | The replica peers, in the same form as the API members. |
| `LAKEDAY_REPLICA_INTERNAL_TOKEN` | Shared between replicas. |
| `LAKEDAY_REPLICA_DATA_DIR` | Where the log lives, `/data` by default. |
| `LAKEDAY_REPLICA_QUORUM` | How many must hold a write, `2` by default. |
| `LAKEDAY_REPLICA_ARCHIVE_BUCKET` | Where committed segments are archived. |
| `LAKEDAY_REPLICA_ARCHIVE_PREFIX` | Prefix within it. |
| `LAKEDAY_REPLICA_ARCHIVE_BATCH_RECORDS` | Records per archived segment. |
| `LAKEDAY_REPLICA_TIER` | Free-text tier label carried in membership. |

`WALLEYE_DATA_KEY` holds the 32-byte key, base64, that records are encrypted
with before they leave the node.

## Starting for the first time

A replica with an empty volume learns the archived prefixes before it serves.
That seeding is retried rather than fatal, because a transient storage failure
at boot would otherwise restart the node, fail again, and turn a passing fault
into a crash loop. The listeners stay unbound while it retries, so the node
never reports healthy while it cannot serve.

## Checking it

```sh
curl -s localhost:8080/readyz -H "authorization: Bearer $TOKEN"
curl -s 'localhost:8080/readyz?require=all' -H "authorization: Bearer $TOKEN"
```

`/readyz` reports write readiness and names which members are healthy and
which are unreachable. `?require=all` asks the stricter question, which is the
one a single address in front of the cluster needs to ask.

Readiness has hysteresis: a single slow probe does not withdraw write
readiness, because a probe timing out under load is not the same as a quorum
being gone.

## Losing a node

**Nothing committed is lost.** A write was acknowledged only once a quorum
held it, so the two surviving replicas have it and the archive receives it.

**Its tables move.** Each node renews a lease in the bucket every few seconds.
Once the survivors have seen a dead node's lease unchanged for the lease time
plus the skew allowance, 12 seconds by default, they claim its tables between
them, each opening its share and replaying the write-ahead log from the
surviving replicas before it serves. With the defaults a dead node's tables
are served again within about 15 seconds. Until then a request for one of them
is answered 503 with `Retry-After` and `x-walleye-route-error:
owner-unreachable`, and nothing is written.

A node that was not dead but cut off - paused, or unable to reach the bucket -
stops acting as an owner when its own lease lapses, which by construction is
before any peer may claim its tables. When it comes back it starts a new
session owning nothing, and a write its old writer tries is refused by the
new owner's epoch.

A node that is stopped hands its tables over instead: it flushes each one,
releases it, and removes its lease, and a peer takes each table on its next
request or within one sampling interval, whichever comes first.

**A node that comes back catches up.** Its replica holds the log only as far
as it got before it went down, and a replica counts towards a quorum only for
positions it actually holds: it refuses an append whose predecessor it does not
have, so until it catches up the cluster runs on two complete copies. As soon
as it restarts it copies what it missed from its peers, oldest first, taking
only records that are committed - a later record's commit watermark covers
them - and on which every copy agrees, and reading back from the archive any range
the peers have already trimmed. A write that needs it in the meantime, because
a second node is gone, brings it up to date first rather than failing.

Copying from its peers alone never finishes while writes continue: the newest
records are never yet known committed to anyone but their writer, and every
pass takes long enough for the writer to move on, so the replica would stay
behind and refuse every live append. So when it refuses an append the others
already carried, the writer's coordinator - the only one that knows where the
tail is, and that each record it acknowledged had its quorum - sends it every
record it is missing in one go, and from then on it takes live appends itself.
It logs `lakeday.replica rejoin outcome=level`. The replica logs
`lakeday.replica catch_up outcome=complete` when its own pass finds nothing
left to copy.

If the node died mid-append, its replica can hold a record that only it ever
received, from a writer the next owner replaced. That record was never
acknowledged, and catch-up withdraws it, recorded durably in the replica's own
log, before copying the committed record for that position. It withdraws only
records from a writer older than the one that wrote the committed log.

Committed segments are archived to `LAKEDAY_REPLICA_ARCHIVE_BUCKET` under
`LAKEDAY_REPLICA_ARCHIVE_PREFIX`, once a second, and the local logs are
trimmed behind them. A restarted replica whose volume survived does not seed
from the archive - it already knows every stream - so a boot line of
`seed_from_archive streams=0` is expected there; the archive is read by
catch-up, for trimmed ranges, by a table's recovery when it changes owner,
and by a replica with an empty volume. Those reads fetch many small segments
at once, so on an object store they cost a few round trips however much
history has built up since a table last flushed.

Restoring a node that lost its disk is the seeding path above: it reads the
archive, catches up from its peers, and rejoins owning nothing; the others
then hand it its share, one key per sweep.

## Ownership in the bucket

Ownership is two kinds of object under the engine root, and the bucket's
create-if-absent write is the only coordination. There is no membership
protocol and no consensus service. The design follows Deno's celld.

`_walleye/nodes/<node>.json` is one process's lease:

```json
{"node": "a.3f2a9c1be04d", "addr": "http://node-a:8080", "renewal": 41,
 "ttl_ms": 10000, "renewed_at_ms": 1790000000000, "draining": false}
```

`node` is the process, not the host: the configured node id and a random
suffix, new on every start. Only that process writes its lease. It renews every
third of `ttl_ms`, and a renewal counts only if it went out while the process
was still an owner and landed within the skew allowance.

`_walleye/own/<table>/<seq>.json` is a table's ownership record, one object
per version:

```json
{"node": "a.3f2a9c1be04d", "epoch": 7}
```

The highest `seq` is the record. A process changes it by creating the next
`seq`, which fails if anyone else created it first, so of any number of
claimants exactly one wins. `epoch` is the Lance write-ahead-log writer epoch:
the owner writes it into the record before its writer claims it, so the record
and the log agree on one fencing epoch. An empty `node` is a released table.

The rules:

- A process is an owner for `ttl_ms` after the last renewal it counted, on its
  own monotonic clock. It acknowledges a write only if it still owns the table,
  at its writer's epoch, after the rows are durable; otherwise the answer is
  409 with `x-walleye-route-error: lost-ownership`.
- A peer calls a lease dead when it has seen the same version of it for
  `ttl_ms` plus the skew allowance on its own monotonic clock, or when it is
  gone. No wall clock is compared anywhere.
- A table is claimed only when its record is absent, released, or names a dead
  lease. A live owner is never displaced.
- Every process works out where each known key belongs: rendezvous hashing
  with bounded load, so no live process is given more than
  `ceil(keys / live processes)`. Every process that sees the same keys and the
  same leases gets the same answer. A key nobody owns is claimed by the
  process it belongs to; a request for it that arrives elsewhere is sent there.
- A process holding more than that share hands one key back per sweep, one
  that belongs elsewhere: it flushes the key's writer and releases it, after
  any alarm firing in flight has finished, and the process it belongs to
  claims it. Keys only ever move to where they belong, so this stops once no
  process is over its share, and a cluster that is not changing moves
  nothing. A restarted or added process therefore gets its share back within
  a few sweeps; a hand-back costs the key's writes about half a second.
- A stopping process marks its lease draining, flushes and releases each table
  at the same epoch, then deletes its lease.

The bucket has to evaluate create-if-absent against its latest state and
show a new object to the next LIST. S3 does, and so does Tigris for requests
made in the region the bucket's data lives in; a Tigris bucket of the default
Global type is only eventually consistent for requests from other regions, so
every node of one deployment must reach it from the same region.

`GET /internal/ownership` shows what a process believes: its session, whether
it is an owner right now, the tables it holds at which epoch, and the leases it
has seen.

The ring built from the member list, or from Kubernetes endpoint discovery,
places cache entries only.
