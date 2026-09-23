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

Restoring a node that lost its disk is the seeding path above: it reads the
archive, catches up from its peers, and rejoins owning nothing. Tables do not
move back to it; they move only when their owner leaves.

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
- A dead owner's tables go to the live process with the highest rendezvous
  score for each table, so they spread across the survivors.
- A stopping process marks its lease draining, flushes and releases each table
  at the same epoch, then deletes its lease.

`GET /internal/ownership` shows what a process believes: its session, whether
it is an owner right now, the tables it holds at which epoch, and the leases it
has seen.

The ring built from the member list, or from Kubernetes endpoint discovery,
places cache entries only.
