# A cluster

Three nodes, each running the API and an embedded replica daemon. A write is
acknowledged once a quorum of replicas holds it, so losing one node loses
nothing and stops nothing.

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

Membership is static and identical on every node. A stream's owner is its
rendezvous winner over that list, so ownership is the same on every node and
survives a restart. A request that arrives at a non-owner is forwarded.

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

With a quorum of two out of three, one node can be down without stopping
writes. Streams it owned move to their next rendezvous winner for as long as
it is away, and move back when it returns, because the member list has not
changed.

Restoring a node that lost its disk is the seeding path above: it reads the
archive, catches up from its peers, and rejoins.
