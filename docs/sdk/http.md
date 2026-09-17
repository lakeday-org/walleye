# The HTTP surface

Everything a node serves, for the calls the LanceDB clients do not have and
for anyone writing a client of their own. Data travels as Arrow IPC; everything
else is JSON. Anything unsupported answers 400 with a plain reason rather than
a wrong answer.

## Authentication

Send the token as `x-api-key`, which is what the LanceDB clients do, or as
`Authorization: Bearer`. Anything else, or a wrong token, is a 401. `/healthz`
and `/readyz` are the probes; every other route needs the token.

One token per deployment. It is the deployment's, not a user's — rotating it
revokes every client at once.

## Tables

| Route | What it does |
|---|---|
| `GET /v1/table/` | List tables |
| `POST /v1/table/{name}/create/` | Create from an Arrow IPC file body |
| `POST /v1/table/{name}/describe/` | The table's schema and version |
| `POST /v1/table/{name}/drop/` | Delete the table and everything it owned |
| `POST /v1/table/{name}/insert/` | Append an Arrow IPC file body |
| `POST /v1/table/{name}/query/` | Filter, project, page, and vector search |
| `POST /v1/table/{name}/count_rows/` | Count, with an optional filter |
| `POST /v1/table/{name}/create_index/` | `{"column": …, "metric_type": …}` |
| `POST /v1/table/{name}/index/list/` | List indexes |

`create/` returns an error naming "already exists" when the table is there, and
that string is what the clients match on.

## Generations

Flushes and compaction run on their own. These are the same operations, now.

| Route | What it does |
|---|---|
| `POST /v1/table/{name}/flush_lsm/` | Flush the memtable to a generation |
| `POST /v1/table/{name}/compact_lsm/` | Merge generations and rebuild indexes |
| `POST /v1/table/{name}/get_lsm_stats/` | Generations, row counts and index names |

## SQL

```
POST /v1/query   {"sql": "SELECT …"}
```

Read-only DataFusion SQL across every table on the node. The response is a JSON
array of row objects. The statement is capped at 64 KiB and the query at a
sixty second deadline. There is no DDL and no DML: write through the table
routes.

This is a Walleye route. LanceDB has no SQL endpoint, so no client has a call
for it.

## Health

| Route | What it answers |
|---|---|
| `GET /healthz` | Can this node serve? Unavailable until it can make a write durable, and healthy from then on |
| `GET /readyz` | Can it make a write durable right now, and which members are serving |
| `GET /readyz?require=all` | Ready only when every configured member is serving |

`/readyz` names reachable and unreachable members either way, so one address in
front of a cluster is enough to tell a whole cluster from a quorum of one. See
[shapes](../concepts/shapes.md).

## Namespaces

`GET /v1/namespace/{namespace}/table/list` exists because clients ask for it.
Walleye has no namespaces: every namespace id lists the one root.

## Streams, views and workers

A node can also be given a pipeline to run: a stream to ingest into, a view
that transforms one table into another, and a worker that does the
transforming.

| Route | What it does |
|---|---|
| `POST /v1/streams` | Define a stream |
| `POST /v1/streams/{name}/events` | Ingest rows as JSON |
| `GET /v1/view/` | List views |
| `POST /v1/view/{name}/create/` | Define a view and its worker |
| `POST /v1/view/{name}/describe/` | What a view is and where its cursor is |
| `POST /v1/view/{name}/refresh/` | Run a turn now |
| `POST /v1/view/{name}/drop/` | Remove it |
| `/v1/worker/{name}/` | Call a worker directly |

The two examples are the documentation for these: they build a pipeline out of
them end to end.
[The Bluesky firehose](../../examples/bluesky-firehose/README.md) holds a socket
open and labels what comes out of it;
[live option flow](../../examples/unusual-whales/README.md) polls an API on a
clock. Both explain the parts they use as they use them. The worker and view
settings they rely on are in
[configuration](../self-hosting/configuration.md).

## Internal

Useful when something is wrong, not part of the contract.

| Route | What it answers |
|---|---|
| `GET /internal/cache/stats` | The budget: total, reserved, available, and who is holding it |
| `GET /internal/snapshot/{name}` | A table's current snapshot |
| `POST /internal/cache/flush` | Drop the cache |

[Budgets](../self-hosting/budgets.md) reads the first one.

## Limits

- request bodies up to 512 MiB; an oversize insert is a 413
- SQL statements up to 64 KiB, and sixty seconds to run
- a write while the quorum is unreachable is a 503 with `Retry-After`
- not implemented anywhere: `update`, `delete`, `merge_insert`, full-text
  search, ordering on a search, and column expressions in a projection
