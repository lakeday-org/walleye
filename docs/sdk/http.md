# The HTTP surface

Everything a node serves, for the calls the LanceDB clients do not have and
for anyone writing a client of their own. Data travels as Arrow IPC; everything
else is JSON.

Something the node understands but will not do answers 400 with a plain reason
rather than a wrong answer, and [refusals](#refusals) is the list. A call it has
no route for is a 404 with an empty body.

## Authentication

Send the token as `x-api-key`, which is what the LanceDB clients do, or as
`Authorization: Bearer`. Anything else, or a wrong token, is a 401. `/healthz`
and `/readyz` are the probes; every other route needs the token.

`x-api-key` shadows `Authorization` rather than sitting beside it: if the header
is present at all, it is the one checked, and a correct Bearer alongside a stale
`x-api-key` is a 401. Send one.

One token per deployment. It is the deployment's, not a user's — rotating it
revokes every client at once.

## Tables

| Route | What it does |
|---|---|
| `GET /v1/table/` | List tables |
| `POST /v1/table/{name}/create/` | Create from an Arrow IPC stream body |
| `POST /v1/table/{name}/describe/` | The table's schema and version |
| `POST /v1/table/{name}/drop/` | Delete the table's catalog entry and its Lance data |
| `POST /v1/table/{name}/insert/` | Append an Arrow IPC stream body |
| `POST /v1/table/{name}/query/` | Filter, project, page, and vector search |
| `POST /v1/table/{name}/count_rows/` | Count, with an optional filter |
| `POST /v1/table/{name}/create_index/` | `{"column": …, "metric_type": …}` |
| `POST /v1/table/{name}/index/list/` | List indexes |

`create/` returns an error naming "already exists" when the table is there, and
that string is what the clients match on.

The two directions are not the same IPC format. A request body is **stream**
format — a real IPC file, with its `ARROW1` magic, is rejected — and a response
body is **file** format, `application/vnd.apache.arrow.file`. The LanceDB
clients get this right on their own; a client of your own has to.

In a cluster a drop removes the table's Lance data under this node's root. The
replication log's archive is a separate prefix in its own bucket and is not
touched.

## Generations

Flushes and compaction run on their own. These force them, and forcing a merge
is not the operation the background schedule runs: the schedule merges at eight
generations, `compact_lsm/` merges from two.

| Route | What it does |
|---|---|
| `POST /v1/table/{name}/flush_lsm/` | Flush the memtable to a generation |
| `POST /v1/table/{name}/compact_lsm/` | Merge generations and rebuild indexes |
| `POST /v1/table/{name}/get_lsm_stats/` | Generations, row counts and index names |

## SQL

```
POST /v1/query   {"sql": "SELECT …"}
```

Read-only DataFusion SQL. The response is a JSON array of row objects, capped
at 8 MiB — past that the query is refused and asks you for a `LIMIT`. The
statement is capped at 64 KiB and the query at a sixty second deadline. There is
no DDL and no DML: write through the table routes.

On one node it reads every table on the node. In a cluster it reaches further:
the node that receives the statement gathers the tables it does not own from
their owners and runs the query locally, so a `SELECT` spans the whole cluster.
That gather refuses any single table over a million rows — see
[SQL across owners](../concepts/shapes.md#sql-across-owners).

This is a Walleye route. LanceDB has no SQL endpoint, so no client has a call
for it.

## Health

| Route | What it answers |
|---|---|
| `GET /healthz` | Can this node serve? Unavailable until it can make a write durable, and healthy from then on |
| `GET /readyz` | Can it make a write durable, as of the last poll, and which members are serving |
| `GET /readyz?require=all` | Ready only when every configured member is serving |

`/readyz` names reachable and unreachable members either way, so one address in
front of a cluster is enough to tell a whole cluster from a quorum of one. See
[shapes](../concepts/shapes.md).

Two things to know before you gate on it. It is served from a value a
background poll refreshes every two seconds, and readiness is withdrawn only
after three consecutive failed polls, so it can answer 200 for about six seconds
after a quorum is gone. And both the member names and `?require=all` come from
the replica gateway: a node without `WALLEYE_BITR_URL` answers a literal
`{"ready": true}`, names nobody, and ignores the parameter rather than
rejecting it.

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
| `GET /internal/cache/stats` | The budget: memory total, reserved and available, and the disk figures |
| `GET /internal/snapshot/{name}` | A table's current snapshot |
| `POST /internal/cache/flush` | Wait for queued cache writes to reach disk |

[Budgets](../self-hosting/budgets.md) reads the first one. It reports totals
only: reservations are made under names, but the names are not surfaced, so it
tells you how much of the budget is held and not by whom.

`flush` is not an eviction. It waits for the cache's queued writes to reach its
persistent tier and answers 204; nothing is dropped and a cold cache is not what
you get from it.

## Refusals

A refusal is a 400 with the reason as the body, so the first place to look is
the response itself. These are the ones a client will meet without asking for
anything unusual.

**Scalar indexes do not exist.** Both SDKs offer `Index.btree()`,
`Index.bitmap()` and `Index.labelList()`, and all three are refused: only
index types beginning `IVF` or `HNSW` are accepted, because there is one
layout — an HNSW graph over the memtable and `IVF_HNSW_SQ` over each
generation.

```
index type BTREE is not supported; vector columns use IVF_HNSW_SQ
```

`Index.fts()` has its own answer, `full-text indexes are not supported yet`,
and a full-text query is `full-text search is not supported yet`. Filtering a
scalar column still works; it is a scan, not an index lookup.

The rest:

| What you did | What comes back |
|---|---|
| `Index.btree()`, `Index.bitmap()`, `Index.labelList()` | `index type BTREE is not supported; vector columns use IVF_HNSW_SQ` |
| `Index.fts()` | `full-text indexes are not supported yet` |
| A full-text query | `full-text search is not supported yet` |
| `order_by` on any query, search or scan | `order_by is not supported; use SQL via /v1/query` |
| An expression in `select` | `column expressions are not supported; use SQL via /v1/query` |
| `insert` with `mode=overwrite` | `insert mode=overwrite is not supported; drop and recreate the table` |
| A create `mode` other than `create`, `exist_ok` or `overwrite` | `unknown create mode <mode>` |
| A column named with a leading `_` | `column _x uses a reserved name` |
| A table name outside `[A-Za-z0-9_-]` | `invalid table name` |
| A nested array in `vector` | `multivector queries are not supported` |
| Searching a table with two vector columns | `table has several vector columns; set vector_column` |
| Searching a table with none | `table has no vector column` |
| `vector_column` naming a column that is not `FixedSizeList<Float32>` | `<column> is not a FixedSizeList<Float32> column` |
| A query metric that differs from the index's | `column <c> is indexed with metric <m>; create_index with metric_type=<q> to change it` |
| A body that does not fit into memory twice over | 413, `request body needs <n> MiB but only <m> MiB … can be held` |
| A SQL result over 8 MiB | `Io error: JSON result exceeds 8 MiB; use a SQL LIMIT` |
| A cross-member gather of a table over a million rows | `stream <name> has more than 1000000 rows; query it on its owner rather than joining it across members` |

The metric comparison is a string compare, not a distance-type one, and a
column is indexed with `l2` from the first row. So asking a query for
`euclidean` — which LanceDB treats as the same thing — is refused too. Use the
spelling the index has.

**What is not a refusal at all.** Several LanceDB calls have no route here, so
they are a **404 with an empty body**: no status to match on beyond the 404, no
reason to read. `update`, `delete` and `merge_insert` are the ones a client will
reach for; `add_columns`, `alter_columns`, `drop_columns`, `version/list`,
`restore`, the tag calls, per-index stats and multipart write are also absent.
The supported set is the two route tables above.

**Bad JSON is three different answers**, from the extractor, before any handler
sees the request. A body that is not valid JSON is a **400**, `Failed to parse
the request body as JSON: …`. A body that is valid JSON of the wrong shape — a
missing `column` on `create_index`, an unknown key on `/v1/query` or
`/v1/streams` — is a **422**. A request without `Content-Type: application/json`
is a **415**. The routes that take a free-form JSON body — `query/`,
`count_rows/` and view creation — never answer 422, because anything valid
deserializes.

`exist_ok` means "this table, as described", not "whatever is there": a create
naming an existing table with a different schema is refused whichever mode you
asked for.

```
table clicks already exists with a different definition
```

## Limits

- the LanceDB routes take request bodies up to 512 MiB; an oversize body is a
  413. The other routes — `/v1/query`, `/v1/streams` and `/internal/*` — are
  capped at 8 MiB
- 512 MiB is not reachable in practice. An inbound Arrow body is held raw and
  decoded at once, so it is reserved at twice its own size before decoding: on
  the default 1 GiB budget an insert over roughly 360 MiB is a 413 whatever the
  route allows. Send it in batches
- SQL statements up to 64 KiB, sixty seconds to run, and an 8 MiB response
- a write while the quorum is unreachable is a 503. The LanceDB routes carry
  `Retry-After`; the native write routes return a bare 503 with the reason in
  the body
- not implemented anywhere: `update`, `delete` and `merge_insert` (404, no
  body), full-text search and full-text indexes, scalar indexes, ordering on a
  search, and column expressions in a projection. [Refusals](#refusals) has
  what each one answers
