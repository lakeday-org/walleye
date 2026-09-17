# Your first query

Three ways to ask, and they are not interchangeable: a filter, a nearest
neighbour search, and SQL.

## Filter and count

```python
table.count_rows()                                          # 4
table.count_rows("city = 'seattle'")                        # 2
table.search().where("id > 1").select(["id", "city"]).limit(10).to_list()
```

```js
await table.countRows();
await table.query().where("city = 'seattle'").limit(10).toArray();
```

`where` takes a SQL expression over the table's columns. `select` takes column
names. `limit` and `offset` page through the result.

## Nearest neighbours

```python
table.search([0.0, 0.9]).limit(2).to_list()                 # nearest first
table.search([0.0, 0.9]).where("city = 'portland'").to_list()
```

Each row comes back with a `_distance`. The search uses the index's own
metric, and asking for a different one is an error rather than a silent
rescoring — set the metric once with `create_index` and queries follow it.

A filter alongside a vector query narrows the same search rather than
filtering afterwards.

## SQL

SQL is a Walleye route rather than a LanceDB one: the client has no call for
it, so you post to it.

```sh
curl -s localhost:8080/v1/query \
  -H "authorization: Bearer $WALLEYE_TOKEN" -H "content-type: application/json" \
  -d '{"sql": "SELECT city, count(*) AS n FROM clicks GROUP BY city ORDER BY n DESC"}'
```

```json
[{"city": "seattle", "n": 2}, {"city": "portland", "n": 1}, {"city": "boise", "n": 1}]
```

It is read-only DataFusion SQL with a sixty second deadline, a 64 KiB limit on
the statement and an 8 MiB limit on the JSON it returns. There is no DDL and no
DML: tables are created and written through the LanceDB calls above, and a
`SELECT` sees the rows in memory as well as the rows on disk. On one node it
reads every table on the node; on three it also gathers the tables this node
does not own from their owners.

Sorting is the common reason to reach for it. `order_by` is not part of the
LanceDB search surface here, and asking for it returns
`order_by is not supported; use SQL via /v1/query`.

On [three nodes](../concepts/shapes.md) a statement spanning tables with
different owners still works, with one limit worth knowing about before you
write it.

## What is not there

Saying this here saves you finding it out from an error:

- **no scalar indexes.** `Index.btree()`, `Index.bitmap()` and
  `Index.labelList()` are all a 400 — `index type BTREE is not supported;
  vector columns use IVF_HNSW_SQ`. Only `IVF*` and `HNSW*` are accepted, and
  vector columns are indexed for you anyway. Filtering a scalar column still
  works; it is a scan
- no full-text search and no full-text indexes, each with its own 400
- no `update`, `delete` or `merge_insert`. These have no route at all, so they
  are a 404 with an empty body rather than a reason — write a row with the same
  key instead, and the newest one wins. So are `add_columns`, `restore`, the
  version and tag calls, and per-index stats
- no ordering — on a search or on a plain scan — and no column expressions in
  `select`: both are a 400 pointing at `/v1/query`
- namespaces are accepted and ignored: every namespace id lists the one root

[Refusals](../sdk/http.md#refusals) is the full list with the message each one
returns. [What you own](../self-hosting/operating.md) keeps the same list for
the managed service as for a node you run. Nothing on it is a managed feature
held back.
