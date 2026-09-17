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

It is DataFusion SQL across every table on the node, read-only, with a sixty
second deadline and a 64 KiB limit on the statement itself. There is no DDL and
no DML: tables are created and written through the LanceDB calls above, and a
`SELECT` sees the rows in memory as well as the rows on disk.

Sorting is the common reason to reach for it. `order_by` is not part of the
LanceDB search surface here, and asking for it returns a 400 that points at
`/v1/query`.

On [three nodes](../concepts/shapes.md) a statement spanning tables with
different owners still works, with one limit worth knowing about before you
write it.

## What is not there

Saying this here saves you finding out from a 400:

- no full-text search and no full-text indexes
- no `update`, `delete` or `merge_insert` — write a row with the same key
  instead, and the newest one wins
- namespaces are accepted and ignored: every namespace id lists the one root

[What you own](../self-hosting/operating.md) keeps that list, for the managed
service as much as for a node you run. Nothing on this list is a managed
feature held back.
