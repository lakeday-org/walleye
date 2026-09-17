# Python

The client is `lancedb`. Walleye speaks the LanceDB remote protocol, so there
is nothing Walleye-specific to import.

```sh
pip install lancedb pyarrow
```

```python
import os
import lancedb

db = lancedb.connect(
    "db://walleye",
    api_key=os.environ["WALLEYE_TOKEN"],
    host_override=os.environ["WALLEYE_URL"],
    region="us-east-1",
)
```

See [connect](../get-started/connect.md) for where the token comes from and why
`region` is there.

## Tables

```python
db.create_table("clicks", data=rows)      # schema inferred from the rows
db.create_table("docs", schema=schema)    # or from an Arrow schema
db.open_table("clicks")
db.table_names()
db.drop_table("clicks")
```

`create_table` on a name that already exists raises; the error says so.

## Writing

```python
table.add(rows)                                   # list of dicts
table.add(pa.Table.from_pylist(rows, schema=schema))
```

`add` returns once the batch is durable. Rows are keyed — by the field you
marked `lance-schema:unenforced-primary-key`, otherwise by a hash of the row —
so re-adding a row you already wrote replaces it instead of duplicating it. See
[your first table](../get-started/first-table.md).

## Reading

```python
table.count_rows()
table.count_rows("city = 'seattle'")

table.search().where("id > 1").select(["id", "city"]).limit(10).offset(20).to_list()
table.search([0.0, 0.9]).limit(5).to_list()
table.search([0.0, 0.9]).where("city = 'portland'").limit(5).to_list()
```

`where` is a SQL expression, `select` takes column names — not expressions —
and a vector search returns `_distance` on each row.

## Indexes

```python
table.create_index(metric="cosine")
table.list_indices()
```

Vector columns are indexed from the first row without this. `create_index`
chooses the metric — `l2`, `cosine` or `dot` — and rewrites the flushed
generations before it returns. Queries use the index's metric; asking a query
for a different one is an error.

**Scalar indexes are refused.** `Index.btree()`, `Index.bitmap()` and
`Index.labelList()` all fail: the node accepts only index types beginning
`IVF` or `HNSW`, because it maintains one layout.

```
index type BTREE is not supported; vector columns use IVF_HNSW_SQ
```

Filtering a scalar column still works — `where` is evaluated as a scan, so it
is correct, and the cost is the scan.

## SQL

The client has no SQL call, so post to the node:

```python
import requests

rows = requests.post(
    f"{os.environ['WALLEYE_URL']}/v1/query",
    headers={"authorization": f"Bearer {os.environ['WALLEYE_TOKEN']}"},
    json={"sql": "SELECT city, count(*) AS n FROM clicks GROUP BY city"},
).json()
```

Read-only DataFusion SQL, sixty second deadline, 8 MiB of JSON back. On one
node it reads every table on the node; in a cluster it also gathers the tables
this node does not own from their owners, so a `SELECT` spans the cluster.
[The HTTP surface](http.md) has the rest of the routes.

## What raises

`update`, `delete` and `merge_insert` are not implemented, and they are not a
refusal with a reason either: there is no route, so the node answers **404 with
an empty body**. Whatever the client raises for that is what you will see. Write
a row with the same key instead — the newest one wins.

Everything else here is a 400 with the reason as the body:

- `Index.btree()`, `Index.bitmap()` and `Index.labelList()` — only `IVF*` and
  `HNSW*` index types are accepted
- full-text search and `Index.fts()`
- `order_by` on a search; use SQL
- column expressions in `select`; use SQL
- `add(..., mode="overwrite")`; drop and recreate instead
- a column named with a leading `_`, or a table name outside `[A-Za-z0-9_-]`
- searching a table with more than one vector column without naming
  `vector_column`, and a multivector query
- `exist_ok=True` against a table whose schema has changed

Namespaces are the exception that is neither: they are accepted and ignored, and
every namespace id lists the one root. A malformed JSON body is a 422 rather
than a 400. [The HTTP surface](http.md#refusals) has the message each one
returns.
