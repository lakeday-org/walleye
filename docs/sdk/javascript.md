# JavaScript

The client is `@lancedb/lancedb`. Walleye speaks the LanceDB remote protocol,
so there is nothing Walleye-specific to install.

```sh
npm install @lancedb/lancedb
```

```js
import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect({
  uri: "db://walleye",
  apiKey: process.env.WALLEYE_TOKEN,
  hostOverride: process.env.WALLEYE_URL,
  region: "us-east-1",
});
```

See [connect](../get-started/connect.md) for where the token comes from and why
`region` is there.

## Tables

```js
await db.createTable("clicks", rows);
await db.openTable("clicks");
await db.tableNames();
await db.dropTable("clicks");
```

## Writing

```js
await table.add(rows);
```

`add` resolves once the batch is durable. Rows are keyed, so re-adding a row
you already wrote replaces it rather than duplicating it.

## Reading

```js
await table.countRows();
await table.countRows("city = 'seattle'");

await table.query().where("id > 1").select(["id", "city"]).limit(10).toArray();
await table.search([0.0, 0.9]).limit(5).toArray();
await table.search([0.0, 0.9]).where("city = 'portland'").limit(5).toArray();
```

A vector search returns `_distance` on each row.

## Indexes

```js
await table.createIndex("vector", { config: lancedb.Index.hnswSq({ distanceType: "cosine" }) });
await table.listIndices();
```

Vector columns are indexed from the first row without this. The call is how you
choose the metric, and it rewrites what is already flushed before it resolves.

What the node takes from the call is the column and the metric — `l2`, `cosine`
or `dot`. A query then uses that metric, and asking it for a different one is an
error.

**Scalar indexes are refused.** `Index.btree()`, `Index.bitmap()` and
`Index.labelList()` all reject: the node accepts only index types beginning
`IVF` or `HNSW`, because it maintains one layout.

```
index type BTREE is not supported; vector columns use IVF_HNSW_SQ
```

Filtering a scalar column still works — `where` is evaluated as a scan, so it
is correct, and the cost is the scan.

## SQL

No client call; post to the node. It is read-only DataFusion SQL with a sixty
second deadline and 8 MiB of JSON back; on three nodes it spans the cluster.

```js
const rows = await fetch(`${process.env.WALLEYE_URL}/v1/query`, {
  method: "POST",
  headers: {
    authorization: `Bearer ${process.env.WALLEYE_TOKEN}`,
    "content-type": "application/json",
  },
  body: JSON.stringify({ sql: "SELECT city, count(*) AS n FROM clicks GROUP BY city" }),
}).then((response) => response.json());
```

[The HTTP surface](http.md) has the rest of the routes.

## What rejects

`update`, `delete` and `mergeInsert` are not implemented, and they are not a
rejection with a reason either: there is no route, so the node answers **404
with an empty body**. Write a row with the same key instead — the newest one
wins.

Everything else here is a 400 with the reason as the body:

- `Index.btree()`, `Index.bitmap()` and `Index.labelList()` — only `IVF*` and
  `HNSW*` index types are accepted
- full-text search and `Index.fts()`
- ordering on a search; use SQL
- column expressions in `select`; use SQL
- `add` with `mode: "overwrite"`; drop and recreate instead
- a column named with a leading `_`, or a table name outside `[A-Za-z0-9_-]`
- searching a table with more than one vector column without naming
  `vectorColumn`, and a multivector query
- creating a table that already exists with a different schema

Namespaces are the exception that is neither: they are accepted and ignored. A
malformed JSON body is a 422 rather than a 400.
[The HTTP surface](http.md#refusals) has the message each one returns.
