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

The node has one index layout, so what it takes from the call is the column and
the metric — `l2`, `cosine` or `dot`. A query then uses that metric, and asking
it for a different one is an error.

## SQL

No client call; post to the node.

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

- `update`, `delete` and `mergeInsert` are not implemented
- full-text search is not implemented
- ordering on a search is not implemented; use SQL
- column expressions in `select` are not implemented; use SQL
- namespaces are accepted and ignored

Each rejects with a 400 and a plain reason.
