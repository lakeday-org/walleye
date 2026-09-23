# Documentation

Walleye is a LanceDB server. It keeps its write-ahead log in object storage,
serves reads from a local disk and memory cache, and speaks the LanceDB remote
protocol, so the stock `lancedb` client is the whole client story.

You can run it yourself from one binary, or use the managed service at
[walleye.dev](https://walleye.dev). It is the same server either way: the
managed side adds operations, not capability, and
[what you own](self-hosting/operating.md) says exactly where the line falls.

## Get started

- [Install](get-started/install.md) — get a binary and start it
- [Connect](get-started/connect.md) — the stock LanceDB client, and where the token comes from
- [Your first table](get-started/first-table.md) — create it, add rows, and what a key is
- [Your first query](get-started/first-query.md) — filters, vector search, and SQL

## Concepts

- [Durability](concepts/durability.md) — what an acknowledged write means
- [Shapes](concepts/shapes.md) — one node or three, and what changes
- [Cache tiers](concepts/cache-tiers.md) — what the cache holds, and how many tables fit
- [Lifecycle](concepts/lifecycle.md) — tables open, close and reopen; nodes stop and start
- [Ingest anything](concepts/ingest.md) — send JSON of any shape, get typed tables

## SDKs

- [Python](sdk/python.md)
- [JavaScript](sdk/javascript.md)
- [The HTTP surface](sdk/http.md) — every route, including the ones LanceDB has no call for

## Running it yourself

- [Running Walleye yourself](self-hosting/README.md)
- [Configuration](self-hosting/configuration.md)
- [A single node](self-hosting/single-node.md)
- [A cluster](self-hosting/cluster.md)
- [Budgets](self-hosting/budgets.md)
- [What you own](self-hosting/operating.md)

## Examples

Two pipelines that run on the binary with nothing managed behind them:
[the Bluesky firehose](../examples/bluesky-firehose/README.md) and
[live option flow](../examples/unusual-whales/README.md).
