# Connect

Connecting is the stock LanceDB client. There is no Walleye adapter, no
plugin, and nothing to install past `lancedb` itself.

```python
import lancedb

db = lancedb.connect(
    "db://walleye",
    api_key=TOKEN,
    host_override="http://127.0.0.1:8080",
    region="us-east-1",
)
```

```js
const db = await lancedb.connect({
  uri: "db://walleye",
  apiKey: TOKEN,
  hostOverride: "http://127.0.0.1:8080",
  region: "us-east-1",
});
```

The client picks its transport from the URI scheme: `db://` is a remote
database and anything else is a local directory. So the node's address goes in
`host_override` (`hostOverride` in JavaScript), never in the URI. The name
after `db://` is a label the client wants and the node ignores — one node is
one database.

**The token.** Self-hosted, it is `WALLEYE_TOKEN`, or the one the node
generated and printed to standard error at startup, or an
[access token](../sdk/http.md#access-tokens) you published for the node. On
the managed service you make access tokens for an instance from its page or
its API: each has a name, the scopes it may use, and an optional expiry, and
its value is shown once, when you make it. Creating an instance does not make
one. Either way the client sends it as `x-api-key`; `Authorization: Bearer`
works too if you are calling [the HTTP surface](../sdk/http.md) directly.

**`region`.** The client requires it and the node ignores it. It is part of
the client's own default addressing, which a host override replaces, so any
value will do.

Next: [your first table](first-table.md).
