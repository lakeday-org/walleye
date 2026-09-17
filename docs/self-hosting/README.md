# Running Walleye yourself

Walleye is one binary. It speaks the LanceDB remote protocol, keeps its
write-ahead log in object storage, and caches on local NVMe and in memory.
Everything in the examples runs on this binary with nothing managed behind it.

- [Configuration](configuration.md), the whole environment surface
- [A single node](single-node.md), against any S3-compatible bucket
- [A cluster](cluster.md), three nodes with quorum durability
- [Budgets](budgets.md), what the node reserves and why
- [What you own](operating.md), and what is different on the managed side

## The shortest version

```sh
export WALLEYE_BUCKET=my-bucket
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT=https://t3.storage.dev   # any S3-compatible endpoint
export AWS_REGION=auto

walleye-node
```

It prints a generated token if you did not set one. Point the LanceDB client
at it and the rest is ordinary:

```python
import lancedb
db = lancedb.connect("http://127.0.0.1:8080", api_key="<token>", region="auto")
db.create_table("docs", data=[{"id": "a", "vector": [0.1, 0.2]}])
```
