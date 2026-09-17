# A single node

One process, one bucket. Durability is the bucket's: a write is durable once
the log reaches object storage, and the local disk holds only cache.

```sh
export WALLEYE_BUCKET=my-bucket
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_ENDPOINT=https://t3.storage.dev
export AWS_REGION=auto

export WALLEYE_TOKEN=$(openssl rand -hex 24)
export WALLEYE_DIR=/var/lib/walleye
export WALLEYE_RAM_GB=8
export WALLEYE_NVME_GB=200

walleye-node
```

## Checking it

```sh
curl -s localhost:8080/healthz -H "authorization: Bearer $WALLEYE_TOKEN"
curl -s localhost:8080/readyz  -H "authorization: Bearer $WALLEYE_TOKEN"
```

`/healthz` answers as soon as the process can route. `/readyz` reports whether
writes can be made durable and names which members are serving. On a single
node the two say the same thing; in a cluster they do not, which is the point
of having both.

## Connecting

The LanceDB client is the primary way in and needs no adapter:

```python
import lancedb

# The address goes in host_override. The name after db:// is a label the
# client wants and the node ignores, and region is required by the client
# and ignored by the node.
db = lancedb.connect(
    "db://walleye",
    api_key=TOKEN,
    host_override="http://127.0.0.1:8080",
    region="local",
)
table = db.create_table("docs", data=[
    {"id": "a", "text": "first", "vector": [0.1, 0.2, 0.3]},
])
table.add([{"id": "b", "text": "second", "vector": [0.2, 0.1, 0.4]}])
table.search([0.1, 0.2, 0.3]).limit(5).to_list()
```

SQL is a separate route, read-only, with a sixty second deadline:

```sh
curl -s localhost:8080/v1/query \
  -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"sql": "SELECT count(*) AS n FROM docs"}'
```

## What a single node does not give you

A write is durable when it reaches the bucket, so losing the node loses
nothing committed. It does mean the node is the only thing serving: while it
is down, nothing answers. If that matters, run [a cluster](cluster.md).
