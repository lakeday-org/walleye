# Configuration

Every setting is an environment variable. There is no configuration file to
write, and the node refuses to start rather than guessing at anything it
needs.

## Storage, and the only thing that is required

| Variable | Meaning |
|---|---|
| `WALLEYE_BUCKET` | Bucket name, optionally `bucket/prefix`. Becomes an `s3://` URI. |
| `WALLEYE_ROOT_URI` | A full URI instead, `s3://…` or `file://…`. Takes precedence. |

One of the two must be set. `file://` is for a local trial, since everything
lives on that one disk.

Credentials come from the usual AWS environment: `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT` for anything that is not AWS itself,
and `AWS_REGION`.

## Listening

| Variable | Default | Meaning |
|---|---|---|
| `WALLEYE_PORT` | `8080` | Port to listen on. |
| `WALLEYE_BIND` | `[::]` | Address to bind. The default is dual-stack. |
| `WALLEYE_TOKEN` | generated | The deployment token, at least 16 characters. It may call every route. |
| `WALLEYE_EDGE_KEY` | unset | When set, every request except `/healthz` must carry it in `x-walleye-edge-key`, or it is refused with 403 before its token is read. |

`WALLEYE_EDGE_KEY` is for a node that is reachable at an address you do not
want used, with a proxy in front that you do: the proxy adds the header, and
the address without it serves nothing but health checks. Members of a cluster
send it to each other, so every member needs the same value. Unset, the node
checks nothing.

The bind default reaches IPv6-only private networks and maps IPv4 clients in.
On a host with no IPv6 at all the node falls back to the same port on
`0.0.0.0` rather than refusing to start.

A generated token is printed to standard error once, at startup. That is fine
for a local trial and wrong for anything else, because it lands in whatever
collects your logs.

Clients other than your own tooling should use scoped
[access tokens](../sdk/http.md#access-tokens), which the node reads from
`_walleye/access.json` under its root and which change without a restart. A
node refuses to start when that file exists and cannot be read.

## Resources

| Variable | Default | Meaning |
|---|---|---|
| `WALLEYE_DIR` | `./walleye-cache` | Where the NVMe cache lives. |
| `WALLEYE_RAM_GB` | `1` | Memory for the whole process, not just the cache. |
| `WALLEYE_NVME_GB` | `8` | Disk for the cache and the replication log together. |

Both are whole-machine numbers. See [budgets](budgets.md) for what the node
holds back from them.

## Cluster

| Variable | Meaning |
|---|---|
| `WALLEYE_MEMBERS` | `id=http://host:8080,id=http://host:8080,…` |
| `WALLEYE_NODE_ID` | Which member in that list this process is. Required with `WALLEYE_MEMBERS`. |
| `WALLEYE_BITR_URL` | The local replica gateway. Setting it turns on quorum durability. |
| `WALLEYE_ADVERTISE_URL` | Without `WALLEYE_MEMBERS`, where other processes reach this one. `http://localhost:<port>` by default. Set it when a replacement may start beside a running node. |

The member list says where each node answers and where the replicas are. It
does not decide who owns a table; ownership is recorded in the bucket and
moves by itself when a node stops or dies. [A cluster](cluster.md#ownership-in-the-bucket)
describes it.

## Ownership leases

| Variable | Default | Meaning |
|---|---|---|
| `WALLEYE_LEASE_TTL_MS` | `10000` | How long a node owns its tables after its last renewal. Renewed every third of it. |
| `WALLEYE_LEASE_SKEW_MS` | `2000` | Slack a peer allows beyond the ttl before it calls a lease dead, and the longest a renewal may take to land. Below a third of the ttl. |
| `WALLEYE_OWNERSHIP_SAMPLE_MS` | `2000` | How often the leases and ownership records are read. |

A dead node's tables move after about ttl plus skew plus one sample, 14
seconds by default, plus the time to replay the log. A shorter ttl moves them
sooner and renews more often.

## Workers

Only relevant if you write workers, which the
[examples](../../examples/) use. All optional.

| Variable | Default | Meaning |
|---|---|---|
| `WALLEYE_WORKER_HEAP_MB` | `128` | Heap per worker, unless the view names its own. |
| `WALLEYE_WORKER_SECONDS` | `15` | Deadline per batch, unless the view names its own. |
| `WALLEYE_WORKER_FETCH_ALLOW` | empty | Hosts a worker may call, comma separated. Empty means none. |

`WALLEYE_WORKER_FETCH_ALLOW` is empty on purpose. A worker that can call
anywhere can send your rows anywhere, so reaching out is a decision you make
rather than something a worker can assume. An entry covers its subdomains.

## Classification

Only relevant if you use `prompt` or `prompt_jev` in SQL.

| Variable | Default | Meaning |
|---|---|---|
| `TYPESAFE_API_KEY` | none | Without it those functions fail saying so. |
| `TYPESAFE_URL` | the public endpoint | |
| `TYPESAFE_MODEL` | `jev-latest` | |
| `TYPESAFE_CONCURRENCY` | `16` | Requests in flight. |

The ingest path uses the same key. It asks Jev to decide what columns mean,
which table a new source belongs to, and which text to embed. Without a key it
still works: every judgement takes its safe default instead. See
[Ingest anything](../concepts/ingest.md).

## Embeddings

Only relevant if you want ingested text to be searchable by meaning, or to use
`embed()` in SQL. This is
configured separately from the model that answers questions in SQL, because
the two are chosen for different things and billed separately.

| Variable | Default | Meaning |
|---|---|---|
| `WALLEYE_EMBEDDING_KEY` | none | Without it nothing is embedded, and no table gets a vector column. |
| `WALLEYE_EMBEDDING_URL` | `https://api.openai.com/v1/embeddings` | Any endpoint that speaks OpenAI's embeddings format. |
| `WALLEYE_EMBEDDING_MODEL` | `text-embedding-3-small` | A table's vectors all come from one model. |
| `WALLEYE_EMBEDDING_TIMEOUT` | `20` | Seconds to wait for one call. |
