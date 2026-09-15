# Walleye

A single-tenant streaming query engine: define a stream, ingest rows, query it with
SQL. Walleye combines Lakeday's Bitr WAL, Lance MemWAL storage, a distributed cache
ring, and the Foyer fork that gives cache RAM and disk back to queries.

One deployment has one API token, one stream namespace, and one cache allocation
per cache pod. There is no tenant routing, tenant partitioning, or shared SaaS boundary.
The previous Walleye code-review application is moving into Lakeday. This extraction does not change Lakeday.

## Kubernetes (default)

Kubernetes runs two StatefulSets: three Bitr replicas with persistent WAL disks,
and a scalable set of cache pods. Cache pod zero also serves the stream/SQL API.
Walleye reads the cache Service's [EndpointSlices](https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/)
every five seconds, so adding, removing, or replacing cache pods requires no
Walleye node-list edits. Pod names provide stable identities; readiness controls
which cache endpoints receive new work. Discovery failures preserve the last good
list, and unavailable cache entries fall back to S3.

For an existing cluster with at least three worker nodes and a default StorageClass:

1. Build and publish the image to your registry. Set its name/tag in
   `deploy/kubernetes/kustomization.yaml`.
2. Set the S3 region, endpoint, data prefix, and archive bucket in the same file.
3. Create `.env.kubernetes` with your storage credentials and deployment keys:
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `WALLEYE_TOKEN`,
   `LAKEDAY_DATAPLANE_ROOT_KEY`, `LAKEDAY_REPLICA_INTERNAL_TOKEN`, and
   `WALLEYE_DATA_KEY`. The two keys are base64-encoded 32-byte values. Retain them
   across pod replacements.
4. Apply the manifests and forward the API:

```sh
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl -n walleye create secret generic walleye-secrets --from-env-file=.env.kubernetes
kubectl apply -k deploy/kubernetes
kubectl -n walleye port-forward service/walleye-api 18085:8080
```

Scale cache capacity with ordinary Kubernetes commands:

```sh
kubectl -n walleye scale statefulset/walleye-cache --replicas=4
```

Bitr remains a separate three-replica group with a quorum of two. Kubernetes
replaces its pods with their existing disks and DNS identities; cache scaling
does not change WAL replication. Changing Bitr's replication group requires its
log repair and agreement on which replicas will acknowledge future writes.
It is not tied to an HPA.
The manifests spread Bitr across three workers and permit only one voluntary
replica disruption at a time.

### Local Kubernetes acceptance

Install Docker, `kubectl`, and [kind](https://kind.sigs.k8s.io/docs/user/quick-start/), then:

```sh
python3 integration/kubernetes_acceptance.py
```

This creates a dedicated `walleye` kind cluster with three workers and local MinIO,
using `.acceptance/kubeconfig` rather than changing your current Kubernetes
context. It checks concurrent stream ingestion/SQL, cache scaling **3 → 4 → 3**, cache hits,
Bitr pod replacement, and recovery after the ingress process crashes. Evidence is
saved to `.acceptance/kubernetes.json`. The cluster stays available for inspection:

```sh
kubectl --kubeconfig .acceptance/kubeconfig -n walleye get pods
kubectl --kubeconfig .acceptance/kubeconfig -n walleye port-forward service/walleye-api 18085:8080
# Remove only this local test cluster when finished:
kind delete cluster --name walleye --kubeconfig .acceptance/kubeconfig
```

## API

All requests use `Authorization: Bearer <deployment token>`. The examples use the local Kubernetes fixture's API port-forward at
`http://localhost:18085`; its token is a public local-test value.

```sh
# 1. Define a stream (one Lance memshard).
curl --fail-with-body http://localhost:18085/v1/streams \
  -H 'Authorization: Bearer acceptance-walleye-token' \
  -H 'Content-Type: application/json' \
  -d '{"name":"clicks","columns":[{"name":"id","type":"int64"},{"name":"city","type":"string"},{"name":"value","type":"float64"}],"primary_key":["id"]}'

# 2. Ingest rows. An acknowledgment follows the selected durable WAL commit.
curl --fail-with-body http://localhost:18085/v1/streams/clicks/events \
  -H 'Authorization: Bearer acceptance-walleye-token' \
  -H 'Content-Type: application/json' \
  -d '{"rows":[{"id":1,"city":"Seattle","value":2.5},{"id":2,"city":"Seattle","value":4.0}]}'

# 3. Query the hot log and persisted Lance generations together.
curl --fail-with-body http://localhost:18085/v1/query \
  -H 'Authorization: Bearer acceptance-walleye-token' \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT city, count(*) AS n, sum(value) AS total FROM clicks GROUP BY city"}'
# [{"city":"Seattle","n":2,"total":6.5}]
```

Column types are `string`, `int64`, `float64`, and `boolean`; set `nullable: true`
explicitly when needed. Primary keys must exist and be non-nullable. Identical
stream definitions are idempotent; conflicting definitions fail. Unknown fields
and invalid row types fail before ingestion. SQL is read-only DataFusion SQL,
including filtering, aggregation, joins, and ordering. Results are JSON arrays.
HTTP request and JSON result bodies are limited to 8 MiB; SQL text to 64 KiB.
Queries have a 60-second execution deadline and share the node's memory/spill pool.

Each stream owns its own memshard, writer lock, WAL sequence, and S3 manifest.
Different streams ingest concurrently in both S3 CAS and Bitr modes. Concurrent
requests to the same stream queue at that stream's writer; overlapping requests
are ordered when they acquire its lock. SQL captures a snapshot of each stream it
reads and releases that stream's lock before execution. Unrelated streams keep
writing, and repeated references within a query use the same captured snapshot.

## Single node: S3 CAS

Set `api.root_uri` to an S3 prefix and omit `api.bitr_url`; set `bitr: false`.
The node owns its memshards and cache. Lance claims writer epochs and publishes
WAL entries and manifests through conditional object-store operations.

```mermaid
flowchart LR
    A[Client: define / ingest / SQL] --> API[Walleye HTTP API]
    API --> C[Stream definitions: create if absent]
    C --> S[(S3 bucket)]
    API --> W1[Stream A: memshard + writer]
    API --> W2[Stream B: memshard + writer]
    W1 & W2 -->|Independent WAL and manifest CAS| S
    W1 & W2 -->|Flush Lance generations| S
    API --> Q[Read-only SQL]
    W1 & W2 -->|Hot and frozen memtables| L[Unified Lance snapshot]
    Q --> L
    L --> F[Foyer: metadata, indexes, data blocks]
    F -->|Exact-version miss| S
    R[Shared query and cache resource budget] --> Q
    R -->|Reclaim RAM and spill disk| F
```

## Cluster: Bitr log writeback

One designated ingress owns live memshards and executes SQL. Kubernetes manages
three Bitr pods and a separate set of cache pods. Bitr acknowledges after two
replicas durably accept an encrypted log entry, then archives it to S3. Lance
separately flushes generations and advances its replay watermark using S3 CAS.

```mermaid
flowchart TB
    A[Client: define / ingest / SQL] --> API[Kubernetes API Service]
    API --> I[Cache pod 0: ingress + SQL]
    I --> M[Lance memshards: independent writer per stream]
    M -->|Concurrent encrypted WAL appends| G[Bitr gateway Service]
    subgraph WAL[Bitr StatefulSet: three replicas]
      B1[(Replica 0 + persistent WAL disk)]
      B2[(Replica 1 + persistent WAL disk)]
      B3[(Replica 2 + persistent WAL disk)]
    end
    G -->|Ack requires 2 of 3| B1
    G --> B2
    G --> B3
    B1 & B2 & B3 -->|Archive committed segments| S[(S3)]
    M -->|Lance generations + per-stream manifest CAS| S
    I --> R[Cache routing]
    K[Kubernetes EndpointSlices] -->|Ready pod addresses| R
    subgraph CACHE[Cache StatefulSet: independently scalable]
      F0[Foyer on ingress pod 0]
      F1[Foyer on cache pod 1]
      F2[Foyer on cache pod 2 and later pods]
    end
    R --> F0
    R --> F1
    R --> F2
    I -->|Cache miss: versioned origin read| S
```

Peer misses never load from S3 or forward to another peer. The requesting query
engine owns origin loading. Cache identities include the storage binding, object
version, range, and geometry. Entries without a Lance serialization codec stay in
local RAM. Internal cache routes use the deployment token.

## Modules and resource ownership

| Crate | Responsibility |
| --- | --- |
| `walleye-bitr` | Encrypted records, quorum client, commit certificates, recovery |
| `walleye-bitr-server` | Replica logs, coordinator, archival, placement, fencing |
| `walleye-lance` | WAL adapter, memshards, stable snapshots, read-only SQL |
| `walleye-cache` | Lance cache backend over Foyer, object blocks, peer envelopes, query resources |
| `walleye-ring` | Weighted rendezvous ownership and bounded previous-owner handoff |
| `walleye-node` | Stream HTTP API, durable definitions, authentication, peer service |
| `walleye-workload` | Executable S3 and Docker acceptance workload |

This uses **Lance's cache interface**. Metadata, decoded indexes, and versioned data blocks share Foyer. On the
ingress, SQL and peer-resident cache entries share the same allocation. Query
memory reservations reclaim Foyer RAM; result buffers retain their reservations
until dropped, with shared Arrow allocations counted once. The first spill shrinks
Foyer's physical disk to its working floor; the last removed spill restores it.
Ring weights describe provisioned capacity, independent of query reservations.

The budget excludes application/OS overhead, HTTP buffers, and active ingestion
buffers. Once Lance publishes a generation and its checkpoint, the Bitr adapter
releases the covered WAL buffers. Restart loads those Lance generations and
replays only later WAL entries. This is not a total-process RSS guarantee or
continuous byte-for-byte disk sharing.

## Docker Compose acceptance

Requirements: Docker with Compose, approximately 20 GB of free build space, and
Rust 1.96.1 for native checks. The first image build compiles Lance/DataFusion.
The fixture uses local MinIO with S3 data and archive buckets. Published ports
bind to loopback; all fixture credentials and encryption keys are test values.

```sh
cargo test --workspace --lib --tests
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
python3 integration/acceptance.py
```

The runner builds the image and starts three cluster nodes, a single-node API, and
MinIO. It verifies:

- S3 conditional create/update and rejection of stale writes.
- Single-node and cluster ingestion with hot, checkpointed, and reopened reads.
- Bitr quorum certificates and archival of every acknowledged log position.
- Workload cache hits on all three nodes, plus RAM and physical disk reclamation.
- Stream definition, ingestion, SQL aggregation, and recovery after killing the ingress.
- Four streams with eight concurrent API clients in each mode: 4,096 rows per mode,
  queries during writes, and counts/checksums preserved after a crash.

JSON reports and workload logs are written under `.acceptance/`. The fixture is
retained for inspection:

```sh
docker compose -f integration/compose.yaml ps
docker compose -f integration/compose.yaml logs node-1
docker compose -f integration/compose.yaml down
# Discard only this fixture's storage when finished:
docker compose -f integration/compose.yaml down --volumes
```

The Docker fixture exposes its cluster API on port 18081 and its single-node API
on port 18084. Its node configuration lives
in `integration/config/`. Only the ingress has `api`
configured. Set `AWS_ENDPOINT`, credentials, region, and bucket URIs to use an
external S3 service. Bitr retains its original `LAKEDAY_REPLICA_*` environment
configuration and protocol framing; Walleye uses one fixed deployment identity.
`WALLEYE_DATA_KEY` is a base64-encoded 32-byte WAL encryption key and must survive
restarts. Source deployment credentials are never copied from Lakeday.

## Scope and provenance

- Bitr protocol/server logic and regression suites come from
  `lakeday/crates/lakeday-replica` and `fly/bitr/replica`.
- The WAL adapter and cache/resource tests come from `lakeday-lance` and
  `lakeday-cache`. Product event, catalog, Worker, and control-plane code is excluded.
- The historical Verglas ring source was deleted before the monorepo import, and
  its old repository is unavailable. This implementation reconstructs documented
  rendezvous and join/drain contracts; it does not claim recovered source or old
  hash-wire compatibility. Kubernetes supplies membership through EndpointSlices; the Docker fixture uses
  a fixed local list.
- The pinned Foyer fork is `lakeday-org/foyer` at
  `799c9768a0a5d0b92488bb1d5c0147ff8892d865`, including physical `FileDevice` resize.
- `vendor/lance` retains Lance 11 WAL/session patches; `vendor/datafusion-execution`
  retains the pre-spill reclamation hook.
- Existing licenses and notices remain intact. Extracted engine code retains
  **FSL-1.1-ALv2** in `LICENSE`; publishing under an open-source license requires an
  explicit licensing decision.

This first implementation runs concurrent stream writers at one ingress and takes
one stable snapshot per referenced stream for SQL. It does not provide distributed SQL,
active/active ingress, schema migration, or cross-stream transactions. A
multi-batch ingestion failure may leave a committed prefix; retry with stable
primary keys. Cache loss is safe; loss of a Bitr quorum blocks writes.
