# Walleye

Wal over S3 (LanceDB) define a stream, ingest rows, query it with
SQL. 

Each stream owns its own memshard, writer lock, WAL sequence, and S3 manifest.
Different streams ingest concurrently in both S3 CAS and Bitr modes.

## API

```sh
# 1. Define a stream (one Lance memshard).
curl http://localhost:18085/v1/streams \
  -H 'Authorization: Bearer <TOKEN>' \
  -H 'Content-Type: application/json' \
  -d '{"name":"clicks","columns":[{"name":"id","type":"int64"},{"name":"city","type":"string"},{"name":"value","type":"float64"}],"primary_key":["id"]}'

# 2. Ingest.
curl  http://localhost:18085/v1/streams/clicks/events \
  -H 'Authorization: Bearer <TOKEN>' \
  -H 'Content-Type: application/json' \
  -d '{"rows":[{"id":1,"city":"Seattle","value":2.5},{"id":2,"city":"Seattle","value":4.0}]}'

# 3. Query.
curl  http://localhost:18085/v1/query \
  -H 'Authorization: Bearer acceptance-walleye-token' \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT city, count(*) AS n, sum(value) AS total FROM clicks GROUP BY city"}'
# [{"city":"Seattle","n":2,"total":6.5}]
```

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

