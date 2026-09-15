# Worklog

- Extracted the first standalone Walleye implementation without changing Lakeday.
- Simplified to one deployment identity and one cache allocation per node. The public API defines streams, ingests rows, and runs read-only SQL.
- Ported source regression tests and added tests for query snapshots, result-buffer accounting, authentication, schema validation, and restart recovery.
- Local MinIO and three-node Docker acceptance passed on 2026-09-14: 8,192 rows per storage mode, committed Bitr positions 1–8 archived, Foyer hits on all nodes, RAM/disk reclamation, and recovery of 1,000 API-ingested rows after killing ingress. Evidence: `.acceptance/acceptance.json`.

- Kubernetes is the default deployment. EndpointSlice discovery updates cache placement without a configured node list; immutable snapshots preserve in-flight readers. Cache replicas scale independently of the three Bitr replicas, which retain their PVCs and support SIGTERM.
- Verified on a dedicated three-worker kind cluster: cache pods 3 → 4 → 3, correct SQL over 8,320 rows, cache hits on all three pods, ingress and Bitr pod replacement, and recovery after SIGKILL. Report: `.acceptance/kubernetes.json`.

- Scan-only tenant integration: query responses carry a process snapshot ETag and conditional stream ingestion rejects stale or previous-boot revisions. Conditional commits fit one atomic WAL batch. Added bounded stateless HTTP processor dispatch from persisted stream rows, with retry and restart acceptance against the real engine; no Durable Object API is introduced.
- Processor shutdown stops new dispatch and drains callbacks while stream commits remain available. The shutdown regression holds a callback open, requests quiescence, and verifies the processor waits for its acknowledgment.
