# Worklog

- Extracted the first standalone Walleye implementation without changing Lakeday.
- Simplified to one deployment identity and one cache allocation per node. The public API defines streams, ingests rows, and runs read-only SQL.
- Ported source regression tests and added tests for query snapshots, result-buffer accounting, authentication, schema validation, and restart recovery.
- Local MinIO and three-node Docker acceptance passed on 2026-09-14: 8,192 rows per storage mode, committed Bitr positions 1–8 archived, Foyer hits on all nodes, RAM/disk reclamation, and recovery of 1,000 API-ingested rows after killing ingress. Evidence: `.acceptance/acceptance.json`.
