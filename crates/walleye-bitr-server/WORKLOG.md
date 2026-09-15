# Worklog

- Extracted the storage-engine implementation and regression tests from Lakeday for the first standalone Walleye implementation. Lakeday is unchanged.

- Kubernetes is the default deployment. EndpointSlice discovery updates cache placement without a configured node list; immutable snapshots preserve in-flight readers. Cache replicas scale independently of the three Bitr replicas, which retain their PVCs and support SIGTERM.
- Verified on a dedicated three-worker kind cluster: cache pods 3 → 4 → 3, correct SQL over 8,320 rows, cache hits on all three pods, ingress and Bitr pod replacement, and recovery after SIGKILL. Report: `.acceptance/kubernetes.json`.
