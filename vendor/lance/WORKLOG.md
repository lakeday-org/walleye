# vendored Lance 11 worklog

- #0: Added the narrow `dataset::mem_wal` backend seam used by Verglas. Lance
  now accepts a caller-owned `WalBackend` for append/read/position/fence
  operations, carries opaque backend commit knowledge through durable writes,
  and keeps the object-store WAL implementation as the default. Focused tests
  prove custom append/replay/fencing, receipt propagation, and absence of
  object-store WAL traffic; the upstream MemWAL suites remain green. The tests
  were run first and failed as expected with unresolved `WalBackend`/`WalWrite`
  types, then passed with `cargo test --manifest-path
  verglas/vendor/lance/Cargo.toml --lib dataset::mem_wal::write::tests
  --no-default-features` (86 tests) and the corresponding WAL suite (24 tests).
- #1: Preserve incoming Arrow schema metadata while MemWAL relabels batches to
  the storage schema and injects `_tombstone`. This keeps authenticated
  producer metadata available to custom WAL backends; a focused regression
  test covers the owner identity path.
- #0: Added a Verglas-focused README section around the retained upstream Lance reference. It explains the vendored library's host-only role and records that it does not provide Verglas authentication, placement, quorum, or public APIs.
- #query-priority-cache: Added a narrow LanceTableProvider builder for explicit scan I/O buffering and decode concurrency, forwarding to existing Scanner controls. The host charges those windows to its shared query/cache memory pool; this avoids the remote store's bandwidth-oriented default consuming the runtime RAM reserve. Host integration tests exercise the resulting FilteredReadExec; Bitr WAL behavior is unchanged.
- #query-priority-cache: The scan-memory rerun also exposed the independent 128-fragment read-ahead default. The host scan-buffer builder now bounds fragment preparation alongside decode concurrency; the plan contract test first failed with None instead of the explicit fragment bound.
- #query-priority-cache: The scan-buffer builder now takes independent fragment and decode windows so a CPU bound does not serialize high-latency object reads. It still forwards only existing Scanner controls; the host contract test verifies the exact fragment and I/O limits in the physical scan.
- #cold-open-fixes: WAL replay on `ShardWriter::open` no longer spawns a manifest cursor update per entry. `WalTailer::read_entry` used to detach a `read_latest` plus conditional manifest write for every entry it returned, so a cold open of an unflushed shard issued about ten object-store requests per replayed entry (2,101 for 200 entries, ~10k for 1,000) and the detached writes kept running after the open returned. Replay now reads through `read_entry_without_cursor` and records the tip once with `record_cursor`; it also reuses the writer's manifest store instead of a second one built with a scan batch of 2, so its manifest probes use the configured `manifest_scan_batch_size`. The public `read_entry` keeps its per-read hint for ordinary tailers.
- #cold-open-fixes: Added `ShardWriter::checkpoint`, the non-closing half of `close`: drain the active memtable's index apply, append its unflushed WAL tail, freeze it, and wait for every frozen memtable to flush, stamping `replay_after_wal_entry_position`. A host whose objects never reach the size-triggered flush can call it on a cadence so an unclosed writer (crash, suspend, deploy) still leaves a bounded replay tail for the next open.
