# DataFusion execution spill hook

This directory contains the published Apache DataFusion `datafusion-execution`
54.1.0 crate, under its original Apache-2.0 license and notices. The source patch
is confined to `src/disk_manager.rs`.

`SpillFileObserver::before_create` lets the runtime reclaim disposable cache
space before DataFusion creates a spill file. Its returned `SpillFileGuard` is
shared by clones of that file and released after the underlying temporary file
is deleted. Reclamation errors prevent creation. A single shared DiskManager
continues to enforce the combined spill quota across query sessions.

The original API exposes only concrete DiskManager and temporary-file types,
with no pre-creation callback. Polling disk usage cannot guarantee reclamation
before a write. The hook provides that boundary without changing SQL operators,
spill formats, or the vendored Lance/Bitr WAL integration.

The current host conservatively lends the available disk allocation to spilling
until the last spill file disappears. It retains Foyer's minimum working blocks.
Finer incremental sharing requires a pre-write reservation hook; DataFusion's
existing disk-size check runs after writes. The outer finite filesystem enforces the final disk ceiling. No fixed disk
reserve is withheld from queries and cache.

Tests are in `crates/lakeday-cache/tests/spill_callbacks.rs` and
`query_resources.rs`, so normal workspace CI exercises the patched dependency.
When updating DataFusion, reapply this small hook to the matching published
execution crate or remove it when an equivalent upstream extension is available.
