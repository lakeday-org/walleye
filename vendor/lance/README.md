# Vendored Lance 11.0.0

Pinned Apache-2.0 Lance 11.0.0 plus Lakeday MemWAL patches in
`src/dataset/mem_wal/`. `lakeday-lance` is the caller. Upstream owns the
columnar format, dataset, and indexes.

Compiled into the host. Not a daemon. Not a hosted Catalog.

```rust
use lance::{dataset::WriteParams, Dataset};

Dataset::write(reader, &uri, Some(WriteParams::default())).await?;
let dataset = Dataset::open(path).await?;
let batches = dataset.scan().try_into_stream().await?;
```

See [lance.org/format](https://lance.org/format).
