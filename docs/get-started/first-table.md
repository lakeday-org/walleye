# Your first table

A table is created by writing to it. The schema comes from the rows unless you
hand the client an Arrow schema, in which case it comes from that.

```python
table = db.create_table("clicks", data=[
    {"id": 1, "city": "seattle",  "vector": [0.0, 1.0]},
    {"id": 2, "city": "seattle",  "vector": [1.0, 0.0]},
    {"id": 3, "city": "portland", "vector": [0.0, -1.0]},
])
table.add([{"id": 4, "city": "boise", "vector": [-1.0, 0.0]}])
table.count_rows()
```

```js
const table = await db.createTable("clicks", [
  { id: 1, city: "seattle", vector: [0.0, 1.0] },
  { id: 2, city: "seattle", vector: [1.0, 0.0] },
  { id: 3, city: "portland", vector: [0.0, -1.0] },
]);
await table.add([{ id: 4, city: "boise", vector: [-1.0, 0.0] }]);
await table.countRows();
```

`create_table` on a name that exists is an error; `open_table` opens it.
`list_tables` lists them and `drop_table` removes one, along with its data in
object storage. A table name may hold only `[A-Za-z0-9_-]`, and a column name
may not begin with `_`; both are refused rather than rewritten.

Each table is its own memshard: an independent writer, its own log sequence and
its own manifest. Two tables ingest in parallel and never wait on each other.

## The write is durable before you are told it worked

`add` does not return until the batch is in the log and the log is durable.
There is no flush to remember and no window in which an acknowledged row is
only in memory. [Durability](../concepts/durability.md) is what that costs and
what it buys.

## Keys, and why a retry is safe

Every row has a key. If you do not name one, Walleye derives it from a hash of
the row's own content, so two identical rows collapse to one and an insert you
retried after a timeout lands once rather than twice.

Name your own by marking the field on the Arrow schema with the Lance metadata
`lance-schema:unenforced-primary-key = "true"`:

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.string(), metadata={"lance-schema:unenforced-primary-key": "true"}),
    pa.field("text", pa.string()),
    pa.field("vector", pa.list_(pa.float32(), 3)),
])
table = db.create_table("docs", schema=schema)
table.add([{"id": "a", "text": "first", "vector": [0.1, 0.2, 0.3]}])
```

Writing `id = "a"` again replaces the row rather than adding a second one. It
is unenforced in the sense that nothing rejects a duplicate at write time: the
newest row for a key is the one that survives compaction and the one a query
sees.

Pick your own key when the source already has an identity — a provider's event
id, a document URI — so that re-reading the same window writes nothing the
second time. Leave it alone otherwise.

## Vector columns

A fixed-size list of `float32` is a vector column, and it is indexed from the
first row. There is no training step and nothing to schedule: the rows in
memory are held in a graph, each flush writes an index for that generation, and
compaction rebuilds them. `create_index` exists to choose the metric — `l2`,
`cosine` or `dot` — and it rewrites what is already flushed before it returns,
so a query never reads a stale index.

That one layout is the only one there is. `Index.btree()`, `Index.bitmap()`
and `Index.labelList()` are refused, so there is no index to build over a
scalar column: `where` on one is a scan, which is correct and costs what a scan
costs. [Refusals](../sdk/http.md#refusals) has the rest.

A table can have several vector columns. Each one costs memory for as long as
the table is open, which is what [cache tiers](../concepts/cache-tiers.md) is
about.

Next: [your first query](first-query.md).
