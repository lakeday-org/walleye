# Ingest anything

`POST /v1/ingest/{source}` takes JSON of any shape and turns it into tables.
You don't declare a schema, map fields or handle optional values. Send the
records and Walleye works out the rest.

```sh
curl -X POST localhost:8080/v1/ingest/stripe-webhooks \
  -H "Authorization: Bearer $WALLEYE_TOKEN" \
  -d '[{"id": "in_1", "amount": "12.50", "created": 1700000000, "zip": "02134"}]'
```

The body is a list of records, a single record, or `{"records": [...]}`. The
`source` is whatever you call the feed, up to 128 letters, digits, `.`, `_` or
`-`. The response says what happened:

```json
{
  "source": "stripe-webhooks",
  "table": "stripe_webhooks",
  "created": true,
  "accepted": 1,
  "quarantined": 0,
  "rescued": 0,
  "rebuilt": true,
  "changes": [
    {"what": "type of amount",  "chose": "decimal",           "by": "jev",  "confidence": 0.82},
    {"what": "type of created", "chose": "timestamp_seconds", "by": "jev",  "confidence": 0.95},
    {"what": "primary key",     "chose": "id",                "by": "jev",  "confidence": 0.91}
  ]
}
```

## Three layers

**Bronze is what arrived.** Every record is written to `ingest_bronze` first,
before anything is decided. It's stored exactly as its bytes came, keyed on a
hash of the source and the record. Numbers are never parsed into floating
point on the way in, so `9007199254740993` keeps its last digit.

**Rules route, and a judge decides once.** The first time a source is seen,
Jev decides:

- which existing table it belongs to, if any
- what the new table is called
- what each column's values mean
- which field, if any, identifies a record

Those decisions are written into the table's rule, with the confidence they
were made at. From then on, records are placed by the rule alone, and Jev is
never asked about individual records.

**Silver is rebuilt, never altered.** The typed table can't grow a column in
place, so a new column or a wider type rebuilds it from bronze. Making a field
optional needs no rebuild, because every column except the key is stored
nullable from the start.

## What the judge decides, and what it doesn't

A type is chosen from a fixed list: `boolean`, `int64`, `float64`, `decimal`,
`timestamp_text`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`,
`string` and `json`. Only types that every value converts to without loss are
offered, so the judge can't pick a number type for a zip code. `"02134"` is
only ever offered `string`, and when only one type fits, no one is asked.

Some things are decided by code, because a judge that's wrong once mustn't be
able to do damage a rebuild can't undo:

- **A key is never optional.** A record without its key is refused, whatever
  a judge would say.
- **A table never has more than 200 columns.** Records keyed by ids, like
  `{"user_123": …}`, are kept whole in the catch-all column instead.
- **A rename to another case convention is a rename.** `userId` is `user_id`.
  The names match once case and separators are ignored, so no one needs to be
  asked.

Renames that change the words, like `customer` to `client`, go to the judge.
It gets one question per new field, "which of these columns is it, or is it
new?", with every column it could be as an option. Each option shows the
values that column already holds, next to the values the new field holds.

Renames are merged on the judge's lean, not only on conviction. Both mistakes
can be fixed by rebuilding from bronze, and a missed rename is the worse one:
it splits a field across two half-empty columns, and a missed key rename
refuses every record from the rename on. In practice the judge is decisive
when a merge would be wrong (a new `weight_kg` field versus a missing `colour`
column: 100% new field) and only moderately sure when it's right (`client`
versus `customer`: 62%).

For everything except renames, when the judge is unsure (below 0.6
confidence) or there's no judge configured, the safe choice is made instead. Safe means recoverable:

- the narrowest type that loses nothing (`decimal` rather than `float64`)
- optional rather than required
- a value that doesn't fit is kept aside rather than refused

## When records change shape

| What arrives | What happens |
|---|---|
| A new field | A column is added, typed the same way, and the table is rebuilt from bronze |
| A field goes missing | Accepted. If the field was required, the judge decides once for the batch whether it's optional now or the records are broken |
| A value that doesn't fit its column | Kept in `walleye_extra`. The judge decides whether the column should widen, which means a rebuild, or whether the value is a mistake |
| Not a JSON object at all | Refused into `ingest_quarantine`, and still kept in bronze |

Every row in a typed table carries `walleye_id`, the bronze id of the record
it came from, and `walleye_extra`. `walleye_extra` holds whatever the record
had that the table has no column for yet, as JSON, with numbers kept to the
digit.

## Where it's kept

| | |
|---|---|
| `ingest_bronze` | every record, as it arrived |
| `ingest_quarantine` | records that were refused, with the reason |
| `ingest/routes/{source}.json` | which table each source goes to |
| `ingest/tables/{table}.json` | each table's rule, and the history of what was decided and by whom |

## Limits

- Routing and rule changes are serialised within one node. Two nodes ingesting
  the same new source at the same moment would each decide, and the later rule
  wins.
- A rebuild reads the table's whole history from bronze. It happens when a
  table gains a column or a type, which is often while a feed is new and
  rarely after, but it's proportional to what bronze holds.
- On a cluster, ingest runs on the node that receives the request, and that
  node needs to own the typed table.
