# Spider, against the assist endpoint

[Spider](https://yale-lily.github.io/spider) is the standard text-to-SQL
benchmark: 1,034 dev questions over 20 databases, each with a gold query and a
real SQLite file to run it against. This harness scores `/v1/assist/query/` on
it, alongside an LLM asked for the same statement, so the two are measured the
same way rather than against each other's press releases.

## What is measured

Execution accuracy. Both systems see the same schema and the same question,
both return one statement, and both statements run against the same SQLite
database the question was written for. A prediction counts only if its rows
match the gold rows — as a multiset, or in order where the gold query states
one. This is the metric the published Spider and BIRD numbers use.

The assist endpoint needs the schema in the node's own catalog before it can
ask its questions, so each database's tables are created there first, empty.
Only the schema is used to generate; the rows that decide the score come from
Spider's own files.

## Coverage is the headline, not the accuracy

The endpoint builds one statement shape: one table, `SELECT *` or `count(*)`
or a grouped count, a conjunction of comparisons, one ordering, one limit. Most
of Spider is not that.

| | questions | share of dev |
| --- | ---: | ---: |
| expressible in the grammar as it stands | 70 | 6.8% |
| + a select-column question and the five aggregates | 438 | 42.4% |

What puts the rest out of reach, counted over all 1,034 dev questions:

| | | | |
| --- | ---: | --- | ---: |
| `select-column` | 42.3% | `having` | 7.3% |
| `join` | 36.6% | `op-in` | 4.8% |
| `multi-select` | 35.1% | `distinct` | 4.6% |
| `subquery-in-where` | 7.8% | `not` | 4.4% |
| `set-op` | 7.4% | `or` | 3.3% |

Joins are the wall. Nothing in the design reaches them: every question is a
choice over one table's columns, and a join is a choice about relationships
between tables that the grammar has no slot for.

**Any accuracy number below is on the 6.8% slice.** It says the endpoint is
accurate on what it covers. It does not say it competes with a language model
at text-to-SQL, because on 93% of Spider it has no answer at all.

## Results

Spider dev, strict slice, n=70, September 2026:

| system | execution accuracy | statements that ran | median latency |
| --- | ---: | ---: | ---: |
| `assist` (Jev, bounded choice) | **97.1%** | 100% | **0.31 s** |
| `gpt-4o-mini` (raw SQL) | 95.7% | 100% | 0.61 s |
| `gpt-4.1` (raw SQL) | 92.9% | 100% | 0.59 s |

Both remaining assist failures are the same one: the phrase says `republic`
and the column holds `Republic`. `gpt-4.1` and `gpt-4o-mini` get those two
wrong as well. Matching a typed word to a stored value needs to read the
values, which nothing here does.

Two things this slice does **not** show:

- **The retry loop never fired.** All 70 statements planned on the first
  reading, so `EXPLAIN` feedback bought nothing. It earns its place on harder
  phrases or not at all.
- **Confidence was not tested as a filter.** No reading scored under 0.50, so
  no threshold would have caught the two failures without discarding good
  readings too. Two errors is not enough to claim confidence predicts them.

`gpt-5-mini` returned `404 organization must be verified` for all 70 and is
omitted rather than scored as zero.

## Running it

```sh
# a node with a catalog to create into, and a decision service
TYPESAFE_API_KEY=... WALLEYE_TOKEN=... ./target/release/walleye-node &

WALLEYE_TOKEN=... OPENAI_API_KEY=... python3 bench/spider/bench.py \
  --data /path/to/spider_data \
  --slice strict \
  --systems assist,gpt-4.1 \
  --out results.json
```

`--slice extended` selects the 438 questions a select-column question and the
aggregates would reach. The endpoint cannot answer them today; the slice is
there to size the work, and scoring it now measures nothing.

Spider's `spider_data` is the dataset's own zip, unpacked: `dev.json`,
`tables.json` and `database/`. Nothing is vendored here.

## What the harness found

Running it is what turned up the bugs, which is the point of having it.

- **Uppercase table names were unqueryable.** `register_table` was given a
  `&str`, which DataFusion parses into a `TableReference` and folds to lower
  case, so `Pets` was filed as `pets` and `FROM "Pets"` — the only form
  available to a name holding a space or a reserved word — could never
  resolve. The catalog, the table API and cluster routing all still said
  `Pets`. Fixed by registering the name as given.
- **Junk words became predicates.** Nine of the first ten failures were a
  spurious `AND`: `"Language" = 'using'`, `"Written_by" = 'written'`. Asked
  which column a word belongs to, with a list of columns and a `none`, the
  service reliably found a column the word was *about*. It was not unsure
  while doing it — those answers scored 0.96 and 0.97, so no confidence
  threshold could have separated them. Whether a word is a value at all is now
  its own two-option question, which moved the slice from 85.7% to 94.3%.
- **Numbers must not be asked that question.** Asked whether `4` in "more than
  4 cylinders" is a value, the service says it is part of the wording — and it
  has a point, since no cell need contain it. Numbers skip the question.
  94.3% to 97.1%.
- **Names and name-parts.** `Joseph Kuhr` was offered as `Joseph` and `Kuhr`
  separately; `Template_Type_Code` made `type` and `code` look like values.
  Adjacent capitalised words are one candidate, and the words a catalog name
  is built from are not candidates.
