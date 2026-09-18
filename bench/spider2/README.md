# Spider 2.0-lite, local split

[Spider 2.0](https://spider2-sql.github.io/) is real enterprise text-to-SQL:
547 questions over 158 databases, with gold queries that are nothing like
Spider 1.0's one-liners. Over the 256 that publish gold SQL, 93% contain a
subquery, 77% a CTE, 72% a join, and the median is **44 lines across 4
SELECTs**.

This harness runs the **135 `local` instances** — the ones backed by SQLite
files rather than BigQuery or Snowflake, which need credentials and a network
account. Only 24 of those publish gold SQL, but every one has a gold **result**
CSV, and results are what the benchmark actually scores, so all 135 count.

## Scoring is the benchmark's own

`compare_pandas_table` and `compare_multi_pandas_table` are copied verbatim
from `spider2-lite/evaluation_suite/evaluate_utils.py`. A prediction passes if
it matches any one of the instance's gold variants, comparing only the columns
named in `condition_cols`, ordered or not per `ignore_order`, to a tolerance of
1e-2. Nothing here re-invents the metric.

The local split is **not** the easy one. Over the 24 with public gold it is the
hardest of the three:

| split | n | median lines | CTE | JOIN | window | UNION/etc |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| local | 24 | 49 | 88% | 92% | 33% | 29% |
| BigQuery | 148 | 37 | 71% | 64% | 19% | 8% |
| Snowflake | 84 | 57 | 85% | 81% | 23% | 19% |

A number here is therefore not comparable to a leaderboard number over all 547.

## The question

Nothing that assembles SQL from a fixed set of slots reaches this benchmark, so
the model writes SQL directly. The question is whether **Jev** — a fast,
calibrated, bounded-choice classifier — earns its place beside it, and in which
of the two shapes.

- **`luna`** — the model alone. The control.
- **`luna+cues`** — Jev first. One call carrying one Noul per table ("must the
  query read, join through, or filter on this table?") plus five shape
  questions drawn from what the gold actually uses. Its answers go into the
  prompt as hints.
- **`luna+jevtool`** — Jev as a tool the model may call, as often as it likes,
  to settle bounded questions while it works.

## Results

`gpt-5.6-luna`, `reasoning_effort: xhigh`, n=135, September 2026. Paired: same
instances, same model, same scoring.

| system | exec acc | ran | median s | median reasoning tokens | Jev questions |
| --- | ---: | ---: | ---: | ---: | ---: |
| `luna` | **52.6%** | 100.0% | 27.9 | 3584 | 0 |
| `luna+cues` | 51.1% | 99.3% | 32.0 | 4096 | 2790 |
| `luna+jevtool` | 51.1% | 97.0% | **16.0** | **1729** | 214 |

**Neither Jev arm improves accuracy.** Against the control, `luna+cues` fixed 8
and broke 10; `luna+jevtool` fixed 9 and broke 11. On 135 questions that is a
coin flip, not an effect. The honest reading is that Jev does not make this
model better at writing enterprise SQL.

**What it does do is make the same answer cheaper.** `luna+jevtool` matches the
control's accuracy on **half the reasoning tokens** (1729 vs 3584 median) and
**43% less wall time** (16.0s vs 27.9s), by moving bounded decisions off the
expensive model. That is the result worth having, and it is an efficiency
result rather than a quality one.

The model used the tool willingly where it was offered — on 89 of 135
questions, 1.6 calls on average, 17 at most — and the questions it asked were
the right kind:

> *"For 'daily toy sales' in this schema, which metric is most likely
> intended?"* → `SUM(order_items.price)` (0.35)
> *"How should Recency be defined for this RFM query?"* → (0.25)

Those are the semantic ambiguities that make Spider 2.0 hard, not schema
lookups.

**Confidence is not yet a usable signal.** Splitting the tool arm by the lowest
confidence Jev returned: 41.0% correct below 0.40 (n=39) against 46.0% at or
above (n=50). The direction is right and the gap is too small to act on.

**Pre-computed cues were the worse of the two shapes.** 2790 questions asked
against the tool arm's 214, for the same accuracy and *more* model reasoning
than the control. Asking about every table whether or not the model wanted to
know is the wrong trade; letting it ask is better and 13x cheaper.

## Running it

```sh
OPENAI_API_KEY=... TYPESAFE_API_KEY=... python3 bench/spider2/bench.py \
  --data /path/to/scratch \
  --systems luna,luna+cues,luna+jevtool \
  --model gpt-5.6-luna --effort xhigh \
  --out results.json
```

`--data` needs `lite.jsonl`, `localdb/*.sqlite`, and the repo's
`evaluation_suite` and `resource/documents`. Nothing is vendored here; see
Spider 2.0's own README for the downloads. Requires `pandas`.

Note `xhigh`, not `max` — this model rejects `max` explicitly. And function
tools plus a reasoning effort are refused on `/v1/chat/completions` for it, so
the tool arm goes through `/v1/responses`, where tools are flat and reasoning
items must be carried forward alongside the calls they explain.

## Caveats recorded rather than buried

- 135 questions puts a 95% interval near ±8 points. The 1.5-point spread
  between arms is inside it; the token and latency differences are much larger
  than their spread and are the only differences worth believing.
- One `luna+cues` instance failed on a harness bug (a null `answers` body,
  since guarded), not on the model. It is counted as a failure above.
- Four `luna+jevtool` instances produced SQL that would not execute, against
  zero for the control. Offering a tool costs a little robustness.
