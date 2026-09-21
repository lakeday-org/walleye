# Text to SQL

Text-to-SQL questions over one schema, for measuring what the reader can and
cannot build. Not Spider: these are the shapes a search box over your own
tables actually receives, and they include the two things Spider has none of —
words that stand for a condition, and full-text search.

`schema.json` is four tables and eighteen columns, with a fixed `today` so
every date question resolves the same way forever.

`questions.jsonl` is 116 questions in 17 kinds. Each carries the SQL it should
become, or `null` where the right answer is to refuse.

| kind | n | what it tests |
| --- | ---: | --- |
| join | 16 | two and three tables, often with grouping |
| date | 12 | today, yesterday, last week, the 3rd, march, past three weeks |
| aggregate | 12 | sum, avg, min, max, count distinct, per-group |
| nested | 11 | HAVING, scalar subqueries, NOT IN, anti-joins |
| filter-number | 8 | 100, five hundred, thirteen, $1,200, twenty five |
| select-column | 8 | naming columns instead of returning every one |
| implied-condition | 8 | "late", "bad", "big", "unshipped", "unreliable" |
| text-search | 7 | matching terms in prose, alone and with a join |
| group | 6 | one and two grouping keys |
| filter-text | 5 | a value that is a word |
| or-not-in | 5 | OR, NOT, IN, BETWEEN |
| count, order, limit, rows | 12 | the shapes that already work |
| ambiguous | 3 | more than one defensible reading |
| unanswerable | 3 | the answer is to refuse |

## How hard it is

Measured against Spider 1.0's dev set, by the structure of the gold SQL:

| | ours | spider 1.0 dev |
| --- | ---: | ---: |
| questions | 116 | 1034 |
| median length | 54 chars | 89 chars |
| join | 25% | 39% |
| aggregate | 24% | 14% |
| group by | 23% | 27% |
| subquery | 4% | 8% |
| having | 5% | 8% |

Comparable on grouping, heavier on aggregation, lighter on joins and about
sixty per cent of the length. Spider's join density comes from schemas built
to be joined; four tables that a dashboard would actually have need fewer.
This is deliberately not Spider 2.0, whose median gold query is 44 lines
across four SELECTs — that is enterprise analytics, not a search box.

What neither Spider has is the last two rows of the first table: fifteen
questions where a word stands for a condition, or where the answer is in prose
and has to be searched for.

Four questions expect `null`: three nonsense, one ("how are we doing") too
vague to be a query. A reader that answers them is wrong in a way a reader
that refuses them is not.

## How to run it

Start a node with a model configured, then:

    benchmarks/text-to-sql/run.py http://127.0.0.1:8080 $WALLEYE_TOKEN

It creates the tables, writes fixed rows, asks every question through
`/v1/query`, and scores an answer by running it beside the gold SQL and
comparing the columns that come back. A different alias or column order is not
a wrong answer. It prints a breakdown by kind and leaves the per-question
detail in `results.json`.

Answering a question calls a model, so a run costs what 116 short completions
cost, and takes about as long as the slowest four of them in sequence.

## What it has measured

With `WALLEYE_MODEL_NAME=gpt-5.6-luna` and reasoning off, the date supplied in
the prompt:

| | |
| --- | ---: |
| correct | 83.5% |
| executed without error | 100% |
| median | ~2s |

Reasoning bought nothing here or on Spider 2.0-lite (51.9 / 52.6 / 51.9 for
default, none and xhigh) and cost 60% more wall time at its highest setting.
Asking the decision service to verify the generated SQL did not help either
(85.3% against 83.5%, inside the noise of a 116-question set).

## Known gap this set exposes

The `text-search` questions cannot be expressed as SQL against this engine at
all. Full-text search is reachable only through the LanceDB search endpoint
(`/v1/table/{t}/query/` with `full_text_query`), and SQL has no function that
reaches an inverted index. Their reference SQL uses `MATCH`, which is what it
would look like, and will not run until a `match(column, terms)` function is
registered in `walleye-lance` beside `prompt` and `prompt_jev`.
