# Seeing data

Ask a question in words, hand over a statement, or name a table, and Walleye
answers with a dashboard: the queries, their rows, and a
[json-render](https://github.com/vercel-labs/json-render) spec saying how to
draw them. Save it, and it is drawn again from fresh rows whenever it is
opened. Say what else you want on it, and it changes.

## Three ways in

**A question.** `POST /v1/see` with `{"question": "How much revenue did we make
each day?"}`. Text to SQL writes the statement, the planner checks it, it runs,
and the rows are drawn - here as a line of revenue over days. Give `sql`
instead of `question` to draw a statement you wrote.

**A table.** `GET /v1/see/tables/{table}` gives the table a dashboard of its own,
made from its columns: how many rows, how they arrive over time, how they break
down by each label with few values, how each measure moves, and the latest
rows. It is saved the first time it is asked for and made again when the
table's columns change, or on `?fresh=true`.

**A dashboard of your own.** `PUT /v1/dashboards/{name}` with `questions`,
`panels` (each a `title` and `sql`), or both, makes one and saves it.
`POST /v1/dashboards/{name}/chat` with `{"message": "Also show revenue by
country, and put it first"}` changes it. `GET` draws it; `DELETE` removes it;
`GET /v1/dashboards` lists them, newest first.

What `POST /v1/see` answers can be saved as it is: send its `panels`, `spec`
and `descriptions` back in a `PUT`.

## How the drawing is chosen

Each side does what it knows. Walleye has the rows, so for every query it works
out what the result could be drawn as, and says so in words:

| The result | Offered as |
|---|---|
| One row, one number | one number |
| A time, and measures | a line, or bars per period |
| One label, and measures | bars; a pie too, for a share across a few values |
| Two measures, nothing else | one against the other |
| Anything else - several labels, names, ids | a table |

Every result can also be a table. A result with two labels, like signups per
day per plan, is only a table: a line through rows of mixed plans draws a
series that is not there.

[Jev](https://typesafe.ai) has judgement, so it picks among those for what was
asked, and lays the panels out. It is steered towards the drawing a reader
takes in at a glance, and towards a table only when the rows themselves were
asked for. It picks through json-render's own composer, run in a V8 worker
like any other: the worker reaches Jev through the host, so the key stays in
the node, and the rows never enter the worker at all - the composer chooses
among descriptions of them.

Without `TYPESAFE_API_KEY`, or if Jev fails, the first thing offered for each
query is used, in order, in a grid. Every dashboard says which happened in
`composed_by`: `jev`, `rules`, or `given` for a spec saved as it was sent.

## What it costs

Composing is two to four rounds of questions to Jev, a few hundred
milliseconds, and happens once: when a dashboard is made or changed. Drawing a
saved one runs its queries and binds their rows, and nothing else - no model,
no Jev. A question pays for text to SQL on top, a second or two.

Each panel draws at most 1,000 rows. A statement that ends in its own `LIMIT`
keeps it.

## What comes back

```json
{
  "name": "sales",
  "title": "How much revenue did we make each day",
  "panels": [{"id": "q0", "title": "...", "sql": "SELECT ...", "question": "..."}],
  "spec": {
    "root": "node_0",
    "elements": {
      "node_0": {"type": "Grid", "props": {"columns": 1}, "children": ["node_1"]},
      "node_1": {"type": "LineChart", "props": {"title": "...", "data": {"$state": "/q0"}, "x": "day", "y": ["revenue"]}}
    },
    "state": {"q0": [{"day": "2026-08-01", "revenue": 1204.0}]}
  },
  "composed_by": "jev",
  "milliseconds": 24
}
```

`spec` is ready for a json-render renderer. The components are `Grid`,
`Metric`, `LineChart`, `BarChart`, `PieChart`, `ScatterChart` and `Table`, and
their props are in
[`composer/src/catalog.js`](../../crates/walleye-node/composer/src/catalog.js),
which is the contract a renderer draws to. A panel whose query failed draws
with no rows and is named in `errors`.

## Access

Seeing is reading: `POST /v1/see`, `GET /v1/see/tables/{table}` and the `GET`s
need a token that may read. Saving, changing and removing a dashboard need one
that may manage.
