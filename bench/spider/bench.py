#!/usr/bin/env python3
"""Execution accuracy on the Spider dev set, for the assist endpoint and an LLM.

Both systems see the same schema and the same question, both produce one SQL
statement, and both statements are executed against the real SQLite database
the question was written for. A prediction counts only if its rows match the
gold rows: as a multiset, or in order where the gold query states one.

The assist endpoint needs the schema in the node's own catalog to ask its
questions at all, so each database's tables are created there first, empty.
Only the schema is used for generation; the rows that decide the score come
from Spider's own SQLite files.
"""
import argparse, collections, json, os, re, sqlite3, sys, threading, time
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

AGG = {0: "none", 1: "max", 2: "min", 3: "count", 4: "sum", 5: "avg"}
OPS = {1: "between", 2: "=", 3: ">", 4: "<", 5: ">=", 6: "<=", 7: "!=", 8: "in", 9: "like"}


def out_of_grammar(s):
    """Every way a gold query steps outside what the assist endpoint can build.

    Returned as labels rather than a boolean so the slices below can be
    defined by which of them they forgive, and so the coverage table can say
    what the rest of Spider is made of.
    """
    out = []
    if s.get("intersect") or s.get("union") or s.get("except"):
        out.append("set-op")
    units = s["from"]["table_units"]
    if any(t[0] != "table_unit" for t in units):
        out.append("subquery-in-from")
    elif len(units) > 1 or s["from"].get("conds"):
        out.append("join")
    if s.get("having"):
        out.append("having")

    distinct, items = s["select"]
    if distinct:
        out.append("distinct")
    star = lambda vu: vu[1][1] == 0
    if len(items) > 1:
        out.append("multi-select")
    elif items:
        agg, vu = items[0]
        if vu[0] != 0:
            out.append("arithmetic")
        if agg == 3 and star(vu):
            pass                                   # count(*)
        elif agg == 0 and star(vu):
            pass                                   # SELECT *
        elif agg == 0:
            out.append("select-column")
        else:
            out.append(f"agg-{AGG[agg]}")
    for cond in s.get("where", []):
        if cond == "or":
            out.append("or")
        elif isinstance(cond, list):
            not_op, op_id, _vu, v1, _v2 = cond
            if not_op:
                out.append("not")
            if op_id not in (2, 3, 4, 9):
                out.append(f"op-{OPS.get(op_id, op_id)}")
            if isinstance(v1, dict):
                out.append("subquery-in-where")
    if len(s.get("groupBy", [])) > 1:
        out.append("multi-groupby")
    order = s.get("orderBy", [])
    if order:
        if len(order[1]) > 1:
            out.append("multi-orderby")
        for vu in order[1]:
            agg = vu[1][0]
            if agg != 0 and not (agg == 3 and vu[1][1] == 0):
                out.append(f"orderby-agg-{AGG[agg]}")
    if s.get("limit") not in (None, 1, 2, 3, 5, 10, 50):
        out.append(f"limit-{s['limit']}")
    return out


# What each slice forgives. "strict" forgives nothing: the gold query is
# already expressible. "extended" is the slice a select-column question and
# the five aggregates would reach, and is reported separately because the
# endpoint cannot answer it today.
SLICES = {
    "strict": set(),
    "extended": {"select-column", "distinct", "multi-select",
                 "agg-max", "agg-min", "agg-sum", "agg-avg",
                 "orderby-agg-count", "orderby-agg-sum", "orderby-agg-avg",
                 "orderby-agg-max", "orderby-agg-min"},
}


def sqlite_schema(path):
    """Real table and column names, with the types SQLite actually stores."""
    conn = sqlite3.connect(path)
    conn.text_factory = lambda b: b.decode("utf-8", "replace")
    tables = {}
    names = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    for table in names:
        cols = []
        for _cid, name, decl, _nn, _dflt, _pk in conn.execute(f'PRAGMA table_info("{table}")'):
            decl = (decl or "").upper()
            if any(k in decl for k in ("INT",)):
                kind = "int64"
            elif any(k in decl for k in ("REAL", "FLOA", "DOUB", "NUMER", "DEC")):
                kind = "float64"
            elif "BOOL" in decl:
                kind = "boolean"
            else:
                kind = "string"
            cols.append({"name": name, "type": kind})
        tables[table] = cols
    conn.close()
    return tables


def ddl(tables):
    """The schema as the LLM baseline sees it: one CREATE TABLE per table."""
    out = []
    for table, cols in tables.items():
        body = ",\n  ".join(f'"{c["name"]}" {c["type"]}' for c in cols)
        out.append(f'CREATE TABLE "{table}" (\n  {body}\n);')
    return "\n".join(out)


class Node:
    """The walleye node under test."""

    def __init__(self, base, token):
        self.base, self.token = base.rstrip("/"), token

    def _post(self, path, body, timeout=120):
        req = urllib.request.Request(
            self.base + path,
            data=json.dumps(body).encode(),
            headers={"authorization": f"Bearer {self.token}",
                     "x-api-key": self.token,
                     "content-type": "application/json"},
            method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"{e.code} {e.read()[:300].decode('utf-8','replace')}") from None

    def create(self, name, columns):
        # A primary key is required; the first column serves, and nothing in
        # this benchmark writes rows for it to key.
        self._post("/v1/streams", {
            "name": name,
            "primary_key": [columns[0]["name"]],
            "columns": [dict(c, nullable=(c["name"] != columns[0]["name"])) for c in columns],
        })

    def drop(self, name):
        try:
            self._post(f"/v1/table/{name}/drop/", {})
        except RuntimeError:
            pass

    def listed(self):
        req = urllib.request.Request(self.base + "/v1/table/",
                                     headers={"x-api-key": self.token})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r).get("tables", [])

    def read(self, phrase):
        return self._post("/v1/assist/query/", {"text": phrase}, timeout=300)


def openai_sql(model, schema_ddl, question, key):
    """The baseline: one model, the same schema, asked for one statement."""
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content":
             "You translate a question into exactly one SQLite SELECT statement. "
             "Reply with the statement alone: no prose, no markdown, no trailing "
             "semicolon commentary. Use only the tables and columns given."},
            {"role": "user", "content": f"{schema_ddl}\n\nQuestion: {question}\nSQL:"},
        ],
    }
    # The reasoning models take max_completion_tokens and no temperature.
    if re.match(r"^(gpt-5|o[0-9])", model):
        body["max_completion_tokens"] = 2000
    else:
        body["max_tokens"] = 300
        body["temperature"] = 0
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(body).encode(),
        headers={"authorization": f"Bearer {key}", "content-type": "application/json"},
        method="POST")
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                text = json.load(r)["choices"][0]["message"]["content"].strip()
            break
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 529) and attempt < 3:
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(f"{e.code} {e.read()[:200].decode('utf-8','replace')}") from None
    else:
        raise RuntimeError("exhausted retries")
    text = re.sub(r"^```(?:sql)?|```$", "", text, flags=re.MULTILINE).strip()
    return text.rstrip(";").strip()


def rows_of(conn, sql, limit=20000):
    cur = conn.execute(sql)
    return cur.fetchmany(limit)


def matches(gold, pred, ordered):
    """Spider's execution match: the same rows, in order only where asked."""
    if len(gold) != len(pred):
        return False
    key = lambda rows: [tuple("" if v is None else str(v) for v in r) for r in rows]
    g, p = key(gold), key(pred)
    return g == p if ordered else sorted(g) == sorted(p)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="path to spider_data")
    ap.add_argument("--slice", default="strict", choices=sorted(SLICES))
    ap.add_argument("--node", default="http://127.0.0.1:8085")
    ap.add_argument("--token", default=os.environ.get("WALLEYE_TOKEN", ""))
    ap.add_argument("--systems", default="assist",
                    help="comma separated: assist, or any OpenAI model id")
    ap.add_argument("--limit", type=int, default=0, help="first N questions of the slice")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    dev = json.load(open(f"{args.data}/dev.json"))
    forgiven = SLICES[args.slice]
    chosen, why = [], collections.Counter()
    for ex in dev:
        reasons = out_of_grammar(ex["sql"])
        for r in set(reasons):
            why[r] += 1
        if not (set(reasons) - forgiven):
            chosen.append(ex)
    if args.limit:
        chosen = chosen[:args.limit]

    print(f"Spider dev: {len(dev)} questions over "
          f"{len({e['db_id'] for e in dev})} databases")
    print(f"slice '{args.slice}': {len(chosen)} questions "
          f"({100*len(chosen)/len(dev):.1f}% of dev)\n")

    node = Node(args.node, args.token)
    systems = [s.strip() for s in args.systems.split(",") if s.strip()]
    key = os.environ.get("OPENAI_API_KEY", "")
    results = {s: [] for s in systems}
    by_db = collections.defaultdict(list)
    for ex in chosen:
        by_db[ex["db_id"]].append(ex)

    lock = threading.Lock()
    for db_id, group in sorted(by_db.items()):
        path = f"{args.data}/database/{db_id}/{db_id}.sqlite"
        if not os.path.exists(path):
            print(f"  {db_id}: no sqlite file, skipped")
            continue
        schema = sqlite_schema(path)
        schema_ddl = ddl(schema)

        for existing in node.listed():
            node.drop(existing)
        made = []
        for table, cols in schema.items():
            try:
                node.create(table, cols)
                made.append(table)
            except RuntimeError as e:
                print(f"  {db_id}.{table}: not created ({e})")

        def one(ex):
            conn = sqlite3.connect(path)
            conn.text_factory = lambda b: b.decode("utf-8", "replace")
            ordered = bool(ex["sql"].get("orderBy"))
            try:
                gold = rows_of(conn, ex["query"])
            except Exception as e:
                conn.close()
                return [(s, {"skip": f"gold failed: {e}"}) for s in systems]
            found = []
            for system in systems:
                rec = {"db": db_id, "question": ex["question"], "gold": ex["query"]}
                started = time.time()
                try:
                    if system == "assist":
                        got = node.read(ex["question"])
                        rec["sql"] = got["sql"]
                        rec["confidence"] = got.get("weakest")
                        rec["attempts"] = got.get("attempts")
                    else:
                        rec["sql"] = openai_sql(system, schema_ddl, ex["question"], key)
                    rec["seconds"] = round(time.time() - started, 2)
                    try:
                        pred = rows_of(conn, rec["sql"])
                        rec["ran"] = True
                        rec["correct"] = matches(gold, pred, ordered)
                    except Exception as e:
                        rec["ran"] = False
                        rec["correct"] = False
                        rec["error"] = str(e)[:200]
                except Exception as e:
                    rec.update(ran=False, correct=False, error=str(e)[:200],
                               seconds=round(time.time() - started, 2))
                found.append((system, rec))
            conn.close()
            return found

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for found in pool.map(one, group):
                with lock:
                    for system, rec in found:
                        if "skip" not in rec:
                            results[system].append(rec)
        done = len(results[systems[0]])
        print(f"  {db_id:24s} {len(group):3d} questions, "
              f"{len(made)}/{len(schema)} tables   [{done}/{len(chosen)}]")
        for table in made:
            node.drop(table)

    print(f"\n{'system':22s} {'exec acc':>9s} {'ran':>7s} {'n':>5s} {'median s':>9s}")
    print("-" * 56)
    for system in systems:
        recs = results[system]
        if not recs:
            continue
        acc = sum(r["correct"] for r in recs) / len(recs)
        ran = sum(r.get("ran", False) for r in recs) / len(recs)
        times = sorted(r.get("seconds", 0) for r in recs)
        print(f"{system:22s} {100*acc:8.1f}% {100*ran:6.1f}% {len(recs):5d} "
              f"{times[len(times)//2]:9.2f}")

    if args.out:
        with open(args.out, "w") as f:
            json.dump({"slice": args.slice, "results": results,
                       "coverage": dict(why)}, f, indent=1)
        print(f"\nper-question detail: {args.out}")


if __name__ == "__main__":
    main()
