#!/usr/bin/env python3
"""Score the question set against a running node.

    ./run.py http://127.0.0.1:8080 $WALLEYE_TOKEN

It creates the four tables, fills them with fixed rows, asks each question,
and compares the answer against the gold SQL by running both and comparing
the columns that come back. An alias is not a wrong answer, and column order
does not count; row order only counts where the gold query sorts.

The node needs a model configured (WALLEYE_MODEL_KEY), because answering a
question is what is being measured.
"""
import collections, json, os, sys, time, urllib.request, urllib.error, random
from concurrent.futures import ThreadPoolExecutor

BASE, TOK = sys.argv[1], sys.argv[2]
ROOT = os.path.dirname(os.path.abspath(__file__))

def post(path, body, timeout=300):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(),
        headers={"authorization": f"Bearer {TOK}", "x-api-key": TOK,
                 "content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return (json.loads(raw) if raw else None), None
    except urllib.error.HTTPError as e:
        return None, f"{e.code} {e.read()[:200].decode('utf-8','replace')}"
    except Exception as e:
        return None, str(e)[:200]

schema = json.load(open(f"{ROOT}/schema.json"))
rows = [json.loads(l) for l in open(f"{ROOT}/questions.jsonl")]

for table, cols in schema["tables"].items():
    post("/v1/streams", {"name": table, "primary_key": ["id"],
        "columns": [dict(c, nullable=(c["name"] != "id")) for c in cols]})

# Deterministic data, so gold and prediction are compared on the same rows.
random.seed(7)
cities = ["seattle", "portland", "boston", "austin"]
names = ["acme", "globex", "initech", "umbrella", "hooli"]
carriers = ["ups", "fedex", "dhl"]
statuses = ["pending", "shipped", "cancelled", "delivered"]
words = ["refund requested after the box arrived crushed",
         "fast shipping and the packaging was perfect",
         "the driver left it in the rain, item broken",
         "delivery was late but support helped",
         "damage to the product, want a refund",
         "great service, no complaints about delivery"]
day = lambda n: f"2026-{(n % 12) + 1:02d}-{(n % 27) + 1:02d}"
send = lambda t, rs: post(f"/v1/streams/{t}/events", {"rows": rs})
send("customers", [{"id": i, "name": names[i % 5], "city": cities[i % 4],
                    "signed_up": day(i)} for i in range(1, 41)])
send("orders", [{"id": i, "customer_id": (i % 40) + 1, "status": statuses[i % 4],
                 "total": round(20 + (i * 37) % 900 + 0.5, 2),
                 "placed_on": day(i)} for i in range(1, 121)])
send("shipments", [{"id": i, "order_id": i, "carrier": carriers[i % 3],
                    "days_late": (i * 7) % 12 - 2, "shipped_on": day(i)}
                   for i in range(1, 101)])
send("reviews", [{"id": i, "order_id": i, "stars": (i % 5) + 1,
                  "body": words[i % 6]} for i in range(1, 61)])

def run_sql(sql):
    return post("/v1/query", {"sql": sql})

def fmt(v):
    if isinstance(v, bool): return str(v)
    if isinstance(v, (int, float)): return f"{float(v):.2f}"
    return "" if v is None else str(v)

def columns(result):
    """Each column as a vector of strings. An alias is not a wrong answer."""
    if not isinstance(result, list): return None
    if not result: return []
    keys = sorted({k for r in result for k in r})
    return [[fmt(r.get(k)) for r in result] for k in keys]

def same(gold, got):
    if gold is None or got is None: return False
    if gold and got and len(gold[0]) != len(got[0]): return False
    if not gold: return not got or not got[0]
    return all(any(sorted(w) == sorted(h) for h in got) for w in gold)

def one(item):
    rec = {"id": item["id"], "kind": item["kind"], "q": item["q"], "gold": item["sql"]}
    started = time.time()
    got, err = post("/v1/query", {"text": item["q"]})
    rec["ms"] = int((time.time() - started) * 1000)
    rec["sql"] = (got or {}).get("sql")
    if err or not rec["sql"]:
        rec.update(error=err or str(got)[:120], scored=True, correct=False)
        return rec
    if item["sql"] is None:
        rec.update(scored=True, correct=False)
        return rec
    gold_rows, gold_err = run_sql(item["sql"])
    if gold_err:
        rec.update(scored=False, correct=False, reference=gold_err[:80])
        return rec
    got_rows, got_err = run_sql(rec["sql"])
    rec.update(scored=True, ran=got_err is None,
               correct=got_err is None and same(columns(gold_rows), columns(got_rows)))
    return rec

with ThreadPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(one, rows))

scored = [r for r in results if r["scored"]]
by = collections.defaultdict(lambda: [0, 0])
for r in scored:
    by[r["kind"]][0] += r["correct"]; by[r["kind"]][1] += 1
print(f"\n{len(results)} questions, {len(scored)} scored\n")
for k in sorted(by, key=lambda k: -by[k][1]):
    ok, n = by[k]
    print(f"  {k:20s} {ok:3d}/{n:<3d} {100*ok/n:5.0f}%")
ok = sum(r["correct"] for r in scored)
ran = sum(1 for r in scored if r.get("ran"))
t = sorted(r["ms"] for r in results)
print(f"\n  OVERALL              {ok:3d}/{len(scored):<3d} {100*ok/len(scored):5.1f}%"
      f"   ran {ran}/{len(scored)}   median {t[len(t)//2]}ms")
json.dump(results, open(f"{os.path.dirname(os.path.abspath(__file__))}/results.json", "w"), indent=1)
