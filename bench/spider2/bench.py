#!/usr/bin/env python3
"""Spider 2.0-lite, local split: real enterprise text-to-SQL, scored officially.

Spider 2.0 is not a harder Spider 1.0. Of the 256 gold queries it publishes,
93% contain a subquery, 77% a CTE, 72% a join, and the median is 44 lines
across 4 SELECTs. Nothing that builds SQL from a fixed set of slots reaches
it, so this harness measures a model writing SQL directly, with the schema and
any external-knowledge document the question depends on.

Only the 135 `local` instances are here. The other 412 are BigQuery and
Snowflake, which need credentials and a network account. Every local instance
has a gold result CSV, so all 135 are scorable even though only 24 publish
their gold SQL.

Scoring is Spider 2.0's own: `compare_pandas_table` and
`compare_multi_pandas_table` are copied verbatim from the benchmark's
evaluation_suite so a number here means the same thing it means on the
leaderboard. A question passes if the predicted table matches any one of its
gold variants, comparing only `condition_cols` where the benchmark names
them, ordered or not according to `ignore_order`, to a tolerance of 1e-2.
"""
import argparse, glob, json, math, os, re, sqlite3, sys, threading, time
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

import pandas as pd


# ---------------------------------------------------------------------------
# Verbatim from spider2-lite/evaluation_suite/evaluate_utils.py, so that a
# score here is the benchmark's score and not an approximation of it.
# ---------------------------------------------------------------------------
def compare_multi_pandas_table(pred, multi_gold, multi_condition_cols=[], multi_ignore_order=False):
    if multi_condition_cols == [] or multi_condition_cols == [[]] or multi_condition_cols == [None] or multi_condition_cols is None:
        multi_condition_cols = [[] for _ in range(len(multi_gold))]
    elif len(multi_gold) > 1 and not all(isinstance(sublist, list) for sublist in multi_condition_cols):
        multi_condition_cols = [multi_condition_cols for _ in range(len(multi_gold))]
    multi_ignore_order = [multi_ignore_order for _ in range(len(multi_gold))]
    for i, gold in enumerate(multi_gold):
        if compare_pandas_table(pred, gold, multi_condition_cols[i], multi_ignore_order[i]):
            return 1
    return 0


def compare_pandas_table(pred, gold, condition_cols=[], ignore_order=False):
    tolerance = 1e-2

    def vectors_match(v1, v2, tol=tolerance, ignore_order_=False):
        if ignore_order_:
            v1, v2 = (sorted(v1, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))),
                      sorted(v2, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))))
        if len(v1) != len(v2):
            return False
        for a, b in zip(v1, v2):
            if pd.isna(a) and pd.isna(b):
                continue
            elif isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if not math.isclose(float(a), float(b), abs_tol=tol):
                    return False
            elif a != b:
                return False
        return True

    if condition_cols != []:
        gold_cols = gold.iloc[:, condition_cols]
    else:
        gold_cols = gold
    pred_cols = pred
    t_gold_list = gold_cols.transpose().values.tolist()
    t_pred_list = pred_cols.transpose().values.tolist()
    score = 1
    for _, gold in enumerate(t_gold_list):
        if not any(vectors_match(gold, pred, ignore_order_=ignore_order) for pred in t_pred_list):
            score = 0
        else:
            for j, pred in enumerate(t_pred_list):
                if vectors_match(gold, pred, ignore_order_=ignore_order):
                    break
    return score
# ---------------------------------------------------------------------------


def schema_of(path):
    """The database's own DDL, which is what the model is given to read."""
    conn = sqlite3.connect(path)
    conn.text_factory = lambda b: b.decode("utf-8", "replace")
    out = []
    for (sql,) in conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL"
    ):
        out.append(" ".join(sql.split()))
    conn.close()
    return ";\n".join(out) + ";"


def strip_fences(text):
    text = text.strip()
    fenced = re.findall(r"```(?:sql)?\s*(.*?)```", text, re.S | re.I)
    if fenced:
        text = max(fenced, key=len)
    return text.strip().rstrip(";").strip()


SYSTEM = ("You are an expert data analyst writing SQLite SQL. Reply with exactly one "
          "SQL statement and nothing else - no prose, no markdown fences, no commentary. "
          "Use only tables and columns present in the schema. Column names in the result "
          "do not matter; the values and their order do.")


def post_openai(url, body, key, timeout=900):
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"authorization": f"Bearer {key}", "content-type": "application/json"},
        method="POST")
    last = ""
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            last = f"{e.code} {e.read()[:300].decode('utf-8', 'replace')}"
            # 520-524 are Cloudflare's, not the API's, and they are transient.
            if e.code in (429, 500, 502, 503, 520, 522, 524, 529) and attempt < 5:
                time.sleep(5 * (attempt + 1))
                continue
            raise RuntimeError(last) from None
        except Exception as e:
            last = str(e)
            if attempt < 5:
                time.sleep(5 * (attempt + 1))
                continue
            raise RuntimeError(last) from None
    raise RuntimeError(last)


def chat(model, effort, messages, key):
    body = {"model": model, "messages": messages, "max_completion_tokens": 32000}
    if effort:
        body["reasoning_effort"] = effort
    return post_openai("https://api.openai.com/v1/chat/completions", body, key)


def respond(model, effort, inputs, key, tools=None):
    body = {"model": model, "input": inputs, "max_output_tokens": 32000, "store": False}
    if effort:
        body["reasoning"] = {"effort": effort}
    if tools:
        body["tools"] = tools
    return post_openai("https://api.openai.com/v1/responses", body, key)


def user_prompt(schema, question, knowledge, cues=None):
    parts = [f"SQLite database schema:\n\n{schema}"]
    if knowledge:
        parts.append(f"External knowledge you must apply:\n\n{knowledge}")
    if cues:
        parts.append(cues)
    parts.append(f"Question: {question}\n\n"
                 "Write one SQLite SELECT statement that answers it. Return only the SQL.")
    return "\n\n".join(parts)


def generate(system, model, effort, schema, question, knowledge, tables, key, jev_key):
    """Return (sql, stats). Three arms, all writing SQL with the same model.

    luna          - the model alone. The control.
    luna+cues     - Jev answers schema-linking and shape questions first, in one
                    call, and those answers go into the prompt as hints.
    luna+jevtool  - Jev is a tool the model may call, as many times as it likes,
                    to settle bounded questions while it works.
    """
    stats = {"jev_calls": 0, "jev_questions": 0}
    cues = None
    if system == "luna+cues":
        ranked, shapes = jev_cues(question, knowledge, tables, jev_key)
        cues = cue_text(ranked, shapes)
        stats.update(jev_calls=1, jev_questions=len(tables) + len(SHAPE),
                     cues=cues, linked=[t for t, p in ranked if p >= 0.5])

    prompt = user_prompt(schema, question, knowledge, cues)
    total, reasoning, asked = 0, 0, []

    if system != "luna+jevtool":
        payload = chat(model, effort,
                       [{"role": "system", "content": SYSTEM},
                        {"role": "user", "content": prompt}], key)
        usage = payload.get("usage", {})
        stats.update(
            prompt_tokens=usage.get("prompt_tokens", 0),
            cached_tokens=usage.get("prompt_tokens_details", {}).get("cached_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            reasoning_tokens=usage.get("completion_tokens_details", {}).get("reasoning_tokens", 0),
            rounds=1, jev_asked=asked)
        return strip_fences(payload["choices"][0]["message"].get("content") or ""), stats

    prompt_total, cached_total, rounds = 0, 0, 0
    inputs = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt}]
    state = json.dumps({"user_question": question,
                        "database_tables": {t: c[:30] for t, c in tables.items()}})
    for _round in range(10):
        payload = respond(model, effort, inputs, key, [JEV_TOOL])
        usage = payload.get("usage", {})
        # Every round resends the conversation, so input is charged again each
        # time. Summing it is the whole point of counting it.
        prompt_total += usage.get("input_tokens", 0)
        cached_total += usage.get("input_tokens_details", {}).get("cached_tokens", 0)
        rounds += 1
        total += usage.get("output_tokens", 0)
        reasoning += usage.get("output_tokens_details", {}).get("reasoning_tokens", 0)
        calls = [o for o in payload.get("output", []) if o.get("type") == "function_call"]
        if not calls:
            text = []
            for item in payload.get("output", []):
                for part in item.get("content", []) or []:
                    if part.get("type") == "output_text":
                        text.append(part.get("text", ""))
            stats.update(prompt_tokens=prompt_total, cached_tokens=cached_total,
                         completion_tokens=total, reasoning_tokens=reasoning,
                         rounds=rounds, jev_asked=asked)
            return strip_fences("\n".join(text)), stats

        # Reasoning items must be carried forward with the calls they explain.
        inputs.extend(o for o in payload.get("output", [])
                      if o.get("type") in ("reasoning", "function_call", "message"))
        for call in calls:
            try:
                args = json.loads(call.get("arguments") or "{}")
                options = [str(o) for o in args.get("options", [])][:60]
                if len(options) < 2:
                    raise ValueError("a choice needs at least two options")
                answers = ask_jev(state, {"q": {
                    "type": "choice",
                    "instructions": str(args.get("question", "")),
                    "criteria": {o: o for o in options}}}, jev_key)
                a = answers["q"]
                reply = {"answer": a.get("choice"),
                         "confidence": round(a.get("confidence", 0), 3)}
                stats["jev_calls"] += 1
                stats["jev_questions"] += 1
                asked.append({"q": str(args.get("question", ""))[:160],
                              "options": options[:10], **reply})
            except Exception as e:
                reply = {"error": str(e)[:200]}
            inputs.append({"type": "function_call_output",
                           "call_id": call.get("call_id"),
                           "output": json.dumps(reply)})
    stats.update(prompt_tokens=prompt_total, cached_tokens=cached_total,
                 completion_tokens=total, reasoning_tokens=reasoning,
                 rounds=rounds, jev_asked=asked)
    return "", stats


# ---------------------------------------------------------------------------
# Jev. Two ways of using it, so the benchmark can say whether either helps.
# ---------------------------------------------------------------------------
JEV_URL = os.environ.get("TYPESAFE_URL", "https://api.typesafe.ai/v1/systemone")
JEV_MODEL = os.environ.get("TYPESAFE_MODEL", "jev-latest")


def ask_jev(state, questions, key, timeout=120):
    """One call, every question. The service answers them in parallel."""
    body = {"state": state, "model": JEV_MODEL, "questions": questions}
    req = urllib.request.Request(
        JEV_URL, data=json.dumps(body).encode(),
        headers={"authorization": f"Bearer {key}", "content-type": "application/json"},
        method="POST")
    last = ""
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                answers = json.load(r).get("answers")
                if not isinstance(answers, dict):
                    raise RuntimeError(f"no answers in response: {str(answers)[:120]}")
                return answers
        except urllib.error.HTTPError as e:
            last = f"{e.code} {e.read()[:200].decode('utf-8','replace')}"
            if e.code in (429, 500, 502, 503, 520, 522, 524, 529) and attempt < 4:
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(last) from None
        except Exception as e:
            last = str(e)
            if attempt < 4:
                time.sleep(2 * (attempt + 1))
                continue
            raise RuntimeError(last) from None
    raise RuntimeError(last)


def table_summaries(path):
    """Each table with its column names, for schema-linking questions."""
    conn = sqlite3.connect(path)
    conn.text_factory = lambda b: b.decode("utf-8", "replace")
    out = {}
    for (t,) in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")')]
        out[t] = cols
    conn.close()
    return out


# Shape cues asked of every question, regardless of schema. These are the
# features the gold queries actually use, measured over the published gold:
# 93% subquery, 77% CTE, 72% join, 21% window, 14% set operation.
SHAPE = {
    "needs_window": "Answering this needs a window function - a ranking, a running total, "
                    "a row-over-row comparison, or a top-N within each group.",
    "needs_dates": "Answering this needs date or time arithmetic - a difference between "
                   "dates, a period, a truncation to month or week, or a moving window.",
    "needs_setop": "Answering this needs UNION, EXCEPT or INTERSECT - combining or "
                   "subtracting two separate result sets.",
    "needs_selfjoin": "Answering this needs the same table used twice - comparing rows of "
                      "one table against other rows of that same table.",
    "needs_ratio": "The answer is a proportion, share, percentage or rate rather than a "
                   "plain count or sum.",
}


def jev_cues(question, knowledge, tables, key):
    """Schema linking and shape, as bounded questions, in one call.

    This is Jev doing what it is for: a lot of narrow decisions at once, fast
    and calibrated, to narrow what the expensive model has to consider.
    """
    questions = {}
    for t, cols in tables.items():
        shown = ", ".join(cols[:30]) + ("..." if len(cols) > 30 else "")
        questions[f"t::{t}"] = {
            "type": "noul",
            "instructions": {
                "question": f'Is the table "{t}" needed to answer the user question?',
                "table": t,
                "columns": shown,
                "note": "Needed means the query must read it, join through it, or filter on it.",
            },
        }
    for name, text in SHAPE.items():
        questions[name] = {"type": "noul", "instructions": text}

    state = {"user_question": question}
    if knowledge:
        state["external_knowledge"] = knowledge[:4000]
    answers = ask_jev(json.dumps(state), questions, key)

    ranked = sorted(
        ((k[3:], a.get("noul", 0.0)) for k, a in answers.items() if k.startswith("t::")),
        key=lambda kv: -kv[1])
    shapes = {k: answers[k].get("noul", 0.0) for k in SHAPE if k in answers}
    return ranked, shapes


def cue_text(ranked, shapes):
    likely = [f"{t} ({p:.2f})" for t, p in ranked if p >= 0.5]
    unlikely = [t for t, p in ranked if p < 0.15]
    lines = ["A fast classifier was asked about this question first. Its read, with "
             "probabilities - treat it as a hint, not an instruction, and check it "
             "against the schema:"]
    lines.append("  tables it thinks are needed: "
                 + (", ".join(likely) if likely else "none confidently"))
    if unlikely:
        lines.append(f"  tables it thinks are irrelevant: {', '.join(unlikely[:25])}")
    flags = [f"{k.replace('needs_', '')} ({p:.2f})" for k, p in shapes.items() if p >= 0.5]
    lines.append("  query features it expects: " + (", ".join(flags) if flags else "none"))
    return "\n".join(lines)


JEV_TOOL = {
    "type": "function",
    "name": "ask_jev",
    "description":
            "Ask a fast, cheap classifier one multiple-choice question and get back the "
            "chosen option with a calibrated confidence (0-1). It is far cheaper and "
            "faster than reasoning it out yourself, and it is calibrated, so a low "
            "confidence genuinely means the answer is doubtful. Use it to settle bounded "
            "questions about this database: which table holds a thing, which column means "
            "a thing, whether two columns refer to each other, which of several readings "
            "of the user's question is meant. Ask as many as you need.",
    "parameters": {
        "type": "object",
        "properties": {
            "question": {"type": "string", "description": "The question to decide."},
            "options": {"type": "array", "items": {"type": "string"},
                        "minItems": 2, "maxItems": 60,
                        "description": "The options it must choose between."},
        },
        "required": ["question", "options"],
        "additionalProperties": False,
    },
}


def gold_frames(exec_dir, instance_id):
    """Every accepted gold table for this instance: <id>.csv or <id>_a/_b/..."""
    names = sorted(glob.glob(os.path.join(exec_dir, f"{instance_id}.csv"))
                   + glob.glob(os.path.join(exec_dir, f"{instance_id}_*.csv")))
    return [pd.read_csv(n, keep_default_na=True) for n in names]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="scratch dir holding lite.jsonl, localdb/, Spider2-main/")
    ap.add_argument("--model", default="gpt-5.6-luna")
    ap.add_argument("--systems", default="luna",
                    help="comma separated: luna, luna+cues, luna+jevtool")
    ap.add_argument("--effort", default="xhigh", help="reasoning_effort, or empty for none")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=120, help="seconds a generated query may run")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    root = args.data
    suite = f"{root}/Spider2-main/spider2-lite/evaluation_suite"
    exec_dir = f"{suite}/gold/exec_result"
    docs = f"{root}/Spider2-main/spider2-lite/resource/documents"

    rows = [json.loads(l) for l in open(f"{root}/lite.jsonl")]
    local = [r for r in rows if r["instance_id"].startswith("local")]
    meta = {}
    for line in open(f"{suite}/gold/spider2lite_eval.jsonl"):
        item = json.loads(line)
        meta[item["instance_id"]] = item
    dbs = {os.path.splitext(os.path.basename(p))[0]: p
           for p in glob.glob(f"{root}/localdb/**/*.sqlite", recursive=True)}

    work = [r for r in local if r["db"] in dbs and gold_frames(exec_dir, r["instance_id"])]
    if args.limit:
        work = work[:args.limit]
    print(f"Spider 2.0-lite, local split: {len(local)} instances, "
          f"{len(work)} with a database and a gold result")
    print(f"model {args.model}"
          + (f", reasoning_effort {args.effort}" if args.effort else "") + "\n")

    key = os.environ["OPENAI_API_KEY"]
    systems = [x.strip() for x in args.systems.split(",") if x.strip()]
    jev_key = os.environ.get("TYPESAFE_API_KEY", "")
    if any(x != "luna" for x in systems) and not jev_key:
        sys.exit("the Jev arms need TYPESAFE_API_KEY")
    schemas = {db: schema_of(path) for db, path in dbs.items()}
    summaries = {db: table_summaries(path) for db, path in dbs.items()}
    results = {x: [] for x in systems}
    lock, done = threading.Lock(), [0]

    def one(ex):
        iid, db = ex["instance_id"], ex["db"]
        knowledge = ""
        if ex.get("external_knowledge"):
            path = os.path.join(docs, ex["external_knowledge"])
            if os.path.exists(path):
                knowledge = open(path).read()
        info = meta.get(iid, {})
        gold = gold_frames(exec_dir, iid)
        found = []
        for system in systems:
            rec = {"system": system, "instance_id": iid, "db": db,
                   "question": ex["question"]}
            started = time.time()
            try:
                sql, stats = generate(system, args.model, args.effort, schemas[db],
                                      ex["question"], knowledge, summaries[db], key, jev_key)
                rec.update(sql=sql, **stats)
                conn = sqlite3.connect(dbs[db])
                conn.text_factory = lambda b: b.decode("utf-8", "replace")
                try:
                    pred = pd.read_sql_query(sql, conn)
                    rec["ran"] = True
                finally:
                    conn.close()
                rec["correct"] = bool(compare_multi_pandas_table(
                    pred, gold, info.get("condition_cols", []),
                    info.get("ignore_order", False)))
                rec["rows"] = len(pred)
            except Exception as e:
                rec.setdefault("ran", False)
                rec["correct"] = False
                rec["error"] = f"{type(e).__name__}: {e}"[:300]
            rec["seconds"] = round(time.time() - started, 1)
            found.append(rec)
        with lock:
            done[0] += 1
            marks = " ".join(
                f"{r['system'].replace('luna', 'L')}:"
                + ("OK " if r["correct"] else ("run" if r.get("ran") else "ERR"))
                for r in found)
            print(f"  [{done[0]:3d}/{len(work)}] {iid:16s} {db:22s} {marks}", flush=True)
        return found

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for found in pool.map(one, work):
            for rec in found:
                results[rec["system"]].append(rec)

    print(f"\n{'system':16s} {'exec acc':>9s} {'ran':>7s} {'n':>5s} {'median s':>9s} "
          f"{'reason tok':>11s} {'jev q':>7s}")
    print("-" * 72)
    for system in systems:
        recs = results[system]
        if not recs:
            continue
        n = len(recs)
        times = sorted(r["seconds"] for r in recs)
        think = sorted(r.get("reasoning_tokens", 0) for r in recs)
        jevq = sum(r.get("jev_questions", 0) for r in recs)
        print(f"{system:16s} {100*sum(r['correct'] for r in recs)/n:8.1f}% "
              f"{100*sum(r.get('ran', False) for r in recs)/n:6.1f}% {n:5d} "
              f"{times[n//2]:9.1f} {think[n//2]:11d} {jevq:7d}")

    if len(systems) > 1:
        base = {(r["instance_id"]): r["correct"] for r in results[systems[0]]}
        for system in systems[1:]:
            other = {(r["instance_id"]): r["correct"] for r in results[system]}
            fixed = sum(1 for k in base if not base[k] and other.get(k))
            broke = sum(1 for k in base if base[k] and not other.get(k))
            print(f"\n{system} vs {systems[0]}: fixed {fixed}, broke {broke}")

    if args.out:
        json.dump({"model": args.model, "effort": args.effort, "results": results},
                  open(args.out, "w"), indent=1)
        print(f"per-question detail: {args.out}")


if __name__ == "__main__":
    main()
