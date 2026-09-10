import json
import os
import subprocess
import sys
from pathlib import Path

import networkx as nx
import pytest

from declank.callgraph import structural_metrics
from declank.discovery import ScanOptions
from declank.scanner import scan


def report(tmp_path, sources):
    for name, source in sources.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
    return scan(tmp_path, ScanOptions(functions=True))


def test_chain_fan_in_reach_and_impact_are_not_file_size(tmp_path):
    result = report(
        tmp_path,
        {
            "a.py": "def leaf(): return 1\ndef middle(): return leaf()\n"
            "def entry(): return middle()\ndef isolated(): return 1\n"
        },
    )
    rows = {r["name"]: r for r in result["records"]}
    assert rows["leaf"]["dependent_count"] == 2
    assert rows["leaf"]["fan_in"] == 1
    assert rows["leaf"]["refactor_priority"] > rows["isolated"]["refactor_priority"]
    assert rows["leaf"]["callers"][0]["line"] == 2
    assert rows["entry"]["dependent_count"] == 0


def test_scc_reach_and_cycle_edges():
    graph = nx.DiGraph([("entry", "a"), ("a", "b"), ("b", "a"), ("b", "leaf")])
    data, summary = structural_metrics(graph)
    assert data["a"]["cycle_size"] == 2
    assert data["b"]["dependent_count"] == 2
    assert data["leaf"]["dependent_count"] == 3
    assert summary["cycles"] == [["a", "b"]]
    assert structural_metrics(nx.DiGraph())[0] == {}


def test_scenario_matches_recomputed_graph_quality(tmp_path):
    result = report(
        tmp_path,
        {"a.py": "def helper(x):\n if x: return x\n return 0\ndef caller(): return helper(1)\n"},
    )
    rows = result["records"]
    row = rows[0]
    weights = sum(r["impact_weight"] for r in rows)
    before = 100 - sum(r["risk_score"] * r["impact_weight"] for r in rows) / weights
    after = (
        100
        - sum(r["risk_score"] * (0.75 if r is row else 1) * r["impact_weight"] for r in rows)
        / weights
    )
    assert row["scenario_gain"] == pytest.approx(after - before, abs=1e-6)


@pytest.mark.parametrize(
    "sources",
    [
        {
            "lib.py": "def helper(): return 1",
            "main.py": "from lib import helper as h\ndef main(): return h()",
        },
        {
            "lib.ts": "export function helper(){return 1}",
            "main.ts": "import {helper as h} from './lib'; function main(){return h()}",
        },
        {
            "lib.tsx": "export function helper(){return 1}",
            "main.tsx": "import * as lib from './lib'; function main(){return lib.helper()}",
        },
        {
            "lib.js": "export function helper(){return 1}",
            "main.js": "import {helper} from './lib.js'; function main(){return helper()}",
        },
        {
            "src/lib.rs": "mod worker; fn main(){worker::helper();}",
            "src/worker.rs": "pub fn helper()->i32{1}",
        },
        {"a.c": "int helper(){return 1;} int main(){return helper();}"},
        {"a.cpp": "int helper(){return 1;} int main(){return helper();}"},
        {"a.go": "package main\nfunc helper() int{return 1}\nfunc main(){helper()}"},
        {"A.java": "class A{int helper(){return 1;} int main(){return helper();}}"},
    ],
)
def test_verified_call_adapters_and_imports(tmp_path, sources):
    result = report(tmp_path, sources)
    assert not result["issues"]
    helper = next(r for r in result["records"] if r["name"] == "helper")
    assert helper["fan_in"] == 1
    assert result["callgraph"]["coverage"]["resolved_calls"] == 1


def test_duplicate_names_parameters_and_dynamic_receivers_not_guessed(tmp_path):
    result = report(
        tmp_path,
        {
            "a.py": "def helper(): return 1\ndef shadowed(helper): return helper()\n"
            "def dynamic(obj): return obj.helper()\nclass A:\n def method(self): return 1\n"
            " def invoke(self): return method()\n",
            "other.py": "def helper(): return 2",
        },
    )
    assert result["callgraph"]["edges"] == []
    assert result["callgraph"]["coverage"]["unresolved_calls"] == 3


def test_nested_call_ownership_and_self_recursion(tmp_path):
    result = report(
        tmp_path, {"a.py": "def outer():\n def inner(): return inner()\n return inner()\n"}
    )
    rows = {r["name"]: r for r in result["records"]}
    assert rows["inner"]["fan_in"] == 1
    assert rows["inner"]["recursive"]
    assert rows["outer"]["fan_out"] == 1
    assert len(rows["outer"]["callees"]) == 1


def test_malformed_and_excluded_files_do_not_enter_graph(tmp_path):
    result = report(
        tmp_path,
        {
            "a.py": "def good(): return 1",
            "bad.py": "def fail(",
            "tests/t.py": "def bad(): return good()",
        },
    )
    assert {r["path"] for r in result["callgraph"]["nodes"]} == {"a.py"}


def test_dynamic_call_reports_location_without_embedding_source(tmp_path):
    result = report(tmp_path, {"a.py": 'def f(): return factory("private-value")()\n'})
    assert "private-value" not in json.dumps(result)
    assert any(row["reference"] == "<dynamic>" for row in result["callgraph"]["unresolved"])


def test_unsupported_graph_does_not_report_zero_dependents(tmp_path):
    result = report(tmp_path, {"a.sql": "SELECT 1;"})
    row = result["records"][0]
    assert row["graph_status"] == "unsupported"
    assert row["dependent_count"] is None
    assert row["refactor_priority"] is None


@pytest.mark.parametrize("launcher", ["module", "console"])
def test_fresh_process_default_cli_has_line_and_callgraph_breakdown(tmp_path, launcher):
    source = tmp_path / "source"
    source.mkdir()
    (source / "app.py").write_text(
        "def helper(x):\n if x: return x\n return 0\ndef caller(): return helper(1)\n"
    )
    (source / "schema.sql").write_text("PRAGMA foreign_keys=ON; BEGIN IMMEDIATE; COMMIT;")
    script = Path(sys.executable).with_name("declank.exe" if os.name == "nt" else "declank")
    command = [sys.executable, "-m", "declank"] if launcher == "module" else [str(script)]
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    run = subprocess.run(
        [*command, "scan", str(source)],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert run.returncode == 0, run.stderr
    assert "refactor_priority" in run.stdout
    assert "app.py:1-3" in run.stdout
    assert "Called by:" in run.stdout and "call site" in run.stdout
    assert "Branch:" in run.stdout
    assert "Maintainability (MI):" in run.stdout
    assert "Halstead counts:" in run.stdout
    assert "Delivered-bug estimate (B):" in run.stdout
    assert "Traceback" not in run.stderr
    run = subprocess.run(
        [*command, "scan", str(source), "--format", "json", "--top", "1"],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
    )
    assert run.returncode == 0, run.stderr
    data = json.loads(run.stdout)
    assert len(data["records"]) == 1
    assert data["summary"]["languages"]["sql"] == 1
    assert data["callgraph"]["edges"]
