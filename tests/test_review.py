import json
import subprocess
import sys
from dataclasses import replace

import pytest

from walleye.discovery import ScanOptions
from walleye.review import ReviewConfig, load_config, prepare_review, rank_candidates, select_queue
from walleye.review_agent import (
    codex_command,
    duplicate,
    invoke_codex,
    make_prompt,
    quote_matches,
    run_review,
    validate_response,
)
from walleye.review_context import SourceIndex, encode, estimate_tokens, expand_context
from walleye.scanner import scan


def source_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "lib.py").write_text(
        "LIMIT = 100\n"
        "def calculate(x):\n"
        "    if x >= 0:\n"
        "        return LIMIT / x\n"
        "    return 0\n"
        "def other(y):\n"
        "    if y: return y + 2\n"
        "    return 0\n"
    )
    (repo / "app.py").write_text(
        "from lib import calculate\ndef main(x):\n    return calculate(x)\n"
    )
    tests = repo / "tests"
    tests.mkdir()
    (tests / "test_lib.py").write_text(
        "from lib import calculate\ndef test_positive():\n    assert calculate(1) == 100\n"
    )
    return repo


@pytest.fixture
def prepared(tmp_path):
    return prepare_review(source_repo(tmp_path), issues=2, output=tmp_path / "review")


def result_for(packet):
    target = packet["target"]
    excerpt = next(s for s in packet["source"] if s["role"] == "target")
    quote = excerpt["code"].splitlines()[0].split(": ", 1)[1]
    return {
        "status": "finding",
        "summary": "One defect with source evidence.",
        "context_requests": [],
        "finding": {
            "objective": "bug",
            "title": "Unhandled zero input",
            "context": "calculate divides a value to produce a result.",
            "root_cause": "Zero is admitted",
            "impact": "Calls with zero fail instead of returning a result.",
            "explanation": [{"text": "Zero reaches the calculation.", "evidence": [1]}],
            "severity": "medium",
            "confidence": "high",
            "trigger": "Call with zero",
            "expected_behavior": "Return a finite result",
            "actual_behavior": "Division fails",
            "proposed_change": "",
            "preserved_behavior": "",
            "expected_benefit": "",
            "validation": "Add a zero-input regression test",
            "evidence": [
                {
                    "path": target["path"],
                    "line": target["line"],
                    "end_line": target["line"],
                    "quote": quote,
                    "annotations": [
                        {"quote_line": 1, "text": "Zero is admitted to this calculation"}
                    ],
                }
            ],
        },
    }


def no_finding():
    return {
        "status": "no_finding",
        "summary": "No supported defect found.",
        "finding": None,
        "context_requests": [],
    }


@pytest.mark.parametrize("missing", ["context", "impact", "explanation", "citation"])
def test_findings_require_context_impact_and_cited_causal_steps(prepared, missing):
    _, packets, index, _ = prepared
    response = result_for(packets[0])
    if missing == "citation":
        response["finding"]["explanation"][0]["evidence"] = [2]
    else:
        response["finding"][missing] = [] if missing == "explanation" else ""
    with pytest.raises(ValueError):
        validate_response(response, packets[0], index)


@pytest.mark.parametrize(
    "annotations",
    [
        [],
        [{"quote_line": 0, "text": "Invalid anchor"}],
        [{"quote_line": 2, "text": "Outside the one-line quote"}],
        [{"quote_line": 1, "text": ""}],
        [{"quote_line": 1, "text": "First line\nsecond line"}],
        [{"quote_line": 1, "text": "x" * 181}],
        [{"quote_line": 1, "text": "First"}, {"quote_line": 1, "text": "Duplicate"}],
    ],
)
def test_agent_callouts_require_a_valid_quoted_line_and_short_explanation(prepared, annotations):
    _, packets, index, _ = prepared
    response = result_for(packets[0])
    response["finding"]["evidence"][0]["annotations"] = annotations
    with pytest.raises(ValueError):
        validate_response(response, packets[0], index)


def test_legacy_findings_load_without_inventing_or_mutating_annotations(prepared):
    from walleye.review_agent import stored_finding

    _, packets, index, _ = prepared
    response = result_for(packets[0])
    legacy = response["finding"]
    del legacy["evidence"][0]["annotations"]
    response["finding"] = stored_finding(legacy)
    validate_response(response, packets[0], index, require_detail=False)
    assert response["finding"]["evidence"][0]["annotations"] == []
    assert "annotations" not in legacy["evidence"][0]


def measured(response, input_tokens=1000, output_tokens=200):
    return {
        "response": response,
        "error": None,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_input_tokens": 0,
        },
    }


def test_snapshot_hashes_match_exact_source_and_staleness_is_rejected(tmp_path):
    repo = source_repo(tmp_path)
    snapshot = {}
    report = scan(repo, ScanOptions(functions=True), source_snapshot=snapshot)
    index = SourceIndex(report, snapshot)
    resource = index.resource("lib.py")
    index.verify([resource])
    (repo / "lib.py").write_text("# changed\n")
    assert b"return LIMIT / x" in snapshot["lib.py"]
    with pytest.raises(ValueError, match="changed"):
        index.verify([resource])


def test_packets_have_local_source_call_arguments_tests_and_constant(prepared):
    manifest, packets, index, output = prepared
    packet = next(p for p in packets if p["target"]["name"] == "calculate")
    assert "tests/test_lib.py" not in {r["path"] for r in index.rows.values()}
    contexts = packet["source"]
    assert any(
        s["role"] == "caller" and s["complete_resource"] and "calculate(x)" in s["code"]
        for s in contexts
    )
    assert any(s["role"] == "test candidate" and "== 100" in s["code"] for s in contexts)
    assert any("LIMIT = 100" in s["code"] for s in contexts)
    assert len(packet["graph"]["edges"]) == 1
    assert "callgraph" not in packet
    assert manifest["usage"]["calls"] == 0
    assert (output / "tasks" / f"{packet['task_id']}.prompt.txt").read_text() == make_prompt(packet)
    assert estimate_tokens(encode(packet)) < ReviewConfig().context_tokens


def test_ranking_uses_unsaturated_signals_and_stable_same_language_groups(tmp_path):
    repo = source_repo(tmp_path)
    snapshot = {}
    report = scan(repo, ScanOptions(functions=True), source_snapshot=snapshot)
    index = SourceIndex(report, snapshot)
    first, second = list(index.rows.values())[:2]
    for row, complexity in ((first, 50), (second, 160)):
        row.update(
            cyclomatic_complexity=complexity,
            risk_score=100,
            max_nesting=5,
            dependent_count=0,
            fan_in=0,
            fan_out=0,
        )
    candidates = rank_candidates(report, index)
    by_name = {c["row"]["name"]: c for c in candidates}
    assert (
        by_name[second["name"]]["percentiles"]["decisions"]
        > by_name[first["name"]]["percentiles"]["decisions"]
    )
    assert select_queue(candidates, 2) == select_queue(list(reversed(candidates)), 2)
    assert all(c["comparison_group"]["language"] == "python" for c in candidates)


def test_one_task_per_cycle_and_no_nested_overlap(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "cycle.py").write_text(
        "def a(x):\n if x: return b(x-1)\n return 0\n"
        "def b(x):\n if x: return a(x-1)\n return 0\n"
        "def outer(x):\n def inner(y):\n  if y: return y\n  return 0\n"
        " if x: return inner(x)\n return 0\n"
    )
    snapshot = {}
    report = scan(repo, ScanOptions(functions=True), source_snapshot=snapshot)
    candidates = rank_candidates(report, SourceIndex(report, snapshot), "refactor")
    queue = select_queue(candidates, 10)
    names = {c["row"]["name"] for c in queue}
    assert len(names & {"a", "b"}) == 1
    assert len(names & {"outer", "inner"}) == 1


def test_issue_limit_stops_dispatch_and_persists_evidence(prepared):
    manifest, packets, index, output = prepared
    manifest["budgets"]["issues"] = 1
    calls = []

    def invoke(prompt, config, limit):
        calls.append(prompt)
        return measured(result_for(packets[0]))

    result = run_review(manifest, packets, index, output, ReviewConfig(), invoke=invoke)
    assert len(calls) == len(result["findings"]) == 1
    assert result["stop_reason"] == "issue_limit"
    assert result["usage"]["total_tokens"] == 1200
    assert json.loads((output / "review.json").read_text())["findings"] == result["findings"]


def test_no_finding_is_valid_and_does_not_force_quota(prepared):
    manifest, packets, index, output = prepared
    result = run_review(
        manifest,
        packets,
        index,
        output,
        ReviewConfig(),
        invoke=lambda *args: measured(no_finding()),
    )
    assert result["findings"] == []
    assert result["stop_reason"] == "queue_exhausted"
    assert result["usage"]["calls"] == len(packets)


@pytest.mark.parametrize("unknown", [False, True])
def test_total_budget_and_unknown_usage_stop_further_calls(prepared, unknown):
    manifest, packets, index, output = prepared
    config = replace(ReviewConfig(), total_tokens=20000)

    def invoke(*args):
        response = measured(no_finding(), input_tokens=1000, output_tokens=19500)
        if unknown:
            response["usage"] = None
        return response

    result = run_review(manifest, packets, index, output, config, invoke=invoke)
    assert result["usage"]["calls"] == 1
    assert result["stop_reason"] == ("usage_unknown" if unknown else "token_limit")
    assert result["usage"]["overshoot_tokens"] == (0 if unknown else 500)


def test_context_expansion_is_indexed_bounded_and_counted(prepared):
    manifest, packets, index, output = prepared
    packet = next(p for p in packets if p["target"]["name"] == "calculate")
    resource = next(r for r in packet["resources"] if r["id"] == "file:lib.py")
    request = {
        "resource_id": resource["id"],
        "line": resource["line"],
        "end_line": resource["end_line"],
        "reason": "Check the complete contract",
    }
    responses = iter(
        [
            measured(
                {
                    "status": "needs_context",
                    "summary": "Need contract source",
                    "finding": None,
                    "context_requests": [request],
                }
            ),
            measured(no_finding()),
        ]
    )
    calls = []

    def invoke(prompt, *args):
        calls.append(prompt)
        return next(responses)

    result = run_review(manifest, [packet], index, output, ReviewConfig(), invoke=invoke)
    assert result["usage"]["calls"] == 2
    assert "REQUESTED SOURCE" in calls[1]
    assert (output / "tasks" / f"{packet['task_id']}.expansion-1.json").exists()
    with pytest.raises(ValueError, match="catalog"):
        expand_context(packet, [{**request, "resource_id": "file:../../secret"}], index, 1000)
    with pytest.raises(ValueError, match="outside"):
        expand_context(packet, [{**request, "end_line": resource["end_line"] + 1}], index, 1000)
    with pytest.raises(ValueError, match="budget"):
        expand_context(packet, [request], index, 1)


@pytest.mark.parametrize("change", ["quote", "path", "objective", "missing_cause", "bool_line"])
def test_fabricated_or_out_of_scope_findings_are_rejected(prepared, change):
    _, packets, index, _ = prepared
    response = result_for(packets[0])
    finding = response["finding"]
    if change == "quote":
        finding["evidence"][0]["quote"] = "made up source"
    elif change == "path":
        finding["evidence"][0]["path"] = "secrets.py"
    elif change == "objective":
        finding["objective"] = "refactor"
    elif change == "bool_line":
        finding["evidence"][0]["line"] = True
    else:
        finding["root_cause"] = ""
    with pytest.raises(ValueError):
        validate_response(response, packets[0], index)


def test_stale_source_stops_before_any_model_call(prepared):
    manifest, packets, index, output = prepared
    target = index.base / packets[0]["target"]["path"]
    target.write_text("# changed\n")
    result = run_review(
        manifest,
        packets,
        index,
        output,
        ReviewConfig(),
        invoke=lambda *args: pytest.fail("Must not invoke on stale source"),
    )
    assert result["stop_reason"] == "stale_source"


def test_refactor_objective_and_config_validation(tmp_path):
    repo = source_repo(tmp_path)
    _, packets, _, _ = prepare_review(repo, objective="refactor", output=tmp_path / "review")
    assert all(p["objective"] == "refactor" for p in packets)
    with pytest.raises(ValueError):
        ReviewConfig(tasks_per_issue=True)
    with pytest.raises(ValueError):
        ReviewConfig(context_tokens=10)
    path = tmp_path / "config.json"
    path.write_text('{"typo": 10}')
    with pytest.raises(ValueError, match="documented"):
        load_config(path)


def test_codex_is_given_packet_only_controls(tmp_path):
    command = codex_command(ReviewConfig(), tmp_path, 2000)
    assert "gpt-5.6-luna" in command
    assert 'model_reasoning_effort="max"' in command
    assert "features.shell_tool=false" in command
    assert "features.apps=false" in command
    assert "features.multi_agent=false" in command
    assert "--ignore-user-config" in command
    assert "features.rollout_budget.limit_tokens=2000" in command
    assert command[-1] == "-"


def test_fresh_process_prepare_cli_is_offline_and_keeps_diagnostics(tmp_path):
    repo = source_repo(tmp_path)
    (repo / "bad.py").write_text("def broken(")
    output = tmp_path / "packets"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "walleye",
            "review",
            str(repo),
            "--issues",
            "1",
            "--budget",
            "2.50",
            "--prepare",
            "-o",
            str(output),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 2
    assert "Prepared; no agents started" in completed.stdout
    manifest = json.loads((output / "review.json").read_text())
    assert manifest["scan"]["issues"][0]["path"] == "bad.py"
    assert manifest["cost"]["budget_usd"] == 2.5
    assert 0 < len(manifest["tasks"]) <= 2


def test_large_bodies_are_supplied_whole_and_not_limited_by_spending_budget(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    body = "".join(f"    if x == {i}: return {i}\n" for i in range(800))
    (repo / "big.py").write_text("def giant(x):\n" + body + "    return -1\n")
    manifest, packets, _, _ = prepare_review(repo, issues=1, output=tmp_path / "review")
    assert len(packets) == 1, manifest["ranking"]["skipped_packets"]
    packet = packets[0]
    assert 6000 < estimate_tokens(encode(packet)) < ReviewConfig().context_tokens
    target = next(s for s in packet["source"] if s["role"] == "target")
    assert target["complete_resource"]
    assert "if x == 799: return 799" in target["code"]
    assert "return -1" in target["code"]
    assert not any("excerpts have gaps" in gap for gap in packet["gaps"])
    assert any(r["id"] == "file:big.py" and r["end_line"] >= 802 for r in packet["resources"])
    _, cheaper, _, _ = prepare_review(
        repo, issues=1, config=ReviewConfig(budget_usd=0.001), output=tmp_path / "cheaper"
    )
    assert cheaper == packets
    too_small, incomplete, _, _ = prepare_review(
        repo, issues=1, config=ReviewConfig(context_tokens=6000), output=tmp_path / "small"
    )
    assert incomplete == []
    assert "Complete target" in str(too_small["ranking"]["skipped_packets"])


def test_declaration_candidates_are_lexical_and_import_resources_have_unique_ids(tmp_path):
    repo = source_repo(tmp_path)
    (repo / "lib.py").write_text(
        "import math\nfrom decimal import Decimal\nGLOBAL = 2\n"
        "def irrelevant():\n    result = 'unrelated value'\n    return result\n"
        "def calculate(x):\n    if x: return math.ceil(x) + GLOBAL\n    return Decimal(0)\n"
    )
    _, packets, index, _ = prepare_review(repo, issues=1, output=tmp_path / "review")
    packet = packets[0]
    assert "unrelated value" not in encode(packet)
    resources = {r["id"]: r for r in packet["resources"]}
    assert len(resources) == len(packet["resources"])
    assert "file:lib.py" in resources
    assert resources["file:lib.py"]["end_line"] == len(index.lines("lib.py"))
    assert len([r for r in resources if r.startswith("import:")]) == 2
    endpoint_ids = {e[k] for e in packet["graph"]["edges"] for k in ("source", "target")}
    assert endpoint_ids <= {n["id"] for n in packet["graph"]["nodes"]}


def test_source_changed_during_model_call_is_not_accepted(prepared):
    manifest, packets, index, output = prepared

    def invoke(*args):
        (index.base / packets[0]["target"]["path"]).write_text("# changed during review\n")
        return measured(result_for(packets[0]))

    result = run_review(manifest, packets, index, output, ReviewConfig(), invoke=invoke)
    assert result["stop_reason"] == "stale_source"
    assert not result["findings"]
    assert result["usage"]["calls"] == 1


def test_already_supplied_context_does_not_start_another_paid_call(prepared):
    manifest, packets, index, output = prepared
    packet = packets[0]
    target = packet["target"]
    request = {
        "status": "needs_context",
        "summary": "Need more enclosing context",
        "finding": None,
        "context_requests": [
            {
                "resource_id": target["id"],
                "line": target["line"],
                "end_line": target["end_line"],
                "reason": "Contract",
            }
        ],
    }
    result = run_review(
        manifest, [packet], index, output, ReviewConfig(), invoke=lambda *args: measured(request)
    )
    assert result["usage"]["calls"] == 1
    assert result["investigations"][0]["status"] == "needs_context"
    assert not result["findings"]


def test_duplicate_root_causes_and_overlapping_evidence_are_not_extra_issues(prepared):
    _, packets, _, _ = prepared
    first = result_for(packets[0])["finding"]
    second = result_for(packets[0])["finding"]
    second["root_cause"] = "Different wording of the same root cause"
    assert duplicate(second, [first])
    second["evidence"][0]["path"] = "different.py"
    assert not duplicate(second, [first])
    second["root_cause"] = first["root_cause"].upper()
    assert duplicate(second, [first])


@pytest.mark.parametrize("mode", ["success", "malformed", "usage_missing", "tool", "error"])
def test_codex_subprocess_protocol_and_failure_handling(tmp_path, mode):
    fake = tmp_path / "fake-codex"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import json, sys\nfrom pathlib import Path\n"
        "prompt = sys.stdin.read()\n"
        "assert 'REVIEW PACKET' in prompt\n"
        "assert 'features.shell_tool=false' in sys.argv\n"
        "assert Path.cwd() == Path(sys.argv[sys.argv.index('--cd') + 1])\n"
        "out = Path(sys.argv[sys.argv.index('--output-last-message') + 1])\n"
        f"out.write_text('invalid' if {mode!r} == 'malformed' else {json.dumps(no_finding())!r})\n"
        f"if {mode!r} != 'usage_missing':\n"
        " print(json.dumps({'type':'turn.completed',"
        "'usage':{'input_tokens':100,'output_tokens':20}}))\n"
        f"if {mode!r} == 'tool':\n"
        " print(json.dumps({'type':'item.completed','item':{'type':'command_execution'}}))\n"
        f"if {mode!r} == 'error':\n"
        " print(json.dumps({'type':'error','message':'model unavailable'})); sys.exit(1)\n"
    )
    fake.chmod(0o755)
    result = invoke_codex("REVIEW PACKET {}", replace(ReviewConfig(), codex=str(fake)), 5000)
    if mode in {"success", "usage_missing"}:
        assert result["response"] == no_finding()
        assert (result["usage"] is None) == (mode == "usage_missing")
    else:
        assert result["error"]
        assert result["response"] is None


def test_missing_codex_and_timeout_are_clean_errors(tmp_path):
    result = invoke_codex("packet", replace(ReviewConfig(), codex=str(tmp_path / "missing")), 1000)
    assert "Cannot start" in result["error"]
    fake = tmp_path / "slow-codex"
    fake.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(30)\n")
    fake.chmod(0o755)
    result = invoke_codex(
        "packet", replace(ReviewConfig(), codex=str(fake), timeout_seconds=1), 1000
    )
    assert "timed out" in result["error"]
    assert result["usage"] is None


def test_numbered_quotes_accept_only_correct_labels_and_source():
    lines = ["def f(x):", "    first = x", "    second = x + 1", "    return first + second"]
    assert quote_matches("2: first = x\n4: return first + second", lines, 2, 4)
    assert not quote_matches("2: first = x\n3: return first + second", lines, 2, 4)
    assert not quote_matches("2: first = x\n9: return first + second", lines, 2, 4)
    assert not quote_matches("2: fabricated source", lines, 2, 4)
    assert not quote_matches("1: def f(x):", lines, 2, 4)
