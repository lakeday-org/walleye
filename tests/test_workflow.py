import json
from dataclasses import replace

import pytest

from declank.review import ReviewConfig, prepare_review, write_json
from declank.workflow import apply_proposal, improve, safe_path
from declank.workflow_validation import (
    replace_function,
    run_cases,
    source_bundle,
)

ORIGINAL = "function clamp(x) { if (x > 10) return 10; return x; }"
FIXED = "function clamp(x) { return Math.max(0, Math.min(x, 10)); }"


def case(name, value, expected, kind="control"):
    return {
        "id": name,
        "kind": kind,
        "arguments_json": json.dumps([value]),
        "expected_json": json.dumps(expected),
        "outcome": "value",
        "reason": "The documented result is clamped to the inclusive range 0 to 10",
    }


CASES = [case("negative", -1, 0, "regression"), case("middle", 5, 5), case("high", 20, 10)]


@pytest.fixture
def saved(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "clamp.js").write_text(ORIGINAL)
    manifest, packets, _, output = prepare_review(root, issues=1, output=tmp_path / "review")
    packet = packets[0]
    finding = {
        "objective": "bug",
        "title": "Negative inputs are not clamped",
        "root_cause": "Lower bound missing",
        "severity": "medium",
        "confidence": "high",
        "trigger": "clamp(-1)",
        "expected_behavior": "Return 0",
        "actual_behavior": "Returns -1",
        "proposed_change": "",
        "preserved_behavior": "",
        "expected_benefit": "",
        "validation": "Check both bounds",
        "evidence": [{"path": "clamp.js", "line": 1, "end_line": 1, "quote": ORIGINAL}],
        "task_id": packet["task_id"],
        "source_sha256": packet["target"]["sha256"],
        "verification": "Source quotes checked; proposed tests not run",
    }
    manifest["findings"] = [finding]
    write_json(output / "review.json", manifest)
    return root, output / "review.json"


def measured(response, tokens=500):
    return {
        "response": response,
        "usage": {"input_tokens": 500, "output_tokens": tokens},
        "error": None,
    }


def approved_review():
    return {
        "status": "ready",
        "summary": "The standard bounds operations simplify the contract",
        "context_requests": [],
        "checks": [
            {"criterion": c, "passed": True, "reason": "Both bounds are explicit and preserved"}
            for c in ("correctness", "relevance", "readability", "simplicity")
        ],
    }


def fake_agent(output, replacement=FIXED, tests=CASES, review=None):
    calls = []
    patches = []

    def invoke(prompt, config, limit, *, schema, instructions):
        calls.append(prompt)
        if len(calls) == 1:
            assert "FIRST STAGE" in prompt
            return measured(
                {
                    "status": "ready",
                    "summary": "Test the clamping contract",
                    "context_requests": [],
                    "tests": tests,
                }
            )
        if "checks" in schema["properties"]:
            assert "INDEPENDENT REVIEW" in prompt
            assert "SECOND STAGE" not in prompt
            return measured(review or approved_review())
        assert "SECOND STAGE" in prompt
        assert (output / "proposals/001/baseline-tests.json").exists()
        assert (output / "proposals/001/tests.json").exists()
        patches.append(1)
        source = (
            replacement[min(len(patches) - 1, len(replacement) - 1)]
            if isinstance(replacement, list)
            else replacement
        )
        return measured(
            {
                "status": "ready",
                "summary": "Handle the missing lower bound",
                "context_requests": [],
                "replacement": source,
            }
        )

    return calls, invoke


def run(saved, tmp_path, **kwargs):
    root, review = saved
    output = tmp_path / "improve"
    calls, invoke = fake_agent(output, **kwargs)
    manifest, output = improve(
        review, output=output, config=ReviewConfig(backend="codex"), invoke=invoke
    )
    return root, manifest, output, calls


def test_live_pipeline_orders_frozen_tests_before_patch_and_preserves_source(saved, tmp_path):
    root, manifest, output, calls = run(saved, tmp_path)
    assert len(calls) == 3
    assert (root / "clamp.js").read_text() == ORIGINAL
    assert manifest["verified_resolutions"] == 1

    assert manifest["usage"]["calls"] == 3 and manifest["usage"]["total_tokens"] == 3000
    assert manifest["cost"]["spent_usd"] == 0.0021
    proposal = output / "proposals/001"
    states = json.loads((proposal / "proposal.json").read_text())["history"]
    assert [s["status"] for s in states] == [
        "baseline",
        "tests-frozen",
        "reproduced",
        "candidate",
        "verified-candidate",
    ]
    card = json.loads((proposal / "scorecard.json").read_text())
    assert card["state"] == "verified-candidate"
    assert card["correctness"]["confirmed_open"] == {"before": 1, "after": 0, "delta": -1}
    assert card["correctness"]["tests_passing"] == {"before": 2, "after": 3, "delta": 1}
    assert card["maintainability"]["target"]["cyclomatic_complexity"]["delta"] == -1
    assert card["maintainability"]["target_quality"]["delta"] > 0
    assert card["architecture"]["changes"]["function_cycles"]["delta"] == 0
    assert card["correctness"]["score"] is None
    assert manifest["baseline_health"]["correctness"]["confirmed_open"] is None


def test_test_passing_but_more_complex_fix_is_rejected(saved, tmp_path):
    ugly = "function clamp(x) { if (x < 0) return 0; if (x > 10) return 10; return x; }"
    root, manifest, output, calls = run(saved, tmp_path, replacement=ugly)
    proposal = manifest["proposals"][0]
    assert proposal["status"] == "quality-rejected"
    assert manifest["verified_resolutions"] == 0
    assert not any("INDEPENDENT REVIEW" in p for p in calls)  # save money on measured failures
    directory = output / "proposals/001"
    card = json.loads((directory / "scorecard.json").read_text())
    assert card["state"] == "measured-candidate"
    assert card["correctness"]["verified_resolutions"] == 0
    assert card["correctness"]["confirmed_open"]["after"] == 1
    assert card["correctness"]["tests_passing"]["after"] == 3
    with pytest.raises(ValueError, match="verified"):
        apply_proposal(directory)
    assert (root / "clamp.js").read_text() == ORIGINAL


def test_rejected_patch_is_revised_with_the_same_frozen_tests(saved, tmp_path):
    ugly = "function clamp(x) { if (x < 0) return 0; if (x > 10) return 10; return x; }"
    _, manifest, output, calls = run(saved, tmp_path, replacement=[ugly, FIXED])
    assert manifest["proposals"][0]["status"] == "verified-candidate"
    assert len(calls) == 4  # tests, failed patch, revised patch, independent review
    assert "REJECTED ATTEMPT" in calls[2]
    directory = output / "proposals/001"
    assert json.loads((directory / "tests.json").read_text()) == CASES
    attempts = [
        json.loads((directory / f"attempts/{n:03}/acceptance.json").read_text()) for n in (1, 2)
    ]
    assert [a["passed"] for a in attempts] == [False, True]
    assert attempts[0]["tests_sha256"] == attempts[1]["tests_sha256"]
    assert manifest["usage"]["calls"] == 4
    assert manifest["cost"]["spent_usd"] == 0.0028


def test_independent_readability_rejection_blocks_passing_metrics(saved, tmp_path):
    review = approved_review()
    review["checks"][2].update(passed=False, reason="Reject this illustrative readability defect")
    _, manifest, output, _ = run(saved, tmp_path, review=review)
    assert manifest["proposals"][0]["status"] == "quality-rejected"
    record = json.loads((output / "proposals/001/acceptance.json").read_text())
    assert record["metrics"]["passed"] and not record["passed"]
    assert manifest["verified_resolutions"] == 0


def test_complexity_cannot_be_hidden_in_a_nested_helper(saved, tmp_path):
    hidden = """function clamp(x) {
      function bounds(x) {
        if (x < 0) return 0;
        if (x > 10) return 10;
        return x;
      }
      return bounds(x);
    }"""
    _, manifest, output, _ = run(saved, tmp_path, replacement=hidden)
    assert manifest["proposals"][0]["status"] == "quality-rejected"
    card = json.loads((output / "proposals/001/scorecard.json").read_text())
    assert card["maintainability"]["target"]["cyclomatic_complexity"]["delta"] == -1
    assert card["maintainability"]["region"]["decisions"]["delta"] == 1


def test_region_includes_helpers_but_not_same_line_siblings(tmp_path):
    from declank.discovery import ScanOptions
    from declank.scanner import scan
    from declank.workflow_scores import _region

    (tmp_path / "inline.js").write_text(
        "function outer(x) { function inner(y) {return y;} return inner(x); } "
        "function sibling(x) { if(x) return x; return 0; }"
    )
    report = scan(tmp_path, ScanOptions(functions=True))
    target = next(r for r in report["records"] if r["name"] == "outer")
    region = _region(report, target)
    assert region["functions"] == 2 and region["decisions"] == 0


def test_unknown_review_usage_cannot_approve_a_test_passing_patch(saved, tmp_path):
    root, review = saved
    output = tmp_path / "improve"
    _, writer = fake_agent(output)

    def invoke(prompt, config, limit, *, schema, instructions):
        if "checks" in schema["properties"]:
            return {"response": approved_review(), "usage": None, "error": "timeout"}
        return writer(prompt, config, limit, schema=schema, instructions=instructions)

    manifest, _ = improve(review, output=output, invoke=invoke)
    assert manifest["workflow_stop_reason"] == "usage_unknown"
    assert manifest["verified_resolutions"] == 0
    assert manifest["proposals"][0]["status"] == "stopped"
    card = json.loads((output / "proposals/001/attempts/001/scorecard.json").read_text())
    assert card["state"] == "measured-candidate"
    assert (root / "clamp.js").read_text() == ORIGINAL


def test_revision_attempt_limit_does_not_weaken_review(saved, tmp_path):
    alternatives = [
        FIXED,
        FIXED.replace("Math.max(0, Math.min(x, 10))", "Math.min(10, Math.max(x, 0))"),
        FIXED.replace("Math.max(0, Math.min(x, 10))", "Math.max(Math.min(10, x), 0)"),
    ]
    review = approved_review()
    review["checks"][2].update(passed=False, reason="An unresolved readability concern")
    _, manifest, output, calls = run(saved, tmp_path, replacement=alternatives, review=review)
    assert len(calls) == 7  # frozen plan + three writer/reviewer pairs
    assert len(list((output / "proposals/001/attempts").iterdir())) == 3
    assert manifest["proposals"][0]["status"] == "quality-rejected"
    assert manifest["verified_resolutions"] == 0


@pytest.mark.parametrize("tamper", ["policy", "review", "missing_review"])
def test_apply_requires_current_acceptance_bound_to_reviewed_source(saved, tmp_path, tamper):
    root, _, output, _ = run(saved, tmp_path)
    directory = output / "proposals/001"
    if tamper == "policy":
        proposal = json.loads((directory / "proposal.json").read_text())
        proposal["profile"] = "declank-improve-v1"
        write_json(directory / "proposal.json", proposal)
    elif tamper == "missing_review":
        (directory / "acceptance.json").unlink()
    else:
        record = json.loads((directory / "acceptance.json").read_text())
        record["review"]["checks"][0]["passed"] = False
        write_json(directory / "acceptance.json", record)
    with pytest.raises((ValueError, OSError)):
        apply_proposal(directory)
    assert (root / "clamp.js").read_text() == ORIGINAL


def test_apply_reverifies_preserves_mode_and_updates_actual_scores(saved, tmp_path):
    root, _, output, _ = run(saved, tmp_path)
    file = root / "clamp.js"
    file.chmod(0o640)
    proposal, card = apply_proposal(output / "proposals/001")
    assert proposal["status"] == card["state"] == "applied"
    assert file.read_text() == FIXED and file.stat().st_mode & 0o777 == 0o640
    assert len(list((root / ".declank/regressions").glob("*.json"))) == 1
    actual = json.loads((output / "proposals/001/applied-scan.json").read_text())
    assert (
        actual["scores"]["overall_score"] == card["maintainability"]["repository"]["score"]["after"]
    )
    assert json.loads((output / "workflow.json").read_text())["proposal_counts"] == {"applied": 1}
    with pytest.raises(ValueError, match="verified"):
        apply_proposal(output / "proposals/001")


@pytest.mark.parametrize("tamper", ["source", "tests", "candidate", "context_symlink"])
def test_apply_rejects_staleness_and_tampering_without_modifying_source(saved, tmp_path, tamper):
    root, _, output, _ = run(saved, tmp_path)
    directory = output / "proposals/001"
    if tamper == "source":
        (root / "clamp.js").write_text(ORIGINAL + "\n// new user edit")
    elif tamper == "tests":
        (directory / "tests.json").write_text("[]")
    elif tamper == "candidate":
        (directory / "candidate.source").write_text("function clamp(x) { return 0; }")
    else:
        (root / "clamp.js").unlink()
        external = tmp_path / "external.js"
        external.write_text(ORIGINAL)
        (root / "clamp.js").symlink_to(external)
    previous = (root / "clamp.js").read_bytes()
    with pytest.raises(ValueError):
        apply_proposal(directory)
    assert (root / "clamp.js").read_bytes() == previous


def test_nonreproducing_tests_stop_before_patch_call(saved, tmp_path):
    tests = [case("alreadyworks", 5, 5, "regression"), case("control", 20, 10)]
    _, manifest, _, calls = run(saved, tmp_path, tests=tests)
    assert len(calls) == 1 and manifest["verified_resolutions"] == 0
    assert manifest["proposals"][0]["status"] == "not-reproduced"


def test_regression_fix_that_breaks_control_is_not_verified(saved, tmp_path):
    root, manifest, output, _ = run(saved, tmp_path, replacement="function clamp(x) { return 0; }")
    assert manifest["proposals"][0]["status"] == "verification-failed"
    assert manifest["verified_resolutions"] == 0
    assert (root / "clamp.js").read_text() == ORIGINAL
    assert not (output / "proposals/001/scorecard.json").exists()


def test_unknown_usage_stops_before_tests_or_patch(saved, tmp_path):
    _, review = saved
    calls = []

    def invoke(*args, **kwargs):
        calls.append(1)
        return {"usage": None, "response": None, "error": "timeout"}

    manifest, _ = improve(review, output=tmp_path / "improve", invoke=invoke)
    assert len(calls) == 1 and manifest["workflow_stop_reason"] == "usage_unknown"
    assert manifest["cost"]["unknown"] and manifest["cost"]["reserved_usd"] > 0


def test_spending_budget_is_shared_between_stages(saved, tmp_path):
    _, review = saved
    calls = []

    def invoke(*args, **kwargs):
        calls.append(1)
        return measured(
            {"status": "ready", "summary": "Tests", "tests": CASES, "context_requests": []},
            tokens=100000,
        )

    manifest, _ = improve(
        review,
        output=tmp_path / "improve",
        invoke=invoke,
        config=replace(ReviewConfig(), budget_usd=0.01, backend="codex"),
    )
    assert len(calls) == 1
    assert manifest["workflow_stop_reason"] == "dollar_limit"
    assert manifest["cost"]["overshoot_usd"] > 0


def test_patch_cannot_change_signatures_or_add_unrelated_source():
    for bad in [
        "function clamp(y) { return y; }",
        FIXED + "\nfunction unrelated() {}",
        "const secret = 1;\n" + FIXED,
    ]:
        with pytest.raises(ValueError):
            replace_function(ORIGINAL.encode(), "javascript", "clamp", bad)


def test_harness_errors_do_not_count_as_reproduced_bugs():
    code = "function clamp(x) { return missingDependency(x); }"
    bundle = source_bundle(code.encode(), "javascript", "clamp")
    result = run_cases(bundle, "clamp", CASES)
    assert result["harness_errors"] == 3


def test_isolated_cases_have_no_host_capabilities():
    code = "function clamp(x) { return [typeof process, typeof require, typeof fetch]; }"
    tests = [case("isolation", 1, ["undefined"] * 3)]
    assert run_cases(code, "clamp", tests)["passed"] == 1


def test_typescript_dependencies_and_limits_are_handled():
    source = (
        b"const LIMIT: number = 10; "
        b"function clamp(x: number): number { return Math.min(x, LIMIT); }"
    )
    bundle = source_bundle(source, "typescript", "clamp")
    assert run_cases(bundle, "clamp", [case("high", 20, 10)])["passed"] == 1
    with pytest.raises(ValueError, match="adapter"):
        source_bundle(b"def clamp(x): return x", "python", "clamp")
    result = run_cases("function clamp(x) { while(true) {} }", "clamp", [case("timeout", 1, 1)])
    assert result["harness_errors"] == 1


def test_path_traversal_is_rejected(tmp_path):
    for path in ["../other", "/tmp/other", "a/../../other", "a\\other"]:
        with pytest.raises(ValueError):
            safe_path(tmp_path, path)


def test_stale_saved_finding_does_not_start_an_agent(saved, tmp_path):
    root, review = saved
    (root / "clamp.js").write_text(ORIGINAL + "\n// changed")
    with pytest.raises(ValueError, match="changed"):
        improve(
            review, output=tmp_path / "improve", invoke=lambda *a, **k: pytest.fail("No model call")
        )


def test_fresh_review_and_fix_share_the_same_usage_ledger(saved, tmp_path):
    from declank.review_agent import response_schema

    root, review = saved
    finding = json.loads(review.read_text())["findings"][0]
    fields = response_schema()["properties"]["finding"]["anyOf"][0]["properties"]

    def review_invoke(*args):
        return measured(
            {
                "status": "finding",
                "summary": finding["title"],
                "finding": {k: finding[k] for k in fields},
                "context_requests": [],
            }
        )

    output = tmp_path / "fresh"
    _, invoke = fake_agent(output)
    manifest, _ = improve(
        root,
        output=output,
        invoke=invoke,
        review_invoke=review_invoke,
        config=ReviewConfig(backend="codex"),
    )
    assert manifest["verified_resolutions"] == 1
    assert manifest["usage"]["calls"] == 4 and manifest["usage"]["total_tokens"] == 4000
    assert manifest["cost"]["spent_usd"] == 0.0028
    assert manifest["correctness"]["confirmed_open_in_original"] == 1
    assert manifest["correctness"]["applied_resolutions"] == 0


def test_characterization_refactor_has_no_invented_bug_credit(saved, tmp_path):
    _, review = saved
    data = json.loads(review.read_text())
    data["objective"] = "refactor"
    data["findings"][0].update(
        objective="refactor",
        proposed_change="Use a standard minimum",
        preserved_behavior="Upper bound preserved",
        expected_benefit="One less branch",
    )
    write_json(review, data)
    controls = [case("negative", -1, -1), case("middle", 5, 5), case("high", 20, 10)]
    output = tmp_path / "improve"
    _, invoke = fake_agent(
        output, replacement="function clamp(x) { return Math.min(x, 10); }", tests=controls
    )
    manifest, _ = improve(review, output=output, invoke=invoke)
    assert manifest["proposals"][0]["status"] == "verified-candidate"
    assert manifest["verified_resolutions"] == 0
    card = json.loads((output / "proposals/001/scorecard.json").read_text())
    assert card["correctness"]["confirmed_open"]["delta"] == 0
    assert card["maintainability"]["target_quality"]["delta"] > 0


def test_new_source_files_make_apply_refuse_before_writing(saved, tmp_path):
    root, _, output, _ = run(saved, tmp_path)
    (root / "new.js").write_text("function added(x) { return x; }")
    with pytest.raises(ValueError, match="cohort changed"):
        apply_proposal(output / "proposals/001")
    assert (root / "clamp.js").read_text() == ORIGINAL


def test_scoring_profile_changes_are_not_comparable(saved, tmp_path):
    root, _, output, _ = run(saved, tmp_path)
    baseline_file = output / "baseline.json"
    baseline = json.loads(baseline_file.read_text())
    baseline["tool"]["quality_profile"] = "different-scoring-rules"
    write_json(baseline_file, baseline)
    with pytest.raises(ValueError, match="versions changed"):
        apply_proposal(output / "proposals/001")
    assert (root / "clamp.js").read_text() == ORIGINAL


def test_workflow_api_uses_stage_schemas_and_reserves_dollars(saved, tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    _, review = saved
    calls = []
    output = tmp_path / "improve"

    def post(path, payload, *args):
        if path == "/responses/input_tokens":
            return {"input_tokens": 500}
        calls.append(payload)
        ledger = json.loads((output / "workflow.json").read_text())
        assert ledger["cost"]["reserved_usd"] > 0
        assert ledger["cost"]["reserved_usd"] + ledger["cost"]["spent_usd"] <= 5
        schema = payload["text"]["format"]["schema"]
        result = {"status": "ready", "summary": "Contract", "context_requests": []}
        if len(calls) == 1:
            assert "tests" in schema["properties"]
            result["tests"] = CASES
        elif "checks" in schema["properties"]:
            result = approved_review()
        else:
            assert "replacement" in schema["properties"]
            result["replacement"] = FIXED
        return {
            "model": "gpt-5.6-luna",
            "service_tier": "default",
            "status": "completed",
            "usage": {"input_tokens": 500, "output_tokens": 500},
            "output": [
                {
                    "type": "message",
                    "content": [{"type": "output_text", "text": json.dumps(result)}],
                }
            ],
        }

    monkeypatch.setattr("declank.review_cost._api_post", post)
    manifest, _ = improve(review, output=output, config=ReviewConfig(backend="api"))
    assert len(calls) == manifest["usage"]["calls"] == 3
    assert manifest["cost"]["spent_usd"] == 0.0021
    assert manifest["verified_resolutions"] == 1
