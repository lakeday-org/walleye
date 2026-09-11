import copy

import pytest

from walleye.workflow_quality import acceptance, metric_gate, quality_policy, verify_acceptance
from walleye.workflow_validation import canonical, digest


def change(before=50, delta=0):
    return {"before": before, "after": before + delta, "delta": delta}


@pytest.fixture
def card():
    return {
        "maintainability": {
            "target_quality": change(),
            "target": {"cyclomatic_complexity": change(3), "max_nesting": change(1)},
            "region": {"quality": change(), "decisions": change(4), "max_nesting": change(1)},
            "repository": {"score": change()},
        },
        "architecture": {"changes": {"function_cycles": change(0), "module_cycles": change(0)}},
    }


@pytest.fixture
def review():
    return {
        "status": "ready",
        "context_requests": [],
        "checks": [
            {
                "criterion": criterion,
                "passed": True,
                "reason": "Readable and preserves the contract",
            }
            for criterion in ("correctness", "relevance", "readability", "simplicity")
        ],
    }


def test_flat_quality_is_allowed_only_for_bugs(card):
    assert metric_gate(card, "bug")["passed"]
    assert not metric_gate(card, "refactor")["passed"]
    card["maintainability"]["region"]["quality"] = change(delta=0.1)
    assert metric_gate(card, "refactor")["passed"]


@pytest.mark.parametrize("scope", ["target", "region", "repository"])
@pytest.mark.parametrize("delta,allowed", [(-0.0036, True), (-0.01, True), (-0.0101, False)])
def test_bug_tolerance_applies_separately_at_each_quality_scope(card, scope, delta, allowed):
    maintainability = card["maintainability"]
    quality = {
        "target": maintainability["target_quality"],
        "region": maintainability["region"]["quality"],
        "repository": maintainability["repository"]["score"],
    }[scope]
    quality.update(change(delta=delta))
    assert metric_gate(card, "bug")["passed"] == allowed
    assert not metric_gate(card, "refactor")["passed"]


@pytest.mark.parametrize(
    "path",
    [
        ("maintainability", "target", "cyclomatic_complexity"),
        ("maintainability", "target", "max_nesting"),
        ("maintainability", "region", "decisions"),
        ("maintainability", "region", "max_nesting"),
        ("architecture", "changes", "function_cycles"),
        ("architecture", "changes", "module_cycles"),
    ],
)
@pytest.mark.parametrize("objective", ["bug", "refactor"])
def test_quality_tolerance_does_not_allow_structural_regressions(card, path, objective):
    card["maintainability"]["region"]["quality"] = change(delta=1)
    card[path[0]][path[1]][path[2]] = change(before=1, delta=1)
    assert not metric_gate(card, objective)["passed"]


def test_unknown_quality_is_not_treated_as_flat(card):
    card["maintainability"]["region"]["quality"] = {"before": None, "after": None, "delta": None}
    assert not metric_gate(card, "bug")["passed"]


@pytest.mark.parametrize("objective", [None, "architecture", ""])
def test_unknown_objective_cannot_choose_a_relaxed_policy(objective):
    with pytest.raises(ValueError, match="objective"):
        quality_policy(objective)


def test_apply_binds_acceptance_to_the_objective_and_current_policy(card, review):
    candidate = b"candidate"
    baseline = {"source_fingerprint": "baseline"}
    record = acceptance(candidate, "tests", "baseline", metric_gate(card, "bug"), review)
    proposal = {
        "finding": {"objective": "bug"},
        "acceptance_sha256": digest(canonical(record).encode()),
    }
    verify_acceptance(proposal, record, candidate, "tests", baseline, card)
    proposal["finding"]["objective"] = "refactor"
    with pytest.raises(ValueError, match="quality|maintainability"):
        verify_acceptance(proposal, record, candidate, "tests", baseline, card)
    proposal["finding"]["objective"] = "bug"
    old = copy.deepcopy(record)
    old["profile"] = "declank-acceptance-v1"
    proposal["acceptance_sha256"] = digest(canonical(old).encode())
    with pytest.raises(ValueError, match="quality|maintainability"):
        verify_acceptance(proposal, old, candidate, "tests", baseline, card)
