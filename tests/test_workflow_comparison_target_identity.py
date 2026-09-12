import pytest

from walleye.workflow_comparison import paired_totals


def metric_row(name, lines, mi, score, qualified_name=None):
    result = {
        "name": name,
        "path": "module.py",
        "sloc": lines,
        "risk_score": 100 - score,
        "maintainability_index": mi,
        "complexity_score": score,
    }
    if qualified_name is not None:
        result["qualified_name"] = qualified_name
    return result


def test_qualified_target_is_grouped_with_same_path_unmatched_rows():
    qualified_name = "module._groups"
    before = [
        metric_row("_groups", 10, 50, 50, qualified_name=qualified_name),
        metric_row("removed", 20, 50, 50),
    ]
    after = [
        metric_row("_groups", 15, 40, 30, qualified_name=qualified_name),
        metric_row("added", 15, 60, 70),
    ]
    target = {
        "path": "module.py",
        "name": "_groups",
        "qualified_name": qualified_name,
    }

    totals, weight = paired_totals(before, after, target)

    expected = pytest.approx((1500, 1500))
    assert totals["mi"] == expected
    assert totals["score"] == expected
    assert weight == 30


def test_target_without_qualified_name_uses_name_identity():
    before = [
        metric_row("_groups", 10, 50, 50),
        metric_row("removed", 20, 50, 50),
    ]
    after = [
        metric_row("_groups", 15, 40, 30),
        metric_row("added", 15, 60, 70),
    ]
    target = {"path": "module.py", "name": "_groups"}

    totals, weight = paired_totals(before, after, target)

    expected = pytest.approx((1500, 1500))
    assert totals["mi"] == expected
    assert totals["score"] == expected
    assert weight == 30
