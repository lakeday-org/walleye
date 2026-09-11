import pytest

from walleye.workflow_comparison import paired_quality, paired_totals


def row(name, lines, quality, path="module.py"):
    return {
        "name": name,
        "qualified_name": name,
        "path": path,
        "sloc": lines,
        "risk_score": 100 - quality,
        "maintainability_index": quality,
        "complexity_score": quality,
    }


TARGET = {"path": "module.py", "name": "target"}


def average(rows):
    return sum(r["sloc"] * (100 - r["risk_score"]) for r in rows) / sum(r["sloc"] for r in rows)


def test_shortening_an_improved_function_does_not_count_as_a_module_regression():
    before = [row("target", 16, 53.2793), row("other", 200, 25)]
    after = [row("target", 14, 53.9104), before[1]]
    assert average(after) < average(before)  # The old comparison rejected this improvement.
    old, new = paired_quality(before, after, TARGET)
    assert new > old
    assert new - old == pytest.approx(16 * 0.6311 / 216, abs=0.0001)


@pytest.mark.parametrize("lines", [1, 10, 1000])
def test_function_growth_cannot_mask_a_quality_regression(lines):
    before = [row("target", 10, 80), row("other", 90, 20)]
    after = [row("target", lines, 70), before[1]]
    assert paired_quality(before, after, TARGET) == (26, 25)


def test_added_helper_cannot_dilute_an_unrelated_low_quality_function():
    before = [row("target", 10, 100), row("other", 90, 20)]
    after = before + [row("helper", 1000, 100)]
    assert average(after) > 90
    assert paired_quality(before, after, TARGET) == (28, 28)
    after[-1] = row("helper", 1000, 0)
    assert paired_quality(before, after, TARGET)[1] < 28


def test_renamed_and_removed_helpers_remain_in_the_comparison():
    before = [row("target", 10, 50), row("old_helper", 10, 50), row("other", 80, 25)]
    after = [row("target", 5, 60), row("new_helper", 10, 60), before[-1]]
    assert paired_quality(before, after, TARGET) == (30, 32)
    assert paired_quality(before, [after[0], after[-1]], TARGET) == (30, 32)


def test_duplicate_names_are_grouped_and_unknown_measurements_stay_unknown():
    before = [row("target", 10, 50), row("overload", 10, 60), row("overload", 10, 40)]
    after = [row("target", 5, 50), row("overload", 10, 50)]
    assert paired_quality(before, after, TARGET) == (50, 50)
    after[0]["maintainability_index"] = None
    totals, weight = paired_totals(before, after, TARGET)
    assert totals["mi"] == (None, None)
    assert totals["score"] == (1500, 1500) and weight == 30


def test_function_identity_changes_outside_the_assigned_file_are_rejected():
    before = [row("target", 10, 50), row("other", 20, 50, "other.py")]
    after = [before[0], row("renamed", 20, 50, "other.py")]
    with pytest.raises(ValueError, match="outside the assigned source"):
        paired_quality(before, after, TARGET)
