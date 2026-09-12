import pytest

from walleye.workflow_quality import quality_policy


def test_quality_policy_accepts_bug_objective():
    assert quality_policy("bug") == {
        "profile": "declank-acceptance-v3",
        "objective": "bug",
        "quality_tolerance_points": 0.01,
        "requires_quality_improvement": False,
    }


def test_quality_policy_accepts_refactor_objective():
    assert quality_policy("refactor") == {
        "profile": "declank-acceptance-v3",
        "objective": "refactor",
        "quality_tolerance_points": 0.0001,
        "requires_quality_improvement": True,
    }


def test_quality_policy_rejects_list_objective():
    with pytest.raises(ValueError, match="objective"):
        quality_policy([])


def test_quality_policy_rejects_dict_objective():
    with pytest.raises(ValueError, match="objective"):
        quality_policy({})
