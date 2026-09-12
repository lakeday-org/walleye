import pytest

from walleye.github_workflow import apply_edits


def test_apply_edits_accepts_unique_match():
    assert apply_edits(b"abc", [{"old": "b", "new": "d"}]) == b"adc"


def test_apply_edits_rejects_empty_old_text():
    with pytest.raises(ValueError, match="Edit 1 matches 0 times"):
        apply_edits(b"abc", [{"old": "", "new": "x"}])


def test_apply_edits_rejects_self_overlapping_old_text():
    with pytest.raises(ValueError, match="Edit 1 matches 2 times"):
        apply_edits(b"aaa", [{"old": "aa", "new": "b"}])
