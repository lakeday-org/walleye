from collections import Counter
from types import SimpleNamespace

import pytest

from walleye.scanner import _coverage


def test_coverage_handles_missing_and_present_complexity_statuses():
    discovery = SimpleNamespace(files=["missing.py", "present.py"])
    files = [{"path": "missing.py"}, {"path": "present.py"}]
    functions = [
        {"path": "missing.py"},
        {"path": "present.py", "complexity_status": "supported"},
    ]

    try:
        coverage = _coverage(discovery, Counter({"python": 2}), files, functions)
    except TypeError as error:
        pytest.fail(f"mixed complexity statuses raised TypeError: {error}")

    assert coverage["complexity_by_function_status"] == {
        None: 1,
        "supported": 1,
    }


def test_coverage_keeps_string_complexity_statuses_sorted():
    discovery = SimpleNamespace(files=["first.py", "second.py"])
    files = [{"path": "first.py"}, {"path": "second.py"}]
    functions = [
        {"path": "first.py", "complexity_status": "unsupported"},
        {"path": "second.py", "complexity_status": "supported"},
    ]

    coverage = _coverage(discovery, Counter({"python": 2}), files, functions)

    statuses = coverage["complexity_by_function_status"]
    assert list(statuses) == ["supported", "unsupported"]
    assert statuses == {"supported": 1, "unsupported": 1}
