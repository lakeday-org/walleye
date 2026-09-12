from walleye.scanner import analyze_units


def _calls(source: bytes, language: str) -> list[dict]:
    graph_facts = []
    _, _, errors = analyze_units(source, language, "example.js", graph_facts=graph_facts)
    assert not errors
    assert len(graph_facts) == 1
    return graph_facts[0].calls


def test_javascript_dollar_callee_is_retained():
    calls = _calls(b"$fetch();", "javascript")

    assert [call["reference"] for call in calls] == ["$fetch"]


def test_typescript_dollar_callee_is_retained():
    calls = _calls(b"$fetch();", "typescript")

    assert [call["reference"] for call in calls] == ["$fetch"]


def test_computed_callee_remains_dynamic():
    calls = _calls(b"make()();", "javascript")
    references = [call["reference"] for call in calls]

    assert len(calls) == 2
    assert references.count("make") == 1
    assert references.count("<dynamic>") == 1


def test_callee_length_limit_is_inclusive():
    accepted = "a" * 256
    overlong = "a" * 257

    assert [call["reference"] for call in _calls(f"{accepted}();".encode(), "javascript")] == [
        accepted
    ]
    assert [call["reference"] for call in _calls(f"{overlong}();".encode(), "javascript")] == [
        "<dynamic>"
    ]
