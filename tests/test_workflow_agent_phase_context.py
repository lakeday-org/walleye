from types import SimpleNamespace

from walleye.review_context import encode, estimate_tokens
from walleye.workflow_agent import WorkflowAgent


class StubSourceIndex:
    def __init__(self, excerpts):
        self.excerpts = excerpts
        self.requested_ranges = []

    def verify(self, resources):
        self.verified_resources = resources

    def excerpt(self, resource, start, end, reason):
        assert reason == "requested context"
        self.requested_ranges.append((resource["id"], start, end))
        return dict(self.excerpts[(start, end)])


def _excerpt(line, end_line, text):
    return {
        "path": "example.py",
        "line": line,
        "end_line": end_line,
        "text": text,
    }


def _run_phase(tmp_path, requests, excerpts, expansion_tokens):
    resource = {
        "id": "file:example.py",
        "path": "example.py",
        "line": 1,
        "end_line": 20,
    }
    packet = {"resources": [resource], "source": []}
    index = StubSourceIndex(excerpts)
    responses = iter(
        [
            {"status": "needs_context", "context_requests": requests},
            {"status": "ready", "context_requests": []},
        ]
    )
    calls = []

    def call(stage, prompt, directory):
        calls.append((stage, prompt, directory))
        return next(responses)

    agent = SimpleNamespace(
        config=SimpleNamespace(max_expansions=1, expansion_tokens=expansion_tokens),
        call=call,
    )
    expansion = []
    result = WorkflowAgent.phase(
        agent,
        "test",
        packet,
        {"objective": "bug"},
        index,
        tmp_path,
        "phase details",
        expansion,
    )
    return result, expansion, index, calls


def test_phase_deduplicates_repeated_requests_at_token_boundary(tmp_path):
    excerpt = _excerpt(4, 5, "return value\n" * 30)
    request = {"resource_id": "file:example.py", "line": 4, "end_line": 5}
    requests = [request, dict(request)]
    one_excerpt_tokens = estimate_tokens(encode([excerpt]))
    two_excerpt_tokens = estimate_tokens(encode([excerpt, excerpt]))
    assert one_excerpt_tokens < two_excerpt_tokens

    result, expansion, index, calls = _run_phase(
        tmp_path,
        requests,
        {(4, 5): excerpt},
        one_excerpt_tokens,
    )

    assert result == {"status": "ready", "context_requests": []}
    assert expansion == [excerpt]
    assert index.requested_ranges == [("file:example.py", 4, 5)]
    assert len(calls) == 2


def test_phase_deduplicates_repeated_requests_when_budget_allows_two(tmp_path):
    excerpt = _excerpt(4, 5, "return value\n" * 30)
    request = {"resource_id": "file:example.py", "line": 4, "end_line": 5}
    requests = [request, dict(request)]
    two_excerpt_tokens = estimate_tokens(encode([excerpt, excerpt]))

    result, expansion, index, calls = _run_phase(
        tmp_path,
        requests,
        {(4, 5): excerpt},
        two_excerpt_tokens,
    )

    assert result == {"status": "ready", "context_requests": []}
    assert expansion == [excerpt]
    assert index.requested_ranges == [("file:example.py", 4, 5)]
    assert len(calls) == 2


def test_phase_preserves_order_for_distinct_context_requests(tmp_path):
    first = _excerpt(4, 5, "first value\n" * 30)
    second = _excerpt(7, 8, "second value\n" * 30)
    requests = [
        {"resource_id": "file:example.py", "line": 4, "end_line": 5},
        {"resource_id": "file:example.py", "line": 7, "end_line": 8},
    ]
    token_limit = estimate_tokens(encode([first, second])) + 1

    result, expansion, index, calls = _run_phase(
        tmp_path,
        requests,
        {(4, 5): first, (7, 8): second},
        token_limit,
    )

    assert result == {"status": "ready", "context_requests": []}
    assert expansion == [first, second]
    assert index.requested_ranges == [
        ("file:example.py", 4, 5),
        ("file:example.py", 7, 8),
    ]
    assert len(calls) == 2
