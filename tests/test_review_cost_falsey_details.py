import json

from walleye.review import ReviewConfig, prepare_review
from walleye.review_agent import run_review
from walleye.review_cost import invoke_api


def _no_finding():
    return {
        "status": "no_finding",
        "summary": "No supported defect found.",
        "finding": None,
        "context_requests": [],
    }


def _api_response():
    return {
        "id": "resp_test",
        "status": "completed",
        "model": "gpt-5.6-luna",
        "service_tier": "default",
        "usage": {
            "input_tokens": 10000,
            "output_tokens": 500,
            "input_tokens_details": {
                "cached_tokens": 100,
                "cache_write_tokens": 200,
            },
        },
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": json.dumps(_no_finding())}],
            }
        ],
    }


def test_invoke_api_rejects_present_falsey_input_tokens_details(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    calls = []

    def post(path, payload, key, timeout):
        calls.append(path)
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        response = _api_response()
        response["usage"]["input_tokens_details"] = []
        return response

    result = invoke_api("prompt", ReviewConfig(), 1, post=post)

    assert calls == ["/responses/input_tokens", "/responses"]
    assert result["dispatched"] is True
    assert result["usage"] is None
    assert result["error"] == "API returned invalid usage details"
    assert result["reserved_usd"] > 0


def test_invoke_api_defaults_missing_input_tokens_details_to_zero_counts(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def post(path, payload, key, timeout):
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        response = _api_response()
        response["usage"].pop("input_tokens_details")
        return response

    result = invoke_api("prompt", ReviewConfig(), 1, post=post)

    assert result["error"] is None
    assert result["response"] == _no_finding()
    assert result["usage"] == {
        "input_tokens": 10000,
        "output_tokens": 500,
        "cached_input_tokens": 0,
        "cache_write_tokens": 0,
    }


def test_run_review_marks_present_falsey_input_tokens_details_as_unknown(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "lib.py").write_text(
        "def first(x):\n    if x: return 1/x\n    return 0\n"
        "def second(x):\n    if x: return x+1\n    return 0\n"
    )
    config = ReviewConfig(backend="api", budget_usd=1)
    manifest, packets, index, output = prepare_review(
        repo, issues=2, config=config, output=tmp_path / "review"
    )

    calls = []

    def post(path, *args):
        calls.append(path)
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        response = _api_response()
        response["usage"]["input_tokens_details"] = []
        return response

    monkeypatch.setattr("walleye.review_cost._api_post", post)
    result = run_review(manifest, packets, index, output, config)

    assert calls == ["/responses/input_tokens", "/responses"]
    assert result["cost"]["unknown"] is True
    assert result["stop_reason"] == "usage_unknown"
    assert result["cost"]["reserved_usd"] > 0
    assert result["status"] == "incomplete"
