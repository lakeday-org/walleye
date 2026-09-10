import argparse
import json
from decimal import Decimal

import pytest

from declank.cli import dollar_budget
from declank.review import ReviewConfig, prepare_review
from declank.review_agent import run_review
from declank.review_cost import (
    LUNA_PRICING,
    backend_for,
    invoke_api,
    output_allowance,
    usage_cost,
)


def no_finding():
    return {
        "status": "no_finding",
        "summary": "No supported defect found.",
        "finding": None,
        "context_requests": [],
    }


def api_response(input_tokens=10000, output_tokens=500, **changes):
    return {
        "id": "resp_test",
        "status": "completed",
        "model": "gpt-5.6-luna",
        "service_tier": "default",
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "input_tokens_details": {"cached_tokens": 100, "cache_write_tokens": 200},
            "output_tokens_details": {"reasoning_tokens": 400},
        },
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": json.dumps(no_finding())}],
            }
        ],
        **changes,
    }


def test_cost_includes_cache_writes_and_does_not_double_charge_reasoning():
    usage = {
        "input_tokens": 1_000_000,
        "cached_input_tokens": 200_000,
        "cache_write_tokens": 100_000,
        "output_tokens": 100_000,
        "reasoning_tokens": 90_000,
    }
    # Long context: 700k * .40 + 200k * .04 + 100k * .50 + 100k * 1.80 per million.
    assert usage_cost(usage, LUNA_PRICING) == Decimal("0.518")
    usage["cached_input_tokens"] = 1_000_000
    with pytest.raises(ValueError, match="exceed"):
        usage_cost(usage, LUNA_PRICING)


@pytest.mark.parametrize("input_tokens,multiplier", [(272000, 1), (272001, 2)])
def test_long_context_pricing_threshold(input_tokens, multiplier):
    assert usage_cost({"input_tokens": input_tokens}, LUNA_PRICING) == (
        Decimal(input_tokens) * Decimal("0.0000002") * multiplier
    )


@pytest.mark.parametrize("budget", ["0.005", "0.01", "1", "5"])
def test_maximum_billable_response_fits_reserved_dollars(budget):
    output, reservation = output_allowance(10000, Decimal(budget), LUNA_PRICING)
    assert 0 < output <= 128000
    assert reservation <= Decimal(budget)
    worst_case = {"input_tokens": 10000, "cache_write_tokens": 10000, "output_tokens": output}
    assert usage_cost(worst_case, LUNA_PRICING) == reservation
    if output < 128000:
        worst_case["output_tokens"] += 1
        assert usage_cost(worst_case, LUNA_PRICING) > Decimal(budget)


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "1e9999", "1e-9999", "cash"])
def test_cli_rejects_invalid_dollar_budgets(value):
    with pytest.raises(argparse.ArgumentTypeError):
        dollar_budget(value)


def test_backend_uses_api_key_without_silently_switching_models(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("CODEX_API_KEY", raising=False)
    assert backend_for(ReviewConfig()) == "codex"
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    assert backend_for(ReviewConfig()) == "api"
    assert backend_for(ReviewConfig(backend="codex")) == "codex"
    with pytest.raises(ValueError, match="price card"):
        ReviewConfig(model="unknown")
    with pytest.raises(ValueError, match="price card"):
        ReviewConfig(pricing={})
    with pytest.raises(ValueError, match="finite numbers"):
        ReviewConfig(pricing={**LUNA_PRICING, "input": "0.2"})


def test_api_counts_complete_prompt_then_reserves_before_generation(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    calls, reservations = [], []
    prompt = "complete caller and target\n" * 2000

    def post(path, payload, key, timeout):
        calls.append((path, dict(payload)))
        assert payload["input"] == prompt
        assert key == "test-key"
        assert payload["tools"] == [] and payload["tool_choice"] == "none"
        assert payload["truncation"] == "disabled"
        if path == "/responses/input_tokens":
            assert not reservations
            return {"input_tokens": 10000}
        assert reservations and Decimal(str(reservations[0])) <= Decimal("0.01")
        assert payload["max_output_tokens"] == 6250
        assert payload["service_tier"] == "default" and payload["store"] is False
        return api_response()

    result = invoke_api(prompt, ReviewConfig(), 0.01, post=post, reserve=reservations.append)
    assert [path for path, _ in calls] == ["/responses/input_tokens", "/responses"]
    assert result["response"] == no_finding() and result["error"] is None
    assert result["usage"]["cache_write_tokens"] == 200
    assert usage_cost(result["usage"], LUNA_PRICING) == Decimal("0.002592")
    assert all(calls[1][1][k] == v for k, v in calls[0][1].items())


@pytest.mark.parametrize(
    "budget,count,remaining_tokens,reason",
    [
        (0.001, 10000, None, "dollar_limit"),
        (5, 200001, None, "context_limit"),
        (5, 10000, 10500, "token_limit"),
    ],
)
def test_preflight_rejects_without_paying_for_generation(
    monkeypatch, budget, count, remaining_tokens, reason
):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    calls = []

    def post(path, *args):
        calls.append(path)
        assert path == "/responses/input_tokens"
        return {"input_tokens": count}

    result = invoke_api(
        "full prompt",
        ReviewConfig(),
        budget,
        post=post,
        reserve=lambda _: pytest.fail("Rejected calls must not reserve funds"),
        remaining_tokens=remaining_tokens,
    )
    assert result["dispatched"] is False and result["stop_reason"] == reason
    assert calls == ["/responses/input_tokens"]


@pytest.mark.parametrize("failure", ["timeout", "missing_usage", "wrong_model", "bad_details"])
def test_unknown_api_cost_keeps_reservation_and_does_not_retry(monkeypatch, failure):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    calls = []

    def post(path, *args):
        calls.append(path)
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        if failure == "timeout":
            raise TimeoutError("Timed out")
        changes = {
            "missing_usage": {"usage": None},
            "wrong_model": {"model": "different-model"},
            "bad_details": {"usage": {"input_tokens_details": []}},
        }
        return api_response(**changes[failure])

    result = invoke_api("prompt", ReviewConfig(), 1, post=post)
    assert result["dispatched"] and result["reserved_usd"] > 0
    assert result["usage"] is None
    assert len(calls) == 2


def review_fixture(tmp_path, budget=0.01):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "lib.py").write_text(
        "def first(x):\n    if x: return 1/x\n    return 0\n"
        "def second(x):\n    if x: return x+1\n    return 0\n"
    )
    config = ReviewConfig(backend="api", budget_usd=budget)
    return config, prepare_review(repo, issues=2, config=config, output=tmp_path / "review")


def test_coordinator_releases_reservation_charges_actual_cost_and_stops_at_dollars(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    config, (manifest, packets, index, output) = review_fixture(tmp_path)
    calls = []

    def post(path, payload, *args):
        calls.append(path)
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        saved = json.loads((output / "review.json").read_text())
        assert saved["cost"]["spent_usd"] + saved["cost"]["reserved_usd"] <= 0.01
        response = api_response(output_tokens=payload["max_output_tokens"])
        response["usage"]["input_tokens_details"] = {"cache_write_tokens": 10000}
        return response

    monkeypatch.setattr("declank.review_cost._api_post", post)
    result = run_review(manifest, packets, index, output, config)
    assert result["usage"]["calls"] == 1
    assert result["cost"]["spent_usd"] == 0.01
    assert result["cost"]["reserved_usd"] == result["cost"]["overshoot_usd"] == 0
    assert result["stop_reason"] == "dollar_limit"
    assert len(calls) == 2


def test_coordinator_stops_with_unknown_spend_after_timeout(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    config, (manifest, packets, index, output) = review_fixture(tmp_path)
    calls = []

    def post(path, *args):
        calls.append(path)
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        raise TimeoutError("Timeout after dispatch")

    monkeypatch.setattr("declank.review_cost._api_post", post)
    result = run_review(manifest, packets, index, output, config)
    assert result["stop_reason"] == "usage_unknown"
    assert result["cost"]["unknown"] and result["cost"]["reserved_usd"] > 0
    assert result["status"] == "incomplete" and len(calls) == 2


def test_unexpected_api_usage_stops_before_another_request(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    config, (manifest, packets, index, output) = review_fixture(tmp_path, budget=5)
    calls = []

    def post(path, *args):
        calls.append(path)
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        return api_response(output_tokens=150000)

    monkeypatch.setattr("declank.review_cost._api_post", post)
    result = run_review(manifest, packets, index, output, config)
    assert result["status"] == "incomplete" and result["stop_reason"] == "agent_error"
    assert result["cost"]["spent_usd"] > 0
    assert "reservation" in result["investigations"][0]["error"]
    assert len(calls) == 2


def test_progressive_context_requests_continue_and_retain_prior_source(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    config, (manifest, packets, index, output) = review_fixture(tmp_path, budget=1)
    packet = next(p for p in packets if p["target"]["name"] == "first")
    calls = []

    def post(path, payload, *args):
        if path == "/responses/input_tokens":
            return {"input_tokens": 10000}
        calls.append(payload["input"])
        response = api_response()
        if len(calls) <= 2:
            start, end = (4, 5) if len(calls) == 1 else (6, 6)
            result = {
                "status": "needs_context",
                "summary": "Need the adjacent contract",
                "finding": None,
                "context_requests": [
                    {
                        "resource_id": "file:lib.py",
                        "line": start,
                        "end_line": end,
                        "reason": "Check the contract",
                    }
                ],
            }
            response["output"][0]["content"][0]["text"] = json.dumps(result)
        return response

    monkeypatch.setattr("declank.review_cost._api_post", post)
    result = run_review(manifest, [packet], index, output, config)
    assert len(calls) == 3
    assert "4: def second(x):" in calls[1] and "4: def second(x):" in calls[2]
    assert "6:     return 0" in calls[2]
    assert result["cost"]["spent_usd"] == 0.002592 * 3
    assert result["investigations"][0]["status"] == "no_finding"
