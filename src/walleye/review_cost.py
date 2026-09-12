"""API price accounting and preflight reservation, independent of context selection."""

import json
import math
import os
from decimal import ROUND_FLOOR, Decimal
from urllib.error import HTTPError
from urllib.request import Request, urlopen

PRICING_SOURCE = "https://developers.openai.com/api/docs/pricing"
PRICING_DATE = "2026-09-09"
LUNA_PRICING = {
    "input": 0.20,
    "cached_input": 0.02,
    "cache_write": 0.25,
    "output": 1.20,
    "long_context_threshold": 272000,
    "long_input_multiplier": 2.0,
    "long_output_multiplier": 1.5,
}
MILLION = Decimal(1_000_000)


def dollars(value):
    if isinstance(value, bool):
        raise ValueError("Dollar amounts must be finite positive numbers")
    try:
        number = Decimal(str(value))
    except ArithmeticError as error:
        raise ValueError("Invalid dollar amount") from error
    if not number.is_finite() or number <= 0:
        raise ValueError("Dollar amounts must be finite positive numbers")
    return number


def pricing_for(config):
    rates = config.pricing
    if rates is None and config.model == "gpt-5.6-luna":
        rates = LUNA_PRICING
    if not isinstance(rates, dict) or set(rates) != set(LUNA_PRICING):
        raise ValueError(f"No complete price card for {config.model}; configure pricing explicitly")
    for key, value in rates.items():
        dollars(value)
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError("Price card values must be finite numbers")
        if key == "long_context_threshold" and (type(value) is not int or value < 1):
            raise ValueError("long_context_threshold must be a positive integer")
    if rates["cached_input"] > rates["input"] or rates["cache_write"] < rates["input"]:
        raise ValueError("Expected cached input <= input <= cache write rates")
    return dict(rates)


def token_rates(rates, input_tokens):
    long = input_tokens > rates["long_context_threshold"]
    return {
        key: Decimal(str(rates[key]))
        * Decimal(
            str(
                rates["long_output_multiplier" if key == "output" else "long_input_multiplier"]
                if long
                else 1
            )
        )
        / MILLION
        for key in ("input", "cached_input", "cache_write", "output")
    }


def usage_cost(usage, rates):
    counts = {
        key: usage.get(key, 0)
        for key in ("input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens")
    }
    if any(type(v) is not int or v < 0 for v in counts.values()):
        raise ValueError("Invalid token usage for cost accounting")
    fresh = counts["input_tokens"] - counts["cached_input_tokens"] - counts["cache_write_tokens"]
    if fresh < 0:
        raise ValueError("Cached and cache-write tokens exceed total input")
    prices = token_rates(rates, counts["input_tokens"])
    # Output includes reasoning tokens; do not charge those a second time.
    return (
        fresh * prices["input"]
        + counts["cached_input_tokens"] * prices["cached_input"]
        + counts["cache_write_tokens"] * prices["cache_write"]
        + counts["output_tokens"] * prices["output"]
    )


def output_allowance(input_tokens, remaining_usd, rates, max_output=128000):
    prices = token_rates(rates, input_tokens)
    # Reserve input at cache-write rates; actual cache hits can only lower cost.
    input_reserve = input_tokens * prices["cache_write"]
    available = Decimal(str(remaining_usd)) - input_reserve
    output = max(
        0,
        min(
            max_output, int((available / prices["output"]).to_integral_value(rounding=ROUND_FLOOR))
        ),
    )
    return output, input_reserve + output * prices["output"]


def api_key():
    return os.environ.get("OPENAI_API_KEY") or os.environ.get("CODEX_API_KEY")


def backend_for(config):
    return ("api" if api_key() else "codex") if config.backend == "auto" else config.backend


def _api_post(path, payload, key, timeout):
    request = Request(
        "https://api.openai.com/v1" + path,
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except HTTPError as error:
        # API errors may quote credentials or source. Do not forward raw response bodies.
        raise RuntimeError(f"OpenAI API HTTP {error.code} at {path}") from error


def invoke_api(
    prompt,
    config,
    remaining_usd,
    *,
    post=None,
    reserve=None,
    remaining_tokens=None,
    schema=None,
    instructions=None,
):
    """Count complete input, reserve dollars, then cap generated tokens at affordable output."""
    from .review_agent import INSTRUCTIONS, response_schema

    key = api_key()
    if not key:
        return {
            "response": None,
            "usage": None,
            "error": "Set OPENAI_API_KEY for API reviews",
            "dispatched": False,
        }
    post = post or _api_post
    payload = {
        "model": config.model,
        "input": prompt,
        "instructions": instructions or INSTRUCTIONS,
        "reasoning": {"effort": config.reasoning_effort},
        "tools": [],
        "tool_choice": "none",
        "text": {
            "format": {
                "type": "json_schema",
                "name": "walleye_review",
                "schema": schema or response_schema(),
                "strict": True,
            }
        },
        "truncation": "disabled",
    }
    rates = pricing_for(config)
    dispatched = False
    reserved = Decimal(0)
    try:
        counted = post("/responses/input_tokens", payload, key, config.timeout_seconds)
        if not isinstance(counted, dict):
            raise ValueError("API preflight did not return an object")
        input_tokens = counted.get("input_tokens")
        if type(input_tokens) is not int or input_tokens < 0:
            raise ValueError("API preflight did not return a valid input count")
        max_output = config.max_output_tokens
        for token_limit in (config.per_call_tokens, remaining_tokens):
            if token_limit is not None:
                max_output = min(max_output, max(0, token_limit - input_tokens))
        if max_output < 1024:
            return {
                "response": None,
                "usage": None,
                "error": None,
                "dispatched": False,
                "stop_reason": "token_limit",
                "counted_input_tokens": input_tokens,
            }
        output_tokens, reserved = output_allowance(input_tokens, remaining_usd, rates, max_output)
        if output_tokens < 1024:
            return {
                "response": None,
                "usage": None,
                "error": None,
                "dispatched": False,
                "stop_reason": "dollar_limit",
                "counted_input_tokens": input_tokens,
            }
        if input_tokens > config.context_tokens:
            return {
                "response": None,
                "usage": None,
                "error": None,
                "dispatched": False,
                "stop_reason": "context_limit",
                "counted_input_tokens": input_tokens,
            }
        if reserve:
            reserve(float(reserved))
        payload.update(max_output_tokens=output_tokens, store=False, service_tier="default")
        dispatched = True
        response = post("/responses", payload, key, config.timeout_seconds)
        if not isinstance(response, dict):
            raise ValueError("API response was not an object")
        raw_usage = response.get("usage")
        usage = None
        if isinstance(raw_usage, dict):
            details = raw_usage.get("input_tokens_details", {})
            if not isinstance(details, dict):
                raise ValueError("API returned invalid usage details")
            usage = {
                "input_tokens": raw_usage.get("input_tokens"),
                "output_tokens": raw_usage.get("output_tokens"),
                "cached_input_tokens": details.get("cached_tokens", 0),
                "cache_write_tokens": details.get("cache_write_tokens", 0),
            }
            usage_cost(usage, rates)  # Validate before persisting any cost as known.
        result = {
            "response": None,
            "usage": usage,
            "error": None,
            "dispatched": True,
            "reserved_usd": float(reserved),
            "counted_input_tokens": input_tokens,
            "max_output_tokens": output_tokens,
            "response_id": response.get("id"),
        }
        if (
            response.get("model") != config.model
            or response.get("service_tier", "default") != "default"
        ):
            result.update(error="API returned a different model or service tier", usage=None)
            return result
        if response.get("status") != "completed":
            result["error"] = f"API response {response.get('status', 'unknown')}"
            return result
        text = "".join(
            part.get("text", "")
            for item in response.get("output", [])
            if item.get("type") == "message"
            for part in item.get("content", [])
            if part.get("type") == "output_text"
        )
        try:
            result["response"] = json.loads(text)
        except (ValueError, TypeError):
            result["error"] = "API did not return a JSON review response"
        return result
    except (
        OSError,
        RuntimeError,
        ValueError,
        TypeError,
        AttributeError,
        KeyboardInterrupt,
    ) as error:
        return {
            "response": None,
            "usage": None,
            "error": str(error) or "API call interrupted",
            "dispatched": dispatched,
            "reserved_usd": float(reserved) if dispatched else 0,
        }
