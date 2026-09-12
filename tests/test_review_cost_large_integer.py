from types import SimpleNamespace

import pytest

from walleye.review_cost import LUNA_PRICING, pricing_for


def config_for(rates):
    return SimpleNamespace(model="gpt-5.6-luna", pricing=rates)


def test_pricing_for_accepts_arbitrary_size_integer_rates():
    rates = dict(LUNA_PRICING)
    huge_rate = 10**400
    rates["input"] = huge_rate
    rates["cache_write"] = huge_rate

    try:
        result = pricing_for(config_for(rates))
    except OverflowError as error:
        raise AssertionError("pricing_for should accept arbitrary-size integer rates") from error

    assert result == rates
    assert result is not rates


def test_pricing_for_accepts_minimum_threshold_and_equal_ordering_rates():
    rates = dict(LUNA_PRICING)
    rates["cached_input"] = 1
    rates["input"] = 1
    rates["cache_write"] = 1
    rates["long_context_threshold"] = 1

    assert pricing_for(config_for(rates)) == rates


def test_pricing_for_rejects_boolean_rate():
    rates = dict(LUNA_PRICING)
    rates["input"] = True

    with pytest.raises(ValueError, match="Dollar amounts"):
        pricing_for(config_for(rates))


def test_pricing_for_rejects_zero_rate():
    rates = dict(LUNA_PRICING)
    rates["input"] = 0

    with pytest.raises(ValueError, match="finite positive numbers"):
        pricing_for(config_for(rates))


def test_pricing_for_rejects_negative_rate():
    rates = dict(LUNA_PRICING)
    rates["input"] = -1

    with pytest.raises(ValueError, match="finite positive numbers"):
        pricing_for(config_for(rates))


def test_pricing_for_rejects_nonfinite_rate():
    rates = dict(LUNA_PRICING)
    rates["input"] = float("inf")

    with pytest.raises(ValueError, match="finite positive numbers"):
        pricing_for(config_for(rates))


def test_pricing_for_rejects_incomplete_price_card():
    rates = dict(LUNA_PRICING)
    rates.pop("output")

    with pytest.raises(ValueError, match="No complete price card"):
        pricing_for(config_for(rates))


def test_pricing_for_rejects_invalid_rate_order():
    rates = dict(LUNA_PRICING)
    rates["cached_input"] = 2
    rates["input"] = 1
    rates["cache_write"] = 2

    with pytest.raises(ValueError, match="cached input"):
        pricing_for(config_for(rates))


def test_pricing_for_rejects_non_integer_threshold():
    rates = dict(LUNA_PRICING)
    rates["long_context_threshold"] = 1.0

    with pytest.raises(ValueError, match="positive integer"):
        pricing_for(config_for(rates))
