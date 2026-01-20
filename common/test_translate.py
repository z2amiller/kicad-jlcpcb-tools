"""Tests for the translate module."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.translate import Price, PriceEntry


def test_price_precision_reduce():
    """Price precision reduction works as expected."""

    # build high precision price entries
    prices: list[PriceEntry] = []
    initial_price = "0.123456789"
    prices.append(PriceEntry(1, 100, initial_price))

    # run through precision change
    lower_precision_prices = Price.reduce_precision(prices)

    # confirm 3 digits of precision remain
    expected_price_str = "0.123"
    expected_price_val = 0.123

    print(f"{lower_precision_prices[0]}")

    assert lower_precision_prices[0].price_dollars_str == expected_price_str
    assert lower_precision_prices[0].price_dollars == expected_price_val


def test_price_filter_below_cutoff():
    """Price filter below cutoff works as expected."""

    # build price list with some prices lower than the cutoff
    prices: list[PriceEntry] = []
    prices.append(PriceEntry(1, 100, "0.4"))
    prices.append(PriceEntry(101, 200, "0.3"))
    prices.append(PriceEntry(201, 300, "0.2"))
    prices.append(PriceEntry(301, 400, "0.1"))

    # run through cutoff deletion filter
    filtered_prices = Price.filter_below_cutoff(prices, 0.3)

    # confirm prices lower than cutoff were deleted
    assert len(filtered_prices) == 2
    assert filtered_prices[0].price_dollars == 0.4
    assert filtered_prices[1].price_dollars == 0.3


def test_price_duplicate_price_filter():
    """Price duplicates are removed."""
    # build price list with duplicates
    prices: list[PriceEntry] = []
    prices.append(PriceEntry(1, 100, "0.4"))
    prices.append(PriceEntry(101, 200, "0.3"))
    prices.append(PriceEntry(201, 300, "0.2"))
    prices.append(PriceEntry(301, 400, "0.1"))
    prices.append(PriceEntry(401, 500, "0.1"))
    prices.append(PriceEntry(501, 600, "0.1"))
    prices.append(PriceEntry(601, None, "0.1"))

    # run duplicate filter
    unique = Price.filter_duplicate_prices(prices)

    # confirm duplicates were removed
    assert len(unique) == 4
    assert unique[len(unique) - 1].price_dollars_str == "0.1"

    # last value max_quantity is None
    assert unique[len(unique) - 1].max_quantity is None
