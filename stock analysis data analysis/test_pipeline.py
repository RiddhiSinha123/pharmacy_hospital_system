"""
tests/test_pipeline.py
Unit tests for transform and loader layers (no live API calls required).
Run with: python -m pytest tests/ -v
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from transform.transformer import transform, transform_batch, _coerce_numeric, _market_cap_category, _price_band


# ── _coerce_numeric ───────────────────────────────────────────────────────────

def test_coerce_float():
    assert _coerce_numeric(123.45) == 123.45

def test_coerce_string_float():
    assert _coerce_numeric("99.5") == 99.5

def test_coerce_none():
    assert _coerce_numeric(None) is None

def test_coerce_empty_string():
    assert _coerce_numeric("") is None

def test_coerce_currency_string():
    assert _coerce_numeric("$1,234.56") == pytest.approx(1234.56, rel=1e-4)


# ── _market_cap_category ──────────────────────────────────────────────────────

def test_mega():
    assert _market_cap_category(500e9) == "mega"

def test_large():
    assert _market_cap_category(50e9) == "large"

def test_small():
    assert _market_cap_category(500e6) == "small"

def test_nano():
    assert _market_cap_category(10e6) == "nano"

def test_none_market_cap():
    assert _market_cap_category(None) is None


# ── _price_band ───────────────────────────────────────────────────────────────

def test_penny():
    assert _price_band(0.5) == "penny"

def test_mid_price():
    assert _price_band(50.0) == "mid"

def test_ultra():
    assert _price_band(1000.0) == "ultra"


# ── transform ─────────────────────────────────────────────────────────────────

VALID_RAW = {
    "id": 1,
    "symbol": "AAPL",
    "name": "Apple Inc.",
    "exchange": "NASDAQ",
    "price": 175.5,
    "changesPercentage": 1.2,
    "change": 2.1,
    "dayLow": 173.0,
    "dayHigh": 177.0,
    "yearHigh": 198.23,
    "yearLow": 124.17,
    "marketCap": 2_800_000_000_000,
    "priceAvg50": 170.0,
    "priceAvg200": 165.0,
    "volume": 55_000_000,
    "avgVolume": 60_000_000,
    "open": 174.0,
    "previousClose": 173.4,
    "eps": 6.11,
    "pe": 28.7,
    "sharesOutstanding": 15_800_000_000,
}

def test_transform_valid():
    record, errors = transform(VALID_RAW)
    assert errors == []
    assert record.symbol == "AAPL"
    assert record.exchange == "NASDAQ"
    assert record.market_cap_category == "mega"
    assert record.price_band == "high"
    assert record.is_above_50ma is True
    assert record.is_above_200ma is True
    assert record.day_range_pct == pytest.approx((177 - 173) / 175.5 * 100, rel=1e-3)

def test_transform_missing_symbol():
    raw = {**VALID_RAW, "symbol": ""}
    record, errors = transform(raw)
    assert any("symbol" in e.lower() for e in errors)

def test_transform_missing_price():
    raw = {**VALID_RAW, "price": None}
    record, errors = transform(raw)
    assert any("price" in e.lower() for e in errors)

def test_transform_string_price():
    raw = {**VALID_RAW, "price": "175.50"}
    record, errors = transform(raw)
    assert errors == []
    assert record.price == 175.5

def test_transform_batch():
    raws = [VALID_RAW, {**VALID_RAW, "symbol": ""}]
    valid, rejected = transform_batch(raws)
    assert len(valid) == 1
    assert len(rejected) == 1
    assert rejected[0]["symbol"] == ""
