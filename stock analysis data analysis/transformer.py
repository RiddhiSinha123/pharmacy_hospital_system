"""
Transform layer – cleans, validates, casts, and enriches raw stock records.

Responsibilities:
  1. Cast numeric strings → float / int
  2. Normalise string fields (strip, upper/title case)
  3. Validate required fields are present and within acceptable ranges
  4. Derive computed fields (market_cap_category, price_band, is_active)
  5. Add ETL metadata (loaded_at, source)
  6. Return structured dataclasses ready for the load layer
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from config.settings import (
    EXCHANGE_ALLOWLIST,
    MAX_PRICE,
    MIN_MARKET_CAP,
    MIN_PRICE,
    NUMERIC_FIELDS,
    REQUIRED_FIELDS,
)
from utils.logger import get_logger

logger = get_logger("transformer")


# ── Output schema ─────────────────────────────────────────────────────────────

@dataclass
class StockRecord:
    # Identifiers
    symbol: str
    name: str
    exchange: str

    # Pricing
    price: Optional[float] = None
    open: Optional[float] = None
    previous_close: Optional[float] = None
    day_low: Optional[float] = None
    day_high: Optional[float] = None
    year_high: Optional[float] = None
    year_low: Optional[float] = None
    changes_pct: Optional[float] = None
    change: Optional[float] = None

    # Moving averages
    price_avg_50: Optional[float] = None
    price_avg_200: Optional[float] = None

    # Fundamentals
    market_cap: Optional[float] = None
    eps: Optional[float] = None
    pe: Optional[float] = None
    shares_outstanding: Optional[float] = None

    # Volume
    volume: Optional[int] = None
    avg_volume: Optional[int] = None

    # Derived / enriched
    market_cap_category: Optional[str] = None   # mega / large / mid / small / micro / nano
    price_band: Optional[str] = None            # penny / low / mid / high / ultra
    day_range_pct: Optional[float] = None       # (dayHigh - dayLow) / price * 100
    is_above_50ma: Optional[bool] = None
    is_above_200ma: Optional[bool] = None

    # Metadata
    etl_loaded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    etl_source: str = "freeapi.app/api/v1/public/stocks"
    raw_id: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _coerce_numeric(value: Any) -> Optional[float]:
    """Return float or None for any input; strips currency symbols."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[^\d.\-]", "", str(value))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _market_cap_category(mc: Optional[float]) -> Optional[str]:
    if mc is None:
        return None
    if mc >= 200e9:
        return "mega"
    if mc >= 10e9:
        return "large"
    if mc >= 2e9:
        return "mid"
    if mc >= 300e6:
        return "small"
    if mc >= 50e6:
        return "micro"
    return "nano"


def _price_band(price: Optional[float]) -> Optional[str]:
    if price is None:
        return None
    if price < 1:
        return "penny"
    if price < 10:
        return "low"
    if price < 100:
        return "mid"
    if price < 500:
        return "high"
    return "ultra"


def _validate(record: StockRecord) -> List[str]:
    """Return list of validation error messages (empty = valid)."""
    errors: List[str] = []

    for f in REQUIRED_FIELDS:
        # map camelCase API names to dataclass attribute names
        attr = {
            "symbol": "symbol", "name": "name",
            "price": "price", "exchange": "exchange",
        }.get(f, f)
        if not getattr(record, attr, None):
            errors.append(f"Missing required field: {f}")

    if record.price is not None:
        if record.price < MIN_PRICE:
            errors.append(f"price {record.price} below MIN_PRICE {MIN_PRICE}")
        if record.price > MAX_PRICE:
            errors.append(f"price {record.price} above MAX_PRICE {MAX_PRICE}")

    if record.market_cap is not None and record.market_cap < MIN_MARKET_CAP:
        errors.append(f"marketCap {record.market_cap} below MIN_MARKET_CAP {MIN_MARKET_CAP}")

    if EXCHANGE_ALLOWLIST and record.exchange not in EXCHANGE_ALLOWLIST:
        errors.append(f"exchange '{record.exchange}' not in allowlist {EXCHANGE_ALLOWLIST}")

    return errors


# ── Public API ────────────────────────────────────────────────────────────────

def transform(raw: Dict[str, Any]) -> Tuple[Optional[StockRecord], List[str]]:
    """
    Transform a single raw API record into a StockRecord.
    Returns (record, errors). If errors list is non-empty the record is invalid.
    """
    try:
        price       = _coerce_numeric(raw.get("price"))
        day_low     = _coerce_numeric(raw.get("dayLow"))
        day_high    = _coerce_numeric(raw.get("dayHigh"))
        price_50    = _coerce_numeric(raw.get("priceAvg50"))
        price_200   = _coerce_numeric(raw.get("priceAvg200"))
        market_cap  = _coerce_numeric(raw.get("marketCap"))
        vol         = _coerce_numeric(raw.get("volume"))
        avg_vol     = _coerce_numeric(raw.get("avgVolume"))

        # Day-range volatility %
        day_range_pct: Optional[float] = None
        if day_high and day_low and price:
            day_range_pct = round((day_high - day_low) / price * 100, 4)

        record = StockRecord(
            symbol          = str(raw.get("symbol", "")).strip().upper(),
            name            = str(raw.get("name", "")).strip(),
            exchange        = str(raw.get("exchange", "")).strip().upper(),
            price           = price,
            open            = _coerce_numeric(raw.get("open")),
            previous_close  = _coerce_numeric(raw.get("previousClose")),
            day_low         = day_low,
            day_high        = day_high,
            year_high       = _coerce_numeric(raw.get("yearHigh")),
            year_low        = _coerce_numeric(raw.get("yearLow")),
            changes_pct     = _coerce_numeric(raw.get("changesPercentage")),
            change          = _coerce_numeric(raw.get("change")),
            price_avg_50    = price_50,
            price_avg_200   = price_200,
            market_cap      = market_cap,
            eps             = _coerce_numeric(raw.get("eps")),
            pe              = _coerce_numeric(raw.get("pe")),
            shares_outstanding = _coerce_numeric(raw.get("sharesOutstanding")),
            volume          = int(vol) if vol is not None else None,
            avg_volume      = int(avg_vol) if avg_vol is not None else None,
            # enriched
            market_cap_category = _market_cap_category(market_cap),
            price_band          = _price_band(price),
            day_range_pct       = day_range_pct,
            is_above_50ma       = (price > price_50) if (price and price_50) else None,
            is_above_200ma      = (price > price_200) if (price and price_200) else None,
            raw_id              = str(raw.get("id", "")),
        )

        errors = _validate(record)
        return record, errors

    except Exception as exc:
        logger.error("Transform error for raw record %s: %s", raw.get("symbol"), exc, exc_info=True)
        return None, [str(exc)]


def transform_batch(
    raw_records: List[Dict[str, Any]],
) -> Tuple[List[StockRecord], List[Dict]]:
    """
    Transform a list of raw records.
    Returns (valid_records, rejected_records_with_reasons).
    """
    valid: List[StockRecord] = []
    rejected: List[Dict] = []

    for raw in raw_records:
        record, errors = transform(raw)
        if errors:
            rejected.append({"symbol": raw.get("symbol"), "errors": errors, "raw": raw})
            logger.warning("Rejected %s – %s", raw.get("symbol"), "; ".join(errors))
        else:
            valid.append(record)

    logger.info(
        "Transform complete – valid=%d rejected=%d", len(valid), len(rejected)
    )
    return valid, rejected
