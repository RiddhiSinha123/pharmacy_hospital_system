"""
Load layer – writes transformed records to:
  1. PostgreSQL database  (primary store, upsert on symbol)
  2. CSV file         (processed/stocks_<run_ts>.csv)
  3. JSON file        (processed/stocks_<run_ts>.json)
  4. Rejected log     (processed/rejected_<run_ts>.json)
"""

import csv
import json
import os
import sqlite3
from dataclasses import asdict, fields
from datetime import datetime, timezone
from typing import List

from config.settings import PROCESSED_DATA_DIR, SQLITE_DB_PATH
from transform.transformer import StockRecord
from utils.logger import get_logger

logger = get_logger("loader")

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stocks (
    symbol              TEXT PRIMARY KEY,
    name                TEXT,
    exchange            TEXT,
    price               REAL,
    open                REAL,
    previous_close      REAL,
    day_low             REAL,
    day_high            REAL,
    year_high           REAL,
    year_low            REAL,
    changes_pct         REAL,
    change              REAL,
    price_avg_50        REAL,
    price_avg_200       REAL,
    market_cap          REAL,
    eps                 REAL,
    pe                  REAL,
    shares_outstanding  REAL,
    volume              INTEGER,
    avg_volume          INTEGER,
    market_cap_category TEXT,
    price_band          TEXT,
    day_range_pct       REAL,
    is_above_50ma       INTEGER,
    is_above_200ma      INTEGER,
    etl_loaded_at       TEXT,
    etl_source          TEXT,
    raw_id              TEXT
);
"""

CREATE_RUN_LOG_SQL = """
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_ts          TEXT,
    records_loaded  INTEGER,
    records_rejected INTEGER,
    duration_sec    REAL
);
"""

UPSERT_SQL = """
INSERT INTO stocks ({cols})
VALUES ({placeholders})
ON CONFLICT(symbol) DO UPDATE SET
  {updates},
  etl_loaded_at = excluded.etl_loaded_at;
"""


def _upsert_sql_for(cols: List[str]) -> str:
    placeholders = ", ".join(["?" for _ in cols])
    updates = ",\n  ".join(
        f"{c} = excluded.{c}" for c in cols if c != "symbol"
    )
    return UPSERT_SQL.format(
        cols=", ".join(cols),
        placeholders=placeholders,
        updates=updates,
    )


# ── SQLite ────────────────────────────────────────────────────────────────────

def load_to_sqlite(
    records: List[StockRecord],
    rejected: List[dict],
    run_ts: str,
    duration_sec: float,
) -> None:
    os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    try:
        cur = conn.cursor()
        cur.executescript(CREATE_TABLE_SQL + CREATE_RUN_LOG_SQL)

        if not records:
            logger.warning("No valid records to load into SQLite.")
        else:
            col_names = [f.name for f in fields(StockRecord)]
            upsert = _upsert_sql_for(col_names)

            rows = []
            for r in records:
                d = asdict(r)
                # SQLite has no bool – store as 0/1
                for k, v in d.items():
                    if isinstance(v, bool):
                        d[k] = int(v)
                rows.append(tuple(d[c] for c in col_names))

            cur.executemany(upsert, rows)
            logger.info("SQLite upserted %d rows → %s", len(rows), SQLITE_DB_PATH)

        # log this run
        cur.execute(
            "INSERT INTO etl_run_log (run_ts, records_loaded, records_rejected, duration_sec) "
            "VALUES (?, ?, ?, ?)",
            (run_ts, len(records), len(rejected), round(duration_sec, 2)),
        )
        conn.commit()
    finally:
        conn.close()


# ── CSV ───────────────────────────────────────────────────────────────────────

def load_to_csv(records: List[StockRecord], run_ts: str) -> str:
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    path = os.path.join(PROCESSED_DATA_DIR, f"stocks_{run_ts}.csv")
    if not records:
        logger.warning("No records – skipping CSV output.")
        return path

    col_names = [f.name for f in fields(StockRecord)]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=col_names)
        writer.writeheader()
        for r in records:
            writer.writerow(asdict(r))

    logger.info("CSV written: %s (%d rows)", path, len(records))
    return path


# ── JSON ──────────────────────────────────────────────────────────────────────

def load_to_json(records: List[StockRecord], rejected: List[dict], run_ts: str) -> str:
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    path = os.path.join(PROCESSED_DATA_DIR, f"stocks_{run_ts}.json")

    payload = {
        "run_ts": run_ts,
        "total_valid": len(records),
        "total_rejected": len(rejected),
        "stocks": [asdict(r) for r in records],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    logger.info("JSON written: %s", path)

    # rejected log
    if rejected:
        rej_path = os.path.join(PROCESSED_DATA_DIR, f"rejected_{run_ts}.json")
        with open(rej_path, "w", encoding="utf-8") as f:
            json.dump(rejected, f, indent=2, default=str)
        logger.warning("Rejected records written: %s (%d)", rej_path, len(rejected))

    return path


# ── Summary report ────────────────────────────────────────────────────────────

def print_summary(records: List[StockRecord], rejected: List[dict], duration_sec: float) -> None:
    exchanges = {}
    cap_cats = {}
    price_bands = {}

    for r in records:
        exchanges[r.exchange] = exchanges.get(r.exchange, 0) + 1
        cap_cats[r.market_cap_category] = cap_cats.get(r.market_cap_category, 0) + 1
        price_bands[r.price_band] = price_bands.get(r.price_band, 0) + 1

    lines = [
        "",
        "╔══════════════════════════════════════════╗",
        "║          ETL PIPELINE SUMMARY            ║",
        "╠══════════════════════════════════════════╣",
        f"║  Records loaded   : {len(records):<22}║",
        f"║  Records rejected : {len(rejected):<22}║",
        f"║  Duration (sec)   : {duration_sec:<22.2f}║",
        "╠══════════════════════════════════════════╣",
        "║  By Exchange                             ║",
    ]
    for ex, cnt in sorted(exchanges.items(), key=lambda x: -x[1]):
        lines.append(f"║    {ex:<12} : {cnt:<25}║")
    lines += [
        "╠══════════════════════════════════════════╣",
        "║  By Market Cap Category                  ║",
    ]
    for cat, cnt in sorted(cap_cats.items(), key=lambda x: -x[1]):
        lines.append(f"║    {str(cat):<12} : {cnt:<25}║")
    lines += [
        "╠══════════════════════════════════════════╣",
        "║  By Price Band                           ║",
    ]
    for band, cnt in sorted(price_bands.items(), key=lambda x: -x[1]):
        lines.append(f"║    {str(band):<12} : {cnt:<25}║")
    lines.append("╚══════════════════════════════════════════╝")

    logger.info("\n".join(lines))
