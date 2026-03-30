"""
stocks_etl/pipeline.py
──────────────────────
ETL pipeline orchestrator for freeapi.app/api/v1/public/stocks

Usage:
    python pipeline.py                        # full run
    python pipeline.py --pages 3              # first 3 pages only
    python pipeline.py --page-size 20         # 20 records per API call
    python pipeline.py --no-sqlite            # skip DB, write files only
    python pipeline.py --dry-run              # extract+transform only, no load
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from extract.extractor import extract_all
from load.loader import load_to_csv, load_to_json, load_to_sqlite, print_summary
from transform.transformer import transform
from utils.logger import get_logger

logger = get_logger("pipeline")


def run(
    page_size: int = 10,
    max_pages: int | None = None,
    use_sqlite: bool = True,
    dry_run: bool = False,
) -> dict:
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    logger.info("=" * 60)
    logger.info("ETL PIPELINE START  run_ts=%s", run_ts)
    logger.info("  page_size=%d  max_pages=%s  use_sqlite=%s  dry_run=%s",
                page_size, max_pages, use_sqlite, dry_run)
    logger.info("=" * 60)

    t0 = time.perf_counter()

    # ── EXTRACT ──────────────────────────────────────────────────────────────
    valid_records = []
    rejected_records = []
    raw_count = 0

    for raw in extract_all(page_size=page_size, max_pages=max_pages):
        raw_count += 1
        record, errors = transform(raw)
        if errors:
            rejected_records.append({
                "symbol": raw.get("symbol"),
                "errors": errors,
                "raw": raw,
            })
        else:
            valid_records.append(record)

    duration = time.perf_counter() - t0
    logger.info(
        "Extract+Transform done in %.2fs – raw=%d valid=%d rejected=%d",
        duration, raw_count, len(valid_records), len(rejected_records),
    )

    # ── LOAD ─────────────────────────────────────────────────────────────────
    if dry_run:
        logger.info("DRY RUN – skipping load step.")
    else:
        csv_path  = load_to_csv(valid_records, run_ts)
        json_path = load_to_json(valid_records, rejected_records, run_ts)

        if use_sqlite:
            load_to_sqlite(valid_records, rejected_records, run_ts, duration)

    print_summary(valid_records, rejected_records, duration)

    result = {
        "run_ts": run_ts,
        "raw_extracted": raw_count,
        "valid_loaded": len(valid_records),
        "rejected": len(rejected_records),
        "duration_sec": round(duration, 2),
    }
    logger.info("ETL PIPELINE COMPLETE – %s", result)
    return result


def main():
    parser = argparse.ArgumentParser(description="Stocks ETL pipeline – freeapi.app")
    parser.add_argument("--pages",     type=int, default=None, help="Max pages to fetch (default: all)")
    parser.add_argument("--page-size", type=int, default=10,   help="Records per API page (default: 10)")
    parser.add_argument("--no-sqlite", action="store_true",    help="Skip SQLite load")
    parser.add_argument("--dry-run",   action="store_true",    help="Extract+transform only")
    args = parser.parse_args()

    result = run(
        page_size  = args.page_size,
        max_pages  = args.pages,
        use_sqlite = not args.no_sqlite,
        dry_run    = args.dry_run,
    )
    sys.exit(0 if result["rejected"] == 0 else 1)


if __name__ == "__main__":
    main()
