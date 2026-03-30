# 📈 Stocks ETL Pipeline

> A production-ready **Extract → Transform → Load** pipeline for the [freeapi.app](https://freeapi.app/api/v1/public/stocks) public stocks endpoint.

![Python](https://img.shields.io/badge/Python-3.8+-blue?style=flat-square) ![SQLite](https://img.shields.io/badge/Database-SQLite-lightblue?style=flat-square) ![Tests](https://img.shields.io/badge/Tests-18%20unit%20tests-green?style=flat-square) ![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)

---

## 📁 Project Structure

```
stocks_etl/
├── pipeline.py              # Orchestrator — entry point
├── requirements.txt
├── config/
│   └── settings.py          # All tuneable parameters
├── extract/
│   └── extractor.py         # HTTP fetch, pagination, raw file dump
├── transform/
│   └── transformer.py       # Cleaning, validation, enrichment
├── load/
│   └── loader.py            # SQLite upsert + CSV + JSON output
├── utils/
│   └── logger.py            # Rotating file + console logger
├── tests/
│   └── test_pipeline.py     # 18 unit tests (pytest)
├── data/
│   ├── raw/                 # Raw JSON per page (audit trail)
│   └── processed/           # Final CSV / JSON outputs
└── logs/
    └── etl.log              # Rotating log file
```

---

## 🚀 Quick Start

```bash
pip install -r requirements.txt

# Full run (all pages)
python pipeline.py

# First 3 pages only
python pipeline.py --pages 3

# 20 records per API call, skip SQLite
python pipeline.py --page-size 20 --no-sqlite

# Dry run (extract + transform, no writes)
python pipeline.py --dry-run

# Run tests
python -m pytest tests/ -v
```

---

## 🔄 Pipeline Stages

### 1 · Extract — `extract/extractor.py`

- Paginates through all pages of the stocks API automatically
- Detects `nextPage: false` to stop fetching
- Retries on `429`/`5xx` with exponential backoff (configurable in `settings.py`)
- Saves each page's raw JSON to `data/raw/` for full auditability

### 2 · Transform — `transform/transformer.py`

- **Type coercion** — numeric fields cast from string/None to `float`/`int`
- **Normalisation** — symbol → UPPER, name stripped, exchange → UPPER
- **Validation** — required fields checked; price and marketCap range-checked
- **Enrichment** — five derived fields added to every valid record:

| Derived Field | Logic |
|---|---|
| `market_cap_category` | mega / large / mid / small / micro / nano |
| `price_band` | penny / low / mid / high / ultra |
| `day_range_pct` | `(dayHigh - dayLow) / price × 100` |
| `is_above_50ma` | `price > priceAvg50` |
| `is_above_200ma` | `price > priceAvg200` |

> ⚠️ Invalid records are **quarantined** (not silently dropped) and written to `data/processed/rejected_<ts>.json`.

### 3 · Load — `load/loader.py`

| Destination | Details |
|---|---|
| `data/stocks.db` | SQLite — upsert on `symbol`; includes `etl_run_log` table |
| `data/processed/*.csv` | All valid records, timestamped filename |
| `data/processed/*.json` | Same records with metadata envelope |
| `data/processed/rejected_*.json` | Quarantined records with per-record error reasons |

---

## ⚙️ Configuration

All parameters live in `config/settings.py`.

| Setting | Default | Description |
|---|---|---|
| `API_BASE_URL` | freeapi.app stocks endpoint | Source API URL |
| `DEFAULT_PAGE_SIZE` | `10` | Records per API call |
| `MAX_RETRIES` | `3` | HTTP retry attempts |
| `RETRY_BACKOFF_SECONDS` | `2` | Backoff multiplier (seconds) |
| `EXCHANGE_ALLOWLIST` | `None` (accept all) | Filter by exchange, e.g. `["NYSE"]` |
| `MIN_PRICE` / `MAX_PRICE` | `0` / `1,000,000` | Price range for validation |

---

## 🕐 Scheduling

```bash
# Run daily at 6 AM via cron
0 6 * * * cd /path/to/stocks_etl && python pipeline.py >> logs/cron.log 2>&1
```

Or import `run()` from `pipeline.py` directly into **Airflow**, **Prefect**, or **Dagster**.

---

## 🔚 Exit Codes

| Code | Meaning |
|---|---|
| `0` | All records loaded successfully |
| `1` | Some records were rejected — check `rejected_*.json` for details |


