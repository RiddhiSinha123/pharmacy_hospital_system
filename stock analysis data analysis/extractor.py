"""
Extract layer – pulls all paginated stock records from freeapi.app.

Endpoint: GET https://freeapi.app/api/v1/public/stocks?page=20&limit=10

Expected response shape:
{
  "statusCode": 200,
  "data": {
    "page": 1,
    "limit": 10,
    "totalPages": 50,
    "previousPage": false,
    "nextPage": true,
    "totalItems": 497,
    "currentPageItems": 10,
    "data": [ { ...stock fields... } ]
  }
}
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import (
    API_BASE_URL,
    API_LIMIT_PARAM,
    API_PAGE_PARAM,
    DEFAULT_PAGE_SIZE,
    MAX_RETRIES,
    RAW_DATA_DIR,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_SECONDS,
)
from utils.logger import get_logger

logger = get_logger("extractor")


def _build_session() -> requests.Session:
    """Return a requests Session with automatic retry + backoff."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_SECONDS,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"Accept": "application/json"})
    return session


def fetch_page(
    session: requests.Session,
    page: int,
    limit: int = DEFAULT_PAGE_SIZE,
) -> dict:
    """Fetch a single page from the stocks API."""
    params = {API_PAGE_PARAM: page, API_LIMIT_PARAM: limit}
    logger.debug("Fetching page %d (limit=%d)", page, limit)

    response = session.get(API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    body = response.json()

    if body.get("statusCode") != 200:
        raise ValueError(f"Unexpected API statusCode: {body.get('statusCode')}")

    return body["data"]


def extract_all(
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: Optional[int] = None,
) -> Generator[dict, None, None]:
    """
    Generator that yields every stock record fetched from all pages.
    Saves raw JSON for each page into data/raw/ for auditability.
    """
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    session = _build_session()
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    page = 1
    total_pages = None
    total_yielded = 0

    while True:
        if max_pages and page > max_pages:
            logger.info("Reached max_pages limit (%d). Stopping.", max_pages)
            break

        try:
            data = fetch_page(session, page, page_size)
        except requests.HTTPError as exc:
            logger.error("HTTP error on page %d: %s", page, exc)
            break
        except Exception as exc:
            logger.error("Unexpected error on page %d: %s", page, exc, exc_info=True)
            break

        if total_pages is None:
            total_pages = data.get("totalPages", "?")
            logger.info(
                "Starting extraction – totalItems=%s totalPages=%s pageSize=%d",
                data.get("totalItems", "?"),
                total_pages,
                page_size,
            )

        records = data.get("data", [])

        # ── persist raw page ──────────────────────────────────────────────────
        raw_file = os.path.join(RAW_DATA_DIR, f"stocks_page_{page:04d}_{run_ts}.json")
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump({"extracted_at": run_ts, "page": page, "records": records}, f, indent=2)

        logger.info(
            "Page %d/%s – extracted %d records (raw → %s)",
            page, total_pages, len(records), os.path.basename(raw_file),
        )

        for record in records:
            yield record
            total_yielded += 1

        if not data.get("nextPage", False):
            logger.info("No more pages. Total records extracted: %d", total_yielded)
            break

        page += 1
        time.sleep(0.1)   # polite delay – avoid hammering the free API
