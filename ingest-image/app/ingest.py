"""Paginates the batch API for each available month and bulk-writes rows into bronze.batch_trips."""
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ingest")

BATCH_API_URL = os.environ.get("BATCH_API_URL", "http://batch-api:8000")
SCHEMA_NAME = os.environ.get("BRONZE_SCHEMA", "bronze")
TABLE_NAME = os.environ.get("BATCH_TABLE_NAME", "batch_trips")
PAGE_SIZE = int(os.environ.get("BATCH_PAGE_SIZE", "20000"))
# Keep individual INSERT statements small even when PAGE_SIZE is huge, otherwise
# SQLAlchemy's pure-Python statement compiler becomes a single-core bottleneck.
SQL_CHUNK_SIZE = int(os.environ.get("INGEST_CHUNK_SIZE", "5000"))
MAX_WORKERS = int(os.environ.get("INGEST_MAX_WORKERS", "4"))

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# JSON has no datetime type, so these known taxi columns arrive as strings and need casting.
DATETIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def _build_engine(pool_size: int = 2):
    return create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
        pool_size=pool_size, max_overflow=0,
    )


def _fetch_months() -> list:
    response = requests.get(f"{BATCH_API_URL}/months", timeout=30)
    response.raise_for_status()
    return response.json()["months"]


def _table_exists(engine) -> bool:
    query = text(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = :table"
    )
    with engine.connect() as conn:
        return conn.execute(query, {"schema": SCHEMA_NAME, "table": TABLE_NAME}).first() is not None


def _month_already_loaded(engine, month: str) -> bool:
    query = text(f"SELECT 1 FROM {SCHEMA_NAME}.{TABLE_NAME} WHERE source_month = :month LIMIT 1")
    with engine.connect() as conn:
        return conn.execute(query, {"month": month}).first() is not None


def _to_frame(rows: list, month: str) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["source_month"] = month
    for column in DATETIME_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _fetch_and_load_page(engine, month: str, offset: int) -> dict:
    response = requests.get(
        f"{BATCH_API_URL}/trips/{month}", params={"offset": offset, "limit": PAGE_SIZE}, timeout=120
    )
    response.raise_for_status()
    payload = response.json()
    rows = payload["rows"]
    if rows:
        frame = _to_frame(rows, month)
        frame.to_sql(
            TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="append", index=False,
            method="multi", chunksize=SQL_CHUNK_SIZE,
        )
    return payload


def _probe_first_page(month: str) -> dict:
    response = requests.get(f"{BATCH_API_URL}/trips/{month}", params={"offset": 0, "limit": 1}, timeout=120)
    response.raise_for_status()
    return response.json()


def _load_page_safe(engine, month: str, offset: int) -> tuple:
    start = time.monotonic()
    payload = _fetch_and_load_page(engine, month, offset)
    return month, offset, len(payload["rows"]), time.monotonic() - start


def _load_page_in_worker(month: str, offset: int) -> tuple:
    # Runs in its own process, so it needs its own engine/connection (not fork-shared).
    return _load_page_safe(_build_engine(pool_size=1), month, offset)


def main() -> None:
    engine = _build_engine(pool_size=2)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))

    months = _fetch_months()
    logger.info("Discovered %d batch months from API: %s", len(months), months)

    pending = [m for m in months if not (_table_exists(engine) and _month_already_loaded(engine, m))]
    already_loaded = len(months) - len(pending)
    if already_loaded:
        logger.info("%d month(s) already loaded, skipping.", already_loaded)
    if not pending:
        logger.info("Nothing to do.")
        return

    # Create the (possibly brand-new) table from a zero-row frame first, so the real page-loads
    # below never race each other trying to create it, and every page -- including offset 0 --
    # can be parallelized uniformly across worker processes.
    probes = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as probe_pool:
        probes.update(zip(pending, probe_pool.map(_probe_first_page, pending)))

    for month, payload in probes.items():
        if payload["rows"]:
            _to_frame(payload["rows"], month).head(0).to_sql(
                TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="append", index=False,
            )

    jobs = []
    for month, payload in probes.items():
        for offset in range(0, payload["total_rows"], PAGE_SIZE):
            jobs.append((month, offset))

    grand_total = 0
    if jobs:
        logger.info("Dispatching %d page(s) across up to %d worker processes.", len(jobs), MAX_WORKERS)
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_load_page_in_worker, m, offset): (m, offset) for m, offset in jobs}
            for future in as_completed(futures):
                month, offset, rows_loaded, elapsed = future.result()
                grand_total += rows_loaded
                logger.info("Month %s offset %d: loaded %d rows in %.1fs", month, offset, rows_loaded, elapsed)

    logger.info("Finished all months, %d rows total.", grand_total)


if __name__ == "__main__":
    main()
