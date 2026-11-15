"""Paginates the batch API for each available month and writes each page as a Parquet file into the MinIO Bronze bucket."""
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing
import pandas as pd
import requests
import s3fs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ingest")

BATCH_API_URL = os.environ.get("BATCH_API_URL", "http://batch-api:8000")
TABLE_NAME = os.environ.get("BATCH_TABLE_NAME", "batch_trips")
PAGE_SIZE = int(os.environ.get("BATCH_PAGE_SIZE", "100000"))
MAX_WORKERS = int(os.environ.get("INGEST_MAX_WORKERS", "4"))

MINIO_BUCKET = os.environ["MINIO_BUCKET"]
STORAGE_OPTIONS = {
    "key": os.environ["MINIO_ROOT_USER"],
    "secret": os.environ["MINIO_ROOT_PASSWORD"],
    "client_kwargs": {"endpoint_url": os.environ["MINIO_ENDPOINT"]},
}

# JSON has no datetime type, so these known taxi columns arrive as strings and need casting.
DATETIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def _page_path(month: str, offset: int) -> str:
    # Hive-style partition folder + deterministic file name = idempotent, collision-free writes.
    return f"s3://{MINIO_BUCKET}/{TABLE_NAME}/source_month={month}/part-{offset:07d}.parquet"


def _fetch_months() -> list:
    response = requests.get(f"{BATCH_API_URL}/months", timeout=30)
    response.raise_for_status()
    return response.json()["months"]


def _to_frame(rows: list) -> pd.DataFrame:
    # No source_month column here: it lives in the folder name (source_month=YYYY-MM).
    frame = pd.DataFrame(rows)
    for column in DATETIME_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    return frame


def _fetch_and_load_page(month: str, offset: int) -> int:
    response = requests.get(
        f"{BATCH_API_URL}/trips/{month}", params={"offset": offset, "limit": PAGE_SIZE}, timeout=120
    )
    response.raise_for_status()
    rows = response.json()["rows"]
    if rows:
        _to_frame(rows).to_parquet(_page_path(month, offset), index=False, storage_options=STORAGE_OPTIONS)
    return len(rows)


def _probe_first_page(month: str) -> dict:
    response = requests.get(f"{BATCH_API_URL}/trips/{month}", params={"offset": 0, "limit": 1}, timeout=120)
    response.raise_for_status()
    return response.json()


def _load_page_in_worker(month: str, offset: int) -> tuple:
    start = time.monotonic()
    rows_loaded = _fetch_and_load_page(month, offset)
    return month, offset, rows_loaded, time.monotonic() - start


def main() -> None:
    fs = s3fs.S3FileSystem(**STORAGE_OPTIONS)

    months = _fetch_months()
    logger.info("Discovered %d batch months from API: %s", len(months), months)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as probe_pool:
        probes = dict(zip(months, probe_pool.map(_probe_first_page, months)))

    # Page-level resume: only pages whose Parquet file is not in MinIO yet become jobs.
    jobs, skipped = [], 0
    for month, payload in probes.items():
        for offset in range(0, payload["total_rows"], PAGE_SIZE):
            if fs.exists(_page_path(month, offset)):
                skipped += 1
                continue
            jobs.append((month, offset))

    if skipped:
        logger.info("%d page(s) already in MinIO, skipping.", skipped)
    if not jobs:
        logger.info("Nothing to do.")
        return

    grand_total = 0
    logger.info("Dispatching %d page(s) across up to %d worker processes.", len(jobs), MAX_WORKERS)
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = {pool.submit(_load_page_in_worker, m, offset): (m, offset) for m, offset in jobs}
        for future in as_completed(futures):
            month, offset, rows_loaded, elapsed = future.result()
            grand_total += rows_loaded
            logger.info("Month %s offset %d: wrote %d rows in %.1fs", month, offset, rows_loaded, elapsed)

    logger.info("Finished all months, %d rows total.", grand_total)


if __name__ == "__main__":
    main()