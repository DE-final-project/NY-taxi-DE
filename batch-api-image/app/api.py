"""Serves NYC yellow-taxi batch months over HTTP, downloading each Parquet file on first request."""
import json
import logging
import os
import threading

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("batch-api")

BASE_URL = os.environ.get("BATCH_SOURCE_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data")
MONTHS = [m.strip() for m in os.environ.get(
    "BATCH_MONTHS",
    "2025-01,2025-03,2025-04,2025-05,2025-06,2025-07,2025-08,2025-09,2025-10,2025-11,2025-12",
).split(",") if m.strip()]
CACHE_DIR = os.environ.get("CACHE_DIR", "/cache")

os.makedirs(CACHE_DIR, exist_ok=True)

app = FastAPI(title="NY Taxi Batch API")

_frames: dict = {}
_lock = threading.Lock()


def _download(month: str) -> str:
    path = os.path.join(CACHE_DIR, f"yellow_tripdata_{month}.parquet")
    if os.path.exists(path):
        logger.info("Using cached file %s", path)
        return path

    url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
    logger.info("Downloading %s", url)
    # Unique per-process suffix: multiple uvicorn workers may race to download the same month.
    tmp_path = f"{path}.{os.getpid()}.part"
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    os.replace(tmp_path, path)
    logger.info("Downloaded %s", path)
    return path


def _get_frame(month: str) -> pd.DataFrame:
    if month not in MONTHS:
        raise HTTPException(status_code=404, detail=f"Unknown month '{month}'")
    with _lock:
        if month not in _frames:
            _frames.clear()  # Bounded cache: keep only one month per worker to avoid OOM.
            path = _download(month)
            logger.info("Reading %s into memory", path)
            _frames[month] = pd.read_parquet(path)
    return _frames[month]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/months")
def months():
    return {"months": MONTHS}


@app.get("/trips/{month}")
def trips(month: str, offset: int = 0, limit: int = 5000):
    frame = _get_frame(month)
    total_rows = len(frame)
    chunk = frame.iloc[offset:offset + limit]
    # pandas handles Timestamp/NaN -> ISO string/null serialization for us.
    records = json.loads(chunk.to_json(orient="records", date_format="iso"))
    return {"month": month, "offset": offset, "limit": limit, "total_rows": total_rows, "rows": records}
