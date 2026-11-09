"""Downloads a Parquet file over HTTP and streams its rows to a Kafka topic, chunked with a delay between chunks."""
import json
import logging
import os
import signal
import time

import pandas as pd
import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("stream-producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SOURCE_URL = os.environ.get(
    "STREAM_SOURCE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet"
)
LOCAL_PATH = os.environ.get("STREAM_LOCAL_PATH", "/tmp/yellow_tripdata_2025-02.parquet")
BATCH_SIZE = int(os.environ.get("STREAM_BATCH_SIZE", "500"))
INTERVAL_SECONDS = float(os.environ.get("STREAM_INTERVAL_SECONDS", "2"))

_running = True


def _handle_shutdown(signum, frame):
    global _running
    logger.info("Received signal %s, stopping after current chunk...", signum)
    _running = False


def _delivery_report(err, msg):
    if err is not None:
        logger.warning("Delivery failed for record: %s", err)


def _download(url: str, path: str) -> str:
    if os.path.exists(path):
        logger.info("Using cached file %s", path)
        return path

    logger.info("Downloading %s", url)
    tmp_path = path + ".part"
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    os.rename(tmp_path, path)
    logger.info("Downloaded %s", path)
    return path


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    parquet_file = _download(SOURCE_URL, LOCAL_PATH)
    logger.info("Reading %s", parquet_file)
    df = pd.read_parquet(parquet_file)
    total_rows = len(df)
    logger.info(
        "Loaded %d rows, streaming to topic '%s' in batches of %d",
        total_rows, KAFKA_TOPIC, BATCH_SIZE,
    )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    sent = 0
    for start in range(0, total_rows, BATCH_SIZE):
        if not _running:
            logger.info("Stopped early at row %d", start)
            break

        chunk = df.iloc[start:start + BATCH_SIZE]
        # pandas handles Timestamp/NaN -> ISO string/null serialization for us.
        records = json.loads(chunk.to_json(orient="records", date_format="iso"))
        for record in records:
            producer.produce(KAFKA_TOPIC, value=json.dumps(record).encode("utf-8"), callback=_delivery_report)
        producer.poll(0)
        producer.flush()
        sent += len(records)
        logger.info("Sent %d/%d rows", sent, total_rows)

        if _running and start + BATCH_SIZE < total_rows:
            time.sleep(INTERVAL_SECONDS)

    producer.flush()
    logger.info("Finished streaming %d rows.", sent)


if __name__ == "__main__":
    main()
