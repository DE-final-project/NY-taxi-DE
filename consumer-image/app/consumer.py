"""Consumes taxi-trip JSON messages from Kafka and writes micro-batches as Parquet files into the MinIO Bronze bucket."""
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("stream-consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "stream-consumer-group")
TABLE_NAME = os.environ.get("STREAM_TABLE_NAME", "stream_trips")
# Larger micro-batches than a DB insert would need: every flush becomes one Parquet file (avoid small files).
BATCH_SIZE = int(os.environ.get("CONSUMER_BATCH_SIZE", "20000"))
FLUSH_INTERVAL_SECONDS = float(os.environ.get("CONSUMER_FLUSH_INTERVAL_SECONDS", "60"))

MINIO_BUCKET = os.environ["MINIO_BUCKET"]
STORAGE_OPTIONS = {
    "key": os.environ["MINIO_ROOT_USER"],
    "secret": os.environ["MINIO_ROOT_PASSWORD"],
    "client_kwargs": {"endpoint_url": os.environ["MINIO_ENDPOINT"]},
}

# JSON has no datetime type, so these known taxi columns arrive as strings and need casting.
DATETIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

_running = True


def _handle_shutdown(signum, frame):
    global _running
    logger.info("Received signal %s, shutting down after flushing buffer...", signum)
    _running = False


def _batch_path(partition: int, first_offset: int) -> str:
    # Named by the batch's first Kafka offset: a redelivered batch (crash before commit) overwrites the same file.
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"s3://{MINIO_BUCKET}/{TABLE_NAME}/ingest_date={ingest_date}/part-p{partition}-o{first_offset:010d}.parquet"


def _flush_batch(buffer: list, first: tuple) -> None:
    if not buffer:
        return
    frame = pd.DataFrame(buffer)
    for column in DATETIME_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    path = _batch_path(*first)
    frame.to_parquet(path, index=False, storage_options=STORAGE_OPTIONS)
    logger.info("Flushed %d rows to %s", len(frame), path)


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    buffer: list = []
    first = None             # (partition, offset) of the first message in the current batch
    batch_started = time.monotonic()
    logger.info("Consuming topic '%s' into s3://%s/%s/", KAFKA_TOPIC, MINIO_BUCKET, TABLE_NAME)

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    logger.warning("Consumer error: %s", msg.error())
                else:
                    if not buffer:
                        first = (msg.partition(), msg.offset())
                        batch_started = time.monotonic()
                    buffer.append(json.loads(msg.value()))

            elapsed = time.monotonic() - batch_started
            if buffer and (len(buffer) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL_SECONDS):
                _flush_batch(buffer, first)
                consumer.commit(asynchronous=False)   # commit only AFTER the file is written = at-least-once
                buffer = []
    finally:
        if buffer:
            _flush_batch(buffer, first)
            consumer.commit(asynchronous=False)
        consumer.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()