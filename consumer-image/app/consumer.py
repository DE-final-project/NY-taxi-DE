"""Consumes taxi-trip JSON messages from Kafka and bulk-writes micro-batches to Postgres."""
import json
import logging
import os
import signal
import time

import pandas as pd
from confluent_kafka import Consumer
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("stream-consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "stream-consumer-group")
SCHEMA_NAME = os.environ.get("BRONZE_SCHEMA", "bronze")
TABLE_NAME = os.environ.get("STREAM_TABLE_NAME", "stream_data")
BATCH_SIZE = int(os.environ.get("CONSUMER_BATCH_SIZE", "500"))
FLUSH_INTERVAL_SECONDS = float(os.environ.get("CONSUMER_FLUSH_INTERVAL_SECONDS", "5"))

POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# JSON has no datetime type, so these known taxi columns arrive as strings and need casting.
DATETIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

_running = True


def _handle_shutdown(signum, frame):
    global _running
    logger.info("Received signal %s, shutting down after flushing buffer...", signum)
    _running = False


def _flush_batch(engine, buffer: list) -> None:
    if not buffer:
        return
    frame = pd.DataFrame(buffer)
    for column in DATETIME_COLUMNS:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    frame.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="append", index=False, method="multi")
    logger.info("Flushed %d rows to '%s.%s'", len(frame), SCHEMA_NAME, TABLE_NAME)


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    buffer: list = []
    last_flush = time.monotonic()
    logger.info("Consuming topic '%s' into table '%s.%s'", KAFKA_TOPIC, SCHEMA_NAME, TABLE_NAME)

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    logger.warning("Consumer error: %s", msg.error())
                else:
                    buffer.append(json.loads(msg.value()))

            elapsed = time.monotonic() - last_flush
            if buffer and (len(buffer) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL_SECONDS):
                _flush_batch(engine, buffer)
                consumer.commit(asynchronous=False)
                buffer = []
                last_flush = time.monotonic()
    finally:
        _flush_batch(engine, buffer)
        if buffer:
            consumer.commit(asynchronous=False)
        consumer.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    main()
