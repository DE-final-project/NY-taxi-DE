"""Incrementally loads new Bronze Parquet files from MinIO into SQL Server raw tables (one file = one unit of work)."""
import logging
import os
import time
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pymssql
import s3fs

class JsonFormatter(logging.Formatter):
    """One JSON object per log line -> easy to filter and aggregate (stage, level, rows, seconds...)."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "stage": record.name,   # the logger name: loader / ingest / batch-api ...
            "msg": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))   # numbers passed with extra={"fields": {...}}
        return json.dumps(entry)


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

logger = logging.getLogger("loader")

MINIO_BUCKET = os.environ["MINIO_BUCKET"]
STORAGE_OPTIONS = {
    "key": os.environ["MINIO_ROOT_USER"],
    "secret": os.environ["MINIO_ROOT_PASSWORD"],
    "client_kwargs": {"endpoint_url": os.environ["MINIO_ENDPOINT"]},
}

MSSQL_HOST = os.environ.get("MSSQL_HOST", "sqlserver")
MSSQL_PORT = os.environ.get("MSSQL_PORT", "1433")
MSSQL_DATABASE = os.environ["MSSQL_DATABASE"]
MSSQL_USER = os.environ.get("MSSQL_USER", "sa")
MSSQL_PASSWORD = os.environ["MSSQL_SA_PASSWORD"]
RAW_SCHEMA = os.environ.get("RAW_SCHEMA", "raw")

# Bronze folder -> its Hive partition key. Each becomes raw.<folder> in SQL Server.
SOURCES = {
    os.environ.get("BATCH_TABLE_NAME", "batch_trips"): "source_month",
    os.environ.get("STREAM_TABLE_NAME", "stream_trips"): "ingest_date",
}
# Optional filter, e.g. "2025-01,2025-03"; empty = load every partition.
LOADER_PARTITIONS = {p.strip() for p in os.environ.get("LOADER_PARTITIONS", "").split(",") if p.strip()}
BULK_BATCH_SIZE = int(os.environ.get("LOADER_BULK_BATCH_SIZE", "102400"))

# VARCHAR, not NVARCHAR: pymssql bulk_copy sends str as single-byte data (NVARCHAR would garble it).
ARROW_TO_SQL = {
    pa.int8(): "SMALLINT", pa.int16(): "SMALLINT", pa.int32(): "INT", pa.int64(): "BIGINT",
    pa.float32(): "REAL", pa.float64(): "FLOAT", pa.bool_(): "BIT", pa.string(): "VARCHAR(100)",
}


def _sql_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_timestamp(arrow_type):
        return "DATETIME2(3)"
    return ARROW_TO_SQL.get(arrow_type, "VARCHAR(400)")


def _connect():
    return pymssql.connect(
        server=MSSQL_HOST, port=MSSQL_PORT, user=MSSQL_USER, password=MSSQL_PASSWORD,
        database=MSSQL_DATABASE, autocommit=True,
    )


def _setup(conn) -> None:
    with conn.cursor() as cur:
        # Bulk loads in FULL recovery would grow the transaction log without bound on a dev box.
        cur.execute("ALTER DATABASE CURRENT SET RECOVERY SIMPLE")
        cur.execute(f"IF SCHEMA_ID('{RAW_SCHEMA}') IS NULL EXEC('CREATE SCHEMA [{RAW_SCHEMA}]')")
        cur.execute(f"""
            IF OBJECT_ID('{RAW_SCHEMA}.load_log') IS NULL
            CREATE TABLE [{RAW_SCHEMA}].[load_log] (
                file_path    NVARCHAR(400) NOT NULL PRIMARY KEY,
                table_name   NVARCHAR(100) NOT NULL,
                rows_loaded  BIGINT        NOT NULL,
                seconds      FLOAT         NOT NULL,
                loaded_at    DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
            )""")


def _loaded_files(conn, table: str) -> set:
    with conn.cursor() as cur:
        cur.execute(f"SELECT file_path FROM [{RAW_SCHEMA}].[load_log] WHERE table_name = %s", (table,))
        return {row[0] for row in cur.fetchall()}


def _table_columns(conn, table: str) -> list:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.name FROM sys.columns c WHERE c.object_id = OBJECT_ID(%s) ORDER BY c.column_id",
            (f"{RAW_SCHEMA}.{table}",),
        )
        return [row[0] for row in cur.fetchall()]


def _ensure_table(conn, table: str, schema: pa.Schema) -> list:
    """Create the raw table on first sight; afterwards ADD any new source columns (schema evolution)."""
    existing = _table_columns(conn, table)
    with conn.cursor() as cur:
        if not existing:
            cols = ", ".join(f"[{f.name}] {_sql_type(f.type)} NULL" for f in schema)
            cur.execute(f"CREATE TABLE [{RAW_SCHEMA}].[{table}] ({cols})")
            # Columnstore: ~10x compression and fast scans for the analytical queries dbt will run.
            cur.execute(f"CREATE CLUSTERED COLUMNSTORE INDEX [cci_{table}] ON [{RAW_SCHEMA}].[{table}]")
            logger.info("Created %s.%s with %d columns", RAW_SCHEMA, table, len(schema))
        else:
            for field in schema:
                if field.name not in existing:
                    cur.execute(f"ALTER TABLE [{RAW_SCHEMA}].[{table}] ADD [{field.name}] {_sql_type(field.type)} NULL")
                    logger.warning("Schema evolution: added column %s to %s.%s", field.name, RAW_SCHEMA, table)
    return _table_columns(conn, table)


def _read_file(fs, path: str, partition_key: str) -> pa.Table:
    table = pq.read_table(fs.open(path))
    partition_value = path.split(f"{partition_key}=")[1].split("/")[0]
    table = table.append_column(partition_key, pa.array([partition_value] * table.num_rows, pa.string()))
    # Lineage: every raw row remembers which lake file it came from.
    return table.append_column("_source_file", pa.array([path] * table.num_rows, pa.string()))


def _load_file(conn, fs, table: str, partition_key: str, path: str) -> int:
    start = time.monotonic()
    arrow_table = _read_file(fs, path, partition_key)
    columns = _ensure_table(conn, table, arrow_table.schema)

    # Build plain-Python rows in the table's column order (missing source columns -> NULL).
    # Timestamps are cast to ms so to_pylist() yields datetime.datetime (pymssql can't bulk-copy pandas.Timestamp).
    values = []
    for name in columns:
        if name not in arrow_table.column_names:
            values.append([None] * arrow_table.num_rows)
            continue
        column = arrow_table.column(name)
        if pa.types.is_timestamp(column.type):
            column = column.cast(pa.timestamp("ms"))
        values.append(column.to_pylist())
    rows = list(zip(*values))

    with conn.cursor() as cur:
        # Clean up a partial load from a crashed previous run before re-loading the file.
        cur.execute(f"DELETE FROM [{RAW_SCHEMA}].[{table}] WHERE _source_file = %s", (path,))
    conn.bulk_copy(f"{RAW_SCHEMA}.{table}", rows, batch_size=BULK_BATCH_SIZE, tablock=True)
    elapsed = time.monotonic() - start
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO [{RAW_SCHEMA}].[load_log] (file_path, table_name, rows_loaded, seconds) VALUES (%s, %s, %s, %s)",
            (path, table, len(rows), elapsed),
        )
    return len(rows)


def main() -> None:
    fs = s3fs.S3FileSystem(**STORAGE_OPTIONS)
    conn = _connect()
    _setup(conn)

    run_start = time.monotonic()
    total_files = total_rows = 0
    for table, partition_key in SOURCES.items():
        all_files = sorted(fs.find(f"{MINIO_BUCKET}/{table}"))
        if LOADER_PARTITIONS:
            all_files = [f for f in all_files if f.split(f"{partition_key}=")[1].split("/")[0] in LOADER_PARTITIONS
                         or partition_key == "ingest_date"]
        done = _loaded_files(conn, table)
        new_files = [f for f in all_files if f.endswith(".parquet") and f not in done]
        logger.info("files discovered", extra={"fields": {
            "table": table, "in_lake": len(all_files), "already_loaded": len(done), "new": len(new_files)}})

        for i, path in enumerate(new_files, 1):
            rows = _load_file(conn, fs, table, partition_key, path)
            total_files += 1
            total_rows += rows
            logger.info("file loaded", extra={"fields": {
                "table": table, "file": path, "rows": rows, "n": i, "of": len(new_files)}})

    conn.close()
    elapsed = time.monotonic() - run_start
    logger.info("load finished", extra={"fields": {
        "files": total_files, "rows": total_rows, "seconds": round(elapsed, 1),
        "rows_per_second": round(total_rows / elapsed) if elapsed else 0}})


if __name__ == "__main__":
    main()
