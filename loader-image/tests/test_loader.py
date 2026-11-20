"""Unit tests for the loader's pure functions (no MinIO / SQL Server needed)."""
import os

# loader.py reads these at import time -> give dummy values BEFORE importing it.
for key in ["MINIO_BUCKET", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_ENDPOINT",
            "MSSQL_DATABASE", "MSSQL_SA_PASSWORD"]:
    os.environ.setdefault(key, "test")

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import loader


@pytest.mark.parametrize("arrow_type, expected", [
    (pa.int64(), "BIGINT"),
    (pa.float64(), "FLOAT"),
    (pa.string(), "VARCHAR(100)"),
    (pa.timestamp("us"), "DATETIME2(3)"),
    (pa.date32(), "VARCHAR(400)"),          # a type we never mapped -> the safe default
])
def test_sql_type(arrow_type, expected):
    assert loader._sql_type(arrow_type) == expected


class LocalFS:
    """Stands in for s3fs: same .open() method, but reads the local disk."""

    def open(self, path):
        return open(path, "rb")


def test_read_file_adds_partition_and_lineage(tmp_path):
    folder = tmp_path / "batch_trips" / "source_month=2025-03"
    folder.mkdir(parents=True)
    path = str(folder / "part-0.parquet")
    pq.write_table(pa.table({"fare_amount": [10.5, 7.0]}), path)

    table = loader._read_file(LocalFS(), path, "source_month")

    assert table.num_rows == 2
    assert table.column("source_month").to_pylist() == ["2025-03", "2025-03"]
    assert table.column("_source_file").to_pylist() == [path, path]