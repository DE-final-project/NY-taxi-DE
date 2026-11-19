"""NY Taxi pipeline: batch API -> MinIO (Bronze) -> SQL Server raw -> dbt Silver/Gold + tests.

Each task runs one of the project's images as a one-shot container on the compose network.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

NETWORK = "de-final-project-net"

# Settings the containers need (values come from .env through the airflow service's env_file).
PUBLIC_KEYS = [
    "BATCH_API_URL", "BATCH_PAGE_SIZE", "BATCH_TABLE_NAME", "STREAM_TABLE_NAME", "INGEST_MAX_WORKERS",
    "MINIO_BUCKET", "MINIO_ENDPOINT", "MSSQL_DATABASE", "RAW_SCHEMA", "LOADER_PARTITIONS",
]
SECRET_KEYS = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MSSQL_SA_PASSWORD"]

PUBLIC_ENV = {k: os.environ[k] for k in PUBLIC_KEYS if k in os.environ}
SECRET_ENV = {k: os.environ[k] for k in SECRET_KEYS if k in os.environ}

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_image(task_id: str, image: str, command: str | None = None) -> DockerOperator:
    """One pipeline step = one one-shot container from our images."""
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        network_mode=NETWORK,
        environment=PUBLIC_ENV,
        private_environment=SECRET_ENV,   # hidden in the Airflow UI
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",            # keep failed containers for debugging
        mount_tmp_dir=False,
    )


with DAG(
    dag_id="ny_taxi_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ny_taxi", "batch"],
) as dag:
    ingest = run_image("ingest_batch_to_lake", "de-final-project-ingest-image")
    load = run_image("load_lake_to_raw", "de-final-project-loader-image")
    dbt_build = run_image("dbt_build", "de-final-project-dbt-image", command="dbt build")

    ingest >> load >> dbt_build