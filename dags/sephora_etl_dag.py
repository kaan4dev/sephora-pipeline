from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"


def _run_script(script_name: str) -> None:
    script_path = SRC_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    env = os.environ.copy()
    env.setdefault("PYTHONPATH", str(PROJECT_ROOT))

    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )


def run_extract() -> None:
    _run_script("extract.py")


def run_transform() -> None:
    _run_script("transform.py")


def run_load() -> None:
    _run_script("load.py")


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sephora_etl",
    description="Run the Sephora extract-transform-load pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["sephora", "etl"],
) as dag:
    extract = PythonOperator(
        task_id="extract_raw_data",
        python_callable=run_extract,
    )

    transform = PythonOperator(
        task_id="transform_processed_data",
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id="load_to_datalake",
        python_callable=run_load,
    )

    extract >> transform >> load
