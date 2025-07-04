from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from pathlib import Path
import sys

root_dir = Path(__file__).resolve().parents[1]
sys.path.append(f"{root_dir}")  # add project root to path

from etl.extract import extract
from etl.clean import clean
from etl.load import load


with DAG(
    "nyc_taxi_dag",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG for NYC taxi ETL",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc_taxi", "etl"]
) as dag:

    @task(task_id="extract raw data")
    def extract_task():
        extract()

    @task(task_id="clean raw data")
    def clean_task():
        clean()

    @task(task_id="load data into snowflake")
    def load_task():
        load()

    extract_task() >> clean_task() >> load_task()
