from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import sys

root_dir = Path(__file__).resolve().parents[1]
sys.path.append(f"{root_dir}")  # add project root to path

from etl.extract import extract
from etl.clean import clean
from etl.load import load


@dag(
    "nyc_taxi_dag",
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    description="DAG for NYC taxi ETL",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 5, 2),
    schedule="@monthly",
    catchup=True,
    tags=["nyc_taxi", "etl"]
)

def nyc_taxi_dag():
    @task(task_id="extract-raw-data")
    def extract_task(**context):
        dt = context["logical_date"]
        year = dt.year
        month = dt.month
        extract(year, month)

    @task(task_id="clean-raw-data")
    def clean_task(**context):
        dt = context["logical_date"]
        year = dt.year
        month = dt.month
        clean(year, month)

    @task(task_id="load-data-into-snowflake")
    def load_task(**context):
        dt = context["logical_date"]
        year = dt.year
        month = dt.month
        load(year, month)

    extract_task() >> clean_task() >> load_task()

nyc_taxi_dag()
