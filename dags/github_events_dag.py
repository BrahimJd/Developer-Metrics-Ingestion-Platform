import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# The ETL script lives at include/scripts, which Astro mounts to
# /usr/local/airflow/include during `astro dev start`.
sys.path.insert(0, "/usr/local/airflow/include/scripts")

from github_to_snowflake import extract_and_load  # noqa: E402

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="github_events_to_snowflake_bronze",
    description="Pull GitHub public events and load them into the Snowflake bronze layer.",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "bronze", "github"],
) as dag:

    load_github_events = PythonOperator(
        task_id="extract_and_load_github_events",
        python_callable=extract_and_load,
    )