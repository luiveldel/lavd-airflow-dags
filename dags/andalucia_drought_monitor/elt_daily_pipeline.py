"""
Daily ELT pipeline: ingest raw data then run dbt (staging → marts).

Flow:
  1. Reservoir catalog (full refresh)
  2. Daily embalses + RIA climate ingest (parallel, partitioned by ds)
  3. dbt run + test
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SCRIPTS_PATH = "/opt/airflow/scripts"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
EXECUTION_DATE = "{{ ds }}"

if SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, SCRIPTS_PATH)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def ingest_embalses(partition_date: str, **kwargs) -> int:
    """Daily reservoir reserves ingest for the logical date (ds)."""
    from extract_embalses import run

    rows_loaded = run(partition_date)
    kwargs["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    return rows_loaded

def ingest_ria_clima(partition_date: str, **kwargs) -> int:
    """Daily RIA agroclimatic ingest for the logical date (ds)."""
    from extract_ria import run

    rows_loaded = run(partition_date)
    kwargs["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    return rows_loaded

dbt_run_test = f"""
set -euo pipefail
cd {DBT_PROJECT_DIR}
dbt run --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
"""

@dag(
    dag_id="elt_daily_pipeline",
    default_args=default_args,
    description="Daily ELT: REDIAM + RIA ingest → dbt staging/marts",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["embalses", "ria", "rediam", "ifapa"], # GIS tags removed
    doc_md=__doc__,
)
def dag_() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")


    embalses = PythonOperator(
        task_id="ingest_embalses_daily",
        python_callable=ingest_embalses,
        op_kwargs={"partition_date": EXECUTION_DATE},
        execution_timeout=timedelta(minutes=15),
    )

    ria = PythonOperator(
        task_id="ingest_ria_clima_daily",
        python_callable=ingest_ria_clima,
        op_kwargs={"partition_date": EXECUTION_DATE},
        execution_timeout=timedelta(minutes=60),
    )

    dbt_transform = BashOperator(
        task_id="dbt_run_and_test",
        bash_command=dbt_run_test,
        retries=1,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=30),
    )

    start >> [embalses, ria] >> dbt_transform >> end

dag_()
