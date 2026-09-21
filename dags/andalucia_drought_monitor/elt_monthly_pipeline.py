from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def ingest_gis_zones() -> int:
    from extract_gis_zones import run

    return run()


def ingest_reservoir_catalog() -> int:
    from extract_embalses_catalog import run

    return run()

# -----------------------------------------------------------------------------
# - VARS (using Airflow Variables with fallbacks to env vars)
# -----------------------------------------------------------------------------
DAG_NAME = "gis_polygons_and_reservoir_catalog"
OWNER = Variable.get("dag_owner", default_var="lavelazquezd@proton.me")

# -----------------------------------------------------------------------------
# - DAG
# -----------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email": ["lavelazquezd@proton.me"],
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


@dag(
    DAG_NAME,
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gis", "sigpac", "spatial", "embalses"],
)
def dag_() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    gis_zones = PythonOperator(
        task_id="ingest_gis_agricultural_zones",
        python_callable=ingest_gis_zones,
        execution_timeout=timedelta(minutes=45),
    )

    catalog = PythonOperator(
        task_id="ingest_reservoir_catalog",
        python_callable=ingest_reservoir_catalog,
    )

    start >> gis_zones >> catalog >> end


dag_()
