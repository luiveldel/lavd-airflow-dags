"""
Daily ELT: REDIAM embalses + RIA + SiAR climate ingest, then dbt staging → marts.

Partition date is data_interval_start (scheduled: previous morning window).

Requires PYTHONPATH to include /opt/airflow/scripts (ingest modules) and
dbt available on PATH (see bash task). Packages: requirements-drought.txt.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DBT_BASH = """\
set -euo pipefail
export PATH="/opt/airflow/.venv/bin:$PATH"
cd /opt/airflow/dbt_project
dbt run --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
"""


def ingest_embalses(partition_date: str) -> int:
    from extract_embalses import run

    return run(partition_date)


def ingest_ria_clima(partition_date: str) -> int:
    from extract_ria import run

    return run(partition_date)


def ingest_siar_clima(partition_date: str) -> int:
    from extract_siar import run

    return run(partition_date)


def ingest_siar_hourly(partition_date: str) -> int:
    """Semihorario/horario SiAR → raw.raw_siar_clima_horario (tras diario, por cuota API)."""
    from extract_siar_hourly import run

    return run(partition_date)


@dag(
    dag_id="elt_daily_pipeline",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    description="Daily ELT: REDIAM + RIA + SiAR ingest → dbt staging/marts",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Una sola ejecución a la vez (evita solapes / doble trigger) y una lógica por día vía schedule.
    max_active_runs=1,
    tags=["embalses", "ria", "siar", "siar-hourly", "rediam", "ifapa", "mapa"],
    doc_md=__doc__,
)
def dag_() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    embalses = PythonOperator(
        task_id="ingest_embalses_daily",
        python_callable=ingest_embalses,
        op_kwargs={"partition_date": "{{ data_interval_start | ds }}"},
        execution_timeout=timedelta(minutes=15),
    )

    ria = PythonOperator(
        task_id="ingest_ria_clima_daily",
        python_callable=ingest_ria_clima,
        op_kwargs={"partition_date": "{{ data_interval_start | ds }}"},
        execution_timeout=timedelta(minutes=60),
    )

    siar = PythonOperator(
        task_id="ingest_siar_clima_daily",
        python_callable=ingest_siar_clima,
        op_kwargs={"partition_date": "{{ data_interval_start | ds }}"},
        execution_timeout=timedelta(minutes=30),
    )

    siar_hourly = PythonOperator(
        task_id="ingest_siar_clima_hourly",
        python_callable=ingest_siar_hourly,
        op_kwargs={"partition_date": "{{ data_interval_start | ds }}"},
        execution_timeout=timedelta(minutes=45),
    )

    dbt_transform = BashOperator(
        task_id="dbt_run_and_test",
        bash_command=DBT_BASH,
        retries=1,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=30),
    )

    # Hourly after daily SiAR to protect MAPA API quota; embalses/RIA stay parallel.
    start >> [embalses, ria, siar]
    siar >> siar_hourly
    [embalses, ria, siar_hourly] >> dbt_transform >> end


dag_()
