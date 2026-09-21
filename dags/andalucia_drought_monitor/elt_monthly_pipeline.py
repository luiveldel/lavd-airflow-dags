"""
Monthly ELT: GIS agricultural zones (PostGIS) + REDIAM reservoir catalog.

Requires PYTHONPATH to include /opt/airflow/scripts. GIS needs
geopandas/pyogrio/GDAL — see requirements-drought.txt.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def ingest_gis_zones() -> int:
    from extract_gis_zones import run

    return run()


def ingest_reservoir_catalog() -> int:
    from extract_embalses_catalog import run

    return run()


@dag(
    dag_id="elt_monthly_pipeline",
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    description="Monthly ELT: GIS polygons + reservoir catalog",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gis", "sigpac", "spatial", "embalses"],
    doc_md=__doc__,
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
