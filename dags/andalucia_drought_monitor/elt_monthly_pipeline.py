"""
Monthly ELT pipeline: ingest static GIS agricultural zones + reservoir catalog.

Flow:
  1. GIS agricultural zones (full refresh to PostGIS) — needs geopandas/pyogrio/GDAL
  2. REDIAM reservoir catalog (full refresh)

Runtime packages: see repo root requirements-drought.txt.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

SCRIPTS_PATH = "/opt/airflow/scripts"

if SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, SCRIPTS_PATH)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

def ingest_gis_zones(**kwargs) -> int:
    """Full-refresh load of agricultural GIS polygons into PostGIS raw."""
    from extract_gis_zones import run
    rows_loaded = run()
    kwargs["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    return rows_loaded

def ingest_reservoir_catalog(**kwargs) -> int:
    """Full-refresh load of REDIAM reservoir metadata into raw."""
    from extract_embalses_catalog import run
    rows_loaded = run()
    kwargs["ti"].xcom_push(key="rows_loaded", value=rows_loaded)
    return rows_loaded

@dag(
    dag_id="elt_monthly_pipeline",
    default_args=default_args,
    description="Monthly ELT: GIS polygons for agricultural zones",
    schedule="@monthly", # Runs once a month. Use "0 0 1 1,7 *" for every 6 months.
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gis", "sigpac", "spatial"],
    doc_md=__doc__,
)
def dag_() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    gis_zones = PythonOperator(
        task_id="ingest_gis_agricultural_zones",
        python_callable=ingest_gis_zones,
        execution_timeout=timedelta(minutes=45), # Extended timeout for heavy spatial processing
    )

    catalog = PythonOperator(
        task_id="ingest_reservoir_catalog",
        python_callable=ingest_reservoir_catalog,
    )

    start >> gis_zones >> catalog >> end

dag_()
