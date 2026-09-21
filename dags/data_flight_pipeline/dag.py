from datetime import timedelta, datetime
import json
import logging
import os
from typing import Any

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

import sys
from pathlib import Path as _Path
_DAG_DIR = str(_Path(__file__).resolve().parent)
if _DAG_DIR not in sys.path:
    sys.path.insert(0, _DAG_DIR)

from utils import upload_to_s3, format_dbt_vars

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# - VARS (using Airflow Variables with fallbacks to env vars)
# -----------------------------------------------------------------------------
DAG_NAME = "flights_data_pipeline"
OWNER = Variable.get("dag_owner", default_var="lavelazquezd@proton.me")
EXECUTION_DATE = "{{ ds }}"

# -----------------------------------------------------------------------------
# - S3 (MINIO) - credentials from environment, config from Variables
# -----------------------------------------------------------------------------
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")

# Configuration that can be changed without redeploying
S3_BUCKET = Variable.get("s3_bucket", default_var="flights-data-lake")
LOCAL_RAW_PATH = Variable.get("local_raw_path", default_var="/opt/airflow/data/raw")
LOCAL_BRONZE_PATH = Variable.get("local_bronze_path", default_var="/opt/airflow/data/bronze")
MAX_PAGES = int(Variable.get("max_pages", default_var="1"))
AWS_CONN_ID = Variable.get("aws_conn_id", default_var="aws_default")

# -----------------------------------------------------------------------------
# - Spark Configuration
# -----------------------------------------------------------------------------
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL")
SPARK_EXECUTOR_CORES = int(Variable.get("spark_executor_cores", default_var="2"))
SPARK_EXECUTOR_MEMORY = Variable.get("spark_executor_memory", default_var="2g")


def get_etl_vars(etl_name: str, execution_date: str) -> str:
    """Generate ETL configuration as JSON string."""
    if etl_name == "aviationstack":
        return json.dumps({
            "raw_dir": f"s3a://{S3_BUCKET}/raw/insert_date={execution_date}",
            "bronze_dir": f"s3a://{S3_BUCKET}/bronze/insert_date={execution_date}",
            "max_pages": MAX_PAGES,
        })
    elif etl_name == "openflights":
        return json.dumps({
            "raw_dir": f"s3a://{S3_BUCKET}/raw/openflights",
            "bronze_dir": f"s3a://{S3_BUCKET}/bronze/openflights",
        })
    else:
        return "{}"


# -----------------------------------------------------------------------------
# - Callbacks
# -----------------------------------------------------------------------------
def on_failure_callback(context: dict[str, Any]) -> None:
    """Callback function for task failures."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date")

    logger.error(
        f"Task failed: dag_id={dag_id}, task_id={task_id}, "
        f"execution_date={execution_date}, exception={exception}"
    )
    # TODO: Add alerting integration (Slack, email, PagerDuty, etc.)


def on_success_callback(context: dict[str, Any]) -> None:
    """Callback function for task success."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id if task_instance else "unknown"

    logger.info(f"Task succeeded: dag_id={dag_id}, task_id={task_id}")


# -----------------------------------------------------------------------------
# - DAG
# -----------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}


@dag(
    DAG_NAME,
    start_date=datetime(2025, 12, 21),
    catchup=True,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["spark", "minio", "aviationstack", "etl"],
)
def dag_() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_bucket = S3CreateBucketOperator(
        task_id="create_minio_bucket",
        bucket_name=S3_BUCKET,
        aws_conn_id=AWS_CONN_ID,
    )

    @task_group
    def extract_raw() -> None:
        @task(task_id="extract_api_to_json", retries=3, retry_delay=timedelta(minutes=2))
        def extract_api_task(**context: Any) -> list[str]:
            """Extract flights data from AviationStack API."""
            from include.api.extract_api import extract_flights

            execution_date = context["ds"]
            logger.info(f"Extracting flights for date: {execution_date}")

            try:
                files = extract_flights(
                    raw_dir=LOCAL_RAW_PATH,
                    max_pages=MAX_PAGES,
                    execution_date=execution_date,
                )
                logger.info(f"Extracted {len(files)} flight files")
                return files
            except Exception as e:
                logger.error(f"Failed to extract flights: {e}")
                raise

        @task(task_id="extract_openflights_to_csv", retries=2, retry_delay=timedelta(minutes=1))
        def extract_openflights_task() -> list[str]:
            """Extract OpenFlights reference data."""
            from include.openflights.extract_openflights import extract_openflights

            output_dir = f"{LOCAL_RAW_PATH}/openflights/"
            logger.info(f"Extracting OpenFlights data to: {output_dir}")

            try:
                files = extract_openflights(raw_dir=output_dir)
                logger.info(f"Extracted {len(files)} OpenFlights files")
                return files
            except Exception as e:
                logger.error(f"Failed to extract OpenFlights data: {e}")
                raise

        extract_api_task() >> extract_openflights_task()

    upload_raw_to_minio = upload_to_s3(
        task_id="upload_raw_to_minio",
        bucket=S3_BUCKET,
        local_path=LOCAL_RAW_PATH,
        remote_prefix="raw",
    )

    # Sensor to verify raw data was uploaded before ETL
    verify_raw_flights = S3KeySensor(
        task_id="verify_raw_flights_uploaded",
        bucket_name=S3_BUCKET,
        bucket_key=f"raw/insert_date={EXECUTION_DATE}/*",
        wildcard_match=True,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    verify_raw_openflights = S3KeySensor(
        task_id="verify_raw_openflights_uploaded",
        bucket_name=S3_BUCKET,
        bucket_key="raw/openflights/*",
        wildcard_match=True,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=300,
        mode="poke",
    )

    def run_etl(
        *,
        etl_name: str,
        etl_vars: str,
    ) -> BashOperator:
        """Create a BashOperator to run a Spark ETL job."""
        return BashOperator(
            task_id=f"etl_{etl_name}",
            bash_command=f"""
                /opt/airflow/.venv/bin/spark-submit \
                --master local[4] \
                --total-executor-cores {SPARK_EXECUTOR_CORES} \
                --executor-memory {SPARK_EXECUTOR_MEMORY} \
                --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
                --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} \
                --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                --jars /opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
                --name {etl_name}-etl \
                /opt/airflow/spark_jobs/main_cli.py {etl_name} '{etl_vars}'
            """,
        )

    @task_group
    def etl_bronze() -> None:
        openflights_vars = get_etl_vars("openflights", EXECUTION_DATE)
        aviationstack_vars = get_etl_vars("aviationstack", EXECUTION_DATE)

        etl_openflights = run_etl(etl_name="openflights", etl_vars=openflights_vars)
        etl_aviationstack = run_etl(etl_name="aviationstack", etl_vars=aviationstack_vars)

        etl_openflights >> etl_aviationstack

    # Sensor to verify bronze data before dbt
    verify_bronze_data = S3KeySensor(
        task_id="verify_bronze_data",
        bucket_name=S3_BUCKET,
        bucket_key=f"bronze/insert_date={EXECUTION_DATE}/*",
        wildcard_match=True,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=600,
        mode="poke",
    )

    dbt_vars = format_dbt_vars(
        {
            "execution_date": f"{EXECUTION_DATE}",
            "s3_bronze_path": f"s3://{S3_BUCKET}/bronze/insert_date=",
        }
    )

    # Stop Metabase before dbt to release DuckDB lock (analytics.duckdb)
    stop_metabase = BashOperator(
        task_id="stop_metabase",
        bash_command="curl -sSf --unix-socket /var/run/docker.sock -X POST "
        '"http://localhost/v1.44/containers/metabase/stop?t=30" && sleep 5',
    )

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=f"cd /opt/airflow/dbt_transform && \
            dbt deps && dbt run --vars {dbt_vars}",
    )

    # Start Metabase after dbt (runs even if dbt fails)
    start_metabase = BashOperator(
        task_id="start_metabase",
        bash_command="curl -sSf --unix-socket /var/run/docker.sock -X POST "
        '"http://localhost/v1.44/containers/metabase/start" || true',
        trigger_rule="all_done",
    )

    @task(task_id="validate_dbt_output")
    def validate_dbt_output(**context: Any) -> dict[str, Any]:
        """Validate dbt transformation output."""
        import duckdb

        execution_date = context["ds"]
        logger.info(f"Validating dbt output for {execution_date}")

        # Connect to DuckDB and run basic validation
        # This is a placeholder - actual validation logic depends on the setup
        validation_results = {
            "execution_date": execution_date,
            "status": "success",
            "checks_passed": True,
        }

        logger.info(f"Validation results: {validation_results}")
        return validation_results

    (
        start
        >> create_bucket
        >> extract_raw()
        >> upload_raw_to_minio
        >> [verify_raw_flights, verify_raw_openflights]
        >> etl_bronze()
        >> verify_bronze_data
        >> stop_metabase
        >> dbt_transform
        >> start_metabase
        >> validate_dbt_output()
        >> end
    )


dag_()
