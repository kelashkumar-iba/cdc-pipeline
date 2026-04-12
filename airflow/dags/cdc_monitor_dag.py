from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import json


def check_cdc_lag(**context):
    """Check consumer group lag from Redpanda."""
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk", "group", "describe", "cdc-consumer-group", "--format", "json"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"Failed to check lag: {result.stderr}")
    
    data = json.loads(result.stdout)
    total_lag = sum(m.get("lag", 0) for m in data.get("members", []) for m in m.get("partitions", []))
    print(f"Total CDC lag: {total_lag}")
    
    if total_lag > 100:
        raise Exception(f"CDC lag is too high: {total_lag} messages behind!")
    
    print("CDC lag is healthy.")


def run_dbt_snapshot(**context):
    """Run dbt snapshot for SCD Type 2."""
    result = subprocess.run(
        ["dbt", "snapshot", "--project-dir", "/opt/airflow/dbt_cdc", "--profiles-dir", "/opt/airflow/dbt_profiles"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt snapshot failed: {result.stderr}")


def run_dbt_test(**context):
    """Run dbt tests."""
    result = subprocess.run(
        ["dbt", "test", "--project-dir", "/opt/airflow/dbt_cdc", "--profiles-dir", "/opt/airflow/dbt_profiles"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test failed: {result.stderr}")


default_args = {
    "owner": "kelash",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cdc_pipeline_monitor",
    default_args=default_args,
    description="Monitor CDC pipeline lag and run dbt transformations",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["cdc", "monitoring", "dbt"],
) as dag:

    check_lag = PythonOperator(
        task_id="check_cdc_lag",
        python_callable=check_cdc_lag,
    )

    dbt_snapshot = PythonOperator(
        task_id="run_dbt_snapshot",
        python_callable=run_dbt_snapshot,
    )

    dbt_test = PythonOperator(
        task_id="run_dbt_test",
        python_callable=run_dbt_test,
    )

    check_lag >> dbt_snapshot >> dbt_test
