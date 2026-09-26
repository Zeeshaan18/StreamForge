"""
Airflow DAG: Hourly Aggregate Rollup & Analytical Mart Builder.
Aggregates transactional fact_orders into high-performance hourly summary tables.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "datapulse_data_eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def run_hourly_rollup(**kwargs):
    from airflow.plugins.custom_operators import execute_hourly_aggregate_rollup
    orders_processed = execute_hourly_aggregate_rollup()
    return {"orders_aggregated": orders_processed}


with DAG(
    dag_id="datapulse_hourly_aggregate_rollup_dag",
    default_args=default_args,
    description="Builds Hourly Aggregate Rollup Mart from Fact Orders",
    schedule_interval="5 * * * *",  # 5 minutes after every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datapulse", "aggregates", "mart"],
) as dag:

    task_rollup = PythonOperator(
        task_id="compute_hourly_order_aggregates",
        python_callable=run_hourly_rollup,
    )
