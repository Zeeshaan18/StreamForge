"""
Airflow DAG: Pipeline Health & Heartbeat Monitor.
Checks database connection health, lag metrics, and table storage freshness.
"""
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "datapulse_data_eng",
    "depends_on_past": False,
    "retries": 1,
}


def check_warehouse_heartbeat(**kwargs):
    from database.db_connection import db_manager
    from sqlalchemy import text
    import json

    health = db_manager.check_health()
    table_stats = db_manager.execute_query("""
        SELECT 
            (SELECT COUNT(*) FROM fact_orders) as fact_orders_count,
            (SELECT COUNT(*) FROM fact_order_items) as items_count,
            (SELECT COUNT(*) FROM quarantine_orders) as quarantine_count
    """)

    with db_manager.engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO pipeline_health_metrics (
                timestamp, component, metric_name, metric_value, metric_unit, status_indicator, details
            ) VALUES (
                :ts, 'AIRFLOW', 'WAREHOUSE_HEARTBEAT', :lat, 'ms', :stat, :det
            )
            """),
            {
                "ts": datetime.now(timezone.utc),
                "lat": health.get("latency_ms", 0.0) or 0.0,
                "stat": health.get("status", "HEALTHY"),
                "det": json.dumps(table_stats[0] if table_stats else {})
            }
        )
    return health


with DAG(
    dag_id="datapulse_pipeline_health_dag",
    default_args=default_args,
    description="Pipeline Health and Freshness Heartbeat Monitor",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datapulse", "monitoring", "heartbeat"],
) as dag:

    task_heartbeat = PythonOperator(
        task_id="check_warehouse_heartbeat",
        python_callable=check_warehouse_heartbeat,
    )
