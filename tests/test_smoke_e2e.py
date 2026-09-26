"""
End-to-End Smoke Test for DataPulse Live Platform.
Verifies complete pipeline lifecycle:
Generator -> Streaming Ingestion -> PySpark Transformations -> PostgreSQL Warehouse -> Airflow DQ -> Streamlit Queries.
"""
import pytest
from generator.order_generator import ContinuousOrderGenerator
from spark.postgres_writer import postgres_stream_writer
from airflow.plugins.custom_operators import DataQualityAuditRunner, execute_hourly_aggregate_rollup
from dashboard.db_queries import (
    get_executive_kpis,
    get_recent_orders_feed,
    get_revenue_by_category,
    get_payment_method_distribution,
    get_quarantine_metrics,
    get_pipeline_health_status
)
from database.db_connection import db_manager


def test_end_to_end_pipeline_flow():
    """Executes full end-to-end data pipeline lifecycle and verifies data contracts."""
    print("\n--- [Step 1] Initializing Synthetic Event Generator ---")
    gen = ContinuousOrderGenerator(error_rate=0.10, duplicate_rate=0.05)
    events = [gen.generate_next_event() for _ in range(30)]
    assert len(events) == 30

    print("--- [Step 2] Processing & Ingesting Stream Batch ---")
    stats = postgres_stream_writer.write_micro_batch(events)
    assert stats["processed"] == 30
    assert stats["valid"] > 0
    print(f"Batch Ingested: Valid={stats['valid']} | Quarantined={stats['quarantined']} | Duplicates={stats['duplicates']}")

    print("--- [Step 3] Verifying Relational Persistence in Warehouse ---")
    kpis = get_executive_kpis()
    assert kpis["total_orders"] >= stats["valid"]
    assert kpis["total_revenue"] > 0
    assert kpis["avg_order_value"] > 0

    print("--- [Step 4] Running Airflow Data Quality Audit Suite ---")
    dq_runner = DataQualityAuditRunner()
    null_result = dq_runner.run_null_checks()
    ref_result = dq_runner.run_referential_integrity_checks()
    fin_result = dq_runner.run_financial_reconciliation_checks()

    assert null_result["status"] == "PASSED"
    assert ref_result["status"] == "PASSED"
    assert fin_result["status"] == "PASSED"

    print("--- [Step 5] Executing Airflow Aggregate Rollup Mart ---")
    aggregated_count = execute_hourly_aggregate_rollup()
    assert aggregated_count >= stats["valid"]

    print("--- [Step 6] Verifying Dashboard Analytical Queries ---")
    recent_feed = get_recent_orders_feed(limit=10)
    assert not recent_feed.empty
    assert "order_id" in recent_feed.columns
    assert "net_amount" in recent_feed.columns

    categories = get_revenue_by_category()
    assert not categories.empty
    assert "category" in categories.columns

    payments = get_payment_method_distribution()
    assert not payments.empty

    summary_df, detail_df, q_stats = get_quarantine_metrics()
    assert q_stats["pass_rate"] > 0

    health = get_pipeline_health_status()
    assert health["database"]["status"] == "HEALTHY"

    print("\n[SUCCESS] DataPulse Live End-to-End Pipeline Smoke Test Passed Perfectly!")


if __name__ == "__main__":
    test_end_to_end_pipeline_flow()
