"""
Custom Data Quality and Pipeline Operators for DataPulse Live.
Can be executed within Apache Airflow DAGs or standalone via CLI.
"""
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
from sqlalchemy import text
from database.db_connection import db_manager
from config.logging_config import setup_logger

logger = setup_logger("airflow_operators")


class DataQualityAuditRunner:
    """Executes automated data quality audit suites against the warehouse."""

    def __init__(self, db=None):
        self.db = db or db_manager

    def run_null_checks(self) -> Dict[str, Any]:
        """Verifies no mandatory columns contain NULLs in fact_orders."""
        query = """
        SELECT 
            COUNT(*) as total_rows,
            SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
            SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
            SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) as null_totals,
            SUM(CASE WHEN order_timestamp IS NULL THEN 1 ELSE 0 END) as null_timestamps
        FROM fact_orders
        """
        rows = self.db.execute_query(query)
        if not rows or rows[0]["total_rows"] == 0:
            return {"status": "PASSED", "passed": 0, "failed": 0, "pass_rate": 100.0}

        row = rows[0]
        total = row["total_rows"]
        failed = (
            (row["null_order_ids"] or 0) +
            (row["null_customer_ids"] or 0) +
            (row["null_totals"] or 0) +
            (row["null_timestamps"] or 0)
        )
        passed = max(0, total - failed)
        pass_rate = round((passed / total) * 100, 2) if total > 0 else 100.0
        status = "PASSED" if failed == 0 else "FAILED"

        self._record_audit(
            check_name="Null Value Constraint Check",
            check_type="NULL_CHECK",
            table="fact_orders",
            evaluated=total,
            passed=passed,
            failed=failed,
            pass_rate=pass_rate,
            status=status,
            details=row
        )
        return {"status": status, "total": total, "passed": passed, "failed": failed, "pass_rate": pass_rate}

    def run_referential_integrity_checks(self) -> Dict[str, Any]:
        """Checks for orphan foreign keys between fact_orders and dimension tables."""
        query = """
        SELECT 
            COUNT(*) as total_orders,
            SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) as orphan_customers,
            SUM(CASE WHEN l.location_id IS NULL THEN 1 ELSE 0 END) as orphan_locations,
            SUM(CASE WHEN p.payment_method_id IS NULL THEN 1 ELSE 0 END) as orphan_payments
        FROM fact_orders f
        LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
        LEFT JOIN dim_locations l ON f.location_id = l.location_id
        LEFT JOIN dim_payment_methods p ON f.payment_method_id = p.payment_method_id
        """
        rows = self.db.execute_query(query)
        if not rows or rows[0]["total_orders"] == 0:
            return {"status": "PASSED", "passed": 0, "failed": 0, "pass_rate": 100.0}

        row = rows[0]
        total = row["total_orders"]
        orphans = (
            (row["orphan_customers"] or 0) +
            (row["orphan_locations"] or 0) +
            (row["orphan_payments"] or 0)
        )
        passed = max(0, total - orphans)
        pass_rate = round((passed / total) * 100, 2) if total > 0 else 100.0
        status = "PASSED" if orphans == 0 else "FAILED"

        self._record_audit(
            check_name="Referential Integrity & Foreign Key Check",
            check_type="REFERENTIAL_INTEGRITY",
            table="fact_orders",
            evaluated=total,
            passed=passed,
            failed=orphans,
            pass_rate=pass_rate,
            status=status,
            details=row
        )
        return {"status": status, "total": total, "passed": passed, "failed": orphans, "pass_rate": pass_rate}

    def run_financial_reconciliation_checks(self) -> Dict[str, Any]:
        """Verifies mathematical consistency of net amounts vs gross, discount, and tax."""
        query = """
        SELECT 
            COUNT(*) as total_orders,
            SUM(CASE 
                WHEN ABS(net_amount - (total_amount - discount_amount + tax_amount)) > 0.05 
                THEN 1 ELSE 0 
            END) as reconciliation_discrepancies
        FROM fact_orders
        """
        rows = self.db.execute_query(query)
        if not rows or rows[0]["total_orders"] == 0:
            return {"status": "PASSED", "passed": 0, "failed": 0, "pass_rate": 100.0}

        row = rows[0]
        total = row["total_orders"]
        discrepancies = row["reconciliation_discrepancies"] or 0
        passed = total - discrepancies
        pass_rate = round((passed / total) * 100, 2) if total > 0 else 100.0
        status = "PASSED" if discrepancies == 0 else "FAILED"

        self._record_audit(
            check_name="Financial Arithmetic Reconciliation",
            check_type="FINANCIAL_RECONCILIATION",
            table="fact_orders",
            evaluated=total,
            passed=passed,
            failed=discrepancies,
            pass_rate=pass_rate,
            status=status,
            details=row
        )
        return {"status": status, "total": total, "passed": passed, "failed": discrepancies, "pass_rate": pass_rate}

    def _record_audit(self, check_name: str, check_type: str, table: str, evaluated: int, passed: int, failed: int, pass_rate: float, status: str, details: dict):
        """Persists audit record to data_quality_audit_log table."""
        with self.db.engine.begin() as conn:
            conn.execute(
                text("""
                INSERT INTO data_quality_audit_log (
                    check_name, check_type, table_audited, records_evaluated,
                    records_passed, records_failed, pass_rate, status, audit_timestamp, details
                ) VALUES (
                    :name, :type, :tbl, :eval, :passed, :failed, :rate, :status, :ts, :det
                )
                """),
                {
                    "name": check_name, "type": check_type, "tbl": table,
                    "eval": evaluated, "passed": passed, "failed": failed,
                    "rate": pass_rate, "status": status,
                    "ts": datetime.now(timezone.utc),
                    "det": json.dumps(details)
                }
            )


def execute_hourly_aggregate_rollup() -> int:
    """Computes hourly aggregate summary rollups."""
    logger.info("Executing hourly order aggregate rollup...")
    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(hours=1)

    query = """
    SELECT 
        COUNT(order_id) as total_orders,
        COALESCE(SUM(net_amount), 0.0) as total_revenue,
        COALESCE(AVG(net_amount), 0.0) as avg_order_value,
        COALESCE(SUM(item_count), 0) as total_items_sold,
        COALESCE(SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END), 0) as successful_orders,
        COALESCE(SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END), 0) as cancelled_orders
    FROM fact_orders
    """
    rows = db_manager.execute_query(query)
    if not rows:
        return 0

    stats = rows[0]
    # Count quarantined orders
    q_rows = db_manager.execute_query("SELECT COUNT(*) as q_count FROM quarantine_orders")
    q_count = q_rows[0]["q_count"] if q_rows else 0

    with db_manager.engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO hourly_order_aggregates (
                hourly_window_start, hourly_window_end, total_orders, total_revenue,
                avg_order_value, total_items_sold, successful_orders, cancelled_orders,
                quarantined_orders, updated_at
            ) VALUES (
                :w_start, :w_end, :orders, :rev, :aov, :items, :succ, :canc, :quar, :updated
            )
            ON CONFLICT (hourly_window_start) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_revenue = EXCLUDED.total_revenue,
                avg_order_value = EXCLUDED.avg_order_value,
                total_items_sold = EXCLUDED.total_items_sold,
                successful_orders = EXCLUDED.successful_orders,
                cancelled_orders = EXCLUDED.cancelled_orders,
                quarantined_orders = EXCLUDED.quarantined_orders,
                updated_at = EXCLUDED.updated_at
            """),
            {
                "w_start": str(window_start),
                "w_end": str(window_end),
                "orders": stats["total_orders"],
                "rev": round(float(stats["total_revenue"]), 2),
                "aov": round(float(stats["avg_order_value"]), 2),
                "items": stats["total_items_sold"],
                "succ": stats["successful_orders"],
                "canc": stats["cancelled_orders"],
                "quar": q_count,
                "updated": now
            }
        )
    logger.info(f"Aggregated {stats['total_orders']} orders. Revenue: ${stats['total_revenue']:.2f}")
    return stats["total_orders"]
