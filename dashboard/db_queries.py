"""
Data Warehouse Query Layer for Streamlit Dashboard.
Executes analytical SQL queries against PostgreSQL / warehouse tables.
"""
import pandas as pd
from typing import Dict, Any, List, Tuple
from sqlalchemy import text
from database.db_connection import db_manager


def get_executive_kpis() -> Dict[str, Any]:
    """Retrieves high-level summary KPIs for Executive Overview."""
    query = """
    SELECT 
        COUNT(order_id) as total_orders,
        COALESCE(SUM(net_amount), 0.0) as total_revenue,
        COALESCE(AVG(net_amount), 0.0) as avg_order_value,
        COALESCE(SUM(item_count), 0) as total_items_sold,
        COALESCE(SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END), 0) as delivered_orders,
        COALESCE(SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END), 0) as cancelled_orders
    FROM fact_orders
    """
    rows = db_manager.execute_query(query)
    q_rows = db_manager.execute_query("SELECT COUNT(*) as q_count FROM quarantine_orders")
    
    res = rows[0] if rows else {}
    q_count = q_rows[0]["q_count"] if q_rows else 0
    total_orders = res.get("total_orders", 0) or 0
    total_rev = float(res.get("total_revenue", 0.0) or 0.0)
    aov = float(res.get("avg_order_value", 0.0) or 0.0)
    items = int(res.get("total_items_sold", 0) or 0)
    delivered = int(res.get("delivered_orders", 0) or 0)
    cancelled = int(res.get("cancelled_orders", 0) or 0)

    delivery_rate = round((delivered / total_orders * 100), 1) if total_orders > 0 else 0.0

    return {
        "total_orders": total_orders,
        "total_revenue": total_rev,
        "avg_order_value": aov,
        "total_items_sold": items,
        "delivered_orders": delivered,
        "cancelled_orders": cancelled,
        "quarantined_count": q_count,
        "delivery_rate": delivery_rate
    }


def get_live_ingest_rate() -> float:
    """Calculates actual live ingestion rate (events per second) over recent time window."""
    try:
        # Check recent fact order count in the last 30 seconds
        if getattr(db_manager, "_is_sqlite_fallback", False):
            query = "SELECT COUNT(*) as cnt FROM fact_orders WHERE processed_at >= datetime('now', '-30 seconds')"
        else:
            query = "SELECT COUNT(*) as cnt FROM fact_orders WHERE processed_at >= NOW() - INTERVAL '30 seconds'"
        rows = db_manager.execute_query(query)
        count = rows[0]["cnt"] if rows else 0
        if count > 0:
            return round(count / 30.0, 1)

        # Check latest pipeline health metric batch
        latest = db_manager.execute_query("""
            SELECT metric_value 
            FROM pipeline_health_metrics 
            WHERE metric_name = 'BATCH_RECORDS_PROCESSED' 
            ORDER BY timestamp DESC 
            LIMIT 1
        """)
        if latest and float(latest[0]["metric_value"]) > 0:
            return round(float(latest[0]["metric_value"]) / 3.0, 1)
    except Exception:
        pass
    return 0.0


def get_recent_orders_feed(limit: int = 50) -> pd.DataFrame:
    """Retrieves most recent stream-ingested order events."""
    query = f"""
    SELECT 
        f.order_id,
        f.event_id,
        c.customer_name,
        c.segment as customer_segment,
        l.city,
        l.country,
        p.method_type as payment_method,
        f.order_status,
        f.item_count,
        f.net_amount,
        f.ingest_latency_ms,
        f.order_timestamp,
        f.processed_at
    FROM fact_orders f
    LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
    LEFT JOIN dim_locations l ON f.location_id = l.location_id
    LEFT JOIN dim_payment_methods p ON f.payment_method_id = p.payment_method_id
    ORDER BY f.order_timestamp DESC
    LIMIT {limit}
    """
    rows = db_manager.execute_query(query)
    return pd.DataFrame(rows)


def get_revenue_by_category() -> pd.DataFrame:
    """Calculates gross revenue and item volume by product category."""
    query = """
    SELECT 
        p.category,
        COUNT(DISTINCT i.order_id) as order_count,
        SUM(i.quantity) as total_units_sold,
        SUM(i.total_item_price) as category_revenue
    FROM fact_order_items i
    JOIN dim_products p ON i.product_id = p.product_id
    GROUP BY p.category
    ORDER BY category_revenue DESC
    """
    rows = db_manager.execute_query(query)
    return pd.DataFrame(rows)


def get_payment_method_distribution() -> pd.DataFrame:
    """Calculates order volume by payment method."""
    query = """
    SELECT 
        p.method_type,
        p.provider,
        COUNT(f.order_id) as order_count,
        SUM(f.net_amount) as total_volume
    FROM fact_orders f
    JOIN dim_payment_methods p ON f.payment_method_id = p.payment_method_id
    GROUP BY p.method_type, p.provider
    ORDER BY order_count DESC
    """
    rows = db_manager.execute_query(query)
    return pd.DataFrame(rows)


def get_geographic_distribution() -> pd.DataFrame:
    """Retrieves order counts and revenue mapped by geographic location."""
    query = """
    SELECT 
        l.city,
        l.country,
        l.region,
        l.latitude,
        l.longitude,
        COUNT(f.order_id) as order_count,
        SUM(f.net_amount) as total_revenue
    FROM fact_orders f
    JOIN dim_locations l ON f.location_id = l.location_id
    GROUP BY l.city, l.country, l.region, l.latitude, l.longitude
    ORDER BY order_count DESC
    """
    rows = db_manager.execute_query(query)
    return pd.DataFrame(rows)


def get_quarantine_metrics() -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Retrieves quarantine error code breakdown and latest quarantined payloads."""
    summary_query = """
    SELECT 
        error_code,
        rejection_reason,
        COUNT(*) as failure_count
    FROM quarantine_orders
    GROUP BY error_code, rejection_reason
    ORDER BY failure_count DESC
    """
    summary_rows = db_manager.execute_query(summary_query)

    detail_query = """
    SELECT 
        quarantine_id,
        event_id,
        order_id,
        error_code,
        rejection_reason,
        quarantined_at,
        raw_payload
    FROM quarantine_orders
    ORDER BY quarantined_at DESC
    LIMIT 30
    """
    detail_rows = db_manager.execute_query(detail_query)

    total_valid = db_manager.execute_query("SELECT COUNT(*) as c FROM fact_orders")[0]["c"]
    total_quar = len(detail_rows) + (sum(r["failure_count"] for r in summary_rows) - len(detail_rows) if summary_rows else 0)
    total_total = total_valid + total_quar
    pass_rate = round((total_valid / total_total * 100), 2) if total_total > 0 else 100.0

    return pd.DataFrame(summary_rows), pd.DataFrame(detail_rows), {
        "pass_rate": pass_rate,
        "valid_count": total_valid,
        "quarantined_count": total_quar
    }


def get_dq_audit_history() -> pd.DataFrame:
    """Retrieves data quality audit logs generated by Airflow audits."""
    query = """
    SELECT 
        check_name,
        check_type,
        table_audited,
        records_evaluated,
        records_passed,
        records_failed,
        pass_rate,
        status,
        audit_timestamp
    FROM data_quality_audit_log
    ORDER BY audit_timestamp DESC
    LIMIT 20
    """
    rows = db_manager.execute_query(query)
    return pd.DataFrame(rows)


def get_pipeline_health_status() -> Dict[str, Any]:
    """Inspects component health and performance metrics."""
    db_health = db_manager.check_health()
    
    # Table counts
    tables = db_manager.execute_query("""
        SELECT 
            (SELECT COUNT(*) FROM fact_orders) as fact_orders,
            (SELECT COUNT(*) FROM fact_order_items) as fact_order_items,
            (SELECT COUNT(*) FROM dim_customers) as dim_customers,
            (SELECT COUNT(*) FROM dim_products) as dim_products,
            (SELECT COUNT(*) FROM quarantine_orders) as quarantine_orders,
            (SELECT AVG(ingest_latency_ms) FROM fact_orders) as avg_latency_ms
    """)
    table_stats = tables[0] if tables else {}

    # Recent micro-batch latency
    recent_metrics = db_manager.execute_query("""
        SELECT timestamp, metric_value, details 
        FROM pipeline_health_metrics
        WHERE metric_name = 'BATCH_RECORDS_PROCESSED'
        ORDER BY timestamp DESC
        LIMIT 15
    """)

    return {
        "database": db_health,
        "table_stats": table_stats,
        "recent_metrics": recent_metrics
    }


def get_order_status_by_id(order_id: str) -> Dict[str, Any]:
    """
    Looks up an order across fact_orders and quarantine_orders to verify real-time Spark persistence.
    """
    if not order_id:
        return {"found": False, "status": "UNKNOWN", "destination": None, "data": None}

    # 1. Check fact_orders
    query_fact = """
    SELECT 
        f.order_id,
        f.event_id,
        f.order_status,
        f.net_amount,
        f.item_count,
        f.ingest_latency_ms,
        f.order_timestamp,
        f.processed_at,
        c.customer_name,
        l.city,
        l.country,
        p.method_type as payment_method
    FROM fact_orders f
    LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
    LEFT JOIN dim_locations l ON f.location_id = l.location_id
    LEFT JOIN dim_payment_methods p ON f.payment_method_id = p.payment_method_id
    WHERE f.order_id = :order_id
    LIMIT 1
    """
    fact_rows = db_manager.execute_query(query_fact, {"order_id": order_id})
    if fact_rows:
        return {
            "found": True,
            "status": "PERSISTED",
            "destination": "fact_orders",
            "data": fact_rows[0]
        }

    # 2. Check quarantine_orders
    query_quarantine = """
    SELECT 
        quarantine_id,
        event_id,
        order_id,
        error_code,
        rejection_reason,
        failed_validation_rule,
        quarantined_at,
        raw_payload
    FROM quarantine_orders
    WHERE order_id = :order_id
    LIMIT 1
    """
    quar_rows = db_manager.execute_query(query_quarantine, {"order_id": order_id})
    if quar_rows:
        return {
            "found": True,
            "status": "QUARANTINED",
            "destination": "quarantine_orders",
            "data": quar_rows[0]
        }

    return {
        "found": False,
        "status": "PENDING_INGESTION",
        "destination": None,
        "data": None
    }

