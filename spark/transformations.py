"""
PySpark schema definitions, DataFrame transformations, and business metric calculations.
"""
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple


def get_spark_order_schema():
    """
    Returns explicit PySpark StructType schema matching JSON event structure.
    Imported lazily to avoid JVM dependency during lightweight tests.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        IntegerType, ArrayType, MapType, LongType
    )

    item_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("total_item_price", DoubleType(), False),
        StructField("discount_applied", DoubleType(), True)
    ])

    order_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("location_id", StringType(), False),
        StructField("payment_method_id", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("items", ArrayType(item_schema), False),
        StructField("item_count", IntegerType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("discount_amount", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("net_amount", DoubleType(), False),
        StructField("order_timestamp", StringType(), False),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])
    return order_schema


def transform_and_enrich_order(event: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Transforms a validated raw order event into relational fact structures:
    1. fact_orders row dictionary with computed latency
    2. list of fact_order_items row dictionaries
    """
    # Parse timestamp & calculate ingestion latency in milliseconds
    order_ts_str = event.get("order_timestamp", datetime.now(timezone.utc).isoformat())
    try:
        order_dt = datetime.fromisoformat(order_ts_str.replace("Z", "+00:00"))
    except Exception:
        order_dt = datetime.now(timezone.utc)

    now_dt = datetime.now(timezone.utc)
    latency_ms = max(0, int((now_dt - order_dt).total_seconds() * 1000))

    fact_order = {
        "order_id": event["order_id"],
        "event_id": event["event_id"],
        "customer_id": event["customer_id"],
        "location_id": event["location_id"],
        "payment_method_id": event["payment_method_id"],
        "order_status": event.get("order_status", "PENDING").upper(),
        "total_amount": float(event["total_amount"]),
        "discount_amount": float(event.get("discount_amount", 0.0)),
        "tax_amount": float(event.get("tax_amount", 0.0)),
        "net_amount": float(event["net_amount"]),
        "item_count": int(event["item_count"]),
        "order_timestamp": order_dt,
        "processed_at": now_dt,
        "ingest_latency_ms": latency_ms
    }

    fact_items = []
    for item in event.get("items", []):
        fact_items.append({
            "order_id": event["order_id"],
            "product_id": item["product_id"],
            "quantity": int(item["quantity"]),
            "unit_price": float(item["unit_price"]),
            "total_item_price": float(item["total_item_price"]),
            "discount_applied": float(item.get("discount_applied", 0.0)),
            "created_at": now_dt
        })

    return fact_order, fact_items
