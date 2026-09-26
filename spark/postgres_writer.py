"""
Idempotent Database Writer for Spark Structured Streaming batches.
Guarantees exactly-once relational persistence, deduplication, and quarantine routing.
"""
import json
from datetime import datetime, timezone
from typing import List, Dict, Any

from sqlalchemy import text

from database.db_connection import db_manager
from spark.validation import EventValidator
from spark.transformations import transform_and_enrich_order
from config.logging_config import setup_logger

logger = setup_logger("db_writer")


class PostgresStreamWriter:
    """Handles transactional, idempotent micro-batch writes to PostgreSQL."""

    def __init__(self, db=None):
        self.db = db or db_manager

    def write_micro_batch(self, raw_events: List[Dict[str, Any]]) -> Dict[str, Any]:

        if not raw_events:
            return {
                "processed": 0,
                "valid": 0,
                "quarantined": 0,
                "duplicates": 0
            }

        valid_orders = []
        valid_items = []
        quarantine_records = []
        duplicate_count = 0

        # 1. Validate and transform events
        for event in raw_events:
            is_valid, error_code, rejection_reason = (
                EventValidator.validate_order_event(event)
            )

            if not is_valid:
                quarantine_records.append({
                    "event_id": event.get("event_id"),
                    "order_id": event.get("order_id"),
                    "raw_payload": json.dumps(event),
                    "error_code": error_code,
                    "rejection_reason": rejection_reason,
                    "failed_validation_rule": f"Rule_{error_code}",
                    "quarantined_at": datetime.now(timezone.utc)
                })
            else:
                order_row, item_rows = transform_and_enrich_order(event)
                valid_orders.append(order_row)
                valid_items.extend(item_rows)

        inserted_orders = 0
        inserted_quarantine = 0

        # 2. One transaction for the entire batch
        with self.db.engine.begin() as conn:

            for order in valid_orders:

                # ---------------------------------------------------------
                # A. ENSURE CUSTOMER EXISTS
                # ---------------------------------------------------------
                conn.execute(
                    text("""
                        INSERT INTO dim_customers (
                            customer_id,
                            customer_name,
                            email,
                            segment,
                            city,
                            country
                        )
                        VALUES (
                            :customer_id,
                            :customer_name,
                            :email,
                            'Standard',
                            NULL,
                            NULL
                        )
                        ON CONFLICT (customer_id) DO NOTHING
                    """),
                    {
                        "customer_id": order["customer_id"],
                        "customer_name": f"Customer {order['customer_id']}",
                        "email": f"{order['customer_id'].lower()}@streamforge.local"
                    }
                )

                # ---------------------------------------------------------
                # B. ENSURE LOCATION EXISTS
                # ---------------------------------------------------------
                conn.execute(
                    text("""
                        INSERT INTO dim_locations (
                            location_id,
                            city,
                            state,
                            country
                        )
                        VALUES (
                            :location_id,
                            :city,
                            NULL,
                            :country
                        )
                        ON CONFLICT (location_id) DO NOTHING
                    """),
                    {
                        "location_id": order["location_id"],
                        "city": "Unknown",
                        "country": "Unknown"
                    }
                )

                # ---------------------------------------------------------
                # C. ENSURE PAYMENT METHOD EXISTS
                # ---------------------------------------------------------
                payment_method = order["payment_method_id"]

                method_type = payment_method

                if payment_method.startswith("PAY-"):
                    method_type = payment_method[4:]

                conn.execute(
                    text("""
                        INSERT INTO dim_payment_methods (
                            payment_method_id,
                            method_type,
                            provider
                        )
                        VALUES (
                            :payment_method_id,
                            :method_type,
                            :provider
                        )
                        ON CONFLICT (payment_method_id) DO NOTHING
                    """),
                    {
                        "payment_method_id": payment_method,
                        "method_type": method_type,
                        "provider": method_type
                    }
                )

                # ---------------------------------------------------------
                # D. CHECK DUPLICATE ORDER
                # ---------------------------------------------------------
                existing = conn.execute(
                    text("""
                        SELECT order_id
                        FROM fact_orders
                        WHERE order_id = :order_id
                    """),
                    {
                        "order_id": order["order_id"]
                    }
                ).fetchone()

                if existing:
                    duplicate_count += 1
                    conn.execute(
                        text("""
                            DELETE FROM fact_order_items
                            WHERE order_id = :order_id
                        """),
                        {"order_id": order["order_id"]}
                    )

                # ---------------------------------------------------------
                # E. INSERT / UPDATE FACT ORDER
                # ---------------------------------------------------------
                conn.execute(
                    text("""
                        INSERT INTO fact_orders (
                            order_id,
                            event_id,
                            customer_id,
                            location_id,
                            payment_method_id,
                            order_status,
                            total_amount,
                            discount_amount,
                            tax_amount,
                            net_amount,
                            item_count,
                            order_timestamp,
                            processed_at,
                            ingest_latency_ms
                        )
                        VALUES (
                            :order_id,
                            :event_id,
                            :customer_id,
                            :location_id,
                            :payment_method_id,
                            :order_status,
                            :total_amount,
                            :discount_amount,
                            :tax_amount,
                            :net_amount,
                            :item_count,
                            :order_timestamp,
                            :processed_at,
                            :ingest_latency_ms
                        )
                        ON CONFLICT (order_id) DO UPDATE SET
                            order_status = EXCLUDED.order_status,
                            processed_at = EXCLUDED.processed_at,
                            ingest_latency_ms = EXCLUDED.ingest_latency_ms
                    """),
                    order
                )

                inserted_orders += 1

            # -------------------------------------------------------------
            # F. INSERT ORDER ITEMS
            # -------------------------------------------------------------
            for item in valid_items:
                conn.execute(
                    text("""
                        INSERT INTO dim_products (
                            product_id,
                            product_name,
                            category,
                            subcategory,
                            base_price,
                            cost_price,
                            supplier_id,
                            inventory_count
                        )
                        VALUES (
                            :product_id,
                            :product_name,
                            'General',
                            'General',
                            :unit_price,
                            :cost_price,
                            'SUPP-AUTO',
                            1000
                        )
                        ON CONFLICT (product_id) DO NOTHING
                    """),
                    {
                        "product_id": item["product_id"],
                        "product_name": f"Product {item['product_id']}",
                        "unit_price": item["unit_price"],
                        "cost_price": round(item["unit_price"] * 0.7, 2)
                    }
                )

                conn.execute(
                    text("""
                        INSERT INTO fact_order_items (
                            order_id,
                            product_id,
                            quantity,
                            unit_price,
                            total_item_price,
                            discount_applied,
                            created_at
                        )
                        VALUES (
                            :order_id,
                            :product_id,
                            :quantity,
                            :unit_price,
                            :total_item_price,
                            :discount_applied,
                            :created_at
                        )
                    """),
                    item
                )

            # -------------------------------------------------------------
            # G. QUARANTINE INVALID EVENTS
            # -------------------------------------------------------------
            for q_rec in quarantine_records:

                conn.execute(
                    text("""
                        INSERT INTO quarantine_orders (
                            event_id,
                            order_id,
                            raw_payload,
                            error_code,
                            rejection_reason,
                            failed_validation_rule,
                            quarantined_at
                        )
                        VALUES (
                            :event_id,
                            :order_id,
                            :raw_payload,
                            :error_code,
                            :rejection_reason,
                            :failed_validation_rule,
                            :quarantined_at
                        )
                    """),
                    q_rec
                )

                inserted_quarantine += 1

            # -------------------------------------------------------------
            # H. PIPELINE HEALTH METRIC
            # -------------------------------------------------------------
            avg_latency = (
                sum(
                    o["ingest_latency_ms"]
                    for o in valid_orders
                ) / len(valid_orders)
                if valid_orders
                else 0.0
            )

            conn.execute(
                text("""
                    INSERT INTO pipeline_health_metrics (
                        timestamp,
                        component,
                        metric_name,
                        metric_value,
                        metric_unit,
                        status_indicator,
                        details
                    )
                    VALUES (
                        :ts,
                        'SPARK',
                        'BATCH_RECORDS_PROCESSED',
                        :count,
                        'records',
                        'HEALTHY',
                        :details
                    )
                """),
                {
                    "ts": datetime.now(timezone.utc),
                    "count": len(raw_events),
                    "details": json.dumps({
                        "valid": inserted_orders,
                        "quarantined": inserted_quarantine,
                        "duplicates_detected": duplicate_count,
                        "avg_latency_ms": round(avg_latency, 2)
                    })
                }
            )

        logger.info(
            f"Batch Committed: Total={len(raw_events)} | "
            f"Valid={len(valid_orders)} | "
            f"Quarantined={len(quarantine_records)} | "
            f"Duplicates={duplicate_count}"
        )

        return {
            "processed": len(raw_events),
            "valid": len(valid_orders),
            "quarantined": len(quarantine_records),
            "duplicates": duplicate_count
        }


postgres_stream_writer = PostgresStreamWriter()