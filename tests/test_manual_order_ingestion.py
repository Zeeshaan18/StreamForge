"""
Unit & Integration Tests for Manual Real-Time Order Ingestion Flow.
Verifies end-to-end contract:
User Input Event -> Kafka Producer -> PySpark Ingestion -> PostgreSQL Warehouse -> Order Status Queries.
"""
import uuid
from datetime import datetime, timezone
import pytest
from generator.schema import OrderEventSchema, OrderItemSchema
from generator.kafka_producer import publish_order_event, IngestionProducer, get_and_clear_manual_buffer
from generator.order_generator import ContinuousOrderGenerator
from spark.validation import EventValidator
from spark.postgres_writer import postgres_stream_writer
from dashboard.db_queries import get_order_status_by_id, get_recent_orders_feed, get_quarantine_metrics
from database.db_connection import db_manager


def test_manual_order_event_schema_compliance():
    """Verifies that manually structured order events strictly adhere to OrderEventSchema data contracts."""
    item = OrderItemSchema(
        product_id="PROD-E101",
        quantity=2,
        unit_price=3499.00,
        total_item_price=6998.00,
        discount_applied=100.00
    )
    assert item.quantity == 2
    assert item.total_item_price == 6998.00

    gross_total = item.total_item_price
    discount = item.discount_applied
    tax = round((gross_total - discount) * 0.08, 2)
    net = round(gross_total - discount + tax, 2)

    event = OrderEventSchema(
        event_id=str(uuid.uuid4()),
        order_id=f"ORD-MANUAL-{uuid.uuid4().hex[:8].upper()}",
        customer_id="CUST-1042",
        location_id="LOC-NY01",
        payment_method_id="PAY-CC-VISA",
        order_status="PENDING",
        items=[item],
        item_count=item.quantity,
        total_amount=gross_total,
        discount_amount=discount,
        tax_amount=tax,
        net_amount=net,
        order_timestamp=datetime.now(timezone.utc).isoformat(),
        metadata={
            "device": "Desktop_Chrome",
            "channel": "Direct",
            "source": "manual_dashboard_ingestion"
        }
    )

    event_dict = event.to_dict()
    assert event_dict["order_id"].startswith("ORD-MANUAL-")
    assert event_dict["customer_id"] == "CUST-1042"
    assert event_dict["net_amount"] == net
    assert len(event_dict["items"]) == 1


def test_manual_order_publishing_and_stream_processing():
    """Verifies publishing a manual order event, stream consumption, and database persistence."""
    order_id = f"ORD-TEST-{uuid.uuid4().hex[:8].upper()}"
    event_id = str(uuid.uuid4())
    now_ts = datetime.now(timezone.utc).isoformat()

    manual_event = {
        "event_id": event_id,
        "order_id": order_id,
        "customer_id": "CUST-1099",
        "location_id": "LOC-CA01",
        "payment_method_id": "PAY-APPLE",
        "order_status": "PROCESSING",
        "items": [
            {
                "product_id": "PROD-E104",
                "quantity": 1,
                "unit_price": 1199.00,
                "total_item_price": 1199.00,
                "discount_applied": 0.0
            }
        ],
        "item_count": 1,
        "total_amount": 1199.00,
        "discount_amount": 0.0,
        "tax_amount": 95.92,
        "net_amount": 1294.92,
        "order_timestamp": now_ts,
        "metadata": {
            "device": "Mobile_iOS",
            "channel": "Direct",
            "source": "manual_dashboard_ingestion"
        }
    }

    # 1. Publish event using Kafka producer interface
    pub_result = publish_order_event(manual_event)
    assert pub_result["success"] is True
    assert pub_result["order_id"] == order_id
    assert pub_result["topic"] == "raw_orders"

    # 2. Process event through PySpark / warehouse stream writer
    stats = postgres_stream_writer.write_micro_batch([manual_event])
    assert stats["processed"] == 1
    assert stats["valid"] == 1
    assert stats["quarantined"] == 0

    # 3. Verify persistence status query
    status = get_order_status_by_id(order_id)
    assert status["found"] is True
    assert status["status"] == "PERSISTED"
    assert status["destination"] == "fact_orders"
    assert status["data"]["order_id"] == order_id
    assert status["data"]["customer_name"] is not None
    assert float(status["data"]["net_amount"]) == 1294.92


def test_invalid_manual_order_quarantine_routing():
    """Verifies that invalid manual orders (e.g. negative price) are properly rejected and routed to quarantine."""
    invalid_order_id = f"ORD-BAD-{uuid.uuid4().hex[:8].upper()}"
    invalid_event = {
        "event_id": str(uuid.uuid4()),
        "order_id": invalid_order_id,
        "customer_id": "CUST-1042",
        "location_id": "LOC-NY01",
        "payment_method_id": "PAY-CC-VISA",
        "order_status": "PENDING",
        "items": [
            {
                "product_id": "PROD-E101",
                "quantity": 1,
                "unit_price": -50.00,
                "total_item_price": -50.00,
                "discount_applied": 0.0
            }
        ],
        "item_count": 1,
        "total_amount": -50.00,
        "discount_amount": 0.0,
        "tax_amount": 0.0,
        "net_amount": -50.00,
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "source": "manual_anomaly_test"
        }
    }

    # 1. Validation rule check
    is_valid, error_code, reason = EventValidator.validate_order_event(invalid_event)
    assert is_valid is False
    assert "NEGATIVE" in error_code or "INVALID" in error_code

    # 2. Stream ingestion should route to quarantine without failing the batch
    stats = postgres_stream_writer.write_micro_batch([invalid_event])
    assert stats["processed"] == 1
    assert stats["valid"] == 0
    assert stats["quarantined"] == 1

    # 3. Status lookup should show QUARANTINED
    status = get_order_status_by_id(invalid_order_id)
    assert status["found"] is True
    assert status["status"] == "QUARANTINED"
    assert status["destination"] == "quarantine_orders"
    assert status["data"]["order_id"] == invalid_order_id


def test_coexistence_with_automatic_generator():
    """Verifies that synthetic background generator and manual orders coexist seamlessly."""
    gen = ContinuousOrderGenerator()
    auto_events = [gen.generate_next_event() for _ in range(5)]

    manual_order_id = f"ORD-MANUAL-{uuid.uuid4().hex[:8].upper()}"
    manual_event = {
        "event_id": str(uuid.uuid4()),
        "order_id": manual_order_id,
        "customer_id": "CUST-1010",
        "location_id": "LOC-UK01",
        "payment_method_id": "PAY-PAYPAL",
        "order_status": "DELIVERED",
        "items": [
            {
                "product_id": "PROD-H301",
                "quantity": 1,
                "unit_price": 999.95,
                "total_item_price": 999.95,
                "discount_applied": 0.0
            }
        ],
        "item_count": 1,
        "total_amount": 999.95,
        "discount_amount": 0.0,
        "tax_amount": 80.00,
        "net_amount": 1079.95,
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {"source": "manual_ingestion"}
    }

    combined_batch = auto_events + [manual_event]
    stats = postgres_stream_writer.write_micro_batch(combined_batch)
    assert stats["processed"] == 6
    assert stats["valid"] >= 1

    manual_status = get_order_status_by_id(manual_order_id)
    assert manual_status["found"] is True
    assert manual_status["status"] == "PERSISTED"
