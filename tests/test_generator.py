"""
Unit tests for continuous synthetic e-commerce event generator.
"""
import pytest
from generator.order_generator import ContinuousOrderGenerator
from generator.schema import OrderEventSchema


def test_generator_produces_unique_ids():
    """Verifies that generated events have globally unique UUIDs."""
    generator = ContinuousOrderGenerator(error_rate=0.0, duplicate_rate=0.0)
    events = [generator.generate_next_event() for _ in range(50)]

    event_ids = [e["event_id"] for e in events]
    order_ids = [e["order_id"] for e in events]

    assert len(event_ids) == len(set(event_ids)), "Duplicate event_id detected!"
    assert len(order_ids) == len(set(order_ids)), "Duplicate order_id detected!"


def test_valid_order_schema_conformance():
    """Verifies that valid order events adhere strictly to Pydantic schema."""
    generator = ContinuousOrderGenerator(error_rate=0.0, duplicate_rate=0.0)
    for _ in range(20):
        raw_event = generator.generate_next_event()
        # Should parse without validation errors
        parsed = OrderEventSchema(**raw_event)
        assert parsed.net_amount > 0
        assert parsed.item_count > 0
        assert len(parsed.items) >= 1
        assert parsed.order_status in {"PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"}


def test_intentional_anomaly_injection():
    """Verifies that error injection generates corrupt events when rate is set."""
    generator = ContinuousOrderGenerator(error_rate=1.0, duplicate_rate=0.0)
    corrupted_event = generator.generate_next_event()

    assert "_injected_anomaly" in corrupted_event
    anomaly = corrupted_event["_injected_anomaly"]
    assert anomaly in [
        "NEGATIVE_PRICE", "NULL_CUSTOMER_ID", "INVALID_ORDER_STATUS",
        "FUTURE_TIMESTAMP", "EMPTY_ITEMS_ARRAY", "ZERO_QUANTITY", "INVALID_PAYMENT_METHOD"
    ]


def test_duplicate_emission_simulation():
    """Verifies that duplicate rate emits cached events."""
    generator = ContinuousOrderGenerator(error_rate=0.0, duplicate_rate=0.0)
    # Prime cache
    for _ in range(10):
        generator.generate_next_event()

    # Enable 100% duplicate rate
    generator.duplicate_rate = 1.0
    dup_event = generator.generate_next_event()
    assert dup_event is not None
    assert generator.total_duplicate_count >= 1
