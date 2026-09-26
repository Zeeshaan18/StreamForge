"""
Unit tests for transformation logic and schema conversions.
"""
from spark.transformations import transform_and_enrich_order
from generator.order_generator import ContinuousOrderGenerator


def test_transformation_structure():
    """Verifies that transformation returns correctly structured fact records."""
    gen = ContinuousOrderGenerator(error_rate=0.0, duplicate_rate=0.0)
    event = gen.generate_next_event()

    fact_order, fact_items = transform_and_enrich_order(event)

    assert fact_order["order_id"] == event["order_id"]
    assert fact_order["event_id"] == event["event_id"]
    assert "ingest_latency_ms" in fact_order
    assert fact_order["ingest_latency_ms"] >= 0

    assert len(fact_items) == len(event["items"])
    for item in fact_items:
        assert item["order_id"] == event["order_id"]
        assert item["quantity"] > 0
        assert item["unit_price"] >= 0
        assert item["total_item_price"] >= 0
