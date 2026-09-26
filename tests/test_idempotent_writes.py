"""
Unit & Integration tests for idempotent database writes and quarantine routing.
"""
from spark.postgres_writer import postgres_stream_writer
from generator.order_generator import ContinuousOrderGenerator
from database.db_connection import db_manager


def test_idempotent_upsert_prevents_duplicates():
    """Verifies that writing the same order event multiple times does not create duplicate rows."""
    gen = ContinuousOrderGenerator(error_rate=0.0, duplicate_rate=0.0)
    event = gen.generate_next_event()
    order_id = event["order_id"]

    # Initial Write
    stats1 = postgres_stream_writer.write_micro_batch([event])
    assert stats1["valid"] == 1

    # Check count in DB
    rows_initial = db_manager.execute_query("SELECT COUNT(*) as c FROM fact_orders WHERE order_id = :oid", {"oid": order_id})
    assert rows_initial[0]["c"] == 1

    # Replay identical event (duplicate simulation)
    stats2 = postgres_stream_writer.write_micro_batch([event])
    assert stats2["duplicates"] == 1

    # Count must remain 1
    rows_after = db_manager.execute_query("SELECT COUNT(*) as c FROM fact_orders WHERE order_id = :oid", {"oid": order_id})
    assert rows_after[0]["c"] == 1, "Duplicate order_id row was incorrectly inserted!"


def test_quarantine_routing():
    """Verifies that invalid events are routed to quarantine table."""
    corrupt_event = {
        "event_id": "ERR-TEST-UUID",
        "order_id": "ORD-CORRUPT",
        "customer_id": None,  # Causes NULL error
        "total_amount": -50.0
    }

    stats = postgres_stream_writer.write_micro_batch([corrupt_event])
    assert stats["quarantined"] == 1

    q_rows = db_manager.execute_query("SELECT * FROM quarantine_orders WHERE event_id = 'ERR-TEST-UUID'")
    assert len(q_rows) >= 1
    assert q_rows[0]["error_code"] in ["NULL_CUSTOMER_ID", "NULL_LOCATION_ID"]
