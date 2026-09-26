"""
Unit tests for data validation rules and quarantine routing classifications.
"""
import pytest
from spark.validation import EventValidator
from generator.order_generator import ContinuousOrderGenerator


@pytest.fixture
def valid_event():
    gen = ContinuousOrderGenerator(error_rate=0.0, duplicate_rate=0.0)
    return gen.generate_next_event()


def test_valid_event_passes(valid_event):
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is True
    assert err_code is None
    assert reason is None


def test_null_event_id_fails(valid_event):
    valid_event["event_id"] = None
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is False
    assert err_code == "NULL_EVENT_ID"


def test_null_customer_id_fails(valid_event):
    valid_event["customer_id"] = None
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is False
    assert err_code == "NULL_CUSTOMER_ID"


def test_negative_price_fails(valid_event):
    valid_event["total_amount"] = -49.99
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is False
    assert err_code == "NEGATIVE_OR_NULL_TOTAL"


def test_invalid_status_fails(valid_event):
    valid_event["order_status"] = "FLYING_TO_SPACE"
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is False
    assert err_code == "INVALID_ORDER_STATUS"


def test_invalid_payment_method_fails(valid_event):
    valid_event["payment_method_id"] = "PAY-FAKE-METHOD"
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is False
    assert err_code == "INVALID_PAYMENT_METHOD"


def test_empty_items_fails(valid_event):
    valid_event["items"] = []
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is False
    assert err_code == "EMPTY_ITEMS_ARRAY"


def test_future_timestamp_fails(valid_event):
    valid_event["order_timestamp"] = "2099-01-01T00:00:00+00:00"
    is_valid, err_code, reason = EventValidator.validate_order_event(valid_event)
    assert is_valid is False
    assert err_code == "FUTURE_TIMESTAMP_ANOMALY"
