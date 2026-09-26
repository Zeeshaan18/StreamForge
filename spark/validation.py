"""
Data validation rules and quarantine classification engine for DataPulse Live.
Applies rigorous data quality checks on incoming event payloads.
"""
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List


# Set of recognized valid lookup IDs seeded in warehouse
VALID_PAYMENT_METHODS = {
    "PAY-CC-VISA", "PAY-CC-MC", "PAY-CC-AMEX", "PAY-DC-VISA",
    "PAY-PAYPAL", "PAY-APPLE", "PAY-GOOGLE", "PAY-KLARNA", "PAY-CRYPTO"
}

VALID_ORDER_STATUSES = {"PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"}


class EventValidator:
    """Validates raw incoming JSON order payloads against strict data quality rules."""

    @staticmethod
    def validate_order_event(event: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Validates an order event.
        
        Returns:
            Tuple[is_valid (bool), error_code (str or None), rejection_reason (str or None)]
        """
        if not isinstance(event, dict):
            return False, "CORRUPT_PAYLOAD", "Event payload is not a valid JSON dictionary."

        # 1. Null / Missing Mandatory Identifiers
        event_id = event.get("event_id")
        order_id = event.get("order_id")
        customer_id = event.get("customer_id")
        location_id = event.get("location_id")
        payment_method_id = event.get("payment_method_id")

        if not event_id:
            return False, "NULL_EVENT_ID", "Field 'event_id' is missing or null."
        if not order_id:
            return False, "NULL_ORDER_ID", "Field 'order_id' is missing or null."
        if not customer_id:
            return False, "NULL_CUSTOMER_ID", "Field 'customer_id' is missing or null."
        if not location_id:
            return False, "NULL_LOCATION_ID", "Field 'location_id' is missing or null."
        if not payment_method_id:
            return False, "NULL_PAYMENT_METHOD", "Field 'payment_method_id' is missing or null."

        # 2. Check Valid Reference Values
        if payment_method_id not in VALID_PAYMENT_METHODS:
            return False, "INVALID_PAYMENT_METHOD", f"Payment method '{payment_method_id}' is not in reference dimensions."

        # 3. Status Integrity
        order_status = str(event.get("order_status", "")).upper()
        if order_status not in VALID_ORDER_STATUSES:
            return False, "INVALID_ORDER_STATUS", f"Order status '{order_status}' is not a recognized state."

        # 4. Financial & Value Validity (Negative / Absurd Prices)
        total_amount = event.get("total_amount")
        net_amount = event.get("net_amount")

        if total_amount is None or not isinstance(total_amount, (int, float)) or total_amount < 0:
            return False, "NEGATIVE_OR_NULL_TOTAL", f"Gross total amount {total_amount} is negative or not a number."

        if net_amount is None or not isinstance(net_amount, (int, float)) or net_amount < 0:
            return False, "NEGATIVE_OR_NULL_NET", f"Net amount {net_amount} is negative or not a number."

        # 5. Line Items Integrity
        items = event.get("items")
        if not isinstance(items, list) or len(items) == 0:
            return False, "EMPTY_ITEMS_ARRAY", "Order contains no line items."

        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                return False, "CORRUPT_LINE_ITEM", f"Line item at index {idx} is malformed."
            
            p_id = item.get("product_id")
            qty = item.get("quantity")
            u_price = item.get("unit_price")

            if not p_id:
                return False, "NULL_PRODUCT_ID", f"Line item at index {idx} has missing product_id."
            if qty is None or not isinstance(qty, int) or qty <= 0:
                return False, "INVALID_QUANTITY", f"Line item {p_id} has invalid quantity: {qty}."
            if u_price is None or not isinstance(u_price, (int, float)) or u_price < 0:
                return False, "INVALID_UNIT_PRICE", f"Line item {p_id} has negative/null unit price: {u_price}."

        # 6. Timestamp Sanity (Future / Far Past Anomalies)
        order_ts_str = event.get("order_timestamp")
        if not order_ts_str:
            return False, "NULL_TIMESTAMP", "Field 'order_timestamp' is missing."

        try:
            # Parse ISO timestamp
            if isinstance(order_ts_str, str):
                ts = datetime.fromisoformat(order_ts_str.replace("Z", "+00:00"))
                now_utc = datetime.now(timezone.utc)
                if ts > now_utc + datetime.resolution * 86400 * 30:  # More than 30 days in future
                    return False, "FUTURE_TIMESTAMP_ANOMALY", f"Timestamp {order_ts_str} is unreasonably far in the future."
        except Exception as e:
            return False, "MALFORMED_TIMESTAMP", f"Could not parse timestamp '{order_ts_str}': {e}"

        return True, None, None
