"""
Continuous Synthetic E-Commerce Order Generator for DataPulse Live.
Generates unlimited realistic orders with dynamic basket composition,
configurable velocity, controlled anomaly injection, and duplicate simulations.
"""
import random
import uuid
import json
from datetime import datetime, timezone, timedelta
from typing import Generator, Dict, Any, List
from generator.schema import OrderEventSchema, OrderItemSchema
from database.seed_dimensions import PRODUCT_CATALOG, LOCATIONS, PAYMENT_METHODS
from config.settings import settings
from config.logging_config import setup_logger

logger = setup_logger("generator")

ORDER_STATUSES = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
STATUS_WEIGHTS = [0.40, 0.25, 0.20, 0.10, 0.05]

DEVICE_TYPES = ["Mobile_iOS", "Mobile_Android", "Desktop_Chrome", "Desktop_Safari", "Tablet_iPad"]
REFERRAL_CHANNELS = ["Direct", "Google_Organic", "Google_Ads", "Instagram", "Email_Campaign", "TikTok"]


class ContinuousOrderGenerator:
    """Generates continuous streams of synthetic e-commerce order events."""

    def __init__(
        self,
        error_rate: float = None,
        duplicate_rate: float = None,
        burst_probability: float = None
    ):
        self.error_rate = error_rate if error_rate is not None else settings.GENERATOR_ERROR_INJECTION_RATE
        self.duplicate_rate = duplicate_rate if duplicate_rate is not None else settings.GENERATOR_DUPLICATE_RATE
        self.burst_probability = burst_probability if burst_probability is not None else settings.GENERATOR_BURST_PROBABILITY
        self._recent_events_cache: List[Dict[str, Any]] = []
        self._max_cache_size = 100
        self.total_generated_count = 0
        self.total_error_count = 0
        self.total_duplicate_count = 0

    def _sample_basket(self) -> List[OrderItemSchema]:
        """Creates a realistic multi-product shopping basket."""
        item_count = random.choices([1, 2, 3, 4, 5], weights=[0.50, 0.25, 0.15, 0.07, 0.03])[0]
        selected_products = random.sample(PRODUCT_CATALOG, min(item_count, len(PRODUCT_CATALOG)))
        
        items = []
        for prod in selected_products:
            sku, name, cat, subcat, base_price, cost_price, supp = prod
            qty = random.choices([1, 2, 3, 4], weights=[0.75, 0.15, 0.07, 0.03])[0]
            discount = round(base_price * qty * 0.10, 2) if random.random() < 0.20 else 0.0
            total_price = round((base_price * qty), 2)
            
            items.append(
                OrderItemSchema(
                    product_id=sku,
                    quantity=qty,
                    unit_price=float(base_price),
                    total_item_price=total_price,
                    discount_applied=discount
                )
            )
        return items

    def _generate_valid_order(self) -> OrderEventSchema:
        """Generates a fully valid, schema-compliant order event."""
        customer_num = random.randint(1001, 1150)
        customer_id = f"CUST-{customer_num}"
        location_id = random.choice(LOCATIONS)[0]
        payment_method_id = random.choice(PAYMENT_METHODS)[0]
        order_status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]
        
        items = self._sample_basket()
        item_count = sum(item.quantity for item in items)
        gross_total = sum(item.total_item_price for item in items)
        total_discount = sum(item.discount_applied for item in items)
        
        # 8% estimated tax on post-discount amount
        taxable_amount = max(0.0, gross_total - total_discount)
        tax_amount = round(taxable_amount * 0.08, 2)
        net_amount = round(gross_total - total_discount + tax_amount, 2)
        
        event = OrderEventSchema(
            event_id=str(uuid.uuid4()),
            order_id=f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}",
            customer_id=customer_id,
            location_id=location_id,
            payment_method_id=payment_method_id,
            order_status=order_status,
            items=items,
            item_count=item_count,
            total_amount=round(gross_total, 2),
            discount_amount=round(total_discount, 2),
            tax_amount=tax_amount,
            net_amount=net_amount,
            order_timestamp=datetime.now(timezone.utc).isoformat(),
            metadata={
                "device": random.choice(DEVICE_TYPES),
                "channel": random.choice(REFERRAL_CHANNELS),
                "session_duration_sec": random.randint(30, 900),
                "ip_address": f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}"
            }
        )
        return event

    def _inject_anomaly(self, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Deliberately corrupts an event payload to test quarantine and data quality pipeline."""
        anomaly_type = random.choice([
            "NEGATIVE_PRICE",
            "NULL_CUSTOMER_ID",
            "INVALID_ORDER_STATUS",
            "FUTURE_TIMESTAMP",
            "EMPTY_ITEMS_ARRAY",
            "ZERO_QUANTITY",
            "INVALID_PAYMENT_METHOD"
        ])
        
        corrupted = dict(event_dict)
        if anomaly_type == "NEGATIVE_PRICE":
            corrupted["total_amount"] = -150.00
            corrupted["net_amount"] = -135.00
        elif anomaly_type == "NULL_CUSTOMER_ID":
            corrupted["customer_id"] = None
        elif anomaly_type == "INVALID_ORDER_STATUS":
            corrupted["order_status"] = "MAGIC_DELIVERY"
        elif anomaly_type == "FUTURE_TIMESTAMP":
            future_dt = datetime.now(timezone.utc) + timedelta(days=365)
            corrupted["order_timestamp"] = future_dt.isoformat()
        elif anomaly_type == "EMPTY_ITEMS_ARRAY":
            corrupted["items"] = []
            corrupted["item_count"] = 0
        elif anomaly_type == "ZERO_QUANTITY":
            if corrupted.get("items"):
                corrupted["items"][0]["quantity"] = 0
        elif anomaly_type == "INVALID_PAYMENT_METHOD":
            corrupted["payment_method_id"] = "PAY-UNRECOGNIZED-X"
            
        corrupted["_injected_anomaly"] = anomaly_type
        return corrupted

    def generate_next_event(self) -> Dict[str, Any]:
        """
        Produces the next order event.
        May return an intentional duplicate, an injected anomaly, or a pristine valid event.
        """
        # 1. Check for intentional duplicate event
        if self._recent_events_cache and random.random() < self.duplicate_rate:
            duplicate_event = random.choice(self._recent_events_cache)
            self.total_duplicate_count += 1
            self.total_generated_count += 1
            logger.debug(f"Emitting simulated duplicate event: {duplicate_event.get('event_id')}")
            return duplicate_event

        # 2. Generate valid event
        valid_event = self._generate_valid_order()
        event_dict = valid_event.to_dict()

        # 3. Check for synthetic anomaly injection
        if random.random() < self.error_rate:
            event_dict = self._inject_anomaly(event_dict)
            self.total_error_count += 1
            logger.debug(f"Emitting synthetic anomaly [{event_dict.get('_injected_anomaly')}]: {event_dict.get('event_id')}")
        else:
            # Cache valid events for duplicate testing
            self._recent_events_cache.append(event_dict)
            if len(self._recent_events_cache) > self._max_cache_size:
                self._recent_events_cache.pop(0)

        self.total_generated_count += 1
        return event_dict

    def stream_events(self, max_events: int = None) -> Generator[Dict[str, Any], None, None]:
        """Infinite (or bounded) generator stream of events."""
        count = 0
        while max_events is None or count < max_events:
            yield self.generate_next_event()
            count += 1


# Default singleton instance
order_generator = ContinuousOrderGenerator()
