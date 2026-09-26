"""
Pydantic schemas and data validation models for DataPulse Live e-commerce events.
"""
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class OrderItemSchema(BaseModel):
    """Line item within an e-commerce order."""
    product_id: str = Field(..., description="Product SKU identifier")
    quantity: int = Field(..., gt=0, description="Quantity ordered (must be > 0)")
    unit_price: float = Field(..., ge=0.0, description="Price per unit in USD")
    total_item_price: float = Field(..., ge=0.0, description="Total price = quantity * unit_price")
    discount_applied: float = Field(default=0.0, ge=0.0, description="Line item discount in USD")


class OrderEventSchema(BaseModel):
    """Complete e-commerce order event emitted to Kafka."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique event UUID")
    order_id: str = Field(default_factory=lambda: f"ORD-{uuid.uuid4().hex[:10].upper()}", description="Business order ID")
    customer_id: str = Field(..., description="Customer identifier")
    location_id: str = Field(..., description="Delivery location ID")
    payment_method_id: str = Field(..., description="Payment method identifier")
    order_status: str = Field(default="PENDING", description="Status: PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED")
    items: List[OrderItemSchema] = Field(..., min_length=1, description="List of items in basket")
    item_count: int = Field(..., gt=0, description="Sum of quantities of all items")
    total_amount: float = Field(..., ge=0.0, description="Gross total amount before discount/tax")
    discount_amount: float = Field(default=0.0, ge=0.0, description="Total discount applied")
    tax_amount: float = Field(default=0.0, ge=0.0, description="Calculated sales tax")
    net_amount: float = Field(..., ge=0.0, description="Final billed amount = total - discount + tax")
    order_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 UTC timestamp of order placement"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Device, session, or tracking metadata")

    @field_validator("order_status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        valid_statuses = {"PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"}
        if v.upper() not in valid_statuses:
            raise ValueError(f"Invalid order status '{v}'. Allowed values: {valid_statuses}")
        return v.upper()

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary for JSON serialization."""
        return self.model_dump()

    def to_json(self) -> str:
        """Convert model to JSON string."""
        return self.model_dump_json()
