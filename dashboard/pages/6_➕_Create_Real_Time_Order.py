"""
Page 6: Create & Submit Real-Time Order.
Demonstrates genuine real-time ingestion:
Streamlit Form -> Kafka Topic [raw_orders] -> PySpark Streaming -> PostgreSQL Warehouse -> Live Dashboard.
"""
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
from config.settings import settings
from dashboard.components.ui_helpers import apply_custom_css, render_metric_card
from dashboard.pipeline_runner import ensure_background_pipeline_running
from dashboard.db_queries import get_order_status_by_id
from database.seed_dimensions import PRODUCT_CATALOG, LOCATIONS, PAYMENT_METHODS
from generator.schema import OrderEventSchema, OrderItemSchema
from generator.kafka_producer import publish_order_event

# Ensure background streaming pipeline is active
ensure_background_pipeline_running()

st.set_page_config(page_title="Create Real-Time Order | DataPulse", page_icon="➕", layout="wide")
apply_custom_css()

# Header Banner
st.title("➕ Create / Submit Real-Time Order")
st.markdown("Produce real-time order events directly into the **Kafka event stream** for **PySpark Structured Streaming** ingestion.")

# Architecture Pipeline Flow Graphic
st.markdown("""
<div style="background: rgba(30, 30, 50, 0.5); border: 1px solid rgba(99, 102, 241, 0.3); border-radius: 12px; padding: 1rem 1.5rem; margin-bottom: 1.5rem;">
    <div style="font-size: 0.85rem; font-weight: 700; color: #818CF8; letter-spacing: 0.5px; text-transform: uppercase; margin-bottom: 0.5rem;">
        ⚡ Real-Time Ingestion Architecture Flow
    </div>
    <div style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 0.5rem; font-size: 0.95rem; color: #E2E8F0;">
        <span style="background: rgba(99, 102, 241, 0.2); padding: 4px 10px; border-radius: 6px; border: 1px solid rgba(99, 102, 241, 0.4);">
            👤 <b>User Form</b>
        </span>
        <span style="color: #94A3B8;">➔</span>
        <span style="background: rgba(236, 72, 153, 0.2); padding: 4px 10px; border-radius: 6px; border: 1px solid rgba(236, 72, 153, 0.4);">
            📬 <b>Kafka</b> [<code>raw_orders</code>]
        </span>
        <span style="color: #94A3B8;">➔</span>
        <span style="background: rgba(245, 158, 11, 0.2); padding: 4px 10px; border-radius: 6px; border: 1px solid rgba(245, 158, 11, 0.4);">
            ⚡ <b>PySpark Streaming</b>
        </span>
        <span style="color: #94A3B8;">➔</span>
        <span style="background: rgba(16, 185, 129, 0.2); padding: 4px 10px; border-radius: 6px; border: 1px solid rgba(16, 185, 129, 0.4);">
            🐘 <b>PostgreSQL Warehouse</b>
        </span>
        <span style="color: #94A3B8;">➔</span>
        <span style="background: rgba(59, 130, 246, 0.2); padding: 4px 10px; border-radius: 6px; border: 1px solid rgba(59, 130, 246, 0.4);">
            📊 <b>Live Dashboard</b>
        </span>
    </div>
    <div style="font-size: 0.8rem; color: #94A3B8; margin-top: 0.5rem;">
        🛡️ <i>Strict Data Contract: Orders are emitted to Kafka and processed asynchronously by Spark. No direct PostgreSQL bypass.</i>
    </div>
</div>
""", unsafe_allow_html=True)

# Build Lookup Dictionaries for Clean UI Presentation
PRODUCT_OPTIONS = {
    f"{sku} - {name} (${price:.2f}) [{category}]": {
        "sku": sku,
        "name": name,
        "category": category,
        "price": float(price),
        "cost": float(cost)
    }
    for sku, name, category, subcat, price, cost, supp in PRODUCT_CATALOG
}

LOCATION_OPTIONS = {
    f"{loc_id} - {city}, {country} ({region})": loc_id
    for loc_id, city, state, country, zip_code, region, lat, lon in LOCATIONS
}

PAYMENT_OPTIONS = {
    f"{p_id} - {provider} ({p_type})": p_id
    for p_id, p_type, provider, active in PAYMENT_METHODS
}

ORDER_STATUS_OPTIONS = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
DEVICE_OPTIONS = ["Desktop_Chrome", "Mobile_iOS", "Mobile_Android", "Desktop_Safari", "Tablet_iPad"]
CHANNEL_OPTIONS = ["Direct", "Google_Organic", "Google_Ads", "Instagram", "Email_Campaign", "TikTok"]

# Two column layout: Form (left) and Live Calculation / Status (right)
col_form, col_summary = st.columns([1.7, 1.3])

with col_form:
    st.subheader("📝 Order Details Form")

    with st.form("manual_order_form", clear_on_submit=False):
        # Section 1: Customer & Logistics
        st.markdown("##### 👤 1. Customer & Delivery Information")
        col_c1, col_c2 = st.columns(2)

        with col_c1:
            customer_mode = st.radio("Customer Selection:", ["Existing Customer", "Custom Customer ID"], horizontal=True)
            if customer_mode == "Existing Customer":
                cust_ids = [f"CUST-{i}" for i in range(1001, 1051)]
                customer_id = st.selectbox("Select Customer:", cust_ids, index=0)
            else:
                customer_id = st.text_input("Enter Custom Customer ID:", value="CUST-9999", max_chars=30)

        with col_c2:
            location_label = st.selectbox("Delivery Location:", list(LOCATION_OPTIONS.keys()), index=0)
            location_id = LOCATION_OPTIONS[location_label]

        # Section 2: Product & Basket
        st.markdown("---")
        st.markdown("##### 🛍️ 2. Product Basket Item")
        
        prod_label = st.selectbox("Select Product SKU from Catalog:", list(PRODUCT_OPTIONS.keys()), index=0)
        selected_prod = PRODUCT_OPTIONS[prod_label]
        
        col_p1, col_p2, col_p3 = st.columns(3)
        with col_p1:
            quantity = st.number_input("Quantity:", min_value=1, max_value=100, value=1, step=1)
        with col_p2:
            unit_price = st.number_input("Unit Price ($ USD):", min_value=0.01, max_value=20000.00, value=selected_prod["price"], step=10.00, format="%.2f")
        with col_p3:
            discount_applied = st.number_input("Line Item Discount ($):", min_value=0.0, max_value=5000.0, value=0.0, step=5.0, format="%.2f")

        # Optional 2nd Item in Basket
        with st.expander("➕ Add Second Item to Basket (Optional)", expanded=False):
            include_item2 = st.checkbox("Include Second Item in this Order", value=False)
            if include_item2:
                prod2_label = st.selectbox("Select 2nd Product SKU:", list(PRODUCT_OPTIONS.keys()), index=3)
                selected_prod2 = PRODUCT_OPTIONS[prod2_label]
                col_2a, col_2b, col_2c = st.columns(3)
                with col_2a:
                    quantity2 = st.number_input("Quantity (Item 2):", min_value=1, max_value=100, value=1, step=1, key="qty2")
                with col_2b:
                    unit_price2 = st.number_input("Unit Price ($ Item 2):", min_value=0.01, max_value=20000.00, value=selected_prod2["price"], step=10.00, format="%.2f", key="price2")
                with col_2c:
                    discount_applied2 = st.number_input("Discount ($ Item 2):", min_value=0.0, max_value=5000.0, value=0.0, step=5.0, format="%.2f", key="disc2")
            else:
                quantity2 = 0
                unit_price2 = 0.0
                discount_applied2 = 0.0

        # Section 3: Payment & Order State
        st.markdown("---")
        st.markdown("##### 💳 3. Payment & Order Status")
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            payment_label = st.selectbox("Payment Method:", list(PAYMENT_OPTIONS.keys()), index=0)
            payment_method_id = PAYMENT_OPTIONS[payment_label]
        with col_m2:
            order_status = st.selectbox("Initial Order Status:", ORDER_STATUS_OPTIONS, index=0)

        # Section 4: Metadata & Channel (Optional)
        st.markdown("---")
        st.markdown("##### 📱 4. Session & Attribution Metadata (Optional)")
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            device = st.selectbox("Device / Client Platform:", DEVICE_OPTIONS, index=0)
        with col_d2:
            channel = st.selectbox("Referral Channel:", CHANNEL_OPTIONS, index=0)

        # Submit button
        st.markdown("<br>", unsafe_allow_html=True)
        submit_order = st.form_submit_button("🚀 Submit Order to Kafka Stream", type="primary", use_container_width=True)

# Calculate financial totals
item1_gross = round(quantity * unit_price, 2)
item1_disc = round(min(item1_gross, discount_applied), 2)

if 'include_item2' in locals() and include_item2:
    item2_gross = round(quantity2 * unit_price2, 2)
    item2_disc = round(min(item2_gross, discount_applied2), 2)
    total_gross = round(item1_gross + item2_gross, 2)
    total_discount = round(item1_disc + item2_disc, 2)
    total_qty = int(quantity + quantity2)
else:
    total_gross = item1_gross
    total_discount = item1_disc
    total_qty = int(quantity)

taxable_amount = max(0.0, total_gross - total_discount)
tax_amount = round(taxable_amount * 0.08, 2)  # 8% estimated tax
net_amount = round(total_gross - total_discount + tax_amount, 2)

# Financial preview column (right)
with col_summary:
    st.subheader("💰 Live Order Pricing Preview")
    
    col_k1, col_k2 = st.columns(2)
    with col_k1:
        render_metric_card("Gross Total", f"${total_gross:,.2f}", f"{total_qty} total items")
    with col_k2:
        render_metric_card("Total Discount", f"${total_discount:,.2f}", "Promotional / Coupon", status="warning" if total_discount > 0 else "success")

    col_k3, col_k4 = st.columns(2)
    with col_k3:
        render_metric_card("Sales Tax (8%)", f"${tax_amount:,.2f}", "State / Local Tax")
    with col_k4:
        render_metric_card("Final Net Total", f"${net_amount:,.2f}", "Billed to Customer", status="success")

    st.markdown("---")
    st.markdown("##### 🧪 Data Quality Anomaly Testing (Optional)")
    st.caption("Deliberately inject anomalies into Kafka to test Spark validation & quarantine routing:")
    
    anomaly_choice = st.selectbox(
        "Inject Synthetic Data Violation (Testing):",
        ["NONE (Valid Order)", "NEGATIVE_PRICE", "ZERO_QUANTITY", "INVALID_PAYMENT_METHOD", "NULL_CUSTOMER_ID", "INVALID_ORDER_STATUS"]
    )

# Processing upon submission
if submit_order:
    # 1. Validation checks
    validation_passed = True
    validation_error = ""

    if not customer_id or not customer_id.strip():
        validation_passed = False
        validation_error = "Customer ID is required."
    elif quantity <= 0 and anomaly_choice == "NONE (Valid Order)":
        validation_passed = False
        validation_error = "Quantity must be greater than 0."
    elif unit_price <= 0 and anomaly_choice == "NONE (Valid Order)":
        validation_passed = False
        validation_error = "Unit price must be greater than $0.00."

    if not validation_passed:
        st.error(f"❌ Input Validation Error: {validation_error}")
    else:
        # 2. Build line items
        items_payload = [
            OrderItemSchema(
                product_id=selected_prod["sku"],
                quantity=quantity,
                unit_price=float(unit_price),
                total_item_price=item1_gross,
                discount_applied=item1_disc
            ).model_dump()
        ]

        if 'include_item2' in locals() and include_item2:
            items_payload.append(
                OrderItemSchema(
                    product_id=selected_prod2["sku"],
                    quantity=quantity2,
                    unit_price=float(unit_price2),
                    total_item_price=item2_gross,
                    discount_applied=item2_disc
                ).model_dump()
            )

        # Generate unique IDs and timestamps
        order_id = f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"
        event_id = str(uuid.uuid4())
        order_ts = datetime.now(timezone.utc).isoformat()

        # Build complete order event payload matching Kafka/Spark contract
        event_payload = {
            "event_id": event_id,
            "order_id": order_id,
            "customer_id": customer_id.strip(),
            "location_id": location_id,
            "payment_method_id": payment_method_id,
            "order_status": order_status,
            "items": items_payload,
            "item_count": total_qty,
            "total_amount": total_gross,
            "discount_amount": total_discount,
            "tax_amount": tax_amount,
            "net_amount": net_amount,
            "order_timestamp": order_ts,
            "metadata": {
                "device": device,
                "channel": channel,
                "source": "manual_dashboard_ingestion",
                "session_duration_sec": "120",
                "ip_address": "127.0.0.1"
            }
        }

        # Apply intentional anomaly injection if chosen
        if anomaly_choice == "NEGATIVE_PRICE":
            event_payload["total_amount"] = -150.00
            event_payload["net_amount"] = -135.00
        elif anomaly_choice == "ZERO_QUANTITY":
            event_payload["items"][0]["quantity"] = 0
            event_payload["item_count"] = 0
        elif anomaly_choice == "INVALID_PAYMENT_METHOD":
            event_payload["payment_method_id"] = "PAY-UNRECOGNIZED-X"
        elif anomaly_choice == "NULL_CUSTOMER_ID":
            event_payload["customer_id"] = None
        elif anomaly_choice == "INVALID_ORDER_STATUS":
            event_payload["order_status"] = "MAGIC_STATUS_123"

        # 3. Publish to Kafka topic raw_orders
        with st.spinner("Publishing event to Kafka topic raw_orders..."):
            pub_result = publish_order_event(event_payload, topic=settings.KAFKA_RAW_ORDERS_TOPIC)

        st.session_state["last_submitted_order_id"] = order_id
        st.session_state["last_submitted_payload"] = event_payload
        st.session_state["last_pub_result"] = pub_result

        st.success("✅ Order submitted successfully — processing through Kafka/Spark.")

# Display Order Submission Summary & Status Inspector
if "last_submitted_order_id" in st.session_state:
    submitted_id = st.session_state["last_submitted_order_id"]
    submitted_payload = st.session_state["last_submitted_payload"]
    pub_result = st.session_state["last_pub_result"]

    st.markdown("---")
    st.subheader("🔍 Real-Time Pipeline Ingestion Inspector")

    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
    with col_stat1:
        st.write(f"**Order ID:** `{submitted_id}`")
        st.write(f"**Event UUID:** `{submitted_payload.get('event_id')}`")
    with col_stat2:
        st.write(f"**Kafka Topic:** `{pub_result.get('topic')}`")
        st.write(f"**Ingestion Mode:** `{pub_result.get('mode')}`")
    with col_stat3:
        st.write(f"**Customer:** `{submitted_payload.get('customer_id')}`")
        st.write(f"**Net Amount:** `${submitted_payload.get('net_amount', 0):,.2f}`")
    with col_stat4:
        st.write(f"**Timestamp (UTC):** `{submitted_payload.get('order_timestamp')}`")
        st.write(f"**Initial Status:** `{submitted_payload.get('order_status')}`")

    # Interactive Live Status Checker
    col_btn, col_res = st.columns([1.0, 2.0])
    with col_btn:
        check_now = st.button("🔄 Check Persistence Status in Warehouse", use_container_width=True)

    status_info = get_order_status_by_id(submitted_id)
    with col_res:
        if status_info["found"]:
            if status_info["status"] == "PERSISTED":
                st.success(f"🟢 **Persisted in fact_orders!** Processed by Spark (Latency: `{status_info['data'].get('ingest_latency_ms', 0)} ms`).")
            elif status_info["status"] == "QUARANTINED":
                st.warning(f"🛡️ **Quarantined by Spark DQ!** Error Code: `{status_info['data'].get('error_code')}` | Reason: `{status_info['data'].get('rejection_reason')}`.")
        else:
            st.info("🟡 **In Pipeline Stream:** Awaiting PySpark micro-batch commit (Window: ~3s). Click 'Check Persistence Status' to refresh.")

    with st.expander("📦 View Full Raw JSON Payload Emitted to Kafka", expanded=False):
        st.json(submitted_payload)
