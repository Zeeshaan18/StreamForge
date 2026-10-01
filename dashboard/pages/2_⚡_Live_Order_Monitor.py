"""
Page 2: Live Order Monitor.
Displays continuous streaming order events, global geographic map, and transaction inspector.
"""
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
from dashboard.components.ui_helpers import apply_custom_css, render_metric_card
from dashboard.components.charts import create_geo_scatter_map
from dashboard.db_queries import get_recent_orders_feed, get_geographic_distribution, get_executive_kpis, get_live_ingest_rate
from dashboard.pipeline_runner import ensure_background_pipeline_running
from database.db_connection import db_manager

# Ensure background streaming pipeline is active
ensure_background_pipeline_running()

st.set_page_config(page_title="Live Order Monitor | DataPulse", page_icon="⚡", layout="wide")
apply_custom_css()

st.title("⚡ Live Order Stream Monitor")
st.markdown("Real-time event stream arriving from Kafka & processed through PySpark.")

st.info("💡 **Want to inject a manual transaction?** Head to **[➕ Create Real-Time Order](Create_Real_Time_Order)** to dispatch a custom order into Kafka and watch it stream here in real time!")

kpis = get_executive_kpis()
orders_df = get_recent_orders_feed(limit=50)
geo_df = get_geographic_distribution()
ingest_rate = get_live_ingest_rate()

# KPI Row
col1, col2, col3 = st.columns(3)
with col1:
    rate_status = "success" if ingest_rate > 0 else "warning"
    rate_label = f"{ingest_rate:.1f} eps" if ingest_rate > 0 else "Idle / Standby"
    render_metric_card("Live Ingest Rate", rate_label, "Streaming Velocity", status=rate_status)
with col2:
    render_metric_card("Total Orders Captured", f"{kpis['total_orders']:,}", "Database Warehouse Fact Rows")
with col3:
    render_metric_card("Global Locations Active", f"{len(geo_df)} Cities", "Multi-region distribution")

# Geographic Map
st.markdown("---")
st.subheader("🌍 Real-Time Global Order Distribution")
st.plotly_chart(create_geo_scatter_map(geo_df), use_container_width=True)

# Live Table Feed
st.markdown("---")
st.subheader("📥 Live Incoming Order Stream (Latest 50 Events)")

if not orders_df.empty:
    display_df = orders_df[[
        "order_id", "customer_name", "customer_segment", "city", "country",
        "payment_method", "order_status", "item_count", "net_amount", "ingest_latency_ms", "order_timestamp"
    ]].copy()
    
    st.dataframe(
        display_df,
        column_config={
            "net_amount": st.column_config.NumberColumn("Net Amount ($)", format="$%.2f"),
            "ingest_latency_ms": st.column_config.NumberColumn("Latency (ms)", format="%d ms"),
            "order_status": st.column_config.TextColumn("Status"),
            "order_timestamp": st.column_config.DatetimeColumn("Order Time (UTC)", format="YYYY-MM-DD HH:mm:ss")
        },
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No orders processed yet. Start the stream generator and processor to see live orders.")

# Order Drilldown Inspector
st.markdown("---")
st.subheader("🔍 Order Item Inspector")
if not orders_df.empty:
    selected_order_id = st.selectbox("Select Order ID to Inspect Line Items:", orders_df["order_id"].tolist())
    if selected_order_id:
        item_query = """
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            i.quantity,
            i.unit_price,
            i.total_item_price,
            i.discount_applied
        FROM fact_order_items i
        JOIN dim_products p ON i.product_id = p.product_id
        WHERE i.order_id = :order_id
        """
        item_rows = db_manager.execute_query(item_query, {"order_id": selected_order_id})
        if item_rows:
            st.dataframe(pd.DataFrame(item_rows), use_container_width=True, hide_index=True)
        else:
            st.warning("No line items found for selected order.")
