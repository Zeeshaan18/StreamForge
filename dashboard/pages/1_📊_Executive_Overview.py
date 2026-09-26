"""
Page 1: Executive Overview Dashboard.
Provides high-level business intelligence, GMV trajectories, and revenue breakdowns.
"""
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
from dashboard.components.ui_helpers import apply_custom_css, render_metric_card
from dashboard.components.charts import (
    create_revenue_trend_chart,
    create_category_bar_chart,
    create_payment_donut_chart
)
from dashboard.db_queries import (
    get_executive_kpis,
    get_recent_orders_feed,
    get_revenue_by_category,
    get_payment_method_distribution
)

st.set_page_config(page_title="Executive Overview | DataPulse", page_icon="📊", layout="wide")
apply_custom_css()

st.title("📊 Executive Overview & Business Metrics")
st.markdown("Real-time revenue intelligence, customer velocity, and product performance.")

kpis = get_executive_kpis()
orders_df = get_recent_orders_feed(limit=200)
category_df = get_revenue_by_category()
payment_df = get_payment_method_distribution()

# KPI Metric Row
col1, col2, col3, col4 = st.columns(4)
with col1:
    render_metric_card("Total Revenue", f"${kpis['total_revenue']:,.2f}", "Cumulative GMV")
with col2:
    render_metric_card("Total Orders", f"{kpis['total_orders']:,}", "Transactions Ingested")
with col3:
    render_metric_card("Average Order Value", f"${kpis['avg_order_value']:,.2f}", "AOV")
with col4:
    render_metric_card("Fulfillment Rate", f"{kpis['delivery_rate']}%", "Delivered / Closed")

# Main Charts Row
st.markdown("---")
col_left, col_right = st.columns([1.5, 1.0])

with col_left:
    st.plotly_chart(create_revenue_trend_chart(orders_df), use_container_width=True)

with col_right:
    st.plotly_chart(create_payment_donut_chart(payment_df), use_container_width=True)

# Category Performance Row
st.markdown("---")
st.subheader("🛍️ Product Category Performance")
st.plotly_chart(create_category_bar_chart(category_df), use_container_width=True)
