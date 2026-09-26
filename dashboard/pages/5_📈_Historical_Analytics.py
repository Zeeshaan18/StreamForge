"""
Page 5: Historical Analytics & Analytical Mart Explorer.
Explores aggregated hourly trends, customer segment lifetime value, and cohort spending.
"""
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.components.ui_helpers import apply_custom_css, render_metric_card
from database.db_connection import db_manager

st.set_page_config(page_title="Historical Analytics | DataPulse", page_icon="📈", layout="wide")
apply_custom_css()

st.title("📈 Historical Analytics & Analytical Marts")
st.markdown("Longitudinal performance analysis derived from `hourly_order_aggregates` and dimension joins.")

# Customer Segment Revenue Breakdown
cust_query = """
SELECT 
    c.segment,
    COUNT(f.order_id) as order_count,
    SUM(f.net_amount) as total_revenue,
    AVG(f.net_amount) as avg_order_value,
    SUM(f.item_count) as total_items
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.segment
ORDER BY total_revenue DESC
"""
cust_df = pd.DataFrame(db_manager.execute_query(cust_query))

# Hourly Rollup Mart Data
mart_query = """
SELECT 
    hourly_window_start,
    total_orders,
    total_revenue,
    avg_order_value,
    total_items_sold,
    successful_orders,
    cancelled_orders
FROM hourly_order_aggregates
ORDER BY hourly_window_start DESC
LIMIT 48
"""
mart_df = pd.DataFrame(db_manager.execute_query(mart_query))

# Order Status Distribution
status_query = """
SELECT 
    order_status,
    COUNT(*) as order_count,
    SUM(net_amount) as status_revenue
FROM fact_orders
GROUP BY order_status
ORDER BY order_count DESC
"""
status_df = pd.DataFrame(db_manager.execute_query(status_query))

# Charts Layout
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("👥 Revenue by Customer Segment")
    if not cust_df.empty:
        fig_cust = px.bar(
            cust_df,
            x="segment",
            y="total_revenue",
            color="segment",
            text_auto=".2s",
            template="plotly_dark",
            title="Gross Revenue by Customer Segment ($)",
            labels={"total_revenue": "Revenue ($)", "segment": "Customer Tier"}
        )
        fig_cust.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif"),
            showlegend=False
        )
        st.plotly_chart(fig_cust, use_container_width=True)
    else:
        st.info("No customer data available.")

with col_right:
    st.subheader("📦 Order Fulfillment Distribution")
    if not status_df.empty:
        fig_status = px.pie(
            status_df,
            names="order_status",
            values="order_count",
            hole=0.45,
            template="plotly_dark",
            title="Order Status Breakdown",
            color_discrete_sequence=["#10B981", "#3B82F6", "#F59E0B", "#8B5CF6", "#EF4444"]
        )
        fig_status.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif")
        )
        st.plotly_chart(fig_status, use_container_width=True)
    else:
        st.info("No status data available.")

# Mart Summary Table
st.markdown("---")
st.subheader("🏛️ Analytical Mart: Hourly Order Aggregates")
if not mart_df.empty:
    st.dataframe(mart_df, use_container_width=True, hide_index=True)
else:
    st.info("No aggregate rollups generated yet. Run the hourly aggregate DAG to populate.")
