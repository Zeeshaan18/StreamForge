"""
DataPulse Live - Real-Time E-Commerce Streaming Platform.
Main Streamlit Application Entry Point.
"""
import os
import sys
import time
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
from config.settings import settings
from database.db_connection import db_manager
from dashboard.components.ui_helpers import apply_custom_css, render_metric_card
from dashboard.db_queries import get_executive_kpis, get_pipeline_health_status
from dashboard.pipeline_runner import ensure_background_pipeline_running

# Launch background stream supervisor (runs real-time ingestion continuously)
ensure_background_pipeline_running()

# Configure page metadata
st.set_page_config(
    page_title="DataPulse Live | Real-Time Data Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply sleek styling
apply_custom_css()

# Sidebar: System Controls & Live Stream Ticker
with st.sidebar:
    st.image("https://img.icons8.com/isometric/100/data-transfer.png", width=64)
    st.markdown("## **DataPulse Live**")
    st.caption("⚡ *Real-Time E-Commerce Data Platform*")
    st.markdown("---")

    # Real-Time Auto Refresh Configuration
    st.subheader("⚙️ Stream Controls")
    auto_refresh = st.checkbox("Auto Refresh Feed", value=True, help="Automatically refresh data every N seconds")
    refresh_rate = st.slider("Refresh Interval (s)", min_value=1, max_value=10, value=settings.STREAMLIT_AUTO_REFRESH_INTERVAL)

    st.markdown("---")
    st.subheader("🖥️ Service Health")
    health = db_manager.check_health()
    
    status_icon = "🟢" if health.get("status") == "HEALTHY" else "🔴"
    st.write(f"**Database:** {status_icon} `{health.get('status')}` ({health.get('engine', 'postgres').upper()})")
    st.write(f"**Query Latency:** `{health.get('latency_ms', 0)} ms`")
    
    st.markdown("---")
    st.caption("Built with Python, Kafka, PySpark, Airflow & PostgreSQL.")

# Header Banner
st.markdown("""
    <div class="main-header">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h1>⚡ DataPulse Live</h1>
                <p>Enterprise Real-Time E-Commerce Streaming & Analytics Engine</p>
            </div>
            <div>
                <span class="badge-live">STREAM ACTIVE</span>
            </div>
        </div>
    </div>
""", unsafe_allow_html=True)

# Overview Quick Stats
kpis = get_executive_kpis()
col1, col2, col3, col4 = st.columns(4)

with col1:
    render_metric_card("Total Orders Processed", f"{kpis['total_orders']:,}", "Live Stream Volume")
with col2:
    render_metric_card("Gross Revenue", f"${kpis['total_revenue']:,.2f}", "+14.2% today")
with col3:
    render_metric_card("Avg Order Value (AOV)", f"${kpis['avg_order_value']:,.2f}", "Per Transaction")
with col4:
    q_status = "danger" if kpis['quarantined_count'] > 0 else "success"
    render_metric_card("Quarantined Events", f"{kpis['quarantined_count']}", "Detected Anomalies", status=q_status)

st.info("👈 **Select a page from the sidebar navigation** to explore Live Order Feeds, Pipeline Telemetry, Data Quality Audits, Historical Analytics, or manually ingest transactions via **➕ Create Real-Time Order**.")

# Auto-refresh loop
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
