"""
Page 4: Data Quality & Quarantine Analytics Dashboard.
Displays validation audit results, quarantine failure code breakdown, and corrupt payload inspector.
"""
import json
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dashboard.components.ui_helpers import apply_custom_css, render_metric_card
from dashboard.db_queries import get_quarantine_metrics, get_dq_audit_history

st.set_page_config(page_title="Data Quality & Quarantine | DataPulse", page_icon="🛡️", layout="wide")
apply_custom_css()

st.title("🛡️ Data Quality & Quarantine Dashboard")
st.markdown("Automated schema enforcement, quarantine routing, and Airflow reconciliation results.")

summary_df, detail_df, stats = get_quarantine_metrics()
audit_df = get_dq_audit_history()

# KPI Metric Row
col1, col2, col3 = st.columns(3)
with col1:
    pass_status = "success" if stats["pass_rate"] >= 95.0 else "warning"
    render_metric_card("Stream Validation Pass Rate", f"{stats['pass_rate']}%", "Schema Conformance", status=pass_status)
with col2:
    render_metric_card("Valid Persisted Orders", f"{stats['valid_count']:,}", "Passed Fact Table Checks")
with col3:
    render_metric_card("Quarantined Corrupt Events", f"{stats['quarantined_count']:,}", "Routed to Dead-Letter Quarantine", status="danger")

# Quarantine Failure Breakdown Chart
st.markdown("---")
col_chart, col_gauge = st.columns([1.5, 1.0])

with col_chart:
    st.subheader("⚠️ Rejection Reasons Breakdown")
    if not summary_df.empty:
        fig = px.bar(
            summary_df,
            x="failure_count",
            y="error_code",
            orientation="h",
            color="failure_count",
            color_continuous_scale="Reds",
            template="plotly_dark",
            labels={"failure_count": "Rejections Count", "error_code": "Violation Code"}
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif"),
            coloraxis_showscale=False,
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.success("🎉 No quarantine violations recorded! All incoming events passed validation.")

with col_gauge:
    st.subheader("🎯 Data Quality Index")
    gauge_fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=stats["pass_rate"],
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Schema Health Score", 'font': {'size': 18, 'color': '#FFFFFF'}},
        gauge={
            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "white"},
            'bar': {'color': "#10B981" if stats["pass_rate"] >= 95 else "#F59E0B"},
            'bgcolor': "#1E293B",
            'borderwidth': 2,
            'bordercolor': "#334155",
            'steps': [
                {'range': [0, 80], 'color': 'rgba(239, 68, 68, 0.3)'},
                {'range': [80, 95], 'color': 'rgba(245, 158, 11, 0.3)'},
                {'range': [95, 100], 'color': 'rgba(16, 185, 129, 0.3)'}
            ]
        }
    ))
    gauge_fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        height=280,
        margin=dict(l=20, r=20, t=30, b=20)
    )
    st.plotly_chart(gauge_fig, use_container_width=True)

# Live Quarantine Record Explorer
st.markdown("---")
st.subheader("🕵️ Quarantine Record Explorer (Raw Payloads)")
if not detail_df.empty:
    st.dataframe(
        detail_df[["quarantine_id", "event_id", "error_code", "rejection_reason", "quarantined_at"]],
        use_container_width=True,
        hide_index=True
    )
    
    selected_qid = st.selectbox("Inspect Full Raw JSON Payload for Quarantine ID:", detail_df["quarantine_id"].tolist())
    if selected_qid:
        raw_row = detail_df[detail_df["quarantine_id"] == selected_qid].iloc[0]
        try:
            parsed_json = json.loads(raw_row["raw_payload"])
            st.json(parsed_json)
        except Exception:
            st.code(raw_row["raw_payload"], language="json")
else:
    st.info("No quarantined records to inspect.")

# Airflow DQ Audit History
st.markdown("---")
st.subheader("📋 Airflow Scheduled Data Quality Audit Log")
if not audit_df.empty:
    st.dataframe(audit_df, use_container_width=True, hide_index=True)
else:
    st.info("No Airflow audit runs logged yet. Execute the DQ reconciliation DAG to populate.")
