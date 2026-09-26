"""
UI styling components and CSS utilities for DataPulse Live Streamlit dashboard.
"""
import streamlit as st


def apply_custom_css():
    """Injects high-end modern dark styling, glowing KPI cards, and sleek typography."""
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

        html, body, [class*="css"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        /* Top Header Styling */
        .main-header {
            background: linear-gradient(135deg, #1E1E38 0%, #0F0F1E 100%);
            border: 1px solid rgba(255, 255, 255, 0.1);
            padding: 1.5rem 2rem;
            border-radius: 16px;
            margin-bottom: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        }

        .main-header h1 {
            color: #FFFFFF;
            font-weight: 800;
            font-size: 2.2rem;
            letter-spacing: -0.5px;
            margin-bottom: 0.2rem;
        }

        .main-header p {
            color: #94A3B8;
            font-size: 1.05rem;
            margin-bottom: 0;
        }

        /* Glassmorphism Metric Cards */
        .metric-card {
            background: rgba(30, 30, 50, 0.6);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 14px;
            padding: 1.25rem 1.5rem;
            margin-bottom: 1rem;
            transition: transform 0.2s ease, border-color 0.2s ease;
        }

        .metric-card:hover {
            transform: translateY(-2px);
            border-color: rgba(99, 102, 241, 0.4);
        }

        .metric-label {
            color: #94A3B8;
            font-size: 0.85rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            margin-bottom: 0.4rem;
        }

        .metric-value {
            color: #F8FAFC;
            font-size: 1.9rem;
            font-weight: 700;
            font-family: 'JetBrains Mono', monospace;
            margin-bottom: 0.2rem;
        }

        .metric-subtext {
            color: #10B981;
            font-size: 0.85rem;
            font-weight: 500;
        }

        .metric-subtext.warning {
            color: #F59E0B;
        }

        .metric-subtext.danger {
            color: #EF4444;
        }

        /* Glowing Live Badges */
        .badge-live {
            display: inline-flex;
            align-items: center;
            background: rgba(16, 185, 129, 0.15);
            border: 1px solid rgba(16, 185, 129, 0.4);
            color: #10B981;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.85rem;
            font-weight: 600;
            letter-spacing: 0.5px;
        }

        .badge-live::before {
            content: "";
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: #10B981;
            margin-right: 6px;
            box-shadow: 0 0 10px #10B981;
            animation: pulse 1.8s infinite;
        }

        @keyframes pulse {
            0% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.4; transform: scale(1.3); }
            100% { opacity: 1; transform: scale(1); }
        }

        /* Status Pills */
        .status-pill {
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .status-healthy { background: rgba(16, 185, 129, 0.2); color: #10B981; }
        .status-warning { background: rgba(245, 158, 11, 0.2); color: #F59E0B; }
        .status-error { background: rgba(239, 68, 68, 0.2); color: #EF4444; }
        </style>
    """, unsafe_allow_html=True)


def render_metric_card(label: str, value: str, subtext: str = "", status: str = "success"):
    """Renders a custom glassmorphic KPI metric card."""
    subtext_class = "metric-subtext"
    if status == "warning":
        subtext_class += " warning"
    elif status == "danger":
        subtext_class += " danger"

    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="{subtext_class}">{subtext}</div>
        </div>
    """, unsafe_allow_html=True)
