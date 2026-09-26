"""
Interactive Plotly charts and visual components for DataPulse Live dashboard.
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

PLOTLY_DARK_TEMPLATE = "plotly_dark"
COLOR_PALETTE = ["#6366F1", "#10B981", "#F59E0B", "#EC4899", "#8B5CF6", "#3B82F6", "#14B8A6"]


def create_revenue_trend_chart(df: pd.DataFrame) -> go.Figure:
    """Generates time-series line chart for streaming revenue velocity."""
    if df.empty or "order_timestamp" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="Awaiting Streaming Order Data...", template=PLOTLY_DARK_TEMPLATE)
        return fig

    df_sorted = df.sort_values("order_timestamp").copy()
    df_sorted["cumulative_revenue"] = df_sorted["net_amount"].cumsum()

    fig = px.area(
        df_sorted,
        x="order_timestamp",
        y="cumulative_revenue",
        title="📈 Real-Time Cumulative Gross Merchandise Value (GMV)",
        labels={"order_timestamp": "Order Timestamp (UTC)", "cumulative_revenue": "Gross Revenue ($)"},
        color_discrete_sequence=["#6366F1"],
        template=PLOTLY_DARK_TEMPLATE
    )
    fig.update_traces(fillcolor="rgba(99, 102, 241, 0.2)", line=dict(color="#6366F1", width=3))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig


def create_category_bar_chart(df: pd.DataFrame) -> go.Figure:
    """Generates category sales & revenue horizontal bar chart."""
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="No Category Data Available", template=PLOTLY_DARK_TEMPLATE)
        return fig

    fig = px.bar(
        df,
        x="category_revenue",
        y="category",
        orientation="h",
        title="🛍️ Revenue by Product Category",
        labels={"category_revenue": "Revenue ($)", "category": "Product Category"},
        color="category_revenue",
        color_continuous_scale="Viridis",
        template=PLOTLY_DARK_TEMPLATE
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        coloraxis_showscale=False,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig


def create_payment_donut_chart(df: pd.DataFrame) -> go.Figure:
    """Generates donut chart for payment method share."""
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="No Payment Data Available", template=PLOTLY_DARK_TEMPLATE)
        return fig

    fig = px.pie(
        df,
        names="method_type",
        values="total_volume",
        hole=0.55,
        title="💳 Payment Method Volume Share",
        color_discrete_sequence=COLOR_PALETTE,
        template=PLOTLY_DARK_TEMPLATE
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig


def create_geo_scatter_map(df: pd.DataFrame) -> go.Figure:
    """Generates global geographic bubble map of active order locations."""
    if df.empty or "latitude" not in df.columns or "longitude" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="No Geographic Data Available", template=PLOTLY_DARK_TEMPLATE)
        return fig

    fig = px.scatter_geo(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="city",
        size="order_count",
        color="total_revenue",
        projection="natural earth",
        title="🌍 Live Global Order Distribution",
        color_continuous_scale="Plasma",
        template=PLOTLY_DARK_TEMPLATE
    )
    fig.update_geos(
        bgcolor="rgba(0,0,0,0)",
        showocean=True,
        oceancolor="#0F172A",
        showland=True,
        landcolor="#1E293B",
        showcountries=True,
        countrycolor="#334155"
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=10, r=10, t=50, b=10)
    )
    return fig


def create_latency_histogram(df: pd.DataFrame) -> go.Figure:
    """Generates distribution histogram of end-to-end ingestion latency."""
    if df.empty or "ingest_latency_ms" not in df.columns:
        fig = go.Figure()
        fig.update_layout(title="No Latency Metrics Available", template=PLOTLY_DARK_TEMPLATE)
        return fig

    fig = px.histogram(
        df,
        x="ingest_latency_ms",
        nbins=20,
        title="⚡ End-to-End Pipeline Ingestion Latency (ms)",
        labels={"ingest_latency_ms": "Latency (Milliseconds)"},
        color_discrete_sequence=["#10B981"],
        template=PLOTLY_DARK_TEMPLATE
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig
