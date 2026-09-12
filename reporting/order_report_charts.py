# reporting/order_report_charts.py
"""
Plotly visualisations for the Daily Order Report.

Called at the end of the ETL pipeline after the Spark Interface layer
has produced the daily_order_report view. Converts the Spark DataFrame
to pandas and renders two charts:
  1. Daily revenue (bar) with mean revenue overlay (line)
  2. Revenue distribution (histogram)

Output: HTML files written to reporting/output/ — openable in any browser.
"""

import os
import logging

import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def _ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def plot_daily_order_revenue(daily_order_df) -> str:
    """
    Renders a dual-axis chart: total daily revenue (bar) + mean order
    value (line) from the daily_order_report Spark DataFrame.

    Args:
        daily_order_df: Spark DataFrame with columns Date, Revenue, Mean Revenue
                        (output of create_daily_order_report_view).

    Returns:
        str: Absolute path to the saved HTML file.
    """
    _ensure_output_dir()

    # Convert Spark → pandas for Plotly
    pdf = daily_order_df.orderBy("Date").toPandas()

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Bar: total daily revenue
    fig.add_trace(
        go.Bar(
            x=pdf["Date"].astype(str),
            y=pdf["Revenue"],
            name="Total Revenue",
            marker_color="#4C78A8",
            opacity=0.85,
        ),
        secondary_y=False,
    )

    # Line: mean order value
    fig.add_trace(
        go.Scatter(
            x=pdf["Date"].astype(str),
            y=pdf["Mean Revenue"],
            name="Mean Order Value",
            mode="lines+markers",
            line=dict(color="#F58518", width=2),
            marker=dict(size=5),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title=dict(
            text="Rainforest — Daily Order Revenue",
            font=dict(size=20),
        ),
        xaxis=dict(title="Date", tickangle=-45),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        height=500,
    )
    fig.update_yaxes(title_text="Total Revenue ($)", secondary_y=False, gridcolor="#EEEEEE")
    fig.update_yaxes(title_text="Mean Order Value ($)", secondary_y=True, showgrid=False)

    output_path = os.path.join(OUTPUT_DIR, "daily_order_revenue.html")
    fig.write_html(output_path)
    logger.info(f"Order revenue chart saved: {output_path}")
    return output_path


def plot_revenue_distribution(daily_order_df) -> str:
    """
    Renders a histogram of daily total revenue values to show distribution
    and identify outlier days.

    Args:
        daily_order_df: Spark DataFrame with columns Date, Revenue, Mean Revenue.

    Returns:
        str: Absolute path to the saved HTML file.
    """
    _ensure_output_dir()

    pdf = daily_order_df.toPandas()

    fig = go.Figure()

    fig.add_trace(
        go.Histogram(
            x=pdf["Revenue"],
            nbinsx=30,
            name="Revenue Distribution",
            marker_color="#4C78A8",
            opacity=0.80,
        )
    )

    # Overlay mean line
    mean_val = pdf["Revenue"].mean()
    fig.add_vline(
        x=mean_val,
        line_dash="dash",
        line_color="#F58518",
        annotation_text=f"Mean: ${mean_val:,.0f}",
        annotation_position="top right",
    )

    fig.update_layout(
        title=dict(text="Rainforest — Daily Revenue Distribution", font=dict(size=20)),
        xaxis=dict(title="Total Daily Revenue ($)"),
        yaxis=dict(title="Number of Days", gridcolor="#EEEEEE"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        bargap=0.05,
        height=450,
    )

    output_path = os.path.join(OUTPUT_DIR, "revenue_distribution.html")
    fig.write_html(output_path)
    logger.info(f"Revenue distribution chart saved: {output_path}")
    return output_path
