# reporting/category_report_charts.py
"""
Plotly visualisations for the Daily Category Report.

Called at the end of the ETL pipeline after the Spark Interface layer
has produced the daily_category_report view. Converts the Spark DataFrame
to pandas and renders two charts:
  1. Mean revenue by category over time (multi-line time series)
  2. Category revenue comparison — latest day (horizontal bar)

Output: HTML files written to reporting/output/ — openable in any browser.
"""

import os
import logging

import plotly.graph_objects as go
import plotly.express as px

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# Consistent colour palette across charts
COLOUR_PALETTE = px.colors.qualitative.Safe


def _ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def plot_category_revenue_over_time(daily_category_df) -> str:
    """
    Renders a multi-line time series of mean revenue per product category
    across all available dates.

    Args:
        daily_category_df: Spark DataFrame with columns Date, Product Category,
                           Mean Revenue, Median Revenue
                           (output of create_daily_category_report_view).

    Returns:
        str: Absolute path to the saved HTML file.
    """
    _ensure_output_dir()

    pdf = daily_category_df.orderBy("Date").toPandas()
    categories = sorted(pdf["Product Category"].unique())

    fig = go.Figure()

    for i, category in enumerate(categories):
        cat_data = pdf[pdf["Product Category"] == category]
        colour = COLOUR_PALETTE[i % len(COLOUR_PALETTE)]

        fig.add_trace(
            go.Scatter(
                x=cat_data["Date"].astype(str),
                y=cat_data["Mean Revenue"],
                name=category,
                mode="lines+markers",
                line=dict(color=colour, width=2),
                marker=dict(size=4),
                hovertemplate=(
                    f"<b>{category}</b><br>"
                    "Date: %{x}<br>"
                    "Mean Revenue: $%{y:,.2f}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title=dict(
            text="Rainforest — Mean Revenue by Product Category (Daily)",
            font=dict(size=20),
        ),
        xaxis=dict(title="Date", tickangle=-45),
        yaxis=dict(title="Mean Revenue ($)", gridcolor="#EEEEEE"),
        legend=dict(title="Category", orientation="v"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        hovermode="x unified",
        height=550,
    )

    output_path = os.path.join(OUTPUT_DIR, "category_revenue_over_time.html")
    fig.write_html(output_path)
    logger.info(f"Category time-series chart saved: {output_path}")
    return output_path


def plot_category_revenue_latest_day(daily_category_df) -> str:
    """
    Renders a horizontal bar chart comparing mean and median revenue
    across all product categories for the latest available date.

    Args:
        daily_category_df: Spark DataFrame with columns Date, Product Category,
                           Mean Revenue, Median Revenue.

    Returns:
        str: Absolute path to the saved HTML file.
    """
    _ensure_output_dir()

    pdf = daily_category_df.toPandas()

    # Latest date available
    latest_date = pdf["Date"].max()
    latest = pdf[pdf["Date"] == latest_date].sort_values("Mean Revenue", ascending=True)

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=latest["Product Category"],
            x=latest["Mean Revenue"],
            name="Mean Revenue",
            orientation="h",
            marker_color="#4C78A8",
            opacity=0.85,
        )
    )

    fig.add_trace(
        go.Bar(
            y=latest["Product Category"],
            x=latest["Median Revenue"],
            name="Median Revenue",
            orientation="h",
            marker_color="#F58518",
            opacity=0.85,
        )
    )

    fig.update_layout(
        title=dict(
            text=f"Rainforest — Revenue by Category ({latest_date})",
            font=dict(size=20),
        ),
        xaxis=dict(title="Revenue ($)", gridcolor="#EEEEEE"),
        yaxis=dict(title="Product Category"),
        barmode="group",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=500,
    )

    output_path = os.path.join(OUTPUT_DIR, "category_revenue_latest_day.html")
    fig.write_html(output_path)
    logger.info(f"Category latest-day chart saved: {output_path}")
    return output_path
