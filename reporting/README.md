# `reporting/`

## Purpose
Plotly visualisation layer — sits at the end of the ETL pipeline, after Spark
has processed and validated all data through Bronze → Silver → Gold →
Interface. Takes the two Interface-layer DataFrames, converts them to pandas,
and renders interactive HTML charts.

This reflects real pipeline design: Spark handles distributed processing at
scale; Plotly handles the final reporting layer where interactivity and visual
clarity matter more than distributed compute.

## Contents

| File | Charts produced |
|---|---|
| `order_report_charts.py` | Daily revenue bar + mean order value line (dual axis); Revenue distribution histogram |
| `category_report_charts.py` | Multi-line time series of mean revenue per category; Horizontal grouped bar comparing mean vs median revenue on the latest date |

## Output

All charts are saved as self-contained HTML files to `reporting/output/`:

| File | Description |
|---|---|
| `daily_order_revenue.html` | Total + mean daily revenue over time |
| `revenue_distribution.html` | Distribution of daily revenue values |
| `category_revenue_over_time.html` | Mean revenue per category across all dates |
| `category_revenue_latest_day.html` | Mean vs median revenue by category for the latest day |

Open any `.html` file in a browser — no server needed. Charts are fully
interactive (hover, zoom, pan, toggle series).

## How Charts Are Generated

`run_etl.py` calls the reporting functions automatically after both Interface
views are created:

```python
from reporting.order_report_charts import (
    plot_daily_order_revenue,
    plot_revenue_distribution,
)
from reporting.category_report_charts import (
    plot_category_revenue_over_time,
    plot_category_revenue_latest_day,
)
```

Charts only run if Plotly is installed. If the import fails (e.g. running in
an environment without Plotly), the pipeline logs a warning and continues — it
never blocks the core ETL.

## Dependencies

`plotly` is included in `spark/requirements.txt` and therefore installed in
the Spark container automatically when running via Docker.

To install locally for development:
```bash
pip install plotly pandas
```
