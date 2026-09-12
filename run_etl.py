# run_etl.py
"""
Pipeline entrypoint — submitted to the Spark cluster via spark-submit.

Execution order:
  1. Daily Category Report  (Bronze → Silver → Gold → Interface)
  2. Daily Order Report     (Bronze → Silver → Gold → Interface)
  3. Plotly reporting layer (Spark DataFrames → interactive HTML charts)
"""

import logging

from pyspark.sql import SparkSession

from etl.layers.gold.daily_category_metrics import DailyCategoryMetricsGoldETL
from etl.layers.gold.daily_order_metrics import DailyOrderMetricsGoldETL
from etl.layers.interface.daily_category_report import create_daily_category_report_view
from etl.layers.interface.daily_order_report import create_daily_order_report_view

logger = logging.getLogger(__name__)

# Plotly reporting is optional — pipeline never blocks if it is unavailable
try:
    from reporting.order_report_charts import (
        plot_daily_order_revenue,
        plot_revenue_distribution,
    )
    from reporting.category_report_charts import (
        plot_category_revenue_over_time,
        plot_category_revenue_latest_day,
    )
    PLOTLY_AVAILABLE = True
except ImportError:
    logger.warning("Plotly not available — skipping chart generation.")
    PLOTLY_AVAILABLE = False


def run_etl(spark: SparkSession) -> None:
    """
    Runs the full ETL pipeline and generates Plotly reports.

    Args:
        spark: Active SparkSession.
    """

    # ------------------------------------------------------------------ #
    # Daily Category Report                                                #
    # ------------------------------------------------------------------ #
    print("===================================")
    print("Daily Category Report")
    print("===================================")

    daily_category_metrics = DailyCategoryMetricsGoldETL(spark=spark)
    daily_category_metrics.run()

    category_df = daily_category_metrics.read().current_data
    create_daily_category_report_view(category_df)

    category_report_df = spark.sql("SELECT * FROM global_temp.daily_category_report")
    category_report_df.show()

    # ------------------------------------------------------------------ #
    # Daily Order Report                                                   #
    # ------------------------------------------------------------------ #
    print("===================================")
    print("Daily Order Report")
    print("===================================")

    daily_orders_metrics = DailyOrderMetricsGoldETL(spark=spark)
    daily_orders_metrics.run()

    order_df = daily_orders_metrics.read().current_data
    create_daily_order_report_view(order_df)

    order_report_df = spark.sql("SELECT * FROM global_temp.daily_order_report")
    order_report_df.show()

    # ------------------------------------------------------------------ #
    # Plotly Reporting Layer                                               #
    # Converts validated Spark DataFrames → interactive HTML charts.      #
    # Plotly works on pandas — toPandas() runs after all Spark processing #
    # is complete so we never trigger unnecessary Spark actions early.     #
    # ------------------------------------------------------------------ #
    if PLOTLY_AVAILABLE:
        print("===================================")
        print("Generating Plotly Charts")
        print("===================================")

        try:
            # Order report charts
            p1 = plot_daily_order_revenue(order_report_df)
            p2 = plot_revenue_distribution(order_report_df)

            # Category report charts
            p3 = plot_category_revenue_over_time(category_report_df)
            p4 = plot_category_revenue_latest_day(category_report_df)

            print(f"Charts saved to reporting/output/:")
            for path in [p1, p2, p3, p4]:
                print(f"  {path}")

        except Exception as e:
            logger.error(f"Chart generation failed: {e}. ETL results are unaffected.")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Rainforest Data Pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    run_etl(spark)
