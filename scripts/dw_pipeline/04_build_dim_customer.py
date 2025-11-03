"""
Gold Layer - Step 03: Build dim_customer (Customer Dimension)

Creates customer dimension with aggregated metrics and segmentation.
"""
from __future__ import annotations
import sys
from pathlib import Path

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from common import get_paths, build_spark, setup_logging


def build_dim_customer(soh: DataFrame, sod: DataFrame) -> DataFrame:
    """
    Build customer dimension from silver layer with aggregated metrics.

    Args:
        soh: Silver layer sales_order_header DataFrame
        sod: Silver layer sales_order_detail DataFrame

    Returns:
        Customer dimension DataFrame
    """
    # Calculate customer-level aggregates

    # First, get total revenue per order (join header with detail)
    order_totals = sod.filter(F.col("IsReturn") == False).groupBy("SalesOrderID").agg(
        F.sum("LineTotal").alias("order_total")
    )

    # Join with header to get customer info
    customer_orders = soh.join(order_totals, "SalesOrderID", "inner")

    # Aggregate by customer
    customer_agg = customer_orders.groupBy("CustomerID").agg(
        F.count("SalesOrderID").alias("total_orders"),
        F.sum("order_total").alias("total_revenue"),
        F.avg("order_total").alias("avg_order_value"),
        F.min("OrderDate").alias("first_order_date"),
        F.max("OrderDate").alias("last_order_date"),
        F.first("AccountNumber").alias("account_number")  # Assuming account number doesn't change
    )

    # Calculate days since last order (for recency analysis)
    customer_agg = customer_agg.withColumn(
        "days_since_last_order",
        F.datediff(F.current_date(), F.col("last_order_date"))
    )

    # Customer segmentation based on RFM (Recency, Frequency, Monetary)
    # For simplicity, we'll use revenue and order count

    # Define percentile windows for segmentation
    window = Window.orderBy(F.col("total_revenue"))

    customer_agg = customer_agg.withColumn(
        "revenue_percentile",
        F.percent_rank().over(window) * 100
    )

    # Segment customers
    customer_agg = customer_agg.withColumn(
        "customer_segment",
        F.when(F.col("revenue_percentile") >= 80, "High Value")  # Top 20%
         .when(F.col("revenue_percentile") >= 50, "Medium Value")  # 50-80%
         .when(F.col("revenue_percentile") >= 20, "Regular")  # 20-50%
         .otherwise("Low Value")  # Bottom 20%
    )

    # Active customer flag (ordered in last 180 days)
    customer_agg = customer_agg.withColumn(
        "is_active",
        F.col("days_since_last_order") <= 180
    )

    # Customer lifecycle stage
    customer_agg = customer_agg.withColumn(
        "lifecycle_stage",
        F.when(F.col("total_orders") == 1, "New")
         .when((F.col("total_orders") >= 2) & (F.col("is_active") == True), "Active")
         .when((F.col("total_orders") >= 2) & (F.col("is_active") == False), "At Risk")
         .otherwise("Unknown")
    )

    # Create final dimension
    dim_customer = customer_agg.select(
        F.col("CustomerID").alias("customer_key"),  # PK
        F.col("CustomerID").alias("customer_id"),    # Natural key
        F.col("account_number"),
        F.col("customer_segment"),
        F.col("lifecycle_stage"),
        F.col("first_order_date"),
        F.col("last_order_date"),
        F.col("days_since_last_order"),
        F.col("total_orders"),
        F.col("total_revenue"),
        F.col("avg_order_value"),
        F.col("is_active")
    )

    return dim_customer.orderBy("customer_key")


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.gold.dim_customer")
    paths = get_paths(script_file)
    spark = build_spark("upstart_gold_dim_customer", paths)

    log.info("Building dim_customer (Customer Dimension)...")

    # Read silver layer
    soh = spark.read.parquet(str(paths["store"] / "store_sales_order_header"))
    sod = spark.read.parquet(str(paths["store"] / "store_sales_order_detail"))

    # Build dimension
    dim_customer = build_dim_customer(soh, sod)

    # Log statistics
    total_customers = dim_customer.count()

    log.info(f"Total customers: {total_customers:,}")

    # Segment distribution
    log.info("Customer distribution by segment:")
    segment_dist = dim_customer.groupBy("customer_segment").agg(
        F.count("*").alias("customer_count"),
        F.sum("total_revenue").alias("segment_revenue"),
        F.avg("total_orders").alias("avg_orders")
    ).orderBy(F.desc("segment_revenue"))
    segment_dist.show()

    # Lifecycle stage distribution
    log.info("Customer distribution by lifecycle stage:")
    lifecycle_dist = dim_customer.groupBy("lifecycle_stage").count().orderBy(F.desc("count"))
    lifecycle_dist.show()

    # Active vs inactive
    active_count = dim_customer.filter(F.col("is_active") == True).count()
    inactive_count = total_customers - active_count
    log.info(f"Active customers: {active_count:,} ({(active_count/total_customers)*100:.1f}%)")
    log.info(f"Inactive customers: {inactive_count:,} ({(inactive_count/total_customers)*100:.1f}%)")

    # Revenue statistics
    log.info("Customer revenue statistics:")
    dim_customer.select("total_revenue").summary("min", "max", "mean", "stddev").show()

    # Sample records
    log.info("Sample customer records (top 10 by revenue):")
    dim_customer.select(
        "customer_key", "customer_segment", "lifecycle_stage",
        "total_orders", "total_revenue", "is_active"
    ).orderBy(F.desc("total_revenue")).show(10)

    # Write to Gold layer (publish)
    publish_path = paths["publish"] / "dim_customer"
    dim_customer.write.mode("overwrite").parquet(str(publish_path))

    log.info(f"dim_customer written to {publish_path}")
    log.info("dim_customer build completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
