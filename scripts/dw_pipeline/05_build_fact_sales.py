"""
Gold Layer - Step 04: Build fact_sales (Sales Fact Table)

Creates the central fact table with all metrics and foreign keys to dimensions.
Grain: One row per order line item (sales_order_detail).
"""
from __future__ import annotations
import sys
from pathlib import Path

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from common import get_paths, build_spark, setup_logging


def build_fact_sales(soh: DataFrame, sod: DataFrame, products: DataFrame) -> DataFrame:
    """
    Build sales fact table from silver layer tables.

    Args:
        soh: Silver layer sales_order_header DataFrame
        sod: Silver layer sales_order_detail DataFrame
        products: Silver layer products DataFrame (for standard_cost)

    Returns:
        Sales fact table DataFrame
    """
    # Start with sales_order_detail (grain of fact table)
    fact = sod.alias("sod")

    # Join with sales_order_header to get dates, customer, salesperson
    fact = fact.join(
        soh.alias("soh"),
        F.col("sod.SalesOrderID") == F.col("soh.SalesOrderID"),
        "inner"
    )

    # Join with products to get standard_cost for profit calculation
    fact = fact.join(
        products.select("ProductID", "StandardCost").alias("p"),
        F.col("sod.ProductID") == F.col("p.ProductID"),
        "left"  # Left join in case product doesn't exist (shouldn't happen with FK integrity)
    )

    # Generate date keys (format: YYYYMMDD)
    fact = fact.withColumn(
        "order_date_key",
        F.date_format(F.col("soh.OrderDate"), "yyyyMMdd").cast(T.IntegerType())
    )

    fact = fact.withColumn(
        "ship_date_key",
        F.date_format(F.col("soh.ShipDate"), "yyyyMMdd").cast(T.IntegerType())
    )

    # Foreign keys to dimensions
    fact = fact.withColumn("product_key", F.col("sod.ProductID"))
    fact = fact.withColumn("customer_key", F.col("soh.CustomerID"))

    # For salesperson_key: NULL values will need special handling
    # We'll use -1 for NULL (representing online/no salesperson)
    fact = fact.withColumn(
        "salesperson_key",
        F.coalesce(F.col("soh.SalesPersonID"), F.lit(-1))
    )

    # Degenerate dimensions (attributes that don't warrant separate dimension)
    fact = fact.withColumn("sales_order_id", F.col("sod.SalesOrderID"))
    fact = fact.withColumn("sales_order_detail_id", F.col("sod.SalesOrderDetailID"))

    # Metrics (measures)
    fact = fact.withColumn("order_quantity", F.col("sod.OrderQty"))
    fact = fact.withColumn("unit_price", F.col("sod.UnitPrice"))
    fact = fact.withColumn("unit_price_discount", F.col("sod.UnitPriceDiscount"))
    fact = fact.withColumn("line_total", F.col("sod.LineTotal"))
    fact = fact.withColumn("is_return", F.col("sod.IsReturn"))

    # Calculate freight allocated to each line item
    # Strategy: Divide order freight equally among line items
    from pyspark.sql.window import Window

    # Count line items per order
    window_order = Window.partitionBy("sod.SalesOrderID")

    fact = fact.withColumn(
        "line_items_in_order",
        F.count("*").over(window_order)
    )

    # Allocate freight
    fact = fact.withColumn(
        "freight_allocated",
        F.col("soh.Freight") / F.col("line_items_in_order")
    )

    # Calculate standard cost total
    fact = fact.withColumn(
        "standard_cost_total",
        F.col("p.StandardCost") * F.col("order_quantity")
    )

    # Calculate gross profit
    # gross_profit = line_total - standard_cost_total
    fact = fact.withColumn(
        "gross_profit",
        F.col("line_total") - F.col("standard_cost_total")
    )

    # Calculate discount amount
    # discount_amount = unit_price * quantity * discount_rate
    fact = fact.withColumn(
        "discount_amount",
        F.col("unit_price") * F.col("order_quantity") * F.col("unit_price_discount")
    )

    # Select final columns
    fact_sales = fact.select(
        # Foreign keys
        "order_date_key",
        "ship_date_key",
        "product_key",
        "customer_key",
        "salesperson_key",

        # Degenerate dimensions
        "sales_order_id",
        "sales_order_detail_id",

        # Metrics
        "order_quantity",
        "unit_price",
        "unit_price_discount",
        "discount_amount",
        "line_total",
        "freight_allocated",
        "standard_cost_total",
        "gross_profit",
        "is_return"
    )

    return fact_sales


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.gold.fact_sales")
    paths = get_paths(script_file)
    spark = build_spark("upstart_gold_fact_sales", paths)

    log.info("Building fact_sales (Sales Fact Table)...")

    # Read silver layer
    soh = spark.read.parquet(str(paths["store"] / "store_sales_order_header"))
    sod = spark.read.parquet(str(paths["store"] / "store_sales_order_detail"))
    products = spark.read.parquet(str(paths["store"] / "store_products"))

    # Build fact table
    fact_sales = build_fact_sales(soh, sod, products)

    # Log statistics
    total_records = fact_sales.count()

    log.info(f"Total fact records: {total_records:,}")

    # Sales vs Returns
    sales_count = fact_sales.filter(F.col("is_return") == False).count()
    returns_count = fact_sales.filter(F.col("is_return") == True).count()

    log.info(f"Sales records: {sales_count:,}")
    log.info(f"Return records: {returns_count:,}")

    # Revenue metrics
    metrics = fact_sales.filter(F.col("is_return") == False).agg(
        F.sum("line_total").alias("total_revenue"),
        F.sum("gross_profit").alias("total_gross_profit"),
        F.sum("discount_amount").alias("total_discounts"),
        F.sum("freight_allocated").alias("total_freight")
    ).collect()[0]

    log.info(f"Total revenue: ${metrics['total_revenue']:,.2f}")
    log.info(f"Total gross profit: ${metrics['total_gross_profit']:,.2f}")
    log.info(f"Total discounts: ${metrics['total_discounts']:,.2f}")
    log.info(f"Total freight: ${metrics['total_freight']:,.2f}")

    # Margin calculation
    if metrics['total_revenue'] > 0:
        margin_pct = (metrics['total_gross_profit'] / metrics['total_revenue']) * 100
        log.info(f"Overall gross margin: {margin_pct:.2f}%")

    # Profitability statistics
    log.info("Gross profit statistics:")
    fact_sales.filter(F.col("is_return") == False).select("gross_profit").summary("min", "max", "mean", "stddev").show()

    # Sample records
    log.info("Sample fact records:")
    fact_sales.select(
        "sales_order_id", "product_key", "customer_key",
        "order_quantity", "unit_price", "line_total", "gross_profit", "is_return"
    ).show(10)

    # Verify foreign key coverage
    log.info("Verifying foreign key coverage...")

    # Check for NULL foreign keys (except salesperson which can be -1)
    null_checks = {
        "order_date_key": fact_sales.filter(F.col("order_date_key").isNull()).count(),
        "ship_date_key": fact_sales.filter(F.col("ship_date_key").isNull()).count(),
        "product_key": fact_sales.filter(F.col("product_key").isNull()).count(),
        "customer_key": fact_sales.filter(F.col("customer_key").isNull()).count()
    }

    all_good = True
    for key, null_count in null_checks.items():
        if null_count > 0:
            log.warning(f"  {key}: {null_count} NULL values found!")
            all_good = False
        else:
            log.info(f"  {key}: OK (no NULLs)")

    if all_good:
        log.info("All foreign keys properly populated")

    # Write to Gold layer (publish) - partitioned by year and month for performance
    log.info("Writing fact_sales to Gold layer (this may take a moment)...")

    publish_path = paths["publish"] / "fact_sales"

    # Add partition columns for better query performance
    fact_sales_partitioned = fact_sales.withColumn(
        "year",
        (F.col("order_date_key") / 10000).cast(T.IntegerType())
    ).withColumn(
        "month",
        ((F.col("order_date_key") % 10000) / 100).cast(T.IntegerType())
    )

    # Write partitioned by year and month
    fact_sales_partitioned.write.mode("overwrite").partitionBy("year", "month").parquet(str(publish_path))

    log.info(f"fact_sales written to {publish_path}")
    log.info("fact_sales build completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
