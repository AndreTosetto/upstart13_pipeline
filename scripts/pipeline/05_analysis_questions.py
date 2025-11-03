"""
Gold layer: Answer the two required analysis questions.

1. Which color generated the highest revenue each year?
2. What's the average lead time by product category?

Results go into analysis_top_color_by_year and analysis_avg_lead_by_category.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F, Window  # noqa: E402
from common import get_paths, build_spark, setup_logging  # noqa: E402


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.analysis")
    paths = get_paths(script_file)
    spark = build_spark("upstart_analysis_outputs", paths)

    orders_path = paths["publish"] / "publish_orders"
    products_path = paths["publish"] / "publish_product"

    log.info(f"Reading publish_orders from {orders_path}")
    orders = spark.read.parquet(str(orders_path))

    log.info(f"Reading publish_product from {products_path}")
    products = spark.read.parquet(str(products_path))

    # Join to get product attributes (Color, Category)
    joined = orders.join(
        products.select("ProductID", "Color", "ProductCategoryName"),
        on="ProductID",
        how="left"
    )

    # Extract year from OrderDate for grouping
    if "OrderDate" in joined.columns:
        joined = joined.withColumn("OrderYear", F.year("OrderDate"))
    else:
        log.warning("OrderDate missing - OrderYear will be null")

    # Question 1: Which color had highest revenue each year?
    log.info("Computing top color by revenue per year...")
    color_revenue = (
        joined.groupBy("OrderYear", "Color")
        .agg(F.sum("TotalLineExtendedPrice").alias("Revenue"))
    )

    # Rank colors within each year by revenue
    window = Window.partitionBy("OrderYear").orderBy(F.col("Revenue").desc_nulls_last())
    top_color = (
        color_revenue
        .withColumn("rank", F.row_number().over(window))
        .filter("rank = 1")
        .drop("rank")
    )

    top_color_path = paths["publish"] / "analysis_top_color_by_year"
    log.info(f"Writing analysis_top_color_by_year to {top_color_path}")
    top_color.write.mode("overwrite").parquet(str(top_color_path))

    # Question 2: Average lead time by product category
    log.info("Computing average lead time by category...")
    avg_lead = (
        joined.groupBy("ProductCategoryName")
        .agg(F.avg("LeadTimeInBusinessDays").alias("AvgLeadTimeInBusinessDays"))
    )

    avg_lead_path = paths["publish"] / "analysis_avg_lead_by_category"
    log.info(f"Writing analysis_avg_lead_by_category to {avg_lead_path}")
    avg_lead.write.mode("overwrite").parquet(str(avg_lead_path))

    log.info("Analysis outputs done")
    spark.stop()


if __name__ == "__main__":
    main()
