"""
Gold Layer - Step 02: Build dim_product (Product Dimension)

Creates product dimension with all attributes and calculated fields like profit margin.
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


def build_dim_product(products: DataFrame) -> DataFrame:
    """
    Build product dimension from silver layer products table.

    Args:
        products: Silver layer products DataFrame

    Returns:
        Product dimension DataFrame
    """
    # Create dimension with all attributes
    dim_product = products.select(
        F.col("ProductID").alias("product_key"),  # PK
        F.col("ProductID").alias("product_id"),    # Natural key
        F.col("ProductDesc").alias("product_desc"),
        F.col("ProductNumber").alias("product_number"),
        F.col("ProductCategoryName").alias("product_category"),
        F.col("ProductSubCategoryName").alias("product_subcategory"),
        F.col("MakeFlag").alias("make_flag"),
        F.col("Color").alias("color"),
        F.col("ListPrice").alias("list_price"),
        F.col("StandardCost").alias("standard_cost"),
        F.col("Weight").alias("weight"),
        F.col("WeightUnitMeasureCode").alias("weight_unit"),
        F.col("Size").alias("size"),
        F.col("SizeUnitMeasureCode").alias("size_unit"),
        F.col("SafetyStockLevel").alias("safety_stock_level"),
        F.col("ReorderPoint").alias("reorder_point")
    )

    # Calculate profit margin percentage
    # profit_margin_pct = (list_price - standard_cost) / list_price * 100
    dim_product = dim_product.withColumn(
        "profit_margin_pct",
        F.when(
            (F.col("list_price").isNotNull()) & (F.col("list_price") > 0),
            ((F.col("list_price") - F.col("standard_cost")) / F.col("list_price")) * 100
        ).otherwise(None)
    )

    # Add product category hierarchy level
    # Useful for drill-down analysis
    dim_product = dim_product.withColumn(
        "category_hierarchy",
        F.concat(
            F.col("product_category"),
            F.lit(" > "),
            F.col("product_subcategory")
        )
    )

    # Price tier categorization (for segmentation analysis)
    dim_product = dim_product.withColumn(
        "price_tier",
        F.when(F.col("list_price") < 100, "Budget")
         .when((F.col("list_price") >= 100) & (F.col("list_price") < 500), "Mid-Range")
         .when((F.col("list_price") >= 500) & (F.col("list_price") < 1000), "Premium")
         .when(F.col("list_price") >= 1000, "Luxury")
         .otherwise("Unknown")
    )

    return dim_product.orderBy("product_key")


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.gold.dim_product")
    paths = get_paths(script_file)
    spark = build_spark("upstart_gold_dim_product", paths)

    log.info("Building dim_product (Product Dimension)...")

    # Read silver layer
    products = spark.read.parquet(str(paths["store"] / "store_products"))

    # Build dimension
    dim_product = build_dim_product(products)

    # Log statistics
    total_products = dim_product.count()

    log.info(f"Total products: {total_products:,}")

    # Category distribution
    log.info("Product distribution by category:")
    category_dist = dim_product.groupBy("product_category").count().orderBy(F.desc("count"))
    category_dist.show()

    # Price tier distribution
    log.info("Product distribution by price tier:")
    tier_dist = dim_product.groupBy("price_tier").count().orderBy(
        F.when(F.col("price_tier") == "Budget", 1)
         .when(F.col("price_tier") == "Mid-Range", 2)
         .when(F.col("price_tier") == "Premium", 3)
         .when(F.col("price_tier") == "Luxury", 4)
         .otherwise(5)
    )
    tier_dist.show()

    # Profit margin statistics
    log.info("Profit margin statistics:")
    dim_product.select("profit_margin_pct").summary("min", "max", "mean", "stddev").show()

    # Sample records
    log.info("Sample product records:")
    dim_product.select(
        "product_key", "product_desc", "product_category",
        "list_price", "profit_margin_pct", "price_tier"
    ).show(10, truncate=False)

    # Write to Gold layer (publish)
    publish_path = paths["publish"] / "dim_product"
    dim_product.write.mode("overwrite").parquet(str(publish_path))

    log.info(f"dim_product written to {publish_path}")
    log.info("dim_product build completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()
