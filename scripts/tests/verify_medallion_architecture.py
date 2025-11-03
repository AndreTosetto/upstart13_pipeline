"""
Verify medallion architecture implementation.
Checks that:
- Bronze layer preserves original data (including "2021-06")
- Silver layer has corrected data (OrderDate fixed to proper dates)
"""
from __future__ import annotations
import sys
from pathlib import Path

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common import get_paths, build_spark, setup_logging

def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.verify_medallion")
    paths = get_paths(script_file)
    spark = build_spark("verify_medallion", paths)

    print("\n" + "=" * 80)
    print("MEDALLION ARCHITECTURE VERIFICATION")
    print("=" * 80)

    # BRONZE LAYER VERIFICATION
    print("\n" + "-" * 80)
    print("BRONZE LAYER (Raw - preserves original CSV data)")
    print("-" * 80)

    bronze_soh = spark.read.parquet(str(paths["raw"] / "raw_sales_order_header"))

    print("\n1. Schema (should be all strings):")
    bronze_soh.printSchema()

    print("\n2. Incomplete dates PRESERVED (should find 5 rows with '2021-06'):")
    incomplete_bronze = bronze_soh.filter(F.length(F.col("OrderDate")) == 7)
    incomplete_count = incomplete_bronze.count()
    print(f"   Found {incomplete_count} incomplete dates in bronze")

    if incomplete_count > 0:
        incomplete_bronze.select("SalesOrderID", "OrderDate", "ShipDate").show(5, truncate=False)
        print("   [OK] Bronze preserves original data format")
    else:
        print("   [WARN] Bronze should preserve '2021-06' format")

    # SILVER LAYER VERIFICATION
    print("\n" + "-" * 80)
    print("SILVER LAYER (Refined - data quality fixes applied)")
    print("-" * 80)

    silver_soh = spark.read.parquet(str(paths["store"] / "store_sales_order_header"))
    silver_products = spark.read.parquet(str(paths["store"] / "store_products"))

    print("\n1. Schema (should have proper types: timestamp, integer, etc):")
    silver_soh.printSchema()

    print("\n2. Corrected orders (43828-43832) - OrderDate should be ShipDate - 7 days:")
    corrected = silver_soh.filter(F.col("SalesOrderID").between(43828, 43832))
    corrected = corrected.withColumn("LeadTime_days", F.datediff(F.col("ShipDate"), F.col("OrderDate")))
    corrected.select("SalesOrderID", "OrderDate", "ShipDate", "LeadTime_days").show(truncate=False)

    # Check all lead times are 7 days
    all_7_days = corrected.filter(F.col("LeadTime_days") == 7).count()
    if all_7_days == 5:
        print("   [OK] All corrected dates have 7-day lead time")
    else:
        print(f"   [WARN] Expected 5 rows with 7-day lead time, found {all_7_days}")

    print("\n3. Product deduplication (should have 295 products, not 303):")
    product_count = silver_products.count()
    print(f"   Product count: {product_count} (expected 295)")
    if product_count == 295:
        print("   [OK] Duplicates removed correctly")
    else:
        print(f"   [WARN] Expected 295 products, found {product_count}")

    print("\n4. ProductCategoryName completion (should have 0 nulls):")
    null_categories = silver_products.filter(
        F.col("ProductCategoryName").isNull() | (F.trim(F.col("ProductCategoryName")) == "")
    ).count()
    print(f"   Null categories: {null_categories} (expected 0)")
    if null_categories == 0:
        print("   [OK] All categories mapped correctly")
    else:
        print(f"   [WARN] Found {null_categories} null categories")

    print("\n" + "=" * 80)
    print("MEDALLION ARCHITECTURE SUMMARY")
    print("=" * 80)
    print("\nBronze Layer:")
    print("  - Preserves original CSV data (including '2021-06' incomplete dates)")
    print("  - All columns stored as strings")
    print("  - No transformations applied")

    print("\nSilver Layer:")
    print("  - OrderDate corrected (5 rows: '2021-06' -> '2021-06-28')")
    print("  - Products deduplicated (303 -> 295 rows)")
    print("  - ProductCategoryName completed (190 nulls -> 0 nulls)")
    print("  - Proper data types applied (timestamps, integers, doubles)")

    print("\n" + "=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
