"""
Gold Layer - Step 05: Validate Star Schema

Comprehensive validation of the dimensional model:
- Referential integrity between fact and dimensions
- Data quality checks
- Business metrics validation
"""
from __future__ import annotations
import sys
from pathlib import Path

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from common import get_paths, build_spark, setup_logging


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.gold.validate")
    paths = get_paths(script_file)
    spark = build_spark("upstart_gold_validate", paths)

    print("\n" + "=" * 80)
    print("GOLD LAYER - STAR SCHEMA VALIDATION")
    print("=" * 80)

    # Read all Gold layer (publish) tables
    dim_date = spark.read.parquet(str(paths["publish"] / "dim_date"))
    dim_product = spark.read.parquet(str(paths["publish"] / "dim_product"))
    dim_customer = spark.read.parquet(str(paths["publish"] / "dim_customer"))
    fact_sales = spark.read.parquet(str(paths["publish"] / "fact_sales"))

    all_ok = True
    issues = []

    # ========================================================================
    # 1. TABLE COUNTS
    # ========================================================================
    print("\n" + "-" * 80)
    print("1. TABLE RECORD COUNTS")
    print("-" * 80)

    dim_date_count = dim_date.count()
    dim_product_count = dim_product.count()
    dim_customer_count = dim_customer.count()
    fact_sales_count = fact_sales.count()

    print(f"dim_date: {dim_date_count:,} records")
    print(f"dim_product: {dim_product_count:,} records")
    print(f"dim_customer: {dim_customer_count:,} records")
    print(f"fact_sales: {fact_sales_count:,} records")

    if fact_sales_count == 0:
        print("[ERROR] Fact table is empty!")
        all_ok = False
        issues.append("fact_sales is empty")

    # ========================================================================
    # 2. REFERENTIAL INTEGRITY - FOREIGN KEYS
    # ========================================================================
    print("\n" + "-" * 80)
    print("2. REFERENTIAL INTEGRITY VALIDATION")
    print("-" * 80)

    # 2a. fact_sales.order_date_key -> dim_date.date_key
    print("\n2a. Foreign Key: fact_sales.order_date_key -> dim_date.date_key")

    orphan_order_dates = fact_sales.join(
        dim_date.select("date_key"),
        fact_sales["order_date_key"] == dim_date["date_key"],
        "left_anti"
    ).count()

    if orphan_order_dates == 0:
        print(f"  [OK] All {fact_sales_count:,} order_date_key values exist in dim_date")
    else:
        print(f"  [ERROR] {orphan_order_dates} orphan order_date_key values!")
        all_ok = False
        issues.append(f"FK violation: {orphan_order_dates} orphan order_date_key")

    # 2b. fact_sales.ship_date_key -> dim_date.date_key
    print("\n2b. Foreign Key: fact_sales.ship_date_key -> dim_date.date_key")

    orphan_ship_dates = fact_sales.join(
        dim_date.select("date_key"),
        fact_sales["ship_date_key"] == dim_date["date_key"],
        "left_anti"
    ).count()

    if orphan_ship_dates == 0:
        print(f"  [OK] All {fact_sales_count:,} ship_date_key values exist in dim_date")
    else:
        print(f"  [ERROR] {orphan_ship_dates} orphan ship_date_key values!")
        all_ok = False
        issues.append(f"FK violation: {orphan_ship_dates} orphan ship_date_key")

    # 2c. fact_sales.product_key -> dim_product.product_key
    print("\n2c. Foreign Key: fact_sales.product_key -> dim_product.product_key")

    orphan_products = fact_sales.join(
        dim_product.select("product_key"),
        fact_sales["product_key"] == dim_product["product_key"],
        "left_anti"
    ).count()

    if orphan_products == 0:
        print(f"  [OK] All {fact_sales_count:,} product_key values exist in dim_product")
    else:
        print(f"  [ERROR] {orphan_products} orphan product_key values!")
        all_ok = False
        issues.append(f"FK violation: {orphan_products} orphan product_key")

    # 2d. fact_sales.customer_key -> dim_customer.customer_key
    print("\n2d. Foreign Key: fact_sales.customer_key -> dim_customer.customer_key")

    orphan_customers = fact_sales.join(
        dim_customer.select("customer_key"),
        fact_sales["customer_key"] == dim_customer["customer_key"],
        "left_anti"
    ).count()

    if orphan_customers == 0:
        print(f"  [OK] All {fact_sales_count:,} customer_key values exist in dim_customer")
    else:
        print(f"  [ERROR] {orphan_customers} orphan customer_key values!")
        all_ok = False
        issues.append(f"FK violation: {orphan_customers} orphan customer_key")

    # ========================================================================
    # 3. PRIMARY KEY UNIQUENESS
    # ========================================================================
    print("\n" + "-" * 80)
    print("3. PRIMARY KEY UNIQUENESS")
    print("-" * 80)

    pk_checks = [
        ("dim_date", "date_key", dim_date),
        ("dim_product", "product_key", dim_product),
        ("dim_customer", "customer_key", dim_customer)
    ]

    for table_name, pk_col, df in pk_checks:
        total = df.count()
        distinct = df.select(pk_col).distinct().count()

        if total == distinct:
            print(f"  [OK] {table_name}.{pk_col}: All {total:,} values are unique")
        else:
            print(f"  [ERROR] {table_name}.{pk_col}: Duplicates found! Total: {total:,}, Distinct: {distinct:,}")
            all_ok = False
            issues.append(f"{table_name} PK duplicates")

    # ========================================================================
    # 4. BUSINESS METRICS VALIDATION
    # ========================================================================
    print("\n" + "-" * 80)
    print("4. BUSINESS METRICS VALIDATION")
    print("-" * 80)

    # Calculate metrics from fact table
    sales_metrics = fact_sales.filter(F.col("is_return") == False).agg(
        F.sum("line_total").alias("total_revenue"),
        F.sum("gross_profit").alias("total_gross_profit"),
        F.sum("discount_amount").alias("total_discounts"),
        F.count("*").alias("sales_count")
    ).collect()[0]

    print(f"\nSales Metrics (from fact_sales):")
    print(f"  Total Revenue: ${sales_metrics['total_revenue']:,.2f}")
    print(f"  Total Gross Profit: ${sales_metrics['total_gross_profit']:,.2f}")
    print(f"  Total Discounts: ${sales_metrics['total_discounts']:,.2f}")
    print(f"  Sales Count: {sales_metrics['sales_count']:,}")

    # Gross margin calculation
    if sales_metrics['total_revenue'] > 0:
        margin_pct = (sales_metrics['total_gross_profit'] / sales_metrics['total_revenue']) * 100
        print(f"  Gross Margin %: {margin_pct:.2f}%")

        # Sanity check: margin should be between 0% and 100%
        if margin_pct < 0 or margin_pct > 100:
            print(f"  [WARN] Gross margin {margin_pct:.2f}% is outside normal range (0-100%)")
            issues.append(f"Gross margin {margin_pct:.2f}% outside normal range")

    # Check for negative profits (could indicate data quality issues)
    negative_profit_count = fact_sales.filter(
        (F.col("is_return") == False) & (F.col("gross_profit") < 0)
    ).count()

    if negative_profit_count > 0:
        pct = (negative_profit_count / sales_metrics['sales_count']) * 100
        print(f"\n  [WARN] {negative_profit_count:,} sales ({pct:.2f}%) have negative gross profit")
        print(f"  This may indicate selling below cost or data quality issues")

    # ========================================================================
    # 5. STAR SCHEMA QUERY TEST
    # ========================================================================
    print("\n" + "-" * 80)
    print("5. STAR SCHEMA QUERY TEST")
    print("-" * 80)

    print("\nTest Query: Monthly sales by product category (2024)")

    test_query = fact_sales.filter(F.col("is_return") == False) \
        .join(dim_date, fact_sales["order_date_key"] == dim_date["date_key"]) \
        .join(dim_product, fact_sales["product_key"] == dim_product["product_key"]) \
        .filter(dim_date["year"] == 2024) \
        .groupBy(dim_date["year"], dim_date["month"], dim_product["product_category"]) \
        .agg(
            F.sum("line_total").alias("revenue"),
            F.count("*").alias("transactions")
        ) \
        .orderBy(dim_date["year"], dim_date["month"], dim_product["product_category"])

    result_count = test_query.count()

    if result_count > 0:
        print(f"  [OK] Query returned {result_count} rows")
        print("\n  Sample results:")
        test_query.show(10)
    else:
        print(f"  [WARN] Query returned 0 rows (may be expected if no 2024 data)")

    # ========================================================================
    # 6. DIMENSION COVERAGE
    # ========================================================================
    print("\n" + "-" * 80)
    print("6. DIMENSION COVERAGE")
    print("-" * 80)

    # Products used in sales
    products_in_sales = fact_sales.select("product_key").distinct().count()
    product_coverage_pct = (products_in_sales / dim_product_count) * 100

    print(f"\nProduct Coverage:")
    print(f"  Products in dim_product: {dim_product_count:,}")
    print(f"  Products with sales: {products_in_sales:,}")
    print(f"  Coverage: {product_coverage_pct:.1f}%")

    if product_coverage_pct < 50:
        print(f"  [WARN] Only {product_coverage_pct:.1f}% of products have sales")

    # Customers with purchases
    customers_in_sales = fact_sales.select("customer_key").distinct().count()
    customer_coverage_pct = (customers_in_sales / dim_customer_count) * 100

    print(f"\nCustomer Coverage:")
    print(f"  Customers in dim_customer: {dim_customer_count:,}")
    print(f"  Customers with purchases: {customers_in_sales:,}")
    print(f"  Coverage: {customer_coverage_pct:.1f}%")

    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)

    print(f"\nStar Schema Statistics:")
    print(f"  Dimensions: 3 tables ({dim_date_count + dim_product_count + dim_customer_count:,} total records)")
    print(f"  Fact Table: 1 table ({fact_sales_count:,} records)")
    print(f"  Total Size: {dim_date_count + dim_product_count + dim_customer_count + fact_sales_count:,} records")

    print(f"\nBusiness Metrics:")
    print(f"  Total Revenue: ${sales_metrics['total_revenue']:,.2f}")
    print(f"  Gross Margin: {margin_pct:.2f}%")

    if all_ok and len(issues) == 0:
        print("\n" + "=" * 80)
        print("[OK] STAR SCHEMA VALIDATION PASSED")
        print("=" * 80)
        print("\nAll checks passed! Gold layer is ready for analytics and BI tools.")
        print("\nThe dimensional model enables efficient queries for:")
        print("  - Time-series analysis (by day, month, quarter, year)")
        print("  - Product performance analysis (by category, subcategory)")
        print("  - Customer segmentation analysis")
        print("  - Profitability analysis")
    else:
        print("\n" + "=" * 80)
        print("[WARN] STAR SCHEMA VALIDATION COMPLETED WITH WARNINGS")
        print("=" * 80)
        if issues:
            print(f"\n{len(issues)} issue(s) found:")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")
        print("\nThe star schema is functional but review warnings above.")

    print("\n" + "=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
