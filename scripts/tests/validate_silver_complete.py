"""
Complete validation of Silver layer before moving to Gold.
Verifies data quality, transformations, data types, and relationships.
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
    log = setup_logging("upstart.validate_silver")
    paths = get_paths(script_file)
    spark = build_spark("validate_silver", paths)

    print("\n" + "=" * 80)
    print("COMPLETE SILVER LAYER VALIDATION")
    print("=" * 80)

    # Read silver layer tables
    products = spark.read.parquet(str(paths["store"] / "store_products"))
    sod = spark.read.parquet(str(paths["store"] / "store_sales_order_detail"))
    soh = spark.read.parquet(str(paths["store"] / "store_sales_order_header"))

    # Read bronze for comparison
    products_bronze = spark.read.parquet(str(paths["raw"] / "raw_products"))
    sod_bronze = spark.read.parquet(str(paths["raw"] / "raw_sales_order_detail"))
    soh_bronze = spark.read.parquet(str(paths["raw"] / "raw_sales_order_header"))

    all_ok = True
    issues = []

    # ========================================================================
    # 1. RECORD COUNT VALIDATION
    # ========================================================================
    print("\n" + "-" * 80)
    print("1. RECORD COUNT VALIDATION")
    print("-" * 80)

    # Products should have FEWER records (duplicates removed)
    products_bronze_count = products_bronze.count()
    products_silver_count = products.count()

    print(f"Products - Bronze: {products_bronze_count:,}, Silver: {products_silver_count:,}")

    expected_removed = 8  # We know 8 duplicates were removed
    if products_bronze_count - products_silver_count == expected_removed:
        print(f"  [OK] {expected_removed} duplicate products removed as expected")
    else:
        print(f"  [ERROR] Expected {expected_removed} removed, got {products_bronze_count - products_silver_count}")
        all_ok = False
        issues.append("Products: Unexpected record count difference")

    # Sales order detail should have SAME records
    sod_bronze_count = sod_bronze.count()
    sod_silver_count = sod.count()

    print(f"Sales Order Detail - Bronze: {sod_bronze_count:,}, Silver: {sod_silver_count:,}")

    if sod_bronze_count == sod_silver_count:
        print(f"  [OK] All {sod_silver_count:,} records preserved")
    else:
        print(f"  [ERROR] Record count mismatch!")
        all_ok = False
        issues.append("Sales Order Detail: Record count mismatch")

    # Sales order header should have SAME records
    soh_bronze_count = soh_bronze.count()
    soh_silver_count = soh.count()

    print(f"Sales Order Header - Bronze: {soh_bronze_count:,}, Silver: {soh_silver_count:,}")

    if soh_bronze_count == soh_silver_count:
        print(f"  [OK] All {soh_silver_count:,} records preserved")
    else:
        print(f"  [ERROR] Record count mismatch!")
        all_ok = False
        issues.append("Sales Order Header: Record count mismatch")

    # ========================================================================
    # 2. DATA QUALITY FIXES VALIDATION
    # ========================================================================
    print("\n" + "-" * 80)
    print("2. DATA QUALITY FIXES VALIDATION")
    print("-" * 80)

    # 2a. OrderDate format fix
    print("\n2a. OrderDate Format Fix:")
    incomplete_dates_bronze = soh_bronze.filter(F.length(F.col("OrderDate")) == 7).count()
    incomplete_dates_silver = soh.filter(F.length(F.col("OrderDate").cast("string")) == 7).count()

    print(f"  Bronze: {incomplete_dates_bronze} incomplete dates (YYYY-MM format)")
    print(f"  Silver: {incomplete_dates_silver} incomplete dates")

    if incomplete_dates_bronze > 0 and incomplete_dates_silver == 0:
        print(f"  [OK] All {incomplete_dates_bronze} incomplete dates fixed")
    else:
        print(f"  [ERROR] Date fix did not work correctly")
        all_ok = False
        issues.append("OrderDate: Incomplete dates not fixed")

    # 2b. ProductCategoryName NULL mapping
    print("\n2b. ProductCategoryName NULL Mapping:")
    null_categories_bronze = products_bronze.filter(
        F.col("ProductCategoryName").isNull() | (F.trim(F.col("ProductCategoryName")) == "")
    ).count()
    null_categories_silver = products.filter(
        F.col("ProductCategoryName").isNull() | (F.trim(F.col("ProductCategoryName")) == "")
    ).count()

    print(f"  Bronze: {null_categories_bronze} NULL categories")
    print(f"  Silver: {null_categories_silver} NULL categories")

    if null_categories_bronze > 0 and null_categories_silver == 0:
        print(f"  [OK] All {null_categories_bronze} NULL categories mapped")
    else:
        print(f"  [ERROR] Category mapping incomplete")
        all_ok = False
        issues.append("ProductCategoryName: NULL values remain")

    # 2c. Incorrect category mappings
    print("\n2c. Incorrect Category Mappings (Road/Touring Bikes):")
    incorrect_mappings = products.filter(
        F.col("ProductSubCategoryName").isin(["Road Bikes", "Touring Bikes"]) &
        (F.col("ProductCategoryName") != "Bikes")
    ).count()

    print(f"  Silver: {incorrect_mappings} products with incorrect category")

    if incorrect_mappings == 0:
        print(f"  [OK] All Road/Touring Bikes correctly mapped to Bikes category")
    else:
        print(f"  [ERROR] {incorrect_mappings} products still have incorrect category")
        all_ok = False
        issues.append("Category Mapping: Incorrect mappings remain")

    # ========================================================================
    # 3. DATA TYPE VALIDATION
    # ========================================================================
    print("\n" + "-" * 80)
    print("3. DATA TYPE VALIDATION")
    print("-" * 80)

    type_checks = []

    # Products
    type_checks.append(("products", "ProductID", products, "int"))
    type_checks.append(("products", "MakeFlag", products, "boolean"))
    type_checks.append(("products", "SafetyStockLevel", products, "int"))
    type_checks.append(("products", "StandardCost", products, "double"))

    # Sales Order Detail
    type_checks.append(("sales_order_detail", "SalesOrderID", sod, "int"))
    type_checks.append(("sales_order_detail", "OrderQty", sod, "int"))
    type_checks.append(("sales_order_detail", "UnitPrice", sod, "double"))
    type_checks.append(("sales_order_detail", "IsReturn", sod, "boolean"))
    type_checks.append(("sales_order_detail", "LineTotal", sod, "double"))

    # Sales Order Header
    type_checks.append(("sales_order_header", "SalesOrderID", soh, "int"))
    type_checks.append(("sales_order_header", "CustomerID", soh, "int"))
    type_checks.append(("sales_order_header", "OnlineOrderFlag", soh, "boolean"))
    type_checks.append(("sales_order_header", "OrderDate", soh, "timestamp"))
    type_checks.append(("sales_order_header", "Freight", soh, "double"))

    type_errors = 0
    for table_name, col_name, df, expected_type in type_checks:
        if col_name in df.columns:
            actual_type = dict(df.dtypes)[col_name]
            if actual_type == expected_type:
                print(f"  [OK] {table_name}.{col_name}: {actual_type}")
            else:
                print(f"  [ERROR] {table_name}.{col_name}: expected {expected_type}, got {actual_type}")
                type_errors += 1
        else:
            print(f"  [ERROR] {table_name}.{col_name}: column missing!")
            type_errors += 1

    if type_errors == 0:
        print(f"\n  [OK] All data types correctly assigned")
    else:
        print(f"\n  [ERROR] {type_errors} data type issues found")
        all_ok = False
        issues.append(f"Data Types: {type_errors} incorrect types")

    # ========================================================================
    # 4. CALCULATED FIELDS VALIDATION
    # ========================================================================
    print("\n" + "-" * 80)
    print("4. CALCULATED FIELDS VALIDATION")
    print("-" * 80)

    # 4a. LineTotal calculation
    print("\n4a. LineTotal Calculation:")

    if "LineTotal" not in sod.columns:
        print("  [ERROR] LineTotal column missing!")
        all_ok = False
        issues.append("LineTotal: Column missing")
    else:
        # Recalculate and compare
        sod_check = sod.withColumn(
            "LineTotal_recalc",
            F.col("UnitPrice") * F.col("OrderQty") * (1 - F.col("UnitPriceDiscount"))
        )

        sod_check = sod_check.withColumn(
            "LineTotal_diff",
            F.abs(F.col("LineTotal") - F.col("LineTotal_recalc"))
        )

        mismatches = sod_check.filter(F.col("LineTotal_diff") > 0.0001).count()

        if mismatches == 0:
            print(f"  [OK] All {sod_silver_count:,} LineTotal values correctly calculated")
        else:
            print(f"  [ERROR] {mismatches} LineTotal calculation errors")
            all_ok = False
            issues.append(f"LineTotal: {mismatches} calculation errors")

    # 4b. IsReturn flag
    print("\n4b. IsReturn Flag:")

    if "IsReturn" not in sod.columns:
        print("  [ERROR] IsReturn column missing!")
        all_ok = False
        issues.append("IsReturn: Column missing")
    else:
        return_count = sod.filter(F.col("IsReturn") == True).count()
        negative_qty_count = sod.filter(F.col("OrderQty") < 0).count()

        print(f"  IsReturn=True: {return_count}")
        print(f"  OrderQty < 0: {negative_qty_count}")

        if return_count == negative_qty_count == 2:
            print(f"  [OK] IsReturn flag correctly identifies 2 returns")
        else:
            print(f"  [ERROR] IsReturn flag mismatch")
            all_ok = False
            issues.append("IsReturn: Incorrect flag assignment")

    # ========================================================================
    # 5. REFERENTIAL INTEGRITY
    # ========================================================================
    print("\n" + "-" * 80)
    print("5. REFERENTIAL INTEGRITY")
    print("-" * 80)

    # 5a. sales_order_detail.ProductID -> products.ProductID
    print("\n5a. Foreign Key: sales_order_detail.ProductID -> products.ProductID")

    orphan_products = sod.join(
        products.select("ProductID"),
        sod["ProductID"] == products["ProductID"],
        "left_anti"
    ).count()

    if orphan_products == 0:
        print(f"  [OK] All ProductIDs in sales_order_detail exist in products")
    else:
        print(f"  [ERROR] {orphan_products} orphan ProductIDs found")
        all_ok = False
        issues.append(f"FK Integrity: {orphan_products} orphan ProductIDs")

    # 5b. sales_order_detail.SalesOrderID -> sales_order_header.SalesOrderID
    print("\n5b. Foreign Key: sales_order_detail.SalesOrderID -> sales_order_header.SalesOrderID")

    orphan_orders = sod.join(
        soh.select("SalesOrderID"),
        sod["SalesOrderID"] == soh["SalesOrderID"],
        "left_anti"
    ).count()

    if orphan_orders == 0:
        print(f"  [OK] All SalesOrderIDs in sales_order_detail exist in sales_order_header")
    else:
        print(f"  [ERROR] {orphan_orders} orphan SalesOrderIDs found")
        all_ok = False
        issues.append(f"FK Integrity: {orphan_orders} orphan SalesOrderIDs")

    # ========================================================================
    # 6. PRIMARY KEY UNIQUENESS
    # ========================================================================
    print("\n" + "-" * 80)
    print("6. PRIMARY KEY UNIQUENESS")
    print("-" * 80)

    # 6a. products.ProductID
    print("\n6a. products.ProductID (Primary Key):")
    products_total = products.count()
    products_distinct = products.select("ProductID").distinct().count()

    if products_total == products_distinct:
        print(f"  [OK] All {products_total} ProductIDs are unique")
    else:
        print(f"  [ERROR] Duplicates found! Total: {products_total}, Distinct: {products_distinct}")
        all_ok = False
        issues.append("Products PK: Duplicates found")

    # 6b. sales_order_header.SalesOrderID
    print("\n6b. sales_order_header.SalesOrderID (Primary Key):")
    soh_total = soh.count()
    soh_distinct = soh.select("SalesOrderID").distinct().count()

    if soh_total == soh_distinct:
        print(f"  [OK] All {soh_total} SalesOrderIDs are unique")
    else:
        print(f"  [ERROR] Duplicates found! Total: {soh_total}, Distinct: {soh_distinct}")
        all_ok = False
        issues.append("Sales Order Header PK: Duplicates found")

    # 6c. sales_order_detail (composite key)
    print("\n6c. sales_order_detail (Composite PK: SalesOrderID + SalesOrderDetailID):")
    sod_total = sod.count()
    sod_distinct = sod.select("SalesOrderID", "SalesOrderDetailID").distinct().count()

    if sod_total == sod_distinct:
        print(f"  [OK] All {sod_total} composite keys are unique")
    else:
        print(f"  [ERROR] Duplicates found! Total: {sod_total}, Distinct: {sod_distinct}")
        all_ok = False
        issues.append("Sales Order Detail PK: Duplicates found")

    # ========================================================================
    # 7. NULL VALIDATION (Critical fields should not be NULL)
    # ========================================================================
    print("\n" + "-" * 80)
    print("7. NULL VALIDATION (Critical Fields)")
    print("-" * 80)

    null_checks = [
        ("products", "ProductID", products),
        ("products", "ProductDesc", products),
        ("sales_order_detail", "SalesOrderID", sod),
        ("sales_order_detail", "ProductID", sod),
        ("sales_order_detail", "OrderQty", sod),
        ("sales_order_detail", "UnitPrice", sod),
        ("sales_order_detail", "LineTotal", sod),
        ("sales_order_header", "SalesOrderID", soh),
        ("sales_order_header", "CustomerID", soh),
        ("sales_order_header", "OrderDate", soh),
    ]

    null_errors = 0
    for table_name, col_name, df in null_checks:
        if col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            if null_count == 0:
                print(f"  [OK] {table_name}.{col_name}: No NULLs")
            else:
                print(f"  [ERROR] {table_name}.{col_name}: {null_count} NULL values found")
                null_errors += 1
        else:
            print(f"  [ERROR] {table_name}.{col_name}: Column missing!")
            null_errors += 1

    if null_errors > 0:
        all_ok = False
        issues.append(f"NULL Validation: {null_errors} critical fields have NULLs")

    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)

    print(f"\nSilver Layer Statistics:")
    print(f"  Products: {products_silver_count:,} records")
    print(f"  Sales Order Header: {soh_silver_count:,} records")
    print(f"  Sales Order Detail: {sod_silver_count:,} records")

    print(f"\nData Quality Improvements:")
    print(f"  - {expected_removed} duplicate products removed")
    print(f"  - {incomplete_dates_bronze} incomplete OrderDates fixed")
    print(f"  - {null_categories_bronze} NULL ProductCategoryNames mapped")
    print(f"  - LineTotal calculated for all order details")
    print(f"  - IsReturn flag added (2 returns identified)")

    if all_ok:
        print("\n" + "=" * 80)
        print("[OK] SILVER LAYER VALIDATION PASSED")
        print("=" * 80)
        print("\nAll checks passed! Silver layer is ready for Gold layer implementation.")
        print("\nNext step: Implement Gold layer with Star Schema (Data Warehouse)")
    else:
        print("\n" + "=" * 80)
        print("[ERROR] SILVER LAYER VALIDATION FAILED")
        print("=" * 80)
        print(f"\n{len(issues)} issue(s) found:")
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")
        print("\nPlease fix these issues before proceeding to Gold layer.")

    print("\n" + "=" * 80 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
