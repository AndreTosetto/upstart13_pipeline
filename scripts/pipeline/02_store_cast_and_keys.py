"""
Silver layer - clean up the raw data and fix typing issues.

Reads bronze tables (everything still strings) and applies quality fixes:
- Backfills incomplete OrderDate values (YYYY-MM -> proper dates using ShipDate-7d)
- Removes duplicate ProductIDs (keeps row with populated category)
- Standardizes category names (Road Bikes/Touring Bikes -> Bikes)
- Casts columns to proper types (int, date, decimal)

Outputs store_* tables ready for the gold layer.
"""

from __future__ import annotations
import sys
from pathlib import Path
from typing import Iterable

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from common import get_paths, build_spark, setup_logging


def trim(df: DataFrame, cols: Iterable[str]) -> DataFrame:
    """Trim string columns to avoid join/group issues caused by trailing spaces."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


def cast_if(df: DataFrame, col: str, dtype: T.DataType) -> DataFrame:
    """Cast a column if present; keep df unchanged otherwise."""
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.store")
    paths = get_paths(script_file)
    spark: SparkSession = build_spark("upstart13_store", paths)

    # Read raw
    products = spark.read.parquet(str(paths["raw"] / "raw_products"))
    sod      = spark.read.parquet(str(paths["raw"] / "raw_sales_order_detail"))
    soh      = spark.read.parquet(str(paths["raw"] / "raw_sales_order_header"))

    # String hygiene
    products = trim(products, ["ProductDesc", "ProductCategoryName", "ProductSubCategoryName", "Color"])
    sod      = trim(sod, ["CarrierTrackingNumber"])
    soh      = trim(soh, ["PurchaseOrderNumber", "AccountNumber"])

    # Data quality fixes for silver layer
    log.info("Applying silver layer quality fixes...")

    # 1. Fix incomplete OrderDate (YYYY-MM format)
    # Bronze layer preserves "2021-06" as-is from CSV
    # Silver layer corrects to proper date format for analysis
    log.info("Fixing incomplete OrderDate values...")

    # Count incomplete dates (length 7 = "YYYY-MM")
    incomplete_count = soh.filter(F.length(F.col("OrderDate")) == 7).count()

    if incomplete_count > 0:
        log.warning(f"Found {incomplete_count} incomplete OrderDate values (YYYY-MM format)")
        log.info("Fixing by inferring from ShipDate - 7 days (median lead time)")

        # Parse ShipDate first (it's a string from bronze)
        soh = soh.withColumn("_ShipDate_parsed", F.to_date(F.col("ShipDate")))

        # For incomplete dates, calculate OrderDate = ShipDate - 7 days
        # For complete dates, keep as-is
        soh = soh.withColumn(
            "OrderDate",
            F.when(
                F.length(F.col("OrderDate")) == 7,
                F.date_format(F.date_sub(F.col("_ShipDate_parsed"), 7), "yyyy-MM-dd")
            ).otherwise(F.col("OrderDate"))
        )

        # Clean up temp column
        soh = soh.drop("_ShipDate_parsed")

        log.info(f"Fixed {incomplete_count} incomplete OrderDate values")
    else:
        log.info("No incomplete OrderDate values found")

    # 2. Remove duplicate ProductIDs
    # Strategy: Keep row with ProductCategoryName filled (not null/empty)
    # This handles cases where same product appears with different categorization
    from pyspark.sql.window import Window

    initial_count = products.count()

    # Add ranking: prioritize rows with non-null category
    window = Window.partitionBy("ProductID").orderBy(
        F.when(F.col("ProductCategoryName").isNull() | (F.trim(F.col("ProductCategoryName")) == ""), 1).otherwise(0),
        F.col("ProductID")
    )
    products = products.withColumn("_rank", F.row_number().over(window))
    products = products.filter(F.col("_rank") == 1).drop("_rank")

    final_count = products.count()
    if initial_count > final_count:
        log.warning(f"Removed {initial_count - final_count} duplicate products (kept rows with category info)")

    # 2. Fix incorrect category mappings (data quality issue)
    # Note: Silver fixes WRONG categories, Gold fills NULL categories
    # Bronze has wrong mappings: "Road Bikes"→Components, "Touring Bikes"→Clothing
    # Silver corrects these to the proper "Bikes" category
    log.info("Fixing incorrect category mappings...")

    incorrect_count = products.filter(
        (F.col("ProductSubCategoryName").isin(["Road Bikes", "Touring Bikes"]) &
         (F.col("ProductCategoryName") != "Bikes"))
    ).count()

    if incorrect_count > 0:
        log.warning(f"Found {incorrect_count} products with incorrect category (Road/Touring Bikes not in Bikes category)")

        products = products.withColumn(
            "ProductCategoryName",
            F.when(
                F.col("ProductSubCategoryName").isin(["Road Bikes", "Touring Bikes", "Mountain Bikes"]),
                F.lit("Bikes")
            ).otherwise(F.col("ProductCategoryName"))
        )

        log.info(f"Fixed {incorrect_count} incorrect category mappings (Bikes)")

    # 3. Complete ProductCategoryName mapping for NULL values
    # This is data quality, not business logic - belongs in silver
    cat = "ProductCategoryName"
    subc = "ProductSubCategoryName"

    cat_null = F.col(cat).isNull() | (F.trim(F.col(cat)) == "")

    # Comprehensive mapping based on subcategory analysis
    clothing_subs = ["Bib-Shorts", "Caps", "Gloves", "Jerseys", "Shorts",
                      "Socks", "Tights", "Vests"]
    accessories_subs = ["Bike Racks", "Bike Stands", "Bottles and Cages",
                         "Cleaners", "Hydration Packs", "Locks", "Lights",
                         "Headsets", "Helmets", "Panniers", "Pedals", "Pumps"]
    components_subs = ["Bottom Brackets", "Brakes", "Chains", "Cranksets",
                        "Derailleurs", "Fenders", "Forks", "Saddles",
                        "Tires and Tubes", "Wheels"]

    # Contains "Frames" check
    frames_check = F.lower(F.col(subc)).contains("frames")

    products = products.withColumn(
        cat,
        F.when(cat_null & F.col(subc).isin(clothing_subs), F.lit("Clothing"))
         .when(cat_null & F.col(subc).isin(accessories_subs), F.lit("Accessories"))
         .when(cat_null & (frames_check | F.col(subc).isin(components_subs)), F.lit("Components"))
         .otherwise(F.col(cat))
    )

    remaining_nulls = products.filter(F.col(cat).isNull() | (F.trim(F.col(cat)) == "")).count()
    log.info(f"ProductCategoryName nulls remaining: {remaining_nulls}")

    # 4. Sales Order Detail transformations
    log.info("Applying sales_order_detail transformations...")

    # 4a. Add IsReturn flag for negative quantities
    # Found 2 records with OrderQty = -1 (product returns/adjustments)
    negative_qty_count = sod.filter(F.col("OrderQty").cast(T.IntegerType()) < 0).count()

    if negative_qty_count > 0:
        log.warning(f"Found {negative_qty_count} records with negative OrderQty (returns/adjustments)")

    # Add IsReturn column (before casting to allow string comparison)
    sod = sod.withColumn(
        "IsReturn",
        F.when(F.col("OrderQty").cast(T.IntegerType()) < 0, True).otherwise(False)
    )

    log.info(f"Added IsReturn flag ({negative_qty_count} returns identified)")

    # Type casting (bronze has everything as strings)
    log.info("Casting data types for silver layer...")

    # === PRODUCTS ===
    # Integer IDs
    products = cast_if(products, "ProductID", T.IntegerType())

    # Numeric fields
    for c in ["SafetyStockLevel", "ReorderPoint"]:
        products = cast_if(products, c, T.IntegerType())

    for c in ["StandardCost", "ListPrice", "Weight"]:
        products = cast_if(products, c, T.DoubleType())

    # Boolean flag (MakeFlag: "True"/"False" strings -> boolean)
    if "MakeFlag" in products.columns:
        products = products.withColumn(
            "MakeFlag",
            F.when(F.lower(F.trim(F.col("MakeFlag"))) == "true", True)
             .when(F.lower(F.trim(F.col("MakeFlag"))) == "false", False)
             .otherwise(None)
        )

    # === SALES ORDER DETAIL ===
    # Integer IDs and quantities
    for c in ["SalesOrderID", "SalesOrderDetailID", "ProductID", "OrderQty"]:
        sod = cast_if(sod, c, T.IntegerType())

    # Money/decimal
    for c in ["UnitPrice", "UnitPriceDiscount"]:
        sod = cast_if(sod, c, T.DoubleType())

    # 4b. Calculate LineTotal (not in source data)
    # Formula: UnitPrice * OrderQty * (1 - UnitPriceDiscount)
    log.info("Calculating LineTotal...")

    sod = sod.withColumn(
        "LineTotal",
        F.col("UnitPrice") * F.col("OrderQty") * (1 - F.col("UnitPriceDiscount"))
    )

    log.info("LineTotal calculated successfully")

    # === SALES ORDER HEADER ===
    # Integer IDs (empty strings become null for SalesPersonID - business logic)
    for c in ["SalesOrderID", "CustomerID", "ShipToAddressID", "BillToAddressID", "SalesPersonID", "TerritoryID"]:
        soh = cast_if(soh, c, T.IntegerType())

    # Dates/timestamps (OrderDate already fixed above in silver layer)
    for c in ["OrderDate", "ShipDate"]:
        if c in soh.columns:
            soh = soh.withColumn(c, F.to_timestamp(F.col(c)))

    # Money/decimal
    for c in ["SubTotal", "TaxAmt", "Freight", "TotalDue"]:
        soh = cast_if(soh, c, T.DoubleType())

    # Boolean flag (OnlineOrderFlag: "True"/"False" strings -> boolean)
    if "OnlineOrderFlag" in soh.columns:
        soh = soh.withColumn(
            "OnlineOrderFlag",
            F.when(F.lower(F.trim(F.col("OnlineOrderFlag"))) == "true", True)
             .when(F.lower(F.trim(F.col("OnlineOrderFlag"))) == "false", False)
             .otherwise(None)
        )

    log.info("Data type casting completed")

    # Write curated/store
    products.write.mode("overwrite").parquet(str(paths["store"] / "store_products"))
    sod.write.mode("overwrite").parquet(str(paths["store"] / "store_sales_order_detail"))
    soh.write.mode("overwrite").parquet(str(paths["store"] / "store_sales_order_header"))

    # Log keys (to reference in README / presentation)
    log.info("PK/FK — suggested:")
    log.info("products: PK(ProductID)")
    log.info("sales_order_header: PK(SalesOrderID)")
    log.info("sales_order_detail: PK(SalesOrderID, SalesOrderDetailID), "
             "FK(ProductID)->products.ProductID, FK(SalesOrderID)->sales_order_header.SalesOrderID")

    # Optional: print schemas to logs for audit
    log.info("Schema products:\n" + products._jdf.schema().treeString())
    log.info("Schema sales_order_detail:\n" + sod._jdf.schema().treeString())
    log.info("Schema sales_order_header:\n" + soh._jdf.schema().treeString())

    spark.stop()


if __name__ == "__main__":
    main()
