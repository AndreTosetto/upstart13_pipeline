"""
Silver layer - type casting and minimal data quality fixes.

Reads bronze (all strings) and applies:
- Trim strings to avoid join issues
- Fix incomplete OrderDate (YYYY-MM -> infer from ShipDate - 7 days)
- Remove duplicate ProductIDs (keep row with category populated)
- Cast to proper types (int, date, decimal, boolean)

Outputs store_* tables ready for gold transformations.
"""

from __future__ import annotations
import sys
from pathlib import Path
from typing import Iterable

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from common import get_paths, build_spark, setup_logging
from config import IMPUTE_ORDERDATE


def trim(df: DataFrame, cols: Iterable[str]) -> DataFrame:
    """Trim string columns to avoid join/group issues."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


def cast_if(df: DataFrame, col: str, dtype: T.DataType) -> DataFrame:
    """Cast a column if present."""
    if col in df.columns:
        df = df.withColumn(col, F.col(col).cast(dtype))
    return df


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.store")
    paths = get_paths(script_file)
    spark: SparkSession = build_spark("upstart13_store", paths)

    # Read bronze
    products = spark.read.parquet(str(paths["raw"] / "raw_products"))
    sod = spark.read.parquet(str(paths["raw"] / "raw_sales_order_detail"))
    soh = spark.read.parquet(str(paths["raw"] / "raw_sales_order_header"))

    # Trim strings
    products = trim(products, ["ProductDesc", "ProductCategoryName", "ProductSubCategoryName", "Color"])
    sod = trim(sod, ["CarrierTrackingNumber"])
    soh = trim(soh, ["PurchaseOrderNumber", "AccountNumber"])

    # Fix incomplete OrderDate (YYYY-MM format) - only if imputation enabled
    incomplete_count = soh.filter(F.length(F.col("OrderDate")) == 7).count()
    if incomplete_count > 0:
        if IMPUTE_ORDERDATE:
            log.info(f"Fixing {incomplete_count} incomplete OrderDate values (YYYY-MM -> ShipDate - 7 days)")
            soh = soh.withColumn("_ShipDate_parsed", F.to_date(F.col("ShipDate")))
            soh = soh.withColumn(
                "OrderDate",
                F.when(
                    F.length(F.col("OrderDate")) == 7,
                    F.date_format(F.date_sub(F.col("_ShipDate_parsed"), 7), "yyyy-MM-dd")
                ).otherwise(F.col("OrderDate"))
            ).drop("_ShipDate_parsed")
        else:
            log.info(f"Found {incomplete_count} incomplete OrderDate values (imputation disabled, setting to NULL)")
            soh = soh.withColumn(
                "OrderDate",
                F.when(F.length(F.col("OrderDate")) == 7, F.lit(None)).otherwise(F.col("OrderDate"))
            )

    # Remove duplicate ProductIDs (keep row with category populated)
    window = Window.partitionBy("ProductID").orderBy(
        F.when(F.col("ProductCategoryName").isNull() | (F.col("ProductCategoryName") == ""), 1).otherwise(0)
    )
    products = products.withColumn("_rank", F.row_number().over(window)) \
                       .filter(F.col("_rank") == 1).drop("_rank")

    # Cast types
    # Products
    products = cast_if(products, "ProductID", T.IntegerType())
    for c in ["SafetyStockLevel", "ReorderPoint"]:
        products = cast_if(products, c, T.IntegerType())
    for c in ["StandardCost", "ListPrice", "Weight"]:
        products = cast_if(products, c, T.DoubleType())
    if "MakeFlag" in products.columns:
        products = products.withColumn(
            "MakeFlag",
            F.when(F.lower(F.col("MakeFlag")) == "true", True)
             .when(F.lower(F.col("MakeFlag")) == "false", False)
             .otherwise(None)
        )

    # Sales Order Detail
    for c in ["SalesOrderID", "SalesOrderDetailID", "ProductID", "OrderQty"]:
        sod = cast_if(sod, c, T.IntegerType())
    for c in ["UnitPrice", "UnitPriceDiscount"]:
        sod = cast_if(sod, c, T.DoubleType())

    # Sales Order Header
    for c in ["SalesOrderID", "CustomerID", "ShipToAddressID", "BillToAddressID", "SalesPersonID", "TerritoryID"]:
        soh = cast_if(soh, c, T.IntegerType())
    for c in ["OrderDate", "ShipDate"]:
        if c in soh.columns:
            soh = soh.withColumn(c, F.to_timestamp(F.col(c)))
    for c in ["SubTotal", "TaxAmt", "Freight", "TotalDue"]:
        soh = cast_if(soh, c, T.DoubleType())
    if "OnlineOrderFlag" in soh.columns:
        soh = soh.withColumn(
            "OnlineOrderFlag",
            F.when(F.lower(F.col("OnlineOrderFlag")) == "true", True)
             .when(F.lower(F.col("OnlineOrderFlag")) == "false", False)
             .otherwise(None)
        )

    # Get counts before write
    products_count = products.count()
    sod_count = sod.count()
    soh_count = soh.count()

    log.info("Silver layer complete")
    log.info(f"Products: {products_count} rows | Orders: {sod_count} details, {soh_count} headers")

    # Write silver
    products.write.mode("overwrite").parquet(str(paths["store"] / "store_products"))
    sod.write.mode("overwrite").parquet(str(paths["store"] / "store_sales_order_detail"))
    soh.write.mode("overwrite").parquet(str(paths["store"] / "store_sales_order_header"))

    spark.stop()


if __name__ == "__main__":
    main()
