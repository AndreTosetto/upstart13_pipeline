"""
Print schemas and sample data for README documentation.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import get_paths, build_spark, setup_logging


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.print_schemas")
    paths = get_paths(script_file)
    spark = build_spark("upstart_print_schemas", paths)

    store_root = paths["store"]
    publish_root = paths["publish"]

    print("\n" + "=" * 80)
    print("SILVER LAYER SCHEMAS (store_*)")
    print("=" * 80)

    # Store products
    print("\n### store_products")
    df_products = spark.read.parquet(str(store_root / "store_products"))
    print(f"Rows: {df_products.count():,}\n")
    for field in df_products.schema.fields:
        print(f"  {field.name:<30} {str(field.dataType):<20}")

    # Store sales_order_header
    print("\n### store_sales_order_header")
    df_soh = spark.read.parquet(str(store_root / "store_sales_order_header"))
    print(f"Rows: {df_soh.count():,}\n")
    for field in df_soh.schema.fields:
        print(f"  {field.name:<30} {str(field.dataType):<20}")

    # Store sales_order_detail
    print("\n### store_sales_order_detail")
    df_sod = spark.read.parquet(str(store_root / "store_sales_order_detail"))
    print(f"Rows: {df_sod.count():,}\n")
    for field in df_sod.schema.fields:
        print(f"  {field.name:<30} {str(field.dataType):<20}")

    print("\n" + "=" * 80)
    print("GOLD LAYER SCHEMA (publish_orders)")
    print("=" * 80)

    df_orders = spark.read.parquet(str(publish_root / "publish_orders"))
    print(f"\nRows: {df_orders.count():,}\n")

    # Group columns by source
    detail_cols = set(df_sod.columns)
    header_cols = set(df_soh.columns) - {"SalesOrderID"}  # Excluded per spec
    calculated_cols = {"LeadTimeInBusinessDays", "TotalLineExtendedPrice",
                       "TotalLineExtendedPrice_Spec", "TotalLineExtendedPrice_Correct",
                       "PriceDiff_Pct", "TotalOrderFreight"}

    print("Columns from SalesOrderDetail:")
    for field in df_orders.schema.fields:
        if field.name in detail_cols:
            print(f"  {field.name:<35} {str(field.dataType):<20}")

    print("\nColumns from SalesOrderHeader (except SalesOrderId):")
    for field in df_orders.schema.fields:
        if field.name in header_cols or field.name == "TotalOrderFreight":
            print(f"  {field.name:<35} {str(field.dataType):<20}")

    print("\nCalculated Columns:")
    for field in df_orders.schema.fields:
        if field.name in calculated_cols and field.name != "TotalOrderFreight":
            print(f"  {field.name:<35} {str(field.dataType):<20}")

    print("\n" + "=" * 80)
    print("SAMPLE DATA (first 10 rows of publish_orders)")
    print("=" * 80 + "\n")

    sample = df_orders.select(
        "SalesOrderID",
        "SalesOrderDetailID",
        "OrderDate",
        "ShipDate",
        "ProductID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LeadTimeInBusinessDays",
        "TotalLineExtendedPrice",
        "TotalOrderFreight"
    ).limit(10)

    sample.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
