"""
Gold layer: Build the publish_orders table.

Joins order details with headers and adds two calculated fields:
- LeadTimeInBusinessDays (weekdays only between OrderDate and ShipDate)
- TotalLineExtendedPrice = OrderQty * (UnitPrice - UnitPriceDiscount)
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F, types as T  # noqa: E402
from common import get_paths, build_spark, setup_logging  # noqa: E402


def business_days_between() -> F.Column:
    """
    Count business days (Mon-Fri only) between OrderDate and ShipDate.
    We generate a sequence of dates and filter out weekends.
    """
    return F.when(
        F.col("OrderDate").isNull() | F.col("ShipDate").isNull(),
        F.lit(None).cast(T.IntegerType())
    ).otherwise(
        F.size(
            F.expr(
                "filter("
                "sequence(cast(OrderDate as date), date_sub(cast(ShipDate as date), 1), interval 1 day), "
                "d -> dayofweek(d) BETWEEN 2 AND 6)"  # Mon=2, Fri=6
            )
        ).cast(T.IntegerType())
    )


def total_line_extended_price() -> F.Column:
    """Calculate line total per specification: OrderQty * (UnitPrice - UnitPriceDiscount).

    Note: The spec formula appears ambiguous - UnitPriceDiscount could be a rate or amount.
    Following the literal spec: OrderQty * (UnitPrice - UnitPriceDiscount).
    """
    return (
        F.col("OrderQty").cast(T.DoubleType()) *
        (F.col("UnitPrice") - F.col("UnitPriceDiscount"))
    )


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.publish_orders")
    paths = get_paths(script_file)
    spark = build_spark("upstart_publish_orders", paths)

    sod_path = paths["store"] / "store_sales_order_detail"
    soh_path = paths["store"] / "store_sales_order_header"

    log.info(f"Reading sales_order_detail from {sod_path}")
    sod = spark.read.parquet(str(sod_path))
    log.info(f"Detail rows: {sod.count():,}")

    log.info(f"Reading sales_order_header from {soh_path}")
    soh = spark.read.parquet(str(soh_path))
    log.info(f"Header rows: {soh.count():,}")

    # Join detail with header
    log.info("Joining detail with header on SalesOrderID")
    joined = sod.join(soh, on="SalesOrderID", how="inner")

    # Add calculated fields
    joined = joined.withColumn("LeadTimeInBusinessDays", business_days_between())
    joined = joined.withColumn("TotalLineExtendedPrice", total_line_extended_price())

    # Rename Freight to TotalOrderFreight as required by spec
    if "Freight" in joined.columns:
        joined = joined.withColumnRenamed("Freight", "TotalOrderFreight")

    # Drop SalesOrderID from header as per spec:
    # "All fields from SalesOrderHeader EXCEPT SalesOrderId"
    # Note: SalesOrderID is kept from detail side (it's the join key)
    # This ensures we have the field for joins downstream but follow spec exactly

    dest = paths["publish"] / "publish_orders"
    log.info(f"Writing publish_orders to {dest}")
    log.info(f"Final row count: {joined.count():,}")

    joined.write.mode("overwrite").parquet(str(dest))

    log.info("publish_orders done")
    spark.stop()


if __name__ == "__main__":
    main()
