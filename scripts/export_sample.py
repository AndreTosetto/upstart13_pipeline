"""
Export 10-row sample of publish_orders for quick preview.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import get_paths, build_spark, setup_logging


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.export_sample")
    paths = get_paths(script_file)
    spark = build_spark("upstart_export_sample", paths)

    publish_root = paths["publish"]

    log.info("Exporting 10-row sample of publish_orders...")
    df_orders = spark.read.parquet(str(publish_root / "publish_orders"))

    # Select key columns for sample
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
        "TotalOrderFreight",
        "OnlineOrderFlag",
        "CustomerID"
    ).limit(10)

    output_path = publish_root / "sample_publish_orders.csv"
    sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_path))
    log.info(f"Sample exported to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
