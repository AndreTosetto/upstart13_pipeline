"""
Bronze layer: Load raw CSV files and save as Parquet.

Just a straight copy with no changes - we keep everything exactly as it came.
Quality fixes happen later in the silver layer.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from common import get_paths, build_spark, setup_logging

def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Read CSV without inferring types - everything stays as strings."""
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # no type inference, keep as strings
        .csv(str(path))
    )

def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.load_raw")
    paths = get_paths(script_file)
    spark = build_spark("upstart13_load_raw", paths)

    log.info(f"project_root={paths['root']}")
    log.info(f"data_dir={paths['data']}")
    log.info(f"raw_dir={paths['raw']}")

    # Read CSVs without any transformations
    log.info("Reading CSVs (all columns as strings)...")
    df_products = read_csv(spark, paths["data"] / "products.csv")
    df_sod      = read_csv(spark, paths["data"] / "sales_order_detail.csv")
    df_soh      = read_csv(spark, paths["data"] / "sales_order_header.csv")

    # Write to bronze - just Parquet format, no changes to data
    log.info("Writing to bronze layer...")
    df_products.write.mode("overwrite").parquet(str(paths["raw"] / "raw_products"))
    df_sod.write.mode("overwrite").parquet(str(paths["raw"] / "raw_sales_order_detail"))
    df_soh.write.mode("overwrite").parquet(str(paths["raw"] / "raw_sales_order_header"))

    log.info("Bronze layer done - data preserved exactly as-is")
    spark.stop()

if __name__ == "__main__":
    main()
