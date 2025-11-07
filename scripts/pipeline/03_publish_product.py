"""
Gold layer: Build the publish_product table.

Applies two transformations as specified:
1. Replace NULL values in Color field with "N/A"
2. Enhance ProductCategoryName when NULL using subcategory mapping rules
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from common import get_paths, build_spark, setup_logging


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.publish_product")
    paths = get_paths(script_file)
    spark = build_spark("upstart_publish_product", paths)

    # Read silver products
    df = spark.read.parquet(str(paths["store"] / "store_products"))

    # Transformation 1: Replace NULL Color with "N/A"
    df = df.withColumn(
        "Color",
        F.when(F.col("Color").isNull() | (F.col("Color") == ""), F.lit("N/A"))
         .otherwise(F.col("Color"))
    )

    # Transformation 2: Enhance ProductCategoryName when NULL
    # Mapping rules from specification:
    cat_is_null = F.col("ProductCategoryName").isNull() | (F.col("ProductCategoryName") == "")

    clothing = ["Gloves", "Shorts", "Socks", "Tights", "Vests"]
    accessories = ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"]
    components_exact = ["Wheels", "Saddles"]
    contains_frames = F.lower(F.col("ProductSubCategoryName")).contains("frames")

    df = df.withColumn(
        "ProductCategoryName",
        F.when(cat_is_null & F.col("ProductSubCategoryName").isin(clothing), F.lit("Clothing"))
         .when(cat_is_null & F.col("ProductSubCategoryName").isin(accessories), F.lit("Accessories"))
         .when(cat_is_null & (contains_frames | F.col("ProductSubCategoryName").isin(components_exact)), F.lit("Components"))
         .otherwise(F.col("ProductCategoryName"))
    )

    # Get count once before write
    count = df.count()
    log.info(f"Processing {count} products")

    # Write publish table
    dest = paths["publish"] / "publish_product"
    df.write.mode("overwrite").parquet(str(dest))
    log.info(f"Wrote publish_product to {dest}")

    spark.stop()


if __name__ == "__main__":
    main()
