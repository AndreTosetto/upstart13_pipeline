"""
Gold layer: Build the publish_product table.

Two transformations required by the exercise:
1. Fill blank colors with "N/A"
2. Map missing categories based on subcategory rules
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F  # noqa: E402
from common import get_paths, build_spark, setup_logging  # noqa: E402


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.publish_product")
    paths = get_paths(script_file)
    spark = build_spark("upstart_publish_product", paths)

    src = paths["store"] / "store_products"
    log.info(f"Reading silver products from {src}")
    df = spark.read.parquet(str(src))

    color_col = "Color"
    cat_col = "ProductCategoryName"
    subcat_col = "ProductSubCategoryName"

    # Make sure these columns exist (defensive coding)
    for col in (color_col, cat_col, subcat_col):
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))

    # Clean up whitespace
    df = df.withColumn(color_col, F.trim(F.col(color_col)))
    df = df.withColumn(subcat_col, F.trim(F.col(subcat_col)))
    df = df.withColumn(cat_col, F.trim(F.col(cat_col)))

    # Requirement 1: Fill blank colors with "N/A"
    df = df.withColumn(
        color_col,
        F.when(F.col(color_col).isNull() | (F.col(color_col) == ""), F.lit("N/A")).otherwise(F.col(color_col))
    )

    # Requirement 2: Map missing categories based on subcategory
    # Note: This fills NULL categories only (Silver already fixed WRONG categories)
    cat_is_missing = F.col(cat_col).isNull() | (F.col(cat_col) == "")

    # These are the rules from the exercise
    clothing = ["Gloves", "Shorts", "Socks", "Tights", "Vests"]
    accessories = ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"]
    components_exact = ["Wheels", "Saddles"]
    frames_check = F.lower(F.col(subcat_col)).contains("frames")

    df = (
        df.withColumn(
            cat_col,
            F.when(cat_is_missing & F.col(subcat_col).isin(clothing), F.lit("Clothing"))
             .when(cat_is_missing & F.col(subcat_col).isin(accessories), F.lit("Accessories"))
             .when(cat_is_missing & (frames_check | F.col(subcat_col).isin(components_exact)), F.lit("Components"))
             .otherwise(F.col(cat_col))
        )
    )

    remaining = df.filter(cat_is_missing).count()
    log.info(f"Remaining blank categories: {remaining}")

    dest = paths["publish"] / "publish_product"
    log.info(f"Writing publish_product to {dest}")
    df.write.mode("overwrite").parquet(str(dest))

    log.info("publish_product done")
    spark.stop()


if __name__ == "__main__":
    main()
