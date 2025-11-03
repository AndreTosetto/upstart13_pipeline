"""
Smoke test for publish-layer outputs.
Run after executing the core pipeline (01-05) to ensure deliverables exist and contain expected fields.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F  # noqa: E402
from common import get_paths, build_spark, setup_logging  # noqa: E402


def expect(condition: bool, message: str) -> None:
    """Raise a runtime error when a logical expectation fails."""
    if not condition:
        raise RuntimeError(message)


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.tests.smoke_publish")
    paths = get_paths(script_file)
    spark = build_spark("upstart_smoke_publish", paths)

    publish_root = Path(paths["publish"])
    log.info(f"Publish root: {publish_root}")
    expect(publish_root.exists(), "Publish directory not found. Run pipeline steps 01-05 first.")

    # === publish_product ===
    product_path = publish_root / "publish_product"
    log.info("Validating publish_product...")
    expect(product_path.exists(), "publish_product parquet folder is missing.")

    df_product = spark.read.parquet(str(product_path))
    expect(df_product.count() > 0, "publish_product is empty.")
    for col in ("ProductID", "Color", "ProductCategoryName"):
        nulls = df_product.filter(F.col(col).isNull() | (F.trim(F.col(col)) == "")).count()
        expect(nulls == 0, f"publish_product has {nulls} null/blank values in {col}.")

    # === publish_orders ===
    orders_path = publish_root / "publish_orders"
    log.info("Validating publish_orders...")
    expect(orders_path.exists(), "publish_orders parquet folder is missing.")

    df_orders = spark.read.parquet(str(orders_path))
    expect(df_orders.count() > 0, "publish_orders is empty.")
    required_columns = {
        "LeadTimeInBusinessDays",
        "TotalLineExtendedPrice",
        "TotalOrderFreight",
        "OrderDate",
        "ShipDate",
    }
    missing = required_columns - set(df_orders.columns)
    expect(not missing, f"publish_orders missing required columns: {sorted(missing)}")

    # Lead time should be numeric with no nulls when both dates exist.
    lead_nulls = (
        df_orders.filter(
            (F.col("OrderDate").isNotNull())
            & (F.col("ShipDate").isNotNull())
            & F.col("LeadTimeInBusinessDays").isNull()
        )
        .count()
    )
    expect(lead_nulls == 0, "publish_orders has null lead times despite valid dates.")

    # === analysis outputs ===
    analysis_sets = [
        ("analysis_top_color_by_year", {"OrderYear", "Color", "Revenue"}),
        ("analysis_avg_lead_by_category", {"ProductCategoryName", "AvgLeadTimeInBusinessDays"}),
    ]
    for name, cols in analysis_sets:
        log.info(f"Validating {name}...")
        path = publish_root / name
        expect(path.exists(), f"{name} parquet folder is missing.")
        df = spark.read.parquet(str(path))
        expect(df.count() > 0, f"{name} is empty.")
        missing_cols = cols - set(df.columns)
        expect(not missing_cols, f"{name} missing columns: {sorted(missing_cols)}")

    log.info("Smoke test passed. Publish outputs are ready.")
    spark.stop()


if __name__ == "__main__":
    main()
