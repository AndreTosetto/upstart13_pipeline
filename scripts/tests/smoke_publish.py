"""
Smoke test - validates publish layer outputs exist and contain required fields.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common import get_paths, build_spark, setup_logging


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.smoke_test")
    paths = get_paths(script_file)
    spark = build_spark("upstart_smoke_test", paths)

    publish_root = Path(paths["publish"])

    # Validate publish_product
    log.info("Checking publish_product...")
    df_product = spark.read.parquet(str(publish_root / "publish_product"))
    assert df_product.count() > 0, "publish_product is empty"
    assert "Color" in df_product.columns, "Missing Color column"
    assert "ProductCategoryName" in df_product.columns, "Missing ProductCategoryName column"

    # Validate publish_orders
    log.info("Checking publish_orders...")
    df_orders = spark.read.parquet(str(publish_root / "publish_orders"))
    assert df_orders.count() > 0, "publish_orders is empty"
    assert "LeadTimeInBusinessDays" in df_orders.columns, "Missing LeadTimeInBusinessDays"
    assert "TotalLineExtendedPrice" in df_orders.columns, "Missing TotalLineExtendedPrice"
    assert "TotalOrderFreight" in df_orders.columns, "Missing TotalOrderFreight"

    # Validate analysis outputs
    log.info("Checking analysis_top_color_by_year...")
    df_color = spark.read.parquet(str(publish_root / "analysis_top_color_by_year"))
    assert df_color.count() > 0, "analysis_top_color_by_year is empty"

    log.info("Checking analysis_avg_lead_by_category...")
    df_lead = spark.read.parquet(str(publish_root / "analysis_avg_lead_by_category"))
    assert df_lead.count() > 0, "analysis_avg_lead_by_category is empty"

    log.info("All smoke tests passed")
    spark.stop()


if __name__ == "__main__":
    main()
