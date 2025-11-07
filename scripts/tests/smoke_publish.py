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

    # Business days edge case tests
    log.info("Testing business days edge cases...")

    # Test 1: Mon -> Wed = 2 business days
    # 2021-05-31 (Mon) -> 2021-06-02 (Wed)
    test1 = df_orders.filter(
        (df_orders.OrderDate == "2021-05-31") & (df_orders.ShipDate == "2021-06-02")
    )
    if test1.count() > 0:
        lead_time = test1.select("LeadTimeInBusinessDays").first()[0]
        assert lead_time == 2, f"Mon->Wed should be 2 business days, got {lead_time}"
        log.info("  Edge case 1: Mon->Wed = 2 days PASSED")

    # Test 2: Fri -> Mon (skipping weekend) = 1 business day
    # 2021-06-04 (Fri) -> 2021-06-07 (Mon)
    test2 = df_orders.filter(
        (df_orders.OrderDate == "2021-06-04") & (df_orders.ShipDate == "2021-06-07")
    )
    if test2.count() > 0:
        lead_time = test2.select("LeadTimeInBusinessDays").first()[0]
        assert lead_time == 1, f"Fri->Mon should be 1 business day, got {lead_time}"
        log.info("  Edge case 2: Fri->Mon (skipping weekend) = 1 day PASSED")

    # Test 3: Same day shipping = 0 business days
    same_day = df_orders.filter(df_orders.OrderDate == df_orders.ShipDate)
    if same_day.count() > 0:
        lead_time = same_day.select("LeadTimeInBusinessDays").first()[0]
        assert lead_time == 0, f"Same day shipping should be 0 business days, got {lead_time}"
        log.info("  Edge case 3: Same day shipping = 0 days PASSED")
    else:
        log.info("  Edge case 3: No same-day shipping in dataset (OK)")

    log.info("Business days logic validated")

    # Business integrity checks
    log.info("Running business integrity checks...")

    # Check 1: TotalOrderFreight >= 0
    negative_freight = df_orders.filter(df_orders.TotalOrderFreight < 0).count()
    assert negative_freight == 0, f"Found {negative_freight} rows with negative freight"
    log.info("  Integrity check 1: TotalOrderFreight >= 0 PASSED")

    # Check 2: Price integrity (negative prices allowed for returns/adjustments)
    # Verify that negative prices only occur when OrderQty is negative (returns)
    invalid_price = df_orders.filter(
        ((df_orders.TotalLineExtendedPrice < 0) & (df_orders.OrderQty >= 0)) |
        ((df_orders.TotalLineExtendedPrice > 0) & (df_orders.OrderQty < 0))
    ).count()
    assert invalid_price == 0, f"Found {invalid_price} rows with price/qty sign mismatch"
    log.info("  Integrity check 2: Price signs match OrderQty (negative = returns) PASSED")

    # Check 3: No data loss in join (publish_orders has same row count as detail)
    detail_path = paths["store"] / "store_sales_order_detail"
    df_detail = spark.read.parquet(str(detail_path))
    detail_count = df_detail.count()
    orders_count = df_orders.count()
    assert detail_count == orders_count, \
        f"Data loss detected: detail={detail_count} vs orders={orders_count}"
    log.info(f"  Integrity check 3: No data loss in join ({orders_count:,} rows preserved) PASSED")

    log.info("All business integrity checks passed")

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
