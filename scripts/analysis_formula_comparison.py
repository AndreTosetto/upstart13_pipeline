"""
Generate formula comparison report: TotalLineExtendedPrice_Spec vs _Correct.

Shows the percentage difference between literal spec formula and standard business logic.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pyspark.sql import functions as F
from common import get_paths, build_spark, setup_logging


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.formula_comparison")
    paths = get_paths(script_file)
    spark = build_spark("upstart_formula_comparison", paths)

    publish_root = paths["publish"]

    log.info("Reading publish_orders...")
    df = spark.read.parquet(str(publish_root / "publish_orders"))

    # Calculate statistics
    log.info("Calculating formula comparison statistics...")

    stats = df.agg(
        F.sum("TotalLineExtendedPrice_Spec").alias("Total_Spec"),
        F.sum("TotalLineExtendedPrice_Correct").alias("Total_Correct"),
        F.avg("PriceDiff_Pct").alias("Avg_Diff_Pct"),
        F.min("PriceDiff_Pct").alias("Min_Diff_Pct"),
        F.max("PriceDiff_Pct").alias("Max_Diff_Pct"),
        F.count("*").alias("Total_Rows")
    ).collect()[0]

    # Print report
    print("\n" + "=" * 80)
    print("FORMULA COMPARISON REPORT")
    print("=" * 80)
    print(f"\nTotal rows analyzed: {stats.Total_Rows:,}")
    print(f"\nFormula A (Spec Literal): OrderQty * (UnitPrice - UnitPriceDiscount)")
    print(f"  Total Revenue: ${stats.Total_Spec:,.2f}")
    print(f"\nFormula B (Standard Logic): OrderQty * UnitPrice * (1 - UnitPriceDiscount)")
    print(f"  Total Revenue: ${stats.Total_Correct:,.2f}")

    diff_amount = stats.Total_Correct - stats.Total_Spec
    diff_pct_total = (diff_amount / stats.Total_Spec * 100) if stats.Total_Spec != 0 else 0

    print(f"\nDifference:")
    print(f"  Amount: ${diff_amount:,.2f}")
    print(f"  Percentage (total): {diff_pct_total:+.2f}%")
    print(f"  Avg per line: {stats.Avg_Diff_Pct:+.2f}%")
    print(f"  Range: {stats.Min_Diff_Pct:+.2f}% to {stats.Max_Diff_Pct:+.2f}%")

    print("\n" + "=" * 80)
    print("RECOMMENDATION:")
    print("=" * 80)
    print("Official deliverable uses Formula A (spec literal) to match requirements.")
    print("Both formulas are preserved in publish_orders for client clarification.")
    print("=" * 80 + "\n")

    # Export sample comparison to CSV
    log.info("Exporting sample comparison to CSV...")
    sample = df.select(
        "SalesOrderID",
        "SalesOrderDetailID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "TotalLineExtendedPrice_Spec",
        "TotalLineExtendedPrice_Correct",
        "PriceDiff_Pct"
    ).limit(100)

    output_path = publish_root / "formula_comparison_sample.csv"
    sample.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_path))
    log.info(f"Sample exported to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
