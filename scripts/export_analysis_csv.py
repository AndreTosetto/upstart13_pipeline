"""
Export analysis tables from Parquet to CSV for easy viewing.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from common import get_paths, build_spark, setup_logging


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.export_csv")
    paths = get_paths(script_file)
    spark = build_spark("upstart_export_csv", paths)

    publish_root = paths["publish"]

    # Export top color by year
    log.info("Exporting analysis_top_color_by_year to CSV...")
    df_color = spark.read.parquet(str(publish_root / "analysis_top_color_by_year"))
    df_color = df_color.orderBy("OrderYear")
    output_color = publish_root / "analysis_top_color_by_year.csv"
    df_color.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_color))
    log.info(f"Exported to {output_color}")

    # Export avg lead by category
    log.info("Exporting analysis_avg_lead_by_category to CSV...")
    df_lead = spark.read.parquet(str(publish_root / "analysis_avg_lead_by_category"))
    df_lead = df_lead.orderBy("ProductCategoryName")
    output_lead = publish_root / "analysis_avg_lead_by_category.csv"
    df_lead.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_lead))
    log.info(f"Exported to {output_lead}")

    log.info("CSV exports complete")
    spark.stop()


if __name__ == "__main__":
    main()
