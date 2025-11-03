"""
Data quality scan for raw CSV files.
Profiles CSV files from data/ folder and writes reports to out/quality_raw/.

Checks performed:
- Row counts per table
- Column null rates
- Basic numeric stats (min/max/avg/std)
- Distinct value counts
- Sample data
- String length stats
"""

from __future__ import annotations
import sys
from pathlib import Path
import csv
import logging

import pandas as pd
import numpy as np

# Add parent directory to path to import common module
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import get_paths, setup_logging


def profile_csv_file(csv_path: Path, table_name: str, out_dir: Path, log) -> None:
    """Profile a single CSV file and write reports."""
    log.info(f"Profiling {table_name} from {csv_path.name}")

    table_out = out_dir / table_name
    table_out.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    # Row count
    row_count = len(df)
    log.info(f"  Row count: {row_count:,}")
    row_count_df = pd.DataFrame([{"table": table_name, "row_count": row_count}])
    row_count_df.to_csv(table_out / "row_count.csv", index=False)

    if row_count == 0:
        log.warning(f"  Empty file: {table_name}")
        return

    # Null rates
    log.info(f"  Computing null rates")
    null_rates = []
    for col in df.columns:
        null_count = (df[col].isna() | (df[col] == '')).sum()
        null_rate = null_count / row_count
        null_rates.append({"column": col, "null_rate": null_rate})
    null_df = pd.DataFrame(null_rates)
    null_df.to_csv(table_out / "null_rates.csv", index=False)

    # Distinct counts
    log.info(f"  Computing distinct counts")
    distinct_counts = []
    for col in df.columns:
        distinct_count = df[col].nunique()
        distinct_counts.append({"column": col, "distinct_count": distinct_count})
    distinct_df = pd.DataFrame(distinct_counts)
    distinct_df.to_csv(table_out / "distinct_counts.csv", index=False)

    # Numeric stats
    log.info(f"  Computing numeric stats")
    numeric_stats = []
    for col in df.columns:
        numeric_col = pd.to_numeric(df[col], errors='coerce')
        non_null_count = numeric_col.notna().sum()

        if non_null_count > 0:
            stats = {
                "column": col,
                "min": numeric_col.min(),
                "max": numeric_col.max(),
                "avg": numeric_col.mean(),
                "stddev": numeric_col.std(),
                "numeric_count": non_null_count
            }
            numeric_stats.append(stats)

    if numeric_stats:
        numeric_df = pd.DataFrame(numeric_stats)
        numeric_df.to_csv(table_out / "numeric_stats.csv", index=False)

    # Sample data
    log.info(f"  Saving sample data")
    sample_df = df.head(20)
    sample_df.to_csv(table_out / "sample_data.csv", index=False)

    # String length stats
    log.info(f"  Computing string length stats")
    length_stats = []
    for col in df.columns:
        lengths = df[col].str.len()
        stats = {
            "column": col,
            "min_length": lengths.min() if not lengths.isna().all() else None,
            "max_length": lengths.max() if not lengths.isna().all() else None,
            "avg_length": lengths.mean() if not lengths.isna().all() else None
        }
        length_stats.append(stats)

    length_df = pd.DataFrame(length_stats)
    length_df.to_csv(table_out / "string_lengths.csv", index=False)

    log.info(f"  Done with {table_name}\n")


def generate_summary_report(out_root: Path, csv_files: dict, log) -> None:
    """Generate a consolidated summary report highlighting issues."""
    summary_lines = []
    summary_lines.append("=" * 80)
    summary_lines.append("DATA QUALITY SUMMARY REPORT")
    summary_lines.append("=" * 80)
    summary_lines.append("")

    issues_found = False

    # Known foreign keys (not duplicates)
    fk_patterns = ["CustomerID", "SalesPersonID"]

    for table_name in csv_files.keys():
        table_out = out_root / table_name
        if not table_out.exists():
            continue

        summary_lines.append(f"\n{table_name.upper()}")
        summary_lines.append("-" * 40)

        # Read reports
        null_df = pd.read_csv(table_out / "null_rates.csv")
        distinct_df = pd.read_csv(table_out / "distinct_counts.csv")
        row_count_df = pd.read_csv(table_out / "row_count.csv")

        row_count = row_count_df["row_count"].iloc[0]
        summary_lines.append(f"Total rows: {row_count:,}")

        # Check for high null rates (only > 50%)
        high_nulls = null_df[null_df["null_rate"] > 0.5]
        if len(high_nulls) > 0:
            issues_found = True
            summary_lines.append("\nISSUE: High null rates detected:")
            for _, row in high_nulls.iterrows():
                pct = row["null_rate"] * 100
                severity = "CRITICAL" if pct > 70 else "WARNING"
                summary_lines.append(f"  - {row['column']}: {pct:.1f}% nulls [{severity}]")

        # Check for actual duplicates in primary keys only
        pk_cols = []
        for _, row in distinct_df.iterrows():
            col_name = row["column"]
            # Only check columns that end with ID and match table name
            if col_name.lower().endswith("id"):
                # Skip known foreign keys
                if any(fk in col_name for fk in fk_patterns):
                    continue
                # Check if it's the table's primary key or detail ID
                base_name = table_name.replace("_", "").lower()
                if base_name in col_name.lower() or "detail" in col_name.lower():
                    pk_cols.append(row)

        for row in pk_cols:
            col_name = row["column"]
            if row["distinct_count"] < row_count:
                issues_found = True
                dup_count = row_count - row["distinct_count"]
                summary_lines.append(f"\nISSUE: Duplicate values in primary key '{col_name}'")
                summary_lines.append(f"  - {dup_count:,} duplicate rows detected")

        # Low cardinality check (informational)
        low_card = distinct_df[(distinct_df["distinct_count"] <= 5) &
                                (~distinct_df["column"].str.contains("Flag|Level|Point", case=False))]
        if len(low_card) > 0:
            summary_lines.append("\nINFO: Low cardinality columns (categorical):")
            for _, row in low_card.iterrows():
                summary_lines.append(f"  - {row['column']}: {row['distinct_count']} values")

    summary_lines.append("\n" + "=" * 80)
    if issues_found:
        summary_lines.append("RECOMMENDATION: Review issues above before proceeding to silver layer")
    else:
        summary_lines.append("STATUS: No critical issues found - ready for bronze layer")
    summary_lines.append("=" * 80)

    # Write summary
    summary_path = out_root / "QUALITY_SUMMARY.txt"
    with open(summary_path, "w") as f:
        f.write("\n".join(summary_lines))

    log.info(f"\nSummary report: {summary_path}")
    print("\n" + "\n".join(summary_lines))


def main(script_file: str | None = __file__) -> None:
    log = setup_logging("upstart.quality_scan_raw")
    log.info("Starting quality scan")

    paths = get_paths(script_file)

    out_root = Path(paths["quality"])
    out_root.mkdir(parents=True, exist_ok=True)
    log.info(f"Output directory: {out_root}\n")

    data_dir = Path(paths["data"])

    csv_files = {
        "products": data_dir / "products.csv",
        "sales_order_header": data_dir / "sales_order_header.csv",
        "sales_order_detail": data_dir / "sales_order_detail.csv"
    }

    for table_name, csv_path in csv_files.items():
        if csv_path.exists():
            try:
                profile_csv_file(csv_path, table_name, out_root, log)
            except Exception as e:
                log.error(f"Error profiling {table_name}: {str(e)}")
                import traceback
                log.error(traceback.format_exc())
        else:
            log.warning(f"CSV file not found: {csv_path}")

    # Generate summary report
    generate_summary_report(out_root, csv_files, log)

    log.info(f"\nQuality scan complete. Reports saved to: {out_root}")
    log.info(f"Check QUALITY_SUMMARY.txt for issues overview")


if __name__ == "__main__":
    main()
