"""
Executes the main pipeline (bronze -> silver -> gold).

Runs scripts 01-05 in sequence to generate publish tables and required analyses.

CLI Flags:
  --price-mode {spec,correct}  Formula for TotalLineExtendedPrice (default: spec)
  --impute-orderdate           Backfill incomplete OrderDate values (default: False)
  --help                       Show this help message
"""
import subprocess
import sys
import argparse
import os
from pathlib import Path
from datetime import datetime

# For publish summary - use parquet metadata instead of Spark
try:
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

# Pipeline scripts in execution order
PIPELINE_SCRIPTS = [
    "scripts/pipeline/01_load_raw.py",
    "scripts/pipeline/02_store_cast_and_keys.py",
    "scripts/pipeline/03_publish_product.py",
    "scripts/pipeline/04_publish_orders.py",
    "scripts/pipeline/05_analysis_questions.py",
]

def print_publish_summary(project_root: Path):
    """Print summary of publish layer outputs using parquet metadata."""
    if not PARQUET_AVAILABLE:
        print("\nNote: Install pyarrow to see publish summary")
        return

    print("\n" + "="*70)
    print("PUBLISH LAYER SUMMARY")
    print("="*70)

    try:
        publish_root = project_root / "out" / "publish(gold)"

        # Publish tables - read row counts from parquet metadata
        print("\nPublish Tables:")
        for table in ["publish_product", "publish_orders"]:
            path = publish_root / table
            if path.exists():
                # Read metadata without loading data
                parquet_files = list(path.glob("*.parquet"))
                if parquet_files:
                    total_rows = sum(pq.read_metadata(str(f)).num_rows for f in parquet_files)
                    print(f"  {table:<20} {total_rows:>8,} rows")

        # Note about analysis outputs
        print("\nAnalysis Outputs:")
        print("  analysis_top_color_by_year      - Top revenue color per year")
        print("  analysis_avg_lead_by_category   - Average lead time by category")
        print("\nRun smoke test for detailed validation:")
        print("  python scripts/tests/smoke_publish.py")

        print("="*70)

    except Exception as e:
        print(f"Could not generate summary: {e}")
        print("="*70)


def run_script(script_path: Path, project_root: Path) -> tuple[bool, float]:
    """
    Run a single pipeline script and return success status and duration.

    Returns:
        (success: bool, duration_seconds: float)
    """
    start_time = datetime.now()
    script_name = script_path.name

    print(f"\n{'='*70}")
    print(f"Running: {script_name}")
    print(f"{'='*70}")

    try:
        # Run script as subprocess with config environment variables
        env = os.environ.copy()
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(project_root),
            capture_output=False,  # Show output in real-time
            text=True,
            timeout=600,  # 10 minute timeout per script
            env=env
        )

        duration = (datetime.now() - start_time).total_seconds()

        if result.returncode == 0:
            print(f"\n[OK] {script_name} completed successfully ({duration:.1f}s)")
            return True, duration
        else:
            print(f"\n[FAIL] {script_name} failed with exit code {result.returncode}")
            return False, duration

    except subprocess.TimeoutExpired:
        duration = (datetime.now() - start_time).total_seconds()
        print(f"\n[TIMEOUT] {script_name} timed out after {duration:.1f}s")
        return False, duration

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        print(f"\n[ERROR] {script_name} failed with error: {e}")
        return False, duration

def main():
    """Execute the complete pipeline."""
    parser = argparse.ArgumentParser(
        description="Upstart13 Data Engineering Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                          # Run with defaults (spec formula, no imputation)
  python run_pipeline.py --impute-orderdate       # Enable OrderDate backfilling
  python run_pipeline.py --price-mode correct     # Use standard business logic formula

Notes:
  - Price mode: 'spec' uses literal spec formula, 'correct' uses standard ERP logic
  - Imputation: Backfills 5 incomplete OrderDate values (+0.004% rows)
  - Both formulas are always preserved in publish_orders for comparison
        """
    )

    parser.add_argument(
        '--price-mode',
        choices=['spec', 'correct'],
        default='spec',
        help="Formula for TotalLineExtendedPrice (default: spec literal)"
    )

    parser.add_argument(
        '--impute-orderdate',
        action='store_true',
        help="Backfill incomplete OrderDate as ShipDate - 7 days (default: False)"
    )

    args = parser.parse_args()

    # Set environment variables for child processes
    os.environ['UPSTART_PRICE_MODE'] = args.price_mode
    os.environ['UPSTART_IMPUTE_ORDERDATE'] = 'true' if args.impute_orderdate else 'false'

    project_root = Path(__file__).resolve().parent

    print("\n" + "="*70)
    print("Starting pipeline execution - bronze/silver/gold layers")
    print("="*70)
    print(f"Project root: {project_root}")
    print(f"Total scripts: {len(PIPELINE_SCRIPTS)}")
    print(f"Config: price-mode={args.price_mode}, impute-orderdate={args.impute_orderdate}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Track results
    results = []
    total_start = datetime.now()

    # Run each script in sequence
    for script_rel_path in PIPELINE_SCRIPTS:
        script_path = project_root / script_rel_path

        if not script_path.exists():
            print(f"\n[NOT FOUND] Script not found: {script_path}")
            results.append((script_path.name, False, 0.0))
            continue

        success, duration = run_script(script_path, project_root)
        results.append((script_path.name, success, duration))

        # Stop pipeline if a script fails
        if not success:
            print(f"\n{'='*70}")
            print("PIPELINE FAILED - Stopping execution")
            print(f"{'='*70}")
            break

    # Summary
    total_duration = (datetime.now() - total_start).total_seconds()
    successful = sum(1 for _, success, _ in results if success)
    failed = sum(1 for _, success, _ in results if not success)

    print(f"\n{'='*70}")
    print("PIPELINE SUMMARY")
    print(f"{'='*70}")
    print(f"Total duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")
    print(f"Scripts executed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print()

    # Detailed results
    print("Script Results:")
    print(f"{'Script':<35} {'Status':<12} {'Duration':<10}")
    print("-" * 70)
    for script_name, success, duration in results:
        status = "[OK] SUCCESS" if success else "[FAIL] FAILED"
        print(f"{script_name:<35} {status:<15} {duration:>8.1f}s")

    print(f"\n{'='*70}")

    # Print publish summary if pipeline succeeded
    if failed == 0:
        print_publish_summary(project_root)

    # Exit with appropriate code
    if failed > 0:
        print("Pipeline completed with errors")
        sys.exit(1)
    else:
        print("Pipeline completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
