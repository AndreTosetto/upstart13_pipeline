"""Shared utilities for project paths and Spark setup."""
from __future__ import annotations
from pathlib import Path
import logging
import os
import sys
from pyspark.sql import SparkSession


def project_root_from_file(file_path: str | None) -> Path:
    """Walk up from file to find project root (has 'data' dir)."""
    if file_path:
        current = Path(file_path).resolve().parent
        while current != current.parent:
            if (current / "data").exists():
                return current
            current = current.parent
        return Path(file_path).resolve().parents[2] if len(Path(file_path).resolve().parents) > 2 else Path(file_path).resolve().parents[1]
    return Path.cwd().resolve()

def get_paths(script_file: str | None) -> dict:
    """Get project directories (bronze/silver/gold layers)."""
    root = project_root_from_file(script_file)
    data = root / "data"
    out = root / "out"

    # Medallion layers with dual naming convention
    raw = out / "raw(bronze)"        # Bronze layer - raw data from CSV
    store = out / "store(silver)"    # Silver layer - cleaned, typed data
    publish = out / "publish(gold)"  # Gold layer - dimensional model / aggregations

    paths = {
        "root": root,
        "data": data,
        "out": out,
        "raw": raw,
        "store": store,
        "publish": publish,
        "quality": out / "quality",
        "spark_warehouse": out / "_spark" / "warehouse",
        "spark_local": out / "_spark" / "temp"
    }
    for p in [paths["raw"], paths["store"], paths["publish"], paths["quality"]]:
        p.mkdir(parents=True, exist_ok=True)
    return paths

def build_spark(app_name: str, paths: dict) -> SparkSession:
    """Create local Spark session with proper paths and configs."""

    for key in ("out", "raw", "store", "publish"):
        Path(paths[key]).mkdir(parents=True, exist_ok=True)

    spark_tmp = Path(paths["spark_local"]).resolve()
    warehouse = Path(paths["spark_warehouse"]).resolve()
    spark_tmp.mkdir(parents=True, exist_ok=True)
    warehouse.mkdir(parents=True, exist_ok=True)

    os.environ["SPARK_LOCAL_DIRS"] = str(spark_tmp)
    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.local.dir", str(spark_tmp))
        .config("spark.pyspark.python", python_exe)                 # executors use this
        .config("spark.executorEnv.PYSPARK_PYTHON", python_exe)     # and this
        .config("spark.driver.memory", "2g")                        # increase driver memory
        .config("spark.executor.memory", "2g")                      # increase executor memory
        .config("spark.sql.shuffle.partitions", "4")                # reduce shuffle partitions for small datasets
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Quick sanity: print the python chosen for workers (helps troubleshooting)
    chosen = spark.sparkContext.getConf().get("spark.pyspark.python")
    print(f"[spark] pyspark.python = {chosen}")
    print(f"[spark] local.dir      = {spark_tmp}")
    print(f"[spark] warehouse      = {warehouse}")

    return spark

def setup_logging(name: str = "upstart") -> logging.Logger:
    """Setup basic logging."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
    return logger
