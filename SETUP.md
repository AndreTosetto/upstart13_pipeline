# Setup Guide

This guide walks you through getting the Upstart13 pipeline running on your machine.

---

## What You'll Need

### Java 11+
PySpark runs on the JVM, so you need Java installed.

**Check if you have it:**
```bash
java -version
```

**Don't have it?** Grab OpenJDK from [Adoptium](https://adoptium.net/) (works on all platforms).

### Python 3.10+
```bash
python --version
```

If you're on an older version, download the latest from [python.org](https://www.python.org/downloads/).

---

## Getting Started

### 1. Clone and Navigate
```bash
git clone <repository-url>
cd upstart
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

This installs PySpark 3.5.0, pandas, matplotlib, and a few other essentials. Should take a minute or two.

### 3. Verify PySpark Works
```bash
python -c "from pyspark.sql import SparkSession; print('PySpark OK')"
```

If you see `PySpark OK`, you're good to go.

---

## Running the Pipeline

Execute the numbered scripts in order from the project root:

```bash
python scripts/pipeline/01_load_raw.py
python scripts/pipeline/02_store_cast_and_keys.py
python scripts/pipeline/03_publish_product.py
python scripts/pipeline/04_publish_orders.py
python scripts/pipeline/05_analysis_questions.py
```

Each script logs what it's doing and writes output to the `out/` directory:
- `out/raw(bronze)/` - Raw CSV data as Parquet
- `out/store(silver)/` - Cleaned, typed data with quality fixes
- `out/publish(gold)/` - Final business-ready tables and analysis results

The whole pipeline takes about 1-2 minutes on a typical laptop.

---

## Optional Steps

### Run the Tests
```bash
python scripts/tests/validate_silver_complete.py
```

This checks that all the data quality fixes were applied correctly.

### View the Charts
```bash
jupyter notebook notebooks/06_visual_analysis.ipynb
```

Opens a notebook with bar charts for the three analysis questions (top color by year, lead time, return rate).

---

## Common Issues

### "JAVA_HOME is not set"
PySpark can't find Java. Set the environment variable:

**Windows (PowerShell):**
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11.0.28.6-hotspot"
```

**macOS/Linux:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

To make it permanent, add this to your shell profile (`.bashrc`, `.zshrc`, etc.).

### "Python worker failed to connect back"
This usually happens on Windows when PySpark can't find the Python executable. The pipeline handles this automatically in `common.py`, but if you still see the error:

```bash
# Make sure you're using the system Python (not WSL or Anaconda)
python -c "import sys; print(sys.executable)"
```

### Scripts are slow
PySpark defaults to 200 shuffle partitions, which is overkill for small datasets. The scripts are already optimized, but if you want to tweak it:

In `scripts/common.py`, add this to `build_spark()`:
```python
.config("spark.sql.shuffle.partitions", "4")
```

---

## Project Structure

```
upstart/
├── data/                          # Input CSVs (products, orders)
├── scripts/
│   ├── common.py                  # Shared utilities (Spark, paths, logging)
│   ├── pipeline/                  # Main pipeline (01-05)
│   ├── dw_pipeline/               # Optional star schema (exploratory)
│   └── tests/                     # Validation scripts
├── notebooks/
│   ├── 06_visual_analysis.ipynb   # Main analysis visualizations
│   └── 07_extra_analysis_visuals.ipynb  # Optional DW analyses
├── out/                           # Generated outputs (gitignored)
├── requirements.txt
├── SETUP.md                       # This file
└── README.md                      # Project overview
```

---

## Notes

- The `data/` folder should have three CSVs: `products.csv`, `sales_order_detail.csv`, `sales_order_header.csv`. These aren't included in the repo (they're part of the case study materials).
- The `out/` directory is created automatically on first run and gitignored.
- Each script is idempotent (safe to re-run), but they depend on previous layers:
  - `01` needs the CSV files in `data/`
  - `02` needs `01` to have run (Bronze layer)
  - `03-05` need `02` to have run (Silver layer)

---

## Next Steps

After the pipeline finishes:
1. Check `out/publish(gold)/` for the final tables
2. Run the validation script to confirm everything worked
3. Open the notebook to see the charts
4. Review `README.md` for the business insights

Questions? The troubleshooting section above covers the most common issues. If you're stuck, make sure Java 11+ and Python 3.10+ are installed and accessible from your PATH.
