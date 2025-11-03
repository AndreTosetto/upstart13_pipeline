# Scripts Overview

This folder contains the data pipeline and validation tools for the Upstart13 project.

## Pipeline Scripts (Main Deliverables)

These are the core scripts that implement the medallion architecture. Run them in order:

### Bronze Layer - Raw Data
**`pipeline/01_load_raw.py`**
- Reads the three CSV files from `data/`
- Writes them as Parquet to `out/raw(bronze)/`
- No transformations - preserves original data exactly as-is
- All columns kept as strings to avoid data type issues

### Silver Layer - Cleaned Data
**`pipeline/02_store_cast_and_keys.py`**
- Fixes 5 incomplete dates (backfills from ShipDate)
- Removes 8 duplicate products
- Corrects 255 category issues (wrong mappings + NULLs)
- Casts everything to proper types (timestamps, integers, booleans, doubles)
- Adds calculated fields: LineTotal, IsReturn
- Writes to `out/store(silver)/`

### Gold Layer - Business-Ready Tables
**`pipeline/03_publish_product.py`**
- Fills NULL/empty colors with "N/A"
- Maps remaining NULL categories using subcategory logic
- Final output: 295 products, 100% category coverage
- Writes to `out/publish(gold)/publish_product/`

**`pipeline/04_publish_orders.py`**
- Joins order details with headers
- Calculates lead time in business days (Mon-Fri only)
- Calculates extended price (qty * (price - discount))
- Renames Freight -> TotalOrderFreight
- Writes to `out/publish(gold)/publish_orders/`

**`pipeline/05_analysis_questions.py`**
- Answers the two required questions:
  1. Top revenue color per year
  2. Average lead time by category
- Writes analysis tables to `out/publish(gold)/analysis_*/`

## Test Scripts

**`tests/00_quality_scan_raw_simple.py`**
- Quick scan of raw CSV files to spot issues
- Uses pandas (no Spark) so it runs fast on Windows

**`tests/validate_silver_complete.py`**
- Comprehensive checks on the silver layer
- Verifies all data quality fixes were applied
- Checks counts, types, NULLs, referential integrity

**`tests/smoke_publish.py`**
- Quick validation of publish layer
- Makes sure required fields exist and aren't NULL

**`tests/verify_medallion_architecture.py`**
- End-to-end validation of the full pipeline
- Bronze → Silver → Gold flow check

## Running the Pipeline

From the project root:
```bash
python scripts/pipeline/01_load_raw.py
python scripts/pipeline/02_store_cast_and_keys.py
python scripts/pipeline/03_publish_product.py
python scripts/pipeline/04_publish_orders.py
python scripts/pipeline/05_analysis_questions.py
```

Each script is idempotent (safe to re-run). Expected runtime: about 1-2 minutes total.

## Optional: Star Schema (Exploratory)

The `dw_pipeline/` folder has an experimental star schema implementation:
- `03_build_dim_product.py` - Product dimension with profit margins
- `04_build_dim_customer.py` - Customer dimension with RFM segmentation
- `05_build_fact_sales.py` - Sales fact table
- `06_validate_star_schema.py` - Validation

This was exploratory work to show what a dimensional model could look like. It's not part of the main deliverable.

## Key Data Quality Fixes

1. **5 incomplete dates** - backfilled using ShipDate - 7 days
2. **8 duplicate products** - removed, keeping the one with populated category
3. **65 wrong categories** - corrected (e.g., Road Bikes → Bikes)
4. **190 NULL categories** - filled using subcategory mapping rules

All fixes happen in script 02 (silver layer). Raw data stays untouched in bronze.
