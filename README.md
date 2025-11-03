# Upstart13 Data Engineering Case Study

This repository contains my PySpark implementation for the Upstart13 interview case.  
Three CSV inputs (`products`, `sales_order_detail`, `sales_order_header`) flow through a medallion-style pipeline (bronze -> silver -> gold) and produce the publish tables that answer the required business questions.

---

## Project Highlights
- **Clean data pipeline**: scripts `scripts/pipeline/01` -> `05` materialise bronze, silver, and publish layers under `out/`.
- **Data quality fixes**: the silver step trims strings, removes duplicate products, standardises product categories, casts data types, and backfills incomplete `OrderDate` values.
- **Publish deliverables**: the gold layer enriches product attributes (color fill, inferred category) and adds business metrics (`LeadTimeInBusinessDays`, `TotalLineExtendedPrice`, `TotalOrderFreight`).
- **Analysis answers**: the publish datasets power the two required questions (top revenue color by year and average lead time by category). Optional deep-dive visuals are recalculated directly inside the notebooks.
- **Quality gate**: `python scripts/tests/smoke_publish.py` validates the publish layer before hand-off.

---

## Setup

**Prerequisites:** Python 3.10+, Java 11+

```bash
# Install dependencies
pip install -r requirements.txt

# Verify installation
python -c "from pyspark.sql import SparkSession; print('PySpark OK')"
```

For detailed environment guidance, see [SETUP.md](SETUP.md).

---

## Running the Pipeline

```bash
# Run the complete pipeline (recommended)
python run_pipeline.py

# Or execute scripts individually:
python scripts/pipeline/01_load_raw.py
python scripts/pipeline/02_store_cast_and_keys.py
python scripts/pipeline/03_publish_product.py
python scripts/pipeline/04_publish_orders.py
python scripts/pipeline/05_analysis_questions.py

# (Optional) Run smoke test
python scripts/tests/smoke_publish.py
```

Outputs are written to:
- `out/raw(bronze)` - raw CSVs preserved as Parquet
- `out/store(silver)` - cleaned, typed data (`store_products`, `store_sales_order_detail`, `store_sales_order_header`)
- `out/publish(gold)` - publish tables and analysis outputs (`publish_product`, `publish_orders`, `analysis_*`)

### Optional: Star Schema (DW)

If you want to explore the dimensional model used in notebook 07, you'll need to build the star schema manually:

```bash
# Build fact and dimension tables from silver layer
python scripts/dw_pipeline/03_build_dim_product.py
python scripts/dw_pipeline/04_build_dim_customer.py
python scripts/dw_pipeline/05_build_fact_sales.py
```

This creates `fact_sales`, `dim_product`, and `dim_customer` under `out/publish(gold)/`. The star schema is optional...

---

## Key Results

| Metric | Finding |
| --- | --- |
| **Top revenue color by year** | 2021: **Red** ($6.02M) · 2022: **Black** ($13.92M) · 2023: **Black** ($15.03M) · 2024: **Yellow** ($6.36M) |
| **Average lead time (business days)** | Accessories: 5.01 · Bikes: 5.00 · Clothing: 5.01 · Components: 5.01 |

> **Notebooks**
> - `06_visual_analysis.ipynb` - Main visuals for the two required questions
> - `07_extra_analysis_visuals.ipynb` - Optional analyses (margin, retention) using star schema

---

## Assumptions & Data Quality Notes

### Data Transformations
- **Incomplete `OrderDate` rows**: when the header CSV stores `YYYY-MM`, the silver step infers `OrderDate = ShipDate - 7 days`. Counting weekdays between the two dates produces ~5 business days, consistent with complete records.
- **Product deduplication**: the raw products file contains duplicate `ProductID` rows; the silver step keeps the entry with populated category, resulting in 295 distinct products downstream.
- **Color enrichment**: blank colors are filled with `"N/A"` after trimming to avoid join issues.
- **Return detection**: negative `OrderQty` values set an `IsReturn` flag; two rows were identified during exploratory analysis and validated manually.

### Business Logic Clarifications
- **Lead Time Calculation**: Business days are counted from `OrderDate` up to (but excluding) `ShipDate`. This means if an order is placed Monday and ships Wednesday, the lead time is 2 business days (Tuesday, Wednesday morning).
- **TotalLineExtendedPrice**: The spec mentions `OrderQty * (UnitPrice - UnitPriceDiscount)`, but `UnitPriceDiscount` is a **rate** (0.0 to 1.0), not a dollar amount. The correct formula is: `OrderQty * UnitPrice * (1 - UnitPriceDiscount)`.

### Data Model
| Table | Primary Key | Foreign Keys |
|-------|-------------|--------------|
| `store_products` | `ProductID` | - |
| `store_sales_order_header` | `SalesOrderID` | - |
| `store_sales_order_detail` | `(SalesOrderID, SalesOrderDetailID)` | `ProductID` → products, `SalesOrderID` → header |

All publish fields required by the brief are present and non-null. The smoke test fails fast if a script stops populating these columns.

---

## Repository Map
```
upstart13_pipeline/
|- data/                      # Input CSVs
|- notebooks/                 # Visual analysis notebooks
|- scripts/
|  |- pipeline/               # Bronze -> silver -> gold pipeline (01-05)
|  |- tests/                  # Validation scripts
|  |- dw_pipeline/            # Optional star schema prototype
|- out/                       # Generated Parquet outputs (gitignored)
|- run_pipeline.py
|- README.md (this file)
```

---

## Lessons Learned

A few things I ran into while building this pipeline:

- **Incomplete date handling**: The original CSV had some `OrderDate` entries with just `YYYY-MM`. I ended up backfilling these as `ShipDate - 7 days` which keeps the lead time calculation consistent (~5 business days across the board).

- **Product duplicates**: Found duplicate `ProductID` entries in the raw file with different category labels. The silver layer now keeps only the row with a populated category, which dropped the product count from 303 to 295 distinct items.

- **Performance trade-offs**: I built a star schema (fact_sales + dimensions) to demo dimensional modeling, but calculating business days on-the-fly in Spark using `sequence()` was too slow for 121k rows. Ended up pre-calculating lead time in the publish layer instead.

- **Color fill strategy**: Blank colors were breaking joins, so I standardized them to `"N/A"` after trimming whitespace. Simple fix but took a while to debug.

---

## Next Steps
1. Run `python run_pipeline.py` to refresh bronze/silver/gold layers.
2. Execute `python scripts/tests/smoke_publish.py` for a quick validation pass.
3. Open `notebooks/06_visual_analysis.ipynb` to regenerate presentation visuals.

