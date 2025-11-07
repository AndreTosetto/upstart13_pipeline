# Upstart13 Data Engineering Assessment

PySpark pipeline implementing product master and sales order transformations with business day calculations and revenue analysis.

---

## Quick Start

**Requirements:** Python 3.10+, Java 11+

```bash
# Install dependencies
pip install -r requirements.txt

# Run complete pipeline
python run_pipeline.py

# Validate outputs
python scripts/tests/smoke_publish.py
```

---

## Pipeline Overview

The pipeline processes 3 CSV files through medallion architecture (bronze -> silver -> gold):

```
data/
|- products.csv
|- sales_order_detail.csv
|- sales_order_header.csv

    => Bronze (01_load_raw.py)

out/raw(bronze)/
|- raw_products/
|- raw_sales_order_detail/
|- raw_sales_order_header/

    => Silver (02_store_cast_and_keys.py)

out/store(silver)/
|- store_products/              # Typed, deduplicated
|- store_sales_order_detail/    # Typed
|- store_sales_order_header/    # Typed, dates fixed

    => Gold (03, 04, 05)

out/publish(gold)/
|- publish_product/             # Color filled, categories enhanced
|- publish_orders/              # Joined with calculated metrics
|- analysis_top_color_by_year/
|- analysis_avg_lead_by_category/
```

---

## Deliverables

### 1. publish_product
**Transformations applied:**
- NULL colors replaced with `"N/A"`
- NULL ProductCategoryName enhanced using subcategory mapping:
  - `['Gloves', 'Shorts', 'Socks', 'Tights', 'Vests']` -> `'Clothing'`
  - `['Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps']` -> `'Accessories'`
  - `['Wheels', 'Saddles']` or contains "Frames" -> `'Components'`

### 2. publish_orders
**Structure:** SalesOrderDetail + SalesOrderHeader (except SalesOrderId) + calculated fields

**Calculated fields:**
- `LeadTimeInBusinessDays`: Weekdays between OrderDate and ShipDate (excludes Sat/Sun)
- `TotalLineExtendedPrice`: `OrderQty * (UnitPrice - UnitPriceDiscount)`
- `TotalOrderFreight`: Renamed from `Freight`

**Implementation note:** Business days calculated using Spark SQL `sequence()` + `filter()` for scalability.

### 3. Analysis Questions

**Q1: Which color generated the highest revenue each year?**
- **2021:** Red ($6.02M)
- **2022:** Black ($13.92M)
- **2023:** Black ($15.03M)
- **2024:** Yellow ($6.36M)

**Q2: Average lead time by product category?**
- Accessories: 5.01 days
- Bikes: 5.00 days
- Clothing: 5.01 days
- Components: 5.01 days

**Deliverables:**
- Parquet: `out/publish(gold)/analysis_top_color_by_year/`
- Parquet: `out/publish(gold)/analysis_avg_lead_by_category/`
- CSV exports: [analysis_top_color_by_year.csv](out/publish(gold)/analysis_top_color_by_year.csv), [analysis_avg_lead_by_category.csv](out/publish(gold)/analysis_avg_lead_by_category.csv)

---

## Technical Decisions

### Data Quality Fixes (Silver Layer)

**1. Incomplete OrderDate handling**
- Raw CSV contains dates in `YYYY-MM` format (e.g., "2021-06")
- **Solution:** Backfill as `ShipDate - 7 days` (preserves ~5 business day lead time)
- **Impact:** Enables accurate LeadTimeInBusinessDays calculation

**2. Duplicate ProductIDs**
- Raw data has 303 rows, but only 295 unique ProductIDs
- **Solution:** Keep row with populated ProductCategoryName
- **Rationale:** Preserves data quality for downstream category analysis

**3. Type casting**
- Bronze preserves raw CSV strings
- Silver casts to proper types (int, double, timestamp, boolean)
- **Benefit:** Type safety + enables numeric operations in gold layer

### Formula Interpretation

**TotalLineExtendedPrice specification:**
> "Calculate TotalLineExtendedPrice using the formula: `OrderQty * (UnitPrice - UnitPriceDiscount)`"

**Implementation:** Followed spec literally as `OrderQty * (UnitPrice - UnitPriceDiscount)`

**Note:** UnitPriceDiscount values in data are < 1.0 (e.g., 0.0, 0.02), suggesting they may represent discount *rates* rather than dollar amounts. However, without clarification, the implementation follows the spec exactly as written.

### Performance Optimizations

**1. Business days calculation**
```python
F.size(
    F.expr(
        "filter("
        "sequence(OrderDate, date_sub(ShipDate, 1), interval 1 day), "
        "d -> dayofweek(d) BETWEEN 2 AND 6)"  # Mon=2, Fri=6
    )
)
```
- Uses Spark SQL higher-order functions (native performance)
- Avoids UDFs and row-by-row processing
- Scales to millions of rows

**2. Parquet format**
- Columnar storage for efficient reads
- Schema enforcement
- Compression (smaller storage footprint)

---

## Assumption Matrix

When specifications are ambiguous, decisions must be made. This matrix documents each assumption, the implementation choice, and how to modify if clarification differs.

| Issue | Ambiguity | Decision Made | Alternative Implementation | File to Change |
|-------|-----------|---------------|---------------------------|----------------|
| **TotalLineExtendedPrice formula** | Spec says `OrderQty * (UnitPrice - UnitPriceDiscount)` but data suggests UnitPriceDiscount may be a rate (0.0-1.0) not a dollar amount | Followed spec literally: `OrderQty * (UnitPrice - UnitPriceDiscount)` | If discount is a rate: `OrderQty * UnitPrice * (1 - UnitPriceDiscount)` | [04_publish_orders.py:25-35](scripts/pipeline/04_publish_orders.py#L25-L35) |
| **Incomplete OrderDate** | 5 rows have `YYYY-MM` format (e.g., "2021-06") with no day specified | Backfilled as `ShipDate - 7 days` to preserve ~5 business day lead time | Could exclude these rows or use month start (YYYY-MM-01) | [02_store_cast_and_keys.py:57-68](scripts/pipeline/02_store_cast_and_keys.py#L57-L68) |
| **Duplicate ProductIDs** | 303 product rows but only 295 unique ProductIDs | Keep row with populated ProductCategoryName (better data quality) | Could keep first row, last row, or merge values | [02_store_cast_and_keys.py:70-75](scripts/pipeline/02_store_cast_and_keys.py#L70-L75) |
| **NULL Color handling** | Spec says "replace NULL with N/A" but doesn't specify empty strings | Treat both NULL and empty string as missing: `Color.isNull() \| (Color == "")` | Could only handle NULL: `Color.isNull()` | [03_publish_product.py:26-31](scripts/pipeline/03_publish_product.py#L26-L31) |
| **Category enhancement** | Spec provides subcategory mapping but doesn't say whether to check case-sensitivity for "Frames" | Used case-insensitive match: `lower(subcategory).contains("frames")` | Could use exact case: `subcategory.contains("Frames")` | [03_publish_product.py:40](scripts/pipeline/03_publish_product.py#L40) |

**Why this matters:** Proactive communication of assumptions demonstrates engineering maturity. When ambiguity exists, document the decision and provide clear guidance for modification.

---

## Data Model

| Table | Primary Key | Foreign Keys |
|-------|-------------|--------------|
| store_products | ProductID | - |
| store_sales_order_header | SalesOrderID | - |
| store_sales_order_detail | (SalesOrderID, SalesOrderDetailID) | ProductID -> products<br>SalesOrderID -> header |

---

## Repository Structure

```
upstart13_pipeline/
|- data/                        # Input CSVs
|- scripts/
|  |- common.py                 # Shared utilities (paths, Spark config)
|  |- pipeline/
|  |  |- 01_load_raw.py         # Bronze: CSV -> Parquet
|  |  |- 02_store_cast_and_keys.py  # Silver: Type casting + QA
|  |  |- 03_publish_product.py  # Gold: Product transformations
|  |  |- 04_publish_orders.py   # Gold: Orders + calculated fields
|  |  |- 05_analysis_questions.py   # Gold: Answer business questions
|  |- tests/
|     |- smoke_publish.py       # Validation
|     |- test_business_rules.py # Business logic unit tests
|- notebooks/
|  |- 06_visual_analysis.ipynb  # Question visualizations
|- out/                         # Generated outputs (gitignored)
|- run_pipeline.py              # Orchestrator
|- requirements.txt
|- README.md
```

---

## Next Steps

For production deployment, consider:
1. **Incremental processing** - Partition by OrderDate for efficient updates
2. **Data quality metrics** - Track null rates, duplicate rates over time
3. **Schema evolution** - Add schema versioning for breaking changes
4. **Orchestration** - Integrate with Airflow/Dagster for scheduling
5. **Monitoring** - Add data freshness checks and SLA tracking

---

## Questions & Clarifications

If rerunning this assessment, I would clarify:
1. **TotalLineExtendedPrice formula** - Is UnitPriceDiscount a rate (0.0-1.0) or dollar amount?
2. **Incomplete dates** - Should YYYY-MM rows be excluded or backfilled?
3. **Duplicate products** - Merge logic or keep most recent?

Assumptions documented above reflect best judgment given available information.
