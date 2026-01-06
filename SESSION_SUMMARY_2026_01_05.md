# Session Summary - January 5, 2026

## Project Status Overview
We have transitioned from a "download-only" pipeline to a robust **Extraction & Sync** framework. The core engine is now stable and protects baseline data while generating specific deliverables.

### Completed Extractors (7/10 Urgent)
The following pipelines are fully implemented with custom extraction logic and dual-output generation:
1.  **`ed_consumer_price_index`**: Greek CPI (ELSTAT).
2.  **`ed_employment`**: Employment & Unemployment (ELSTAT). Handles side-by-side Adjusted/Unadjusted rows.
3.  **`ed_eu_consumer_confidence_index`**: EU Consumer Confidence (Eurostat API).
4.  **`ed_eu_gdp`**: EU GDP (Eurostat API). Features complex pivoting of multiple metrics into one row.
5.  **`ed_eu_hicp`**: Harmonized CPI (Eurostat API).
6.  **`ed_eu_unemployment_rate`**: EU Unemployment (Eurostat API).
7.  **`cy_13_new_loans_millions`**: Cyprus Loans (CBC). Features a complex multi-sheet join across Tables 12, 9, 6.1, 6.2, and 6.3.

---

## Technical Architecture & Rules

### 1. Baseline Protection Rule
Files in `data/db/` are **Read-Only Baseline References**. 
- The ETL logic reads these files to see what we already have.
- **NEVER** overwrite these files automatically. They serve as the fixed point of comparison.

### 2. Output Logic
Every pipeline execution produces two files in `data/outputs/<pipeline_id>/`:
- **`new_entries.csv`**: Contains **only** the rows that were newly found in the source or updated (different value for the same period).
- **`mock_db_snapshot.csv`**: A full copy of the database (Baseline + New/Updated rows), sorted and cleaned.

### 3. Sync Logic (`etl/core/compare_csv.py`)
- **Key-Based Matching**: Uses primary keys (e.g., `Year`, `Month`, `Geopolitical Entity`) to identify rows.
- **Upsert Behavior**: If keys match but values differ, it marks the row as an `UPDATE` and includes it in `new_entries.csv`.
- **Numeric Sorting**: Automatically converts keys to numbers before sorting to ensure `2025, 10` comes after `2025, 2`.

---

## Important Data Mappings

### Cyprus New Loans (`cy_13`)
- **T12**: Pure New/Renegotiated (Cols D, E, G, H)
- **T9**: Floating Rates/APRC (Cols D, E, F, G)
- **T6.1, T6.2, T6.3**: Outstanding Amounts (Targeting the first occurrence of each month to avoid "Monthly Change" rows).

---

## Next Steps
When resuming, proceed with the remaining items from the urgent list once the mock DB and format are provided:
1.  **`ed_loan_amounts_millions`** (Greece - Bank of Greece)
2.  **`ed_loan_interest_rates`** (Greece - Bank of Greece)
3.  **`gdp_greece`** (ELSTAT SEL84)

**To view current status:**
Run `python run.py --dashboard`
