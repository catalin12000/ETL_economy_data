# Economy Data ETL (Greece/Cyprus/EU)

This repository is a reusable ETL framework for collecting economic datasets from public sources (PDFs, Excels, HTML pages, APIs), extracting structured data, comparing it to what we already have, and producing **deliverables** + **audit reports**.

---

## Current Status: Urgent Phase Verified & Delivered

As of **January 20, 2026**, we have successfully completed the **Postgres Integration** for the 10 most critical datasets. The system now performs a live `READ-ONLY` comparison against production databases (`athena` and `zeus`).

### Completed & Verified Pipelines (10/10 Urgent)
1.  **`ed_consumer_price_index`**: Greek CPI (ELSTAT).
2.  **`ed_employment`**: Employment & Unemployment (ELSTAT).
3.  **`ed_eu_consumer_confidence_index`**: EU Consumer Confidence (Eurostat API).
4.  **`ed_eu_gdp`**: EU GDP (Eurostat API).
5.  **`ed_eu_hicp`**: Harmonized CPI (Eurostat API).
6.  **`ed_eu_unemployment_rate`**: EU Unemployment (Eurostat API).
7.  **`cy_13_new_loans_millions`**: Cyprus New Loans (CBC).
8.  **`gdp_greece`**: Quarterly GDP (ELSTAT).
9.  **`ed_loan_interest_rates`**: Greece Loan Rates (Bank of Greece).
10. **`ed_loan_amounts_millions`**: Greece Loan Volumes (Bank of Greece).

---

## Latest Deliverable (2026-01-20)
- **Folder:** `deliverables_2026_01_20/`
- **Scope:** Data from **2024 onwards** only.
- **Improvements:** 
  - All files are **Sorted Chronologically** (Year -> Month/Quarter).
  - Differences are calculated against the **Live PostgreSQL DB** using a **0.05 tolerance** for numeric values.

---

## How to run

### Prerequisites
- Python 3.12+
- `pip install -r requirements.txt`
- Set `DATABASE_URL` environment variable for live sync.

### Run one pipeline
```powershell
# Set DB URL (PowerShell)
$env:DATABASE_URL = "postgresql://..."

python run.py --pipeline ed_consumer_price_index
python run.py --pipeline cy_13_new_loans_millions
```

### Run all pipelines
```powershell
python run.py --all
```

---

## Project structure

```
.
├── run.py
├── etl/
│   ├── core/
│   │   ├── database.py           # Live Postgres comparison (athena/zeus)
│   │   ├── download.py           # Robust file downloader
│   │   ├── elstat.py             # ELSTAT dynamic link resolver
│   │   ├── fingerprint.py        # Data hashing (detect actual changes)
│   │   ├── compare_csv.py        # Local CSV baseline sync
│   │   └── runner.py             # Pipeline orchestration
│   └── pipelines/
│       └── <pipeline_id>/
│           ├── pipeline.py       # Orchestration & DB mapping
│           ├── extract.py        # Extraction logic
│           └── <id>.sql          # Reference query for DB sync
└── data/
    ├── db/         (Local CSV baselines)
    ├── downloads/  (Raw files from publishers)
    ├── outputs/    (Full snapshots & DB Deltas)
    └── state/      (SHA256 tracking)
```

### Database Sync Logic (Postgres)
The framework uses a **Read-Only** approach for database safety:
1.  **Fetch**: Queries the current state of `public.<pipeline_id>` from Postgres.
2.  **Normalize**: Standardizes string keys (lowercase, trim, dash-cleaning).
3.  **Compare**: Identifies missing rows or numeric differences > **0.05**.
4.  **Deliver**: Generates a CSV containing *only* those insertions and updates.

---

## Freshness checks: why two hashes?

We store two hashes in state:
- **`file_sha256`**: Detects if the downloaded file bytes changed.
- **`data_sha256`**: Detects if extracted data actually changed (skips processing if the publisher re-exported the same content).

---

## Notes
- **Sorting**: All final deliverables are sorted chronologically to ensure they are easy to audit.
- **Filtering**: The `deliverables_2026_01_20` folder was manually filtered to exclude "very old" data (pre-2024).