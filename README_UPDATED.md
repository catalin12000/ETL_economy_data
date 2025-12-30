# Economy Data ETL (Greece/Cyprus/EU)

This repository is a reusable ETL framework for collecting economic datasets from public sources (PDFs, Excels, HTML pages, APIs), extracting structured data, comparing it to what we already have, and producing **deliverables** + **audit reports**.

The goal is to scale to many pipelines (e.g., 54 datasets) without rewriting the same boilerplate each time.

---

## What this framework does

For each dataset (“pipeline”), we do (some pipelines may temporarily run in **download-only** mode while we build extractors):

1. **Download** the latest source file (PDF/XLS/XLSX/HTML/etc.)
2. **Freshness check** to avoid unnecessary work:
   - `file_sha256`: detects if the downloaded file bytes changed
   - `data_sha256`: detects if extracted data actually changed (even if the file changed)
3. **Extract** the raw file into a clean, tidy `DataFrame` (standard schema)
4. **Compare** extracted data with our current “DB” (CSV/Excel for now)
5. **Update + deliver**:
   - produce an updated deliverable file (CSV or Excel)
   - produce an update report showing what changed
6. **Persist state** so next runs are incremental

---

## Project structure

```
.
├── run.py
├── etl/
│   ├── core/
│   │   ├── download.py
│   │   ├── fingerprint.py
│   │   ├── compare_excel.py
│   │   ├── compare_csv.py
│   │   ├── runner.py
│   │   ├── state.py
│   │   └── elstat.py              # ELSTAT helper (latest month/quarter + link by title)
│   └── pipelines/
│       ├── ed_apartments_price_index_table/
│       │   ├── pipeline.py
│       │   └── extract.py
│       ├── ed_building_permits_table/                # ELSTAT monthly (SOP03) item title resolver
│       │   ├── pipeline.py
│       │   └── (extractor optional / may be download-only)
│       ├── ed_building_permits_by_no_of_rooms/       # ELSTAT monthly (SOP03) another item title
│       │   └── pipeline.py
│       ├── ed_construction_index/                    # ELSTAT quarterly (DKT66)
│       │   └── pipeline.py
│       ├── ed_consumer_price_index/                  # ELSTAT monthly (DKT87, Greek locale)
│       │   └── pipeline.py
│       └── ed_economic_forecast/ (or ed_eu_economic_forecast_greece)
│           ├── pipeline.py                           # EU page download + extract HTML table
│           └── extract.py
└── data/
    ├── db/         (your “database” files: NOT committed)
    ├── downloads/  (raw downloads: NOT committed)
    ├── outputs/    (deliverables: NOT committed)
    ├── reports/    (audit reports: NOT committed)
    └── state/      (pipeline state JSON: NOT committed)
```

### Core modules (`etl/core`)
These are shared by all pipelines:

- **`download.py`**  
  Downloads a file to disk and returns metadata:
  - `downloaded_at_utc`
  - `etag`, `last_modified` (if server provides)
  - `content_length`, `final_url`, etc.

- **`elstat.py`**  
  ELSTAT helper utilities:
  - resolves the **latest** publication page for a publication code:
    - monthly: `YYYY-MMM` (e.g., `2025-M11`)
    - quarterly: `YYYY-QN` (e.g., `2025-Q3`)
  - finds the correct downloadable file link by matching a **target title**
  - supports `locale="en"` and `locale="el"`

- **`fingerprint.py`**  
  Computes `data_sha256` from extracted data (stable sorting + canonical bytes).  
  This lets us skip runs where the publisher re-exports the same data.

- **`compare_excel.py`**  
  Treats an Excel file as your current DB, merges extracted rows:
  - updates changed values
  - appends new rows
  - (optionally) prevents inserting rows older than DB baseline  
  Outputs:
  - updated deliverable `.xlsx`
  - `update_report.csv`

- **`compare_csv.py`**  
  Same idea as `compare_excel.py`, but for DB CSV files.

- **`state.py`**  
  Reads/writes pipeline state to `data/state/<pipeline_id>.json`.  
  Stores hashes + latest period + output locations.

- **`runner.py`**  
  Loads the pipeline, loads state, executes `pipe.run(state)`, saves returned state.
  Also provides `list_pipelines()` so `--all` can run everything.

### Pipelines (`etl/pipelines/<pipeline_id>`)
Each pipeline contains only dataset-specific code:

- **`pipeline.py`**  
  “Orchestration” for this dataset:
  - where to download from
  - how to name the file locally
  - which extractor to use (if implemented)
  - which DB file to compare against (if implemented)
  - where to output deliverables/reports

- **`extract.py`** (when applicable)  
  Pure extraction logic:
  - input: downloaded file path (PDF/XLS/HTML/etc.)
  - output: clean tidy dataframe with correct types/columns
  - no downloading, no state, no deliverable file writing (except optional debugging)

---

## How to run

### Run one pipeline
```powershell
python run.py --pipeline ed_apartments_price_index_table
python run.py --pipeline ed_consumer_price_index
python run.py --pipeline ed_economic_forecast
```

### Run all pipelines
```powershell
python run.py --all
```

---

## Required “DB” files (your current database)

For now we treat an Excel/CSV you maintain as the “database”.  
These are expected locally in `data/db/` (not committed to git).

Examples:

- Apartments pipeline expects:
  - `data/db/1503 Ed Apartments Price Index November 2025 (3).xlsx`

- Building permits (when compare step is enabled) expects:
  - `data/db/ed_building_permits_table.csv`

If the DB file is missing, the pipeline will fail with `FileNotFoundError`.

> Later this will be replaced with queries to the actual database.

---

## Outputs you get

After running a pipeline:

- Raw downloads:
  - `data/downloads/<pipeline_id>/...`

- Deliverables (when extraction is enabled):
  - `data/outputs/<pipeline_id>/...(.csv/.xlsx)`

- Audit reports (diff log, when compare is enabled):
  - `data/reports/<pipeline_id>/update_report.csv`

- State (for incremental runs):
  - `data/state/<pipeline_id>.json`

The report CSV tells you exactly what changed:
- new rows added
- existing rows updated
- which keys/periods changed

---

## Freshness checks: why two hashes?

We store two hashes in state:

- **`file_sha256`**
  - computed from the raw downloaded file bytes
  - skips when the file is literally identical

- **`data_sha256`**
  - computed from the extracted dataframe
  - skips when the file changed but the extracted data did not (common when publishers re-export)

This makes the framework efficient and prevents unnecessary updates.

---

## How to add a new pipeline (standard recipe)

1. Create a folder:
   ```
   etl/pipelines/<new_pipeline_id>/
   ```

2. Add:
   - `pipeline.py`
   - `extract.py` (optional at first; download-only is OK)
   - `__init__.py`

3. Implement `pipeline.py`:
   - download URL (or use ELSTAT dynamic resolver via `etl/core/elstat.py`)
   - compute `file_sha256`
   - if unchanged: skip
   - **optional next step:** extract dataframe + `data_sha256`
   - **optional next step:** compare/update DB + write deliverables/reports
   - update state

4. Test:
   - Run twice:
     - first run should download + (deliver if enabled)
     - second run should skip (same hash)

---

## Notes / gotchas

- PDFs are messy: table extraction depends on layout. Extractors should be defensive.
- ELSTAT links can change by period (month/quarter). Use `etl/core/elstat.py` to resolve latest.
- For EU pages with embedded HTML tables, the most reliable approach is:
  - download HTML
  - parse tables from HTML
  - output CSV
- If you change state key names, delete old state JSON files once.

---

## License / Data
This repo contains code only. Downloaded data and local DB files are not committed.
You remain responsible for respecting the terms of the data providers.
