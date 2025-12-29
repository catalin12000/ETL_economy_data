# Economy Data ETL (Greece/Cyprus)

This repository is a reusable ETL framework for collecting economic datasets from public sources (PDFs, Excels, APIs), extracting structured data, comparing it to what we already have, and producing **deliverables** + **audit reports**.

The goal is to scale to many pipelines (e.g., 54 datasets) without rewriting the same boilerplate each time.

---

## What this framework does

For each dataset (“pipeline”), we do:

1. **Download** the latest source file (PDF/XLS/XLSX/etc.)
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
│   │   └── state.py
│   └── pipelines/
│       ├── ed_apartments_price_index_table/
│       │   ├── pipeline.py
│       │   └── extract.py
│       └── ed_building_permits_table/
│           ├── pipeline.py
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
  - which extractor to use
  - which DB file to compare against
  - where to output deliverables/reports

- **`extract.py`**  
  Pure extraction logic:
  - input: downloaded file path
  - output: clean tidy dataframe with correct types/columns
  - no downloading, no state, no file writing (except optional debugging)

---

## How to run

### Run one pipeline
```powershell
python run.py --pipeline ed_apartments_price_index_table
python run.py --pipeline ed_building_permits_table
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

- Building permits pipeline expects:
  - `data/db/ed_building_permits_table.csv`

If the DB file is missing, the pipeline will fail with `FileNotFoundError`.

----> will later be changed with queries to the actual database

---

## Outputs you get

After running a pipeline:

- Raw downloads:
  - `data/downloads/<pipeline_id>/...`

- Deliverables:
  - `data/outputs/<pipeline_id>/Ed ... Table.xlsx` (or `.csv`)

- Audit reports (diff log):
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
  - skips when the file changed but the extracted data did not (common when publishers re-exports)

This makes the framework efficient and prevents unnecessary updates.

---

## How to add a new pipeline (the standard recipe)

1. Create a folder:
   ```
   etl/pipelines/<new_pipeline_id>/
   ```

2. Add:
   - `pipeline.py`
   - `extract.py`
   - `__init__.py`

3. Implement `extract.py`:
   - take a file path
   - return a tidy dataframe
   - ensure types are correct (Year/Month/Quarter ints, numeric columns floats/ints)

4. Implement `pipeline.py` using the same pattern:
   - download URL
   - compute `file_sha256`
   - if unchanged: skip
   - extract dataframe
   - compute `data_sha256`
   - if unchanged: skip
   - compare/update DB (CSV or Excel)
   - write deliverable + report
   - update state

5. Test:
   - Run twice:
     - first run should download + deliver
     - second run should skip (same hash)

---

## How we continue building from here

### Next improvements (recommended roadmap)
1. **Central pipeline configuration**
   - Move DB paths and URLs out of code into config (YAML/JSON)
   - Pipelines become “config + extractor”

2. **Unified engine**
   - Create `etl/core/engine.py` so pipelines are minimal:
     - declare config
     - provide extractor
   - This avoids touching 54 pipelines for framework-level changes.

3. **Database backend**
   - Replace Excel/CSV DB with Postgres (or SQLite) later:
     - `compare_db.py`
     - store history + revisions
     - use migrations for schema evolution

4. **Observability**
   - Add structured logs
   - Add a “run summary” report for `--all`
   - Optional: Slack/email notifications

5. **Scheduled execution**
   - Run daily/weekly with cron/Task Scheduler
   - Only produce deliverables when data changes

---

## Notes / gotchas

- PDFs are messy: table extraction depends on layout. Extractors should be defensive.
- ELSTAT links may be stable but can redirect; download uses `allow_redirects=True`.
- If you change state key names (e.g. `last_sha256` → `file_sha256`), delete old state JSON files once.

---

## License / Data
This repo contains code only. Downloaded data and local DB files are not committed.
You remain responsible for respecting the terms of the data providers.
