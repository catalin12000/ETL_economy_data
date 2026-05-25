# Economy Data ETL — Greece & Cyprus

ETL framework for collecting economic datasets from public sources (PDFs, Excel, APIs),
extracting structured data, comparing against a live Postgres DB, and producing deliverables
for a downstream Backend to load.

---

## Project structure

```
ETL_economy_data/
├── run.py                          # main entry point
├── .env                            # DB + AWS credentials (not committed)
├── requirements.txt
├── scripts/
│   └── sync_s3.py                  # S3 upload step (run after pipelines)
└── etl/
    ├── core/
    │   ├── runner.py               # discovers + runs pipelines, prints summary
    │   ├── download.py             # file download + SHA256 hash
    │   ├── fingerprint.py          # skip-signal helpers per source type
    │   ├── database.py             # compare_with_postgres (read-only)
    │   ├── output.py               # write_deliverable_csv (snake_case headers)
    │   ├── s3_upload.py            # S3 path logic + boto3 upload
    │   ├── paths.py                # PipelinePaths — centralised I/O paths
    │   ├── state.py                # load/save state.json per pipeline
    │   └── pipeline_logging.py     # structured run + sync logs
    └── pipelines/
        └── <pipeline_id>/
            ├── pipeline.py         # download → skip? → extract → DB compare → deliverable
            ├── extract.py          # pure extraction logic (file → DataFrame)
            ├── __init__.py
            ├── downloaded/         # raw downloaded files (not committed)
            ├── output/             # deliverables (not committed)
            └── state.json          # per-run cache: hashes, paths, last status (not committed)
```

---

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=eu-central-1
S3_BUCKET=your-bucket-name
```

---

## How to run

```bash
# Run all pipelines
python run.py --all

# Run one pipeline
python run.py --pipeline gdp_greece
```

A summary table is printed at the end showing each pipeline's status, rows inserted/updated, deliverable name, and any errors.

---

## Pipeline flow

Each `Pipeline.run(state)` follows the same pattern:

1. **Download** — fetch source file(s) and hash them
2. **Skip check** — compare against `state.json`; return `skipped` if source is unchanged
3. **Extract** — parse the file into a tidy DataFrame
4. **DB Compare** — `compare_with_postgres` against the live DB to find the Delta (inserts + updates)
5. **Deliverable** — write the Delta as a CSV via `write_deliverable_csv`

The skip signal depends on the pipeline's `source_type`:

| `source_type` | Skip when |
|---|---|
| `api` | `data_sha256` unchanged |
| `dynamic_file` | `file_sha256` unchanged + DB has no delta |
| `static_file` | `file_sha256` unchanged + DB has no delta |
| `scraped` | `latest_period_seen` unchanged |

---

## S3 sync

S3 upload is a **separate step** — run it after verifying deliverables locally.

```bash
# Preview what would be uploaded (no actual upload)
python scripts/sync_s3.py --dry-run

# Upload all pipelines
python scripts/sync_s3.py

# Upload specific pipelines
python scripts/sync_s3.py gdp_greece cy_11_lro_transfers
```

S3 path structure:
```
raw_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{stem}_{timestamp}.{ext}
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.csv
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.log
```

Pipelines whose last run was `skipped` or `error` are not uploaded.

---

## DB usage

The DB is **read-only**. This ETL never inserts or updates rows — it only reads to find the Delta.

- **athena** — Greek datasets (`ed_` pipelines)
- **zeus** — Cyprus datasets (`cy_` pipelines)

---

## Data sources

| Source | Country | Type |
|---|---|---|
| ELSTAT (statistics.gr) | Greece | dynamic_file / api |
| Bank of Greece (bankofgreece.gr) | Greece | static_file / dynamic_file |
| Eurostat (ec.europa.eu) | Greece + Cyprus | api |
| migration.gov.gr | Greece | scraped |
| CYSTAT (cystatdb.cystat.gov.cy) | Cyprus | api |
| Central Bank of Cyprus (centralbank.cy) | Cyprus | static_file |
| DLS Portal (portal.dls.moi.gov.cy) | Cyprus | scraped |

---

## Adding a new pipeline

1. Create `etl/pipelines/<pipeline_id>/` with `__init__.py`, `pipeline.py`, `extract.py`
2. In `Pipeline`, set `pipeline_id`, `country`, `source`, `source_type`, `db_table_name`, `display_name`
3. Implement `run(state)`:
   - Download + hash
   - Skip check via the appropriate `fingerprint.py` helper
   - Extract to DataFrame
   - `compare_with_postgres` for the Delta
   - `write_deliverable_csv`
4. Add the pipeline to `_PIPELINE_META` in `etl/core/s3_upload.py`
5. Run once and check the summary

---

## Notes

- Deliverable CSV headers are automatically snake_cased by `write_deliverable_csv`.
- `...`, `u`, `N/A`, `:` and similar null tokens are normalised to `pd.NA` before DB compare.
- `state.json` is a local cache — safe to delete; losing it costs one redundant run.
- Per-pipeline `.gitignore` excludes `downloaded/`, `output/`, and `state.json`.
