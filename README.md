# Economy Data ETL — Greece & Cyprus

ETL framework for collecting economic datasets from public sources (PDFs, Excel, APIs),
extracting structured data, comparing against a live Postgres DB, and producing deliverables.

For the current operational picture, including the run flow, logging policy, S3 Sync behavior,
and what changed in the latest refactor, see [docs/ETL_CURRENT_OVERVIEW.md](docs/ETL_CURRENT_OVERVIEW.md).

---

## Pipeline count

| | Count |
|---|---|
| Total pipelines | 65 |
| Generating a deliverable | 63 |
| Fully wired (download + extract + DB compare) | 61 |
| Download = deliverable (raw XLS) | 3 |
| Pending (no table in DB yet / no extractor) | 2 |

---

## Project structure

```
ETL_economy_data/
├── run.py                          # main entry point
├── .env                            # DB + AWS credentials (not committed)
├── requirements.txt
├── scripts/
│   ├── sync_s3.py                  # S3 upload step (run after pipelines)
│   └── ...
└── etl/
    ├── core/
    │   ├── runner.py               # loads + runs pipelines, prints summary
    │   ├── download.py             # file download + hash
    │   ├── compare_csv.py          # baseline CSV comparison
    │   ├── database.py             # compare_with_postgres (read-only)
    │   ├── output.py               # write_deliverable_csv (snake_case headers)
    │   ├── s3_upload.py            # S3 path logic + boto3 upload
    │   ├── paths.py                # PipelinePaths — centralised I/O paths
    │   ├── state.py                # load/save state.json per pipeline
    │   └── nulls.py                # normalise null tokens (..., u, N/A, :)
    └── pipelines/
        └── <pipeline_id>/
            ├── pipeline.py         # download → extract → DB compare → deliverable
            ├── extract.py          # pure extraction logic (file → DataFrame)
            ├── __init__.py
            ├── scripts/            # SQL query files for DB compare
            ├── downloaded/
            │   └── YYYY-MM/        # raw downloaded files (not committed)
            ├── output/
            │   └── YYYY-MM/        # deliverables + reports (not committed)
            ├── baseline.csv        # local snapshot DB (not committed)
            └── state.json          # run state / hashes (not committed)
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

### Run all pipelines
```bash
python run.py --all
```

### Run one pipeline
```bash
python run.py --pipeline gdp_greece
```

A summary table is printed at the end showing status, rows added/updated, deliverable name, and any errors.

---

## When does a pipeline skip?

A pipeline returns `skipped` (no deliverable produced) when:

1. **File unchanged** — the downloaded file has the same SHA256 as the previous run.
2. **No new data** — the file is new but DB compare finds 0 missing + 0 different rows and the baseline shows 0 new/updated rows.

This prevents empty deliverables and unnecessary S3 uploads when the source hasn't published new data.

---

## S3 sync

S3 upload is a **separate step** — run it after verifying deliverables.

### Dry run (preview keys without uploading)
```bash
python scripts/sync_s3.py --dry-run
```

### Sync all pipelines
```bash
python scripts/sync_s3.py
```

### Sync specific pipelines
```bash
python scripts/sync_s3.py gdp_greece cy_11_lro_transfers
```

### S3 path structure
```
raw_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{filename}_{timestamp}.{ext}
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{filename}_{timestamp}.csv
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{filename}_{timestamp}.log
```

**Sources:** `elstat`, `bank_of_greece`, `eurostat`, `migration_gov` (Greece) · `cystat`, `central_bank_cy`, `dls`, `eurostat` (Cyprus)

---

## DB usage

The DB is **read-only** from ETL. We never insert or update DB rows from code.
The DB is used only for:
- Comparing extracted data to find missing / different rows
- Looking up existing IDs to include in deliverables

Two databases:
- **athena** — Greek datasets (`ed_` pipelines)
- **zeus** — Cyprus datasets (`cy_` pipelines)

---

## Data sources

| Source | Country | Pipelines |
|---|---|---|
| ELSTAT (statistics.gr) | Greece | ~25 pipelines (DKT, SOP, SEL, SFC series) |
| Bank of Greece (bankofgreece.gr) | Greece | 11 pipelines |
| Eurostat (ec.europa.eu) | Greece + Cyprus | 7 pipelines |
| migration.gov.gr | Greece | 8 pipelines |
| CYSTAT (cystatdb.cystat.gov.cy) | Cyprus | 11 pipelines |
| Central Bank of Cyprus (centralbank.cy) | Cyprus | 3 pipelines |
| DLS / DLS Portal (portal.dls.moi.gov.cy) | Cyprus | 2 pipelines |

---

## Adding a new pipeline

1. Create folder: `etl/pipelines/<pipeline_id>/`
2. Add `__init__.py`, `pipeline.py`, `extract.py`
3. Add a SQL query file to `scripts/` named after the DB table
4. Follow the standard pattern:
   - Download → hash check → extract → `compare_and_update_csv` → `compare_with_postgres` → `write_deliverable_csv`
5. Add the pipeline to `_PIPELINE_META` in `etl/core/s3_upload.py`
6. Run once, check console for any QA warnings (unmapped codes, etc.)

---

## Notes

- All deliverable CSV headers are automatically snake_cased by `write_deliverable_csv`.
- `...`, `u`, `N/A`, `:` and similar null tokens are normalised to `pd.NA` before DB compare.
- Per-pipeline `.gitignore` excludes `downloaded/`, `output/`, `state.json`, `baseline.csv`.
