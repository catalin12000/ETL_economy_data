# ETL Current Overview

This repo runs the Greece and Cyprus economy-data ETL pipelines. It downloads public source files, extracts DB-shaped rows, compares them with the read-only Postgres databases, writes Delta Deliverables, and syncs delivered artifacts to S3 for the Backend to load.

## What Is Here

- `run.py` — entry point: run one Pipeline or all Pipelines.
- `etl/core/` — shared runner, state, download, fingerprint, DB compare, output, logging, and S3 helpers.
- `etl/pipelines/<pipeline_id>/` — one folder per Pipeline, each with its own `pipeline.py` and an `extract.py`.
- `scripts/sync_s3.py` — separate Sync step; run after pipelines to upload Raw files + Deliverables to S3.
- `CONTEXT.md` — domain glossary.
- `docs/adr/` — architecture decision records.

## Current Flow

```powershell
# 1. Run pipelines
python run.py --all          # or --pipeline <id> for one

# 2. Verify summary, then sync to S3
python scripts/sync_s3.py
```

The runner auto-discovers every `etl/pipelines/<pipeline_id>/pipeline.py`, loads its `Pipeline` class, loads `state.json`, runs the Pipeline, saves new state, prints a summary, and writes a local Run Log.

Sync uploads Raw files + Deliverable + Pipeline Run Log for each Pipeline whose last run was `delivered`. Pipelines in `skipped` or `error` state are shown in the summary but not uploaded.

## Pipeline Flow

Each `Pipeline.run(state)` follows the same pattern:

1. Download source file(s) and compute SHA256
2. **Skip check** — return `skipped` early if source hasn't changed (see skip signals below)
3. Extract to a tidy DataFrame
4. **DB Compare** — `compare_with_postgres` finds the Delta (inserts + updates) against the live DB
5. Write Delta as a Deliverable CSV via `write_deliverable_csv`

Skip signals per `source_type` (ADR-0005):

| `source_type` | Skip when |
|---|---|
| `api` | `data_sha256` unchanged |
| `dynamic_file` | `file_sha256` unchanged + DB has no delta |
| `static_file` | `file_sha256` unchanged + DB has no delta |
| `scraped` | `latest_period_seen` unchanged |

## Deliverables

Deliverables are CSVs containing only the Delta (inserts + updates) against the live DB.

- Empty `id` = Insert (Backend assigns the id).
- Populated `id` = Update (Backend overwrites that row).
- Headers are snake_cased automatically.
- Local path: `etl/pipelines/<pipeline_id>/output/<YYYY-MM>/deliverable_<pipeline_id>_<Month_Year>.csv`
- S3 path: `transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.csv`

## Logs

All logs are human-readable `.log` files (no JSONL).

| Log | When created | Synced to S3? |
|---|---|---|
| Pipeline Run Log | Every non-skipped run | Yes, beside its Deliverable |
| Run Log | Every `python run.py --all` | No (local only) |
| Sync Log | Every `scripts/sync_s3.py` | No (local only) |

Error Pipeline Run Logs stay local — only successful deliveries reach S3.

S3 pairs:
```
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.csv
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.log
```

## What Changed (restructure/data-layout)

- **Removed `dashboard.py`** — was reading from a stale state path, replaced by the run summary printed to stdout.
- **Removed `compare_and_update_csv` from all pipelines** — DB is the single source of truth (ADR-0001); the local `baseline.csv` comparison step is gone from all 50 affected pipelines. `state.json` no longer stores `rows_before`, `rows_after`, `new_rows`, `updated_cells`, `delta_path`, or `mock_db_snapshot_path`.
- **Added skip signals to `cy_03`, `cy_05`** (`api` → `data_sha256`) and **`cy_10`, `cy_11`** (`scraped` → `latest_period_seen`).
- **Fixed `db_comparison` key names** in `cy_10` and `cy_11` state (`missing_in_db` / `different_in_db`) to match what `runner.py` reads.
- **Added missing S3 raw path keys** for `cy_10` (contracts), `ed_industrial_production_index`, and `ed_office_price_volume_index`.
- **Removed dead code block** in `etl/core/database.py` (`cols_to_restore` variable that was never used).
- **Added test suite** under `tests/` covering skip logic, S3 raw path collection, and smoke tests for all pipeline imports.
