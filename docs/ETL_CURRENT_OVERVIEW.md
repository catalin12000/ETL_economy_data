# ETL Current Overview

This repo runs the Greece and Cyprus economy-data ETL pipelines. It downloads public source files, extracts DB-shaped rows, compares them with the read-only Postgres databases, writes Delta Deliverables, and syncs delivered artifacts to S3 for the Backend to load.

## What Is Here

- `run.py`: main entry point for running one Pipeline or all Pipelines.
- `etl/core/`: shared runner, state, download, comparison, output, logging, and S3 Sync helpers.
- `etl/pipelines/<pipeline_id>/`: one folder per Pipeline, each with its own `pipeline.py` and usually an `extract.py`.
- `scripts/sync_s3.py`: separate Sync step that uploads Raw files, Deliverables, and matching Pipeline Run Logs to S3.
- `CONTEXT.md`: glossary for the project language.
- `docs/adr/`: short architecture decision records.

## Current Flow

Run all Pipelines:

```powershell
python run.py --all
```

The runner discovers every `etl/pipelines/<pipeline_id>/pipeline.py`, loads its `Pipeline` class, loads its `state.json`, runs the Pipeline, saves new state, prints a summary, regenerates the dashboard, and writes a local Run Log.

Sync to S3 after checking the run:

```powershell
python scripts/sync_s3.py
```

Sync uploads only Pipelines whose latest state is deliverable. Latest `skipped` and `error` states are shown in the Sync summary but are not uploaded as stale Deliverables.

## Deliverables

Deliverables are CSV files containing the Delta against the live DB:

- Inserts have an empty `id`.
- Updates have an existing `id`.
- CSV headers are normalized to snake_case.
- Local Deliverables live under `etl/pipelines/<pipeline_id>/output/<YYYY-MM>/`.
- S3 Deliverables are named with the DB table name:

```text
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.csv
```

## Logs

Logs are human-readable `.log` files only.

- Pipeline Run Log: created only for non-skipped Pipeline runs.
- Skipped Pipeline: creates no Pipeline Run Log.
- Error Pipeline: keeps its Pipeline Run Log local only.
- Delivered Pipeline: Sync uploads its Pipeline Run Log beside the S3 Deliverable with the same stem.
- Run Log: local-only summary of `python run.py --all`.
- Sync Log: local-only summary of `scripts/sync_s3.py`.

S3 pairs look like this:

```text
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.csv
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.log
```

## What We Changed

- Finished snake_case normalization across Pipeline configs, extracted columns, comparison columns, and Deliverables.
- Added shared Pipeline logging around runner, download, DB compare, baseline compare, and Deliverable writing.
- Removed JSONL log artifacts from the active policy.
- Added local Run Logs and Sync Logs.
- Made S3 Sync upload Pipeline Run Logs only after the matching Deliverable CSV upload succeeds.
- Removed the extra S3 `deliverable/` folder so transformed CSV and `.log` files sit together.
- Documented the logging and S3 audit decision in ADR-0006.

## Latest Verified State

The latest full test run completed with:

- `64` Pipelines discovered.
- `62` delivered.
- `2` skipped because their Source files were unchanged.
- `0` errors.
- S3 Sync: `62` OK, `2` skipped, `0` partial, `0` failed.
- No `.jsonl` log files under `etl/logs` or `etl/pipelines`.

The remaining known cleanup is pandas `FutureWarning` noise from `etl/core/compare_csv.py`; it does not block running or Sync.
