# How To Build ETL Pipelines (Current Project Pattern)

This document captures the workflow used in the recent pipelines we implemented/fixed:
- `ed_housing_finances`
- `ed_imports_exports_millions`
- `ed_industrial_production_index`
- `ed_key_partners_primary_goods` downloader targeting fix

It is the standard pattern to follow for new pipelines in this repo.

## 1) Core Rules

- Database is **read-only** from ETL.
- We never insert/update DB rows from code.
- DB is used only for:
  - comparison
  - ID lookup for updates
  - producing deliverable rows (new + changed)

## 2) Pipeline Structure

Each pipeline lives under:
- `etl/pipelines/<pipeline_id>/pipeline.py`

Optional helper files:
- `extract.py` (source parsing logic)
- `<pipeline_id>.sql` (explicit DB select query used by compare)

Typical `pipeline.py` flow:
1. Resolve source URL(s)
2. Download source file(s)
3. Extract normalized dataframe
4. Build local compare outputs:
- `mock_db_snapshot.csv`
- `new_entries.csv`
- `data/reports/<prefix>_<pipeline_id>/update_report.csv`
5. Compare against Postgres (read-only)
6. Build deliverable CSV with `ID` column
7. Save state in `data/state/<pipeline_id>.json`

## 3) Downloading Strategy

Use `etl/core/elstat.py` helpers:
- `get_latest_publication_url(...)`
- `get_latest_publication_year_url(...)`
- `get_download_url_by_title(...)`

Important:
- Use stable title substrings, not fragile full titles.
- If a table can lag behind latest month, add month lookback logic (example used in `ed_key_partners_primary_goods`).

## 4) Extraction Strategy

Keep extraction deterministic.

Best practice:
- Map fixed worksheet rows/columns to output fields.
- Parse year/month/quarter carefully (carry-forward year when source does that).
- Round numeric metrics only at final output stage (normally to 2 decimals).
- Reject placeholder blocks (for example all-zero future quarter placeholders).

## 5) Local Compare Outputs

Local compare uses `compare_and_update_csv(...)`.

Outputs under `data/outputs/<prefix>_<pipeline_id>/`:
- `mock_db_snapshot.csv`: full local-updated view vs baseline `data/db/...csv`
- `new_entries.csv`: rows added/changed from local baseline compare
- `deliverable_<pipeline_id>_<Month_Year>.csv`: DB delta deliverable

## 6) DB Compare + ID Logic

Use `compare_with_postgres(...)` with:
- `match_cols`: business keys (for example `year, month` or `year, quarter, group, category, sub_category`)
- `sync_cols`: numeric/text values to compare
- `sql_file_path`: explicit query from pipeline folder

Deliverable rules:
- include `ID` as first column
- rows existing in DB carry DB `id`
- new rows have blank `ID`

## 7) Deliverable Rules

Deliverable should be:
- sorted by period ascending unless business asks otherwise
- in client-required column names/order
- only DB delta rows (missing + different) by default

If needed, include helper columns like `Subcategory` for readability while keeping DB key logic unchanged.

## 8) Smoke Test Process

Run one pipeline:
- `python run.py --pipeline <pipeline_id>`

Run all:
- `python run.py --all`

Check:
- console status (`delivered/skipped/error`)
- `data/state/<pipeline_id>.json`
- output files in `data/outputs/<prefix>_<pipeline_id>/`

## 9) Recent Implementation Notes

### `ed_housing_finances`
- Added full extractor + SQL compare.
- Fixed year carry-forward and quarter placeholder filtering.
- Added `ID` and `Subcategory` in deliverable.
- Ensured historical rows up to DB coverage match IDs.

### `ed_imports_exports_millions`
- Upgraded from downloader-only to full pipeline.
- Extracts annual metrics from ELSTAT workbook row mapping.
- Added DB compare + deliverable with `ID`.

### `ed_industrial_production_index`
- Upgraded from downloader-only to full pipeline.
- Uses both downloaded files:
  - file 03 for `overall_index`
  - file 04 for seasonally adjusted overall + section indices
- Added DB compare + deliverable with `ID`.

### `ed_key_partners_primary_goods`
- Fixed downloader target to SITC-1 country-value file.
- Added backward month search when latest month does not include SITC-1 table.

## 10) Checklist For New Pipelines

1. Create/confirm `pipeline.py` in `etl/pipelines/<pipeline_id>/`
2. Add `extract.py` if parsing needed
3. Add `<pipeline_id>.sql` query for DB compare
4. Implement local compare outputs
5. Implement DB compare (`match_cols`, `sync_cols`, tolerance)
6. Build deliverable with `ID`
7. Run pipeline and verify outputs
8. Validate against expected sample file/user business format
9. Update dashboard/tracker docs if required

## 11) Non-Negotiable Constraint

- **DB is read-only.**
- ETL must never execute INSERT/UPDATE/DELETE on DB.
