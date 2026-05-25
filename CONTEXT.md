# ETL Economy Data

ETL framework that collects Greek and Cypriot economic datasets from public sources, extracts structured data, compares against the live Postgres databases, and produces deliverables that a downstream backend uploads to the DB.

## Language

### Workflow

**Pipeline**:
A single dataset's end-to-end flow: download from a Source, extract to a tidy DataFrame, compare with the DB, and produce a Deliverable. Identified by a `pipeline_id` (e.g. `ed_wage_growth_index`).
_Avoid_: Job, task, dataset (these mean different things below).

**Source**:
The upstream publisher of a dataset — ELSTAT, Bank of Greece, Eurostat, CYSTAT, Central Bank of Cyprus, DLS, migration.gov.gr. Each Pipeline has exactly one Source.
_Avoid_: Provider, vendor, origin.

**Extractor**:
The pure transformation step inside a Pipeline. Takes a downloaded file (PDF/XLS/CSV/HTML), returns a DataFrame whose columns match the target DB Table.
_Avoid_: Parser, reader.

**Backend**:
The downstream system (outside this repo) that picks up Deliverables from S3 and writes the rows into the DB. We have no write access; the Backend does. Cadence and trigger are owned by the Backend team.
_Avoid_: Ingestor, loader, consumer.

### Data

**Deliverable**:
A CSV file produced by a successful Pipeline run, containing only the Delta (Inserts + Updates) to apply to the DB Table. Named locally `deliverable_{pipeline_id}_{Month_Year}.csv`; named in S3 `{db_table_name}_{timestamp}.csv` so the Backend can route it.
_Avoid_: Output, file, dump, export.

**Delta**:
The set of rows that differ between the Extractor output and the DB. Two kinds: rows missing from the DB (Inserts) and rows whose values differ from the DB (Updates).
_Avoid_: Diff (used in pandas for something different), changes.

**Insert** vs **Update**:
A row in a Deliverable with an empty `id` column is an Insert (new row, Backend assigns id). A row with a populated `id` is an Update (Backend overwrites the matched row).
_Avoid_: Create/modify, new/changed.

**Raw file**:
The downloaded source artifact (PDF/XLS/etc.) before any extraction. Uploaded to S3 for audit only — the Backend does not read these.
_Avoid_: Download, source file, raw data.

**Source type**:
A category that determines which skip signal each Pipeline uses. Declared as a class attribute on every Pipeline. See [ADR-0005](./docs/adr/0005-source-type-skip-signals.md).
- `static_file` — fixed URL, file content updates in place (skip on `file_sha256`)
- `dynamic_file` — URL changes per publication period (skip on `latest_period_seen`)
- `api` — programmatic endpoint with varying response bytes (skip on `data_sha256`)
- `scraped` — file discovered from an index page, one per period (skip on `latest_period_seen`)

**State**:
A `state.json` per Pipeline holding `file_sha256`, `last_download_path`, `last_run_at_utc` and similar. Treated as a **cache** to drive the freshness check (skip when the source SHA is unchanged). Safe to delete — losing it costs one redundant run, not correctness. Audit history lives in S3 (raw files + Deliverables, all timestamped). Future work: sync `state.json` to S3 alongside Raw files so history survives a wiped working copy.
_Avoid_: Run log, audit log (state is a cache, not the audit trail).

**Pipeline Run Log**:
A human-readable `.log` audit file for one non-skipped Pipeline run. It is uploaded beside its Deliverable in S3 with the same `{db_table_name}_{timestamp}` stem only when a Deliverable exists; error logs stay local. It explains what the Pipeline did, what Delta it produced, and what went wrong when the Pipeline fails.
_Avoid_: JSONL, total run log, state, debug log, console output.

**Skipped Pipeline**:
A Pipeline run that stops because the Source has not published a new file or the file content has not changed since the previous run. It creates no Pipeline Run Log artifacts, but still appears as Skipped in local summaries. A changed Source that produces an empty Delta after DB Compare is not Skipped.
_Avoid_: Empty Deliverable, no-op Delta.

**Run Log**:
A local-only human-readable `.log` summary of a batch run across Pipelines, containing counts by final status such as Delivered, Skipped, and Error. It is not synced to S3 because S3 audit objects are scoped to Deliverables and Raw files.
_Avoid_: Pipeline Run Log, state, dashboard.

Local logs are gitignored and kept indefinitely unless manually removed; S3 is the durable audit store for delivered artifacts.

**DB Compare**:
The only meaningful comparison in a Pipeline. Calls `compare_with_postgres` against the live Postgres database to produce the Delta. The DB is the single source of truth for "what already exists". See [ADR-0001](./docs/adr/0001-db-as-single-source-of-truth.md).
_Avoid_: Baseline (deprecated — see ADR).

**Tolerance**:
An **absolute** numeric threshold passed to `compare_with_postgres`. Values are considered equal if `|extracted - db| <= tolerance`. Exists because the DB rounds stored values to 2 decimals, so without a tolerance every row would appear as an Update. Calibrated per-Pipeline to the magnitude of the values (`0.05` for index-style values, `0.11` for indices with larger rounding drift, `1.0` for trade values in millions).
_Avoid_: Threshold, epsilon.

### Databases

**DB Table**:
A specific Postgres table that a Pipeline targets. The `pipeline_id` is not always identical to the Table name (e.g. `ed_wholesale_trade_turnover_index` writes to `ed_wholesales_turnover_index`). The mapping lives in `_PIPELINE_META`.
_Avoid_: Schema (means something else in SQL), entity.

**athena** / **zeus**:
The two production Postgres databases. **athena** holds Greek datasets, **zeus** holds Cypriot datasets. The `db_name` of each Pipeline determines which one.
_Avoid_: Greek DB / Cyprus DB (use the names).

**Read-only**:
This ETL has read-only credentials for both databases. We never INSERT or UPDATE from code. All writes go through the Backend.
_Avoid_: Sync, write, push (these wrongly imply we write).

### S3

**S3 path structure**:
```
raw_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{stem}_{timestamp}.{ext}
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.csv
transformed_data/{cy|gr}/economy_data/{source}/{YYYYMMDD}/{db_table_name}_{timestamp}.log
```
The transformed `.csv` and `.log` use the same S3 timestamp so they are an obvious pair; the Pipeline run id remains inside the Pipeline Run Log content.

**Sync**:
The standalone step (`scripts/sync_s3.py`) that uploads Raw files and the Deliverable for each Pipeline to S3. Decoupled from pipeline execution by design — we run pipelines, verify, then Sync.
_Avoid_: Upload (too generic — Sync means the specific multi-pipeline batch upload).

**Partial Sync**:
A Sync result where the Deliverable reaches S3 but a required audit artifact, such as a Raw file or Pipeline Run Log, fails to upload. It is not a data delivery failure, but it is an audit failure and must be visible in the Sync summary.
_Avoid_: OK, warning.

**Sync Log**:
A local-only human-readable `.log` summary of one Sync batch, containing counts for OK, Partial, Failed, and Skipped Pipelines plus S3 keys for uploaded Deliverables, Pipeline Run Logs, and Raw files. Sync does not introduce a domain identifier; pairing is done by the S3 Deliverable name and timestamp.
_Avoid_: Run Log, Pipeline Run Log, S3 report.

**S3 Sync Section**:
The final section appended to a local Pipeline Run Log before Sync uploads that same log beside the Deliverable in S3. It records the S3 Deliverable name, S3 Deliverable key, and S3 Pipeline Run Log key; the S3 Pipeline Run Log is uploaded only if the Deliverable CSV upload succeeds.
_Avoid_: Upload section, S3 footer.

## Flagged ambiguities

- **"Dataset"** is intentionally avoided. It conflates Source (publication), Pipeline (our processing unit), DB Table (storage), and Deliverable (output). Use the precise term.
- **"Cy" prefix is not the same as zeus**. A Pipeline whose id starts with `cy_` usually targets zeus, but the DB Table name may still start with `ed_` (e.g. `cy_14` → `ed_per_day_expenditure_of_tourists` on zeus).

## Example dialogue

> **Catalin:** The wholesale turnover Pipeline is producing the wrong column.
> **Claude:** Which Source — ELSTAT? Which DB Table does it write to?
> **Catalin:** ELSTAT. The pipeline_id is `ed_wholesale_trade_turnover_index` but the DB Table is `ed_wholesales_turnover_index`.
> **Claude:** Got it. After the next run, the Deliverable will land in S3 as `ed_wholesales_turnover_index_{timestamp}.csv` and the Backend will route it to athena.
> **Catalin:** Right. And if the column is wrong, the Delta will mark every row as Update, not Insert?
> **Claude:** Only if the `id` is already populated from the DB lookup. New rows still arrive as Inserts.
