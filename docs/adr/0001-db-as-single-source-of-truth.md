# DB as the single source of truth for Pipeline comparison

Each Pipeline previously did two comparisons: first against a per-pipeline `baseline.csv` (via `compare_and_update_csv`), then against the live Postgres DB (via `compare_with_postgres`). Only the second one produced the Deliverable — the baseline output was never consumed downstream.

**Decision:** The live Postgres DB is the only "what already exists" reference. The Deliverable's Delta (Inserts + Updates) is built solely from `compare_with_postgres`. The baseline.csv / `compare_and_update_csv` step is to be removed from every Pipeline.

**Why:** The baseline added local files (`baseline.csv`, `mock_db_snapshot.csv`, `update_report.csv`, `new_entries.csv`) and code per Pipeline without ever feeding the output. Worse, it forced authors to think about a second mental model ("local snapshot diff" vs "DB diff") when only one matters. Read-only access to the DB makes it both authoritative and cheap to query.

**Consequences:**
- Removing it touches 60+ `pipeline.py` files and deletes `etl/core/compare_csv.py`.
- We lose the local "extraction stability" cross-check between runs, but this never caught a real bug — the DB compare surfaces the same drift.
- New Pipelines become shorter and easier to write correctly.
