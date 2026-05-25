# Pipeline class owns its identity (country, source, DB table name)

Today, each Pipeline's metadata (country folder `cy|gr`, source folder `elstat|cystat|...`, DB Table name) lives in a single `_PIPELINE_META` dict in `etl/core/s3_upload.py`. The DB Table name is **also** repeated inline inside `pipeline.py` (passed to `compare_with_postgres(table_name=...)`). New Pipelines must be added to the dict manually or S3 sync silently skips them.

**Decision:** Each Pipeline class declares its own identity as class attributes:

```python
class Pipeline:
    pipeline_id = "ed_wage_growth_index"
    country = "gr"                          # "cy" | "gr"
    source = "elstat"                       # "elstat" | "bank_of_greece" | "cystat" | ...
    db_table_name = "ed_wage_growth_index"  # what goes into S3 deliverable filename and DB compare
```

`_PIPELINE_META` becomes a derived view, built by introspecting `etl/pipelines/`. `compare_with_postgres` reads `db_table_name` from `self` so the value only exists in one place.

**Why:** Eliminates the silent "new Pipeline not in registry" failure mode. Eliminates the two-places-for-the-same-truth bug class (we already hit it once when the S3 sync used the wrong name). The Pipeline class becomes self-describing — a future engineer can read one file to know everything about the Pipeline, including where it ends up in S3 and the DB.

**Consequences:**
- One-time refactor across ~64 `pipeline.py` files.
- New Pipelines stop being able to "forget" to register themselves.
- `_PIPELINE_META` stays as a helper, but is constructed from the Pipeline classes rather than maintained by hand.
