# Source-type-driven skip signals

The "should I skip this run?" check needs a different signal depending on what the upstream Source looks like. A single `file_sha256` check (the original convention) is wrong for APIs and for sources whose URL changes every period.

**Decision:** Each Pipeline declares a `source_type` class attribute, one of:

- `static_file` — fixed URL, content updates in place (Bank of Greece, Eurostat, Central Bank of Cyprus). Skip signal: `file_sha256`.
- `dynamic_file` — URL changes every publication period (ELSTAT publications). Skip signal: `latest_period_seen`.
- `api` — programmatic endpoint; response bytes vary even when the data is identical (CYSTAT PX-API). Skip signal: `data_sha256` of the extracted DataFrame.
- `scraped` — file links discovered from an index page, one file per period (DLS, migration.gov.gr). Skip signal: `latest_period_seen` (same semantics as `dynamic_file`).

`etl/core/fingerprint.py` provides one helper per source type (`should_skip_static_file`, `should_skip_dynamic_file`, `should_skip_api`, `should_skip_scraped`) and a generic `should_skip(source_type, state, ...)` dispatcher.

**Why:** Picking the wrong signal causes either:
- False positives — skip when the data has actually changed (silent staleness), or
- False negatives — re-extract every run because the bytes always differ for cosmetic reasons (wasted work).

Examples we saw before this was formalised:
- An API pipeline that compared `file_sha256` of the response would never skip — every API call has timestamp/ordering jitter.
- A scraped pipeline (LRO transfers) that didn't track period would re-process the same PDFs every run.

**Consequences:**
- Every new Pipeline must declare `source_type`. Adding `Literal` typing in `fingerprint.py` makes typos surface in editors.
- Existing per-Pipeline skip checks still work — `source_type` is metadata; the optional follow-up is to refactor each pipeline.py to call `should_skip(self.source_type, state, ...)` for consistency. Not done in this change.
