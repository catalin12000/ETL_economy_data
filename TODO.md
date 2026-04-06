# TODO

This file tracks the repo-level work we do not want to lose between sessions.

## Production Hardening

- [ ] Standardize the pipeline contract across all fully built pipelines.
- [ ] Define one canonical deliverable policy per pipeline type: DB delta only, full replacement, or business-facing extract.
- [ ] Standardize state keys across pipelines (`source_url_used`, `download_url_used`, `api_url_used`, multi-file hashes, timestamps).
- [ ] Standardize numeric comparison tolerances and document when a pipeline is allowed to diverge.
- [ ] Decide where semantic data hashing (`data_sha256`) is required and apply it consistently where source files are noisy.
- [ ] Refactor shared DB compare logic into smaller, testable units and reduce pipeline-specific duplication around column remapping.
- [ ] Add structured logging for pipeline runs instead of relying mainly on `print`.
- [ ] Persist richer failure details for runs, not just the final exception string.
- [ ] Add retry/backoff rules for flaky network downloads where the publisher allows it.
- [ ] Define a production readiness checklist that every pipeline must satisfy before being considered complete.

## Testing

- [ ] Add automated tests for `etl/core/database.py` compare behavior.
- [ ] Add automated tests for `etl/core/compare_csv.py`.
- [ ] Add fixture-based extraction tests for the newer pipelines that are already fully wired.
- [ ] Add regression tests for period parsing edge cases: month carry-forward, quarter carry-forward, placeholder future blocks, and multi-file joins.
- [ ] Add smoke tests for representative pipeline families: ELSTAT, Eurostat, Bank of Greece, CYSTAT, and PDF-based pipelines.
- [ ] Add a CI-friendly test mode that does not require live Postgres access.

## Documentation And Tracking

- [ ] Update `PIPELINE_TRACKER_STATUS.md` so it matches the current codebase.
- [ ] Decide which dashboard/tracker file is the source of truth and deprecate stale ones.
- [ ] Keep `HOW_TO_PIPELINE_WORKFLOW.md` aligned with the actual code pattern, especially around `data_sha256` usage and deliverable semantics.
- [ ] Document pipeline maturity levels clearly: downloader-only, extractor-only, local compare, DB compare, production-ready.
- [ ] Add a short operator guide for running a single pipeline, reading outputs, and diagnosing failed runs.

## Consistency Gaps In Built Pipelines

- [ ] Normalize deliverable behavior across built pipelines where possible.
- [ ] Normalize file freshness logic across built pipelines.
- [ ] Normalize folder/pipeline/table naming where it has drifted.
- [ ] Normalize output column naming conventions where possible.
- [ ] Normalize how DB SQL files are named and referenced.
- [ ] Review built pipelines for inconsistent sorting and final output ordering.

## Immediate Cleanup Candidates

- [ ] Review fully built pipelines and mark which ones already follow the new standard pattern.
- [ ] Create a gap list for the built pipelines that still differ from the standard pattern.
- [ ] Prioritize stale or misleading repo docs before adding more pipelines.
- [ ] Decide whether `TODO.md` should become the main cross-session engineering backlog.
