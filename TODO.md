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
- [ ] Clarify pipeline `45 ed_services_sector_turnover_monthly_index`: the target CSV is monthly with 26 codes, but the current DB table `ed_services_sector_turnover_index` is quarterly with 16 codes. Decide whether `45` should follow the target CSV/workbook shape or the existing SQL table before building the extractor.
- [ ] Clarify pipeline `cy_05_consumer_price_index`: ask why the pipeline was explicitly switched to base year `2025` in commit `b6fb38c`, and confirm whether that is the intended long-term business rule or just a temporary choice.
- [ ] Clarify pipeline `cy_16_total_households_loans_millions`: the current output/deliverable logic is not yet understood well enough, so review the source-to-output mapping before changing it further.

## Ticket Notes — Eurostat Euro Area Code Rollover (EA20 → EA21)

Bulgaria joined the euro area on 2025-01-01, so Eurostat's "euro area" aggregate rolled from `EA20` (20 countries, 2023-01 → 2024-12) to `EA21` (21 countries, from 2025-01). Eurostat retired `EA20` from the `une_rt_m` dataset, which broke `ed_eu_unemployment_rate` with a 400 (`INVALID_QUERY_DIMENSION_VALUE: GEO=EA20`).

- [x] `ed_eu_unemployment_rate`: FILTER updated `EA20` → `EA21`. Pipeline runs again.
- [ ] **Open data-continuity question for the ticket**: new `EA21` rows arrive with no `GEO_MAP` entry, so they currently land under the literal label `"EA21"` instead of joining the historical `"Euro area – 20 countries (from 2023)"` series in the DB. Decide which convention to apply across all EU pipelines:
  - (a) relabel `EA21` to a single canonical "Euro Area" so the time series stays continuous (caveat: pre-2025 = 20 countries, post-2025 = 21 countries — not strictly comparable);
  - (b) keep `EA20` historical and add a new entity `"Euro area – 21 countries (from 2025)"` for `EA21`;
  - (c) ingest both `EA20+EA21` and store as separate series, let consumers choose.
- [ ] Pre-emptively review the four other pipelines still hardcoding `EA20` (`ed_eu_gdp`, `ed_eu_hicp`, `ed_eu_consumer_confidence_index`, `ed_economic_sentiment_indicator`). They still work today, but Eurostat will likely retire `EA20` from those datasets too. Either swap to `EA21` now or use `EA20+EA21` as a transition filter.
- [ ] Once the labeling decision is made, add the chosen `EA21` mapping to every EU pipeline's `GEO_MAP` (extract.py).
