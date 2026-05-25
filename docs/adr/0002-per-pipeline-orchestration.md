# Per-Pipeline orchestration over a shared runner

Most Pipelines (~56) duplicate the same 8-step orchestration in their `pipeline.py`: resolve URL → download → extract → rename → DB compare → shape delta → write deliverable → update state. We considered extracting this into a shared `run_standard_pipeline()` function (similar to `migration_appendix_b_runner` which is shared across the 9 residence-permits pipelines).

**Decision:** Keep the duplication. Each Pipeline owns its full orchestration in its own folder, even when most of the code is similar to another Pipeline's. The only shared runners are `migration_appendix_b_runner` (genuinely one PDF, many extractions) and the shared core utilities (`download`, `compare_with_postgres`, `write_deliverable_csv`).

**Why:** Pipelines diverge in small ways that would push a shared runner toward many config options — different source resolvers (ELSTAT publication lookup vs BoG static URL vs CYSTAT API), different download patterns (single file vs multi-file merge), different rename maps, different deliverable shaping rules (formatting decimals, handling `id`, integer conversions). The maintainability of "I can change one Pipeline without breaking 50 others" was judged more valuable than the DRY win.

**Consequences:**
- Adding a new Pipeline copies an existing one as a template — that is the expected workflow.
- Cross-cutting changes (e.g. column normalization, new logging fields) require touching every Pipeline file. Done once already during the snake_case header migration.
- A future reader should resist the urge to "fix" the duplication unless a concrete change forces touching all files anyway.
