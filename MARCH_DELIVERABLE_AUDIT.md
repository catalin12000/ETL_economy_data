# March 2026 Deliverable Audit

Inspection of the 9 files in `deliveralbe_March_EconomyData/`. For each table: the error noticed, and how we could fix it.

| # | Table | Error noticed | How to fix |
|---|-------|---------------|------------|
| 1 | `ed_apartments_price_index` | 5 of 15 rows have empty `ID` — every 2025 Q3 row (latest quarter, all regions). 2025 Q1/Q2 rows do have IDs. | Expected for rows not yet in DB. Either backfill IDs after the consumer's load step, or stop emitting the `ID` column for delta deliverables. Pick one and apply uniformly. |
| 2 | `ed_building_permits_by_no_of_rooms` | The `.xls` is the **raw publication snapshot**, not a normalized deliverable. Sheet `ΠΙΝ4` has multi-row headers and bilingual column labels with `Unnamed: 1..6` columns; sheet `ΕΛ` is all zeros. No `ID`/`Year`/`Month` columns. | Build the extractor for this pipeline so it produces a clean normalized CSV (Year, Month, Region, Permits Number, Rooms, Volume, Surface) the way `ed_building_permits_table` does. Currently the pipeline only downloads and ships the source file as-is. |
| 3 | `ed_building_permits_table` | No `ID` column at all (only Year, Month, Permits Number, Area, Volume). Inconsistent with peer tables. | Add `ID` to `target_cols` in `etl/pipelines/ed_building_permits_table/pipeline.py` and let the runner populate it from DB, like the other ED tables. |
| 4 | `ed_construction_index` | 2 of 4 rows (2025 Q3, 2025 Q4) have empty `ID`. | Same root cause as #1 — "missing in DB" rows. Either accept blank IDs and document it, or sequence DB insert before deliverable so IDs come back populated. |
| 5 | `ed_consumer_price_index` | Rows are not sorted: top row is 2026-02, then jumps to 2022-03, 2018-07, 2017-03, 2014-11, 2014-04, ... 2006-07. Top row (2026-02) also has empty `ID`. | Add a final `sort_values(["Year","Month"])` in the deliverable build step. The current order is `concat(inserted_df, updated_df)` with no resort, which leaves the "diff" history rows mixed with the new insert. |
| 6 | `ed_geo_distribution_of_issued_and_pending_permits` | (a) All 56 rows have empty `ID` (first-load scenario). (b) **4 rows have an empty `Revoked` cell** mid-row: Renewal/12-months/Attica (2026-02), Renewal/12-months/Macedonia and Thrace (2026-01), Renewal/12-months/Epirus and Western Macedonia (2026-01 and 2026-02). The cell sits between two valid integers, so it's a parsing miss, not a data gap. | Investigate the PDF extractor for the Renewal/12-months block — the column alignment slips for those specific region rows. Likely a column-detection regex that treats a low-digit number as a delimiter. Once fixed, rerun and Revoked should populate. All-empty IDs are fine if this was the table's first load; verify against DB whether the table was empty in March. |
| 7 | `ed_gross_fixed_capital_formation` | Last 6 rows (2025 Q2/Q3/Q4 in both Unadjusted and Adjusted) have empty `ID`. `Cultivated biological resources` is negative (-3, -4) for 2023 Q4, 2024 Q1, 2024 Q2, 2024 Q3, 2024 Q4. | Empty IDs: same as #1. Negative biological-resources values: cross-check against the ELSTAT source — could be legitimate (disinvestment / revisions write down stock) but worth confirming with the upstream publication before shipping. |
| 8 | `ed_imports_exports_millions` | (a) Numeric columns have 10–15 decimal places, e.g. `44526.552473667485`. (b) Last row (2025) has empty `ID`. | (a) **Already fixed** in commit `4b00df9` — all deliverable CSVs now go through `write_deliverable_csv` which rounds numeric columns to 3 decimals. Rerun the pipeline to regenerate. (b) Empty ID is the recurring new-row issue. |
| 9 | `ed_industrial_production_index` | Last 4 rows (2025-10, 2025-11, 2025-12, 2026-01) have empty `ID`. There is also an `ID` gap: row with ID 838 (2025-05) is followed by row with ID 840 (2025-06) — no row with ID 839. | Empty IDs: same as #1. Gap at 839: investigate the DB compare — either the DB row 839 was deleted upstream, or the compare's WHERE clause is filtering it out. Check `etl/pipelines/ed_industrial_production_index/ed_industrial_production_index.sql` and confirm `SELECT *` doesn't drop rows. |

## Cross-cutting findings (worth fixing once, not per-table)

- **Empty `ID` on new rows** shows up in 7 of 9 files. It is a consequence of the delta-deliverable design (insert rows have no DB-assigned ID yet). Decide: keep the column blank and document it, drop it for inserts, or run an upsert step before emitting. Whichever option you pick, apply it through the shared `etl/core/migration_appendix_b_runner.py` and the generic pipeline structure so every table behaves the same way.
- **Unsorted output** (#5) is also concat-without-resort. Add a `sort_values(...)` in `_format_delta` (migration appendix B runner) and in the standalone pipelines that don't go through it. Cheap fix, table-wide impact.
- **Decimal precision** (#8) is already addressed globally by the rounding helper landed in `4b00df9`. Rerunning the pipelines regenerates clean files.

## Suggested next steps

1. Rerun the 9 affected pipelines now that decimals are bounded.
2. Decide ID-on-insert policy (one of: blank, drop column, pre-insert) and codify in the runner.
3. Add the final sort in `_format_delta`.
4. Open targeted fixes for: `ed_building_permits_by_no_of_rooms` extractor, `ed_geo_distribution` Renewal/12-months column alignment, `ed_industrial_production_index` ID-839 gap investigation.
