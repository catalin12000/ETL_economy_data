# Schema validation in compare_with_postgres

A Pipeline's Extractor produces a DataFrame that should match the DB Table on `match_cols + sync_cols`. Previously, if the Extractor renamed a column slightly wrong (e.g. `permit_granted_to_women` instead of `permits_granted_to_women`), the DB merge silently joined anyway: the missing column came through as NaN on every row, producing a fake Delta where every row appeared to be an Update. The bug only surfaced when a human noticed empty values in the Deliverable.

**Decision:** `compare_with_postgres` validates that every column listed in `match_cols + sync_cols` exists in **both** the extracted DataFrame **and** the DB Table before doing any work. If anything is missing, it returns an `error` dict and the Pipeline aborts loudly. Errors include the full lists of available columns on both sides to make the typo obvious.

**Why:** A silent fake-Delta corrupts the Deliverable in a way that's hard to spot by eye (22 rows × N columns of `None`) and propagates downstream to the DB. Failing loudly costs one obvious error message and a quick rename; failing silently costs hours of detective work after the fact.

**Consequences:**
- Schema drift on the DB side (column rename, drop) now surfaces immediately on the next Pipeline run.
- Adding a new `sync_col` requires the column to exist in both the Extractor output and the DB. This is the desired contract.
