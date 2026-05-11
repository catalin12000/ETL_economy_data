# Pipeline Fix Log - 2026-01-09

## Pipeline: cy_13_new_loans_millions
### Issue: Missing Data for Specific Months (e.g., August, June-Nov 2019)
The extraction logic was failing to capture several months of data due to inconsistencies in the source Excel file (`cbc_mfs_monetary_statistics.xls`).

### Root Causes:
1.  **Restrictive Month Mapping:** The original mapping looked for exact strings like `"Aug."`. The source file intermittently used `"Aug"` (without a period), causing those rows to be skipped.
2.  **Inconsistent Year Labeling:** The source Excel only provides explicit year labels (e.g., "2019") on specific rows (usually December or June). The previous logic relied on a simple top-down carry-forward which failed when the year label was placed at the end of a sequence rather than the beginning.

### Solution - Grounding and Interpolation:
1.  **Robust Month Parsing:** Updated `get_time_series_map` to normalize month strings (lowercase, remove dots, slice first 3 characters). This ensures `"Aug"`, `"Aug."`, and `"August"` all map correctly to Month 8.
2.  **Two-Pass Year Interpolation:**
    *   **Pass 1:** Identify all rows with a valid month and any explicit year labels.
    *   **Pass 2:** Use the explicit labels as "anchors."
    *   **Interpolation:** The logic now fills in missing years by detecting sequence rollovers (e.g., if a row is Month 12 and the next is Month 1, the year is incremented). It also back-fills years from the first available anchor.

### Result:
Full data coverage for all periods, including the previously missing 2019 gaps and August data points.
