"""
Regenerate the 44 March deliverables in the *current* snake_case header
convention, populated from today's pipeline run (so any new rows / revised
values vs. March are included).

Why this script exists:
- The DB already contains everything that was uploaded in March, so the normal
  pipeline run produces a deliverable that is DB-delta only (mostly empty).
- We need a full snapshot (all rows >= MIN_DELIVERABLE_YEAR) so the user can
  re-upload using the corrected snake_case headers and the upload script does
  not skip rows whose composite keys are already present in the DB.

Approach:
1. Backup each pipeline's existing May 2026 deliverable.
2. Monkey-patch ``etl.core.database.compare_with_postgres`` so it returns
   *every* extracted row as ``updated_df``, joined to the DB on match_cols
   so existing DB ids are preserved.
3. Run all 44 pipelines.  Each pipeline still does its own deliverable
   shaping / target_cols filtering / write_deliverable_csv call, so the
   output is in the new lowercase snake_case format.
4. Copy each freshly-written deliverable into
   ``deliverable_march_updated_format/`` with the name
   ``<pipeline_id>_March_updated_format.<ext>``.
5. Restore the original May 2026 deliverables.
"""
from __future__ import annotations

import importlib
import shutil
import sys
from pathlib import Path
from typing import Any

# Ensure the project root (parent of `scripts/`) is on sys.path so `etl.*`
# imports resolve when this script is invoked directly.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pandas as pd

PIDS: list[str] = [
    "cy_01_average_monthly_earnings",
    "cy_02_building_permits_by_district",
    "cy_03_building_permits_by_property_type",
    "cy_04_construction_index_cy",
    "cy_05_consumer_price_index",
    "cy_06_economic_forecast_cy",
    "cy_09_gross_value_added_sector",
    "cy_10_lro_contracts_of_sale",
    "cy_11_lro_transfers",
    "cy_12_monthly_gross_earnings_distribution",
    "cy_15_residential_price_indices",
    "cy_17_tourist_arrivals_country",
    "cy_18_tourist_arrivals_revenue",
    "ed_apartments_price_index_table",
    "ed_building_permits_by_no_of_rooms",
    "ed_building_permits_table",
    "ed_construction_index",
    "ed_consumer_price_index",
    "ed_economic_sentiment_indicator",
    "ed_eu_consumer_confidence_index",
    "ed_eu_unemployment_rate",
    "ed_geo_distribution_of_issued_and_pending_permits",
    "ed_gross_fixed_capital_formation",
    "ed_imports_exports_millions",
    "ed_industrial_production_index",
    "ed_loan_amounts_millions",
    "ed_loan_interest_rates",
    "ed_new_residential_buildings_work_categories",
    "ed_office_price_volume_index",
    "ed_residence_permits_aggregate",
    "ed_residence_permits_application",
    "ed_residence_permits_current",
    "ed_residence_permits_golden_visa",
    "ed_residence_permits_issued",
    "ed_residence_permits_top10_countries",
    "ed_residence_permits_top10_countries_golden_visa",
    "ed_residents_di_activity",
    "ed_residents_di_country",
    "ed_retail_price_rental_index",
    "ed_retail_trade_turnover_index",
    "ed_retail_trade_volume_index",
    "ed_tourists_arrivals_revenue",
    "ed_wage_growth_index",
    "ed_wholesale_trade_turnover_index",
]

OUTPUT_DIR = Path("deliverable_march_updated_format")
BACKUP_DIR = Path(".tmp_may_backup")
RUN_MONTH = "2026-05"


# --------------------------------------------------------------------------- #
# Step 1: backup the existing May 2026 deliverable files                        #
# --------------------------------------------------------------------------- #
def backup_may_deliverables() -> dict[str, list[str]]:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backups: dict[str, list[str]] = {}
    for pid in PIDS:
        out_dir = Path(f"etl/pipelines/{pid}/output/{RUN_MONTH}")
        if not out_dir.exists():
            continue
        per_pid_backup = BACKUP_DIR / pid
        per_pid_backup.mkdir(parents=True, exist_ok=True)
        for f in out_dir.glob("deliverable_*"):
            shutil.copy2(f, per_pid_backup / f.name)
            backups.setdefault(pid, []).append(f.name)
    return backups


def restore_may_deliverables(backups: dict[str, list[str]]) -> None:
    for pid, files in backups.items():
        out_dir = Path(f"etl/pipelines/{pid}/output/{RUN_MONTH}")
        out_dir.mkdir(parents=True, exist_ok=True)
        for fname in files:
            src = BACKUP_DIR / pid / fname
            if src.exists():
                shutil.copy2(src, out_dir / fname)
    if BACKUP_DIR.exists():
        shutil.rmtree(BACKUP_DIR)


# --------------------------------------------------------------------------- #
# Step 2: monkey-patched compare_with_postgres                                  #
# --------------------------------------------------------------------------- #
def make_full_snapshot_compare(original_get_engine):
    def full_snapshot_compare(
        df: pd.DataFrame,
        table_name: str,
        db_name: str,
        match_cols: list[str],
        sync_cols: list[str],
        tolerance: float = 0.11,
        sql_file_path: str | None = None,
    ) -> dict[str, Any]:
        """Return EVERY extracted row in ``updated_df`` (joined to DB id where
        match keys align), bypassing the diff filter."""
        # Pull DB columns we need so existing ids can be carried into the
        # deliverable (so upload script can keep them stable).
        if sql_file_path:
            try:
                with open(sql_file_path, "r", encoding="utf-8") as f:
                    query = f.read()
            except Exception as e:  # pragma: no cover
                return {"error": f"Failed to read SQL file {sql_file_path}: {e}"}
        else:
            cols_to_fetch = list(match_cols) + list(sync_cols)
            if "id" not in cols_to_fetch:
                cols_to_fetch.append("id")
            cols_str = ", ".join(cols_to_fetch)
            query = f'SELECT {cols_str} FROM "public"."{table_name}"'

        try:
            engine = original_get_engine(db_name)
            df_db = pd.read_sql(query, engine)
        except Exception as e:
            return {"error": str(e)}

        df_db.columns = [c.lower() for c in df_db.columns]
        df_local = df.copy()
        df_local.columns = [c.lower() for c in df_local.columns]

        match_cols_lower = [c.lower() for c in match_cols]
        for col in match_cols_lower:
            if col in ("year", "month", "quarter"):
                df_local[col] = (
                    pd.to_numeric(df_local[col], errors="coerce").fillna(0).astype(int)
                )
                if col in df_db.columns:
                    df_db[col] = (
                        pd.to_numeric(df_db[col], errors="coerce")
                        .fillna(0)
                        .astype(int)
                    )

        # Left-join the DB id back onto the local rows so the deliverable
        # carries existing ids (the upload script can then keep keys stable).
        if "id" in df_db.columns and all(c in df_db.columns for c in match_cols_lower):
            id_lookup = (
                df_db[match_cols_lower + ["id"]]
                .drop_duplicates(subset=match_cols_lower, keep="last")
            )
            df_local = df_local.merge(id_lookup, on=match_cols_lower, how="left")
        elif "id" not in df_local.columns:
            df_local["id"] = pd.NA

        return {
            "status": "success",
            "inserted": 0,
            "updated": len(df_local),
            "inserted_df": pd.DataFrame(),
            "updated_df": df_local,
        }

    return full_snapshot_compare


def patch_compare_globally() -> None:
    import etl.core.database as db_module

    patched = make_full_snapshot_compare(db_module.get_engine)
    db_module.compare_with_postgres = patched

    # Patch every place that imported the function by name.
    for mod_path in ["etl.core.migration_appendix_b_runner"]:
        mod = importlib.import_module(mod_path)
        if hasattr(mod, "compare_with_postgres"):
            mod.compare_with_postgres = patched

    for pid in PIDS:
        try:
            mod = importlib.import_module(f"etl.pipelines.{pid}.pipeline")
        except Exception:
            continue
        if hasattr(mod, "compare_with_postgres"):
            mod.compare_with_postgres = patched


# --------------------------------------------------------------------------- #
# Step 3 + 4: run + copy                                                        #
# --------------------------------------------------------------------------- #
def run_and_collect() -> list[dict[str, Any]]:
    from etl.core.runner import run_one

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for i, pid in enumerate(PIDS, 1):
        print(f"[{i}/{len(PIDS)}] {pid}", flush=True)
        try:
            res = run_one(pid)
        except Exception as e:  # pragma: no cover
            res = {"pipeline": pid, "status": "error", "message": str(e)}
        results.append(res)

        out_dir = Path(f"etl/pipelines/{pid}/output/{RUN_MONTH}")
        if not out_dir.exists():
            continue
        for f in out_dir.glob("deliverable_*"):
            dst = OUTPUT_DIR / f"{pid}_March_updated_format{f.suffix}"
            shutil.copy2(f, dst)
            print(f"  -> {dst}", flush=True)
    return results


def main() -> None:
    print("Step 1: backing up May 2026 deliverables...", flush=True)
    backups = backup_may_deliverables()
    try:
        print("Step 2: patching compare_with_postgres for full snapshot...", flush=True)
        patch_compare_globally()
        print("Step 3+4: running pipelines and collecting outputs...", flush=True)
        results = run_and_collect()
    finally:
        print("Step 5: restoring May 2026 deliverables...", flush=True)
        restore_may_deliverables(backups)

    print("\n--- Summary ---")
    counts: dict[str, int] = {}
    for r in results:
        counts[r.get("status", "unknown")] = counts.get(r.get("status", "unknown"), 0) + 1
    print(counts)
    files = sorted(OUTPUT_DIR.glob("*"))
    print(f"\nFiles in {OUTPUT_DIR}: {len(files)}")
    for f in files:
        print(f"  {f.name}")


if __name__ == "__main__":
    main()
