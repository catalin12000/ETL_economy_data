from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import pandas as pd

from etl.core.download import is_new_by_hash
from etl.core.migration_source import get_latest_pdf_path, get_source_fingerprint
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres, get_engine, _normalize_match_value
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_loan_amounts


class Pipeline:
    pipeline_id = "ed_loan_amounts_millions"
    country = "gr"
    source = "bank_of_greece"
    db_table_name = "ed_loan_amounts_millions"
    display_name = "Housing & Consumer Loans (Amounts) - New Business"

    SOURCE_PIPELINE_ID = "ed_loan_interest_rates"

    @staticmethod
    def _build_full_replace_df(df_for_db: pd.DataFrame, sql_path: Path) -> pd.DataFrame:
        """
        Build a full replacement deliverable:
        - keep all extracted rows
        - attach DB id when keys match
        - use DB key labels for group/loan_type when matched
        """
        match_cols = ["year", "month", "group", "loan_type"]
        df_local = df_for_db.copy()

        query = sql_path.read_text(encoding="utf-8")
        df_db = pd.read_sql(query, get_engine("athena"))
        df_db.columns = [c.lower() for c in df_db.columns]
        df_local.columns = [c.lower() for c in df_local.columns]

        for col in match_cols:
            if col in {"year", "month", "quarter"}:
                df_local[col] = pd.to_numeric(df_local[col], errors="coerce").fillna(0).astype(int)
                df_db[col] = pd.to_numeric(df_db[col], errors="coerce").fillna(0).astype(int)
            else:
                df_local[f"{col}_norm"] = df_local[col].apply(lambda x: _normalize_match_value(col, x))
                df_db[f"{col}_norm"] = df_db[col].apply(lambda x: _normalize_match_value(col, x))

        norm_match_cols = [
            c if c in {"year", "month", "quarter"} else f"{c}_norm"
            for c in match_cols
        ]

        lookup_cols = norm_match_cols + ["id", "group", "loan_type"]
        df_lookup = df_db[lookup_cols].copy()
        df_lookup = df_lookup.sort_values("id", na_position="last").drop_duplicates(norm_match_cols, keep="first")

        merged = df_local.merge(df_lookup, on=norm_match_cols, how="left", suffixes=("", "_db"))
        merged["group"] = merged["group_db"].combine_first(merged["group"])
        merged["loan_type"] = merged["loan_type_db"].combine_first(merged["loan_type"])
        merged["id"] = pd.to_numeric(merged["id"], errors="coerce").astype("Int64")

        drop_cols = [f"{c}_norm" for c in match_cols if c not in {"year", "month", "quarter"}]
        drop_cols += ["group_db", "loan_type_db"]
        merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns], errors="ignore")
        return merged

    @staticmethod
    def _attach_ids_to_local_output(local_df: pd.DataFrame, full_replace_df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill local compare outputs with DB ids when a business-key match exists.
        This keeps `new_entries.csv` / `mock_db_snapshot.csv` aligned with the
        DB-backed deliverable rather than leaving ids blank for post-baseline rows.
        """
        if local_df.empty:
            return local_df

        out = local_df.copy()
        if "id" in out.columns:
            existing_id = pd.to_numeric(out["id"], errors="coerce").astype("Int64")
        else:
            existing_id = pd.Series(pd.NA, index=out.index, dtype="Int64")

        lookup = full_replace_df.copy()
        if lookup.empty or "id" not in lookup.columns:
            if "id" not in out.columns:
                insert_at = out.columns.get_loc("Loan Type") + 1 if "Loan Type" in out.columns else len(out.columns)
                out.insert(insert_at, "id", existing_id)
            else:
                out["id"] = existing_id
            return out

        local_key_map = {
            "Year": "year",
            "Month": "month",
            "Group": "group",
            "Loan Type": "loan_type",
        }

        local_keys = out[list(local_key_map.keys())].copy()
        ref_keys = lookup[list(local_key_map.values()) + ["id"]].copy()

        local_keys["year"] = pd.to_numeric(local_keys["Year"], errors="coerce").fillna(0).astype(int)
        local_keys["month"] = pd.to_numeric(local_keys["Month"], errors="coerce").fillna(0).astype(int)
        ref_keys["year"] = pd.to_numeric(ref_keys["year"], errors="coerce").fillna(0).astype(int)
        ref_keys["month"] = pd.to_numeric(ref_keys["month"], errors="coerce").fillna(0).astype(int)

        for local_col, ref_col in [("Group", "group"), ("Loan Type", "loan_type")]:
            local_keys[f"{ref_col}_norm"] = local_keys[local_col].apply(lambda x: _normalize_match_value(ref_col, x))
            ref_keys[f"{ref_col}_norm"] = ref_keys[ref_col].apply(lambda x: _normalize_match_value(ref_col, x))

        ref_lookup = (
            ref_keys[["year", "month", "group_norm", "loan_type_norm", "id"]]
            .sort_values("id", na_position="last")
            .drop_duplicates(["year", "month", "group_norm", "loan_type_norm"], keep="first")
        )

        matched = local_keys.merge(
            ref_lookup,
            on=["year", "month", "group_norm", "loan_type_norm"],
            how="left",
        )
        matched_id = pd.to_numeric(matched["id"], errors="coerce").astype("Int64")
        final_id = existing_id.combine_first(matched_id)

        if "id" not in out.columns:
            out.insert(0, "id", final_id)
        else:
            out["id"] = final_id
            cols = ["id"] + [c for c in out.columns if c != "id"]
            out = out[cols]

        return out

    @staticmethod
    def _format_db_compare_output(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert DB-compare output frames (`inserted_df` / `updated_df`) to the
        public CSV column layout used by this pipeline.
        """
        target_cols = [
            'id', 'Year', 'Month', 'Group', 'Loan_Type', 'Total_Loan_Amount',
            'Total_Collateral_Guarantees_Loans', 'Total_Small_Medium_Enterprises_Loans',
            'Floating_Rate_1_Year_Fixation', 'Floating_Rate_1_Year_Rate_Fixation_Collateral_Guarantees',
            'Floating_Rate_1_Year_Rate_Fixation_Floating_Rate', 'Over_1_To_5_Years_Rate_Fixation',
            'Over_5_Years_Rate_Fixation', 'Over_5_To_10_Years_Rate_Fixation', 'Over_10_Years_Rate_Fixation'
        ]

        if df.empty:
            return pd.DataFrame(columns=target_cols)

        out = df.copy()
        rev_map = {
            "id": "id",
            "year": "Year", "month": "Month", "group": "Group", "loan_type": "Loan_Type",
            "total_loan_amount": "Total_Loan_Amount",
            "total_collateral_guarantees_loans": "Total_Collateral_Guarantees_Loans",
            "total_small_medium_enterprises_loans": "Total_Small_Medium_Enterprises_Loans",
            "floating_rate_1_year_fixation": "Floating_Rate_1_Year_Fixation",
            "floating_rate_1_year_rate_fixation_collateral_guarantees": "Floating_Rate_1_Year_Rate_Fixation_Collateral_Guarantees",
            "floating_rate_1_year_rate_fixation_floating_rate": "Floating_Rate_1_Year_Rate_Fixation_Floating_Rate",
            "over_1_to_5_years_rate_fixation": "Over_1_To_5_Years_Rate_Fixation",
            "over_5_years_rate_fixation": "Over_5_Years_Rate_Fixation",
            "over_5_to_10_years_rate_fixation": "Over_5_To_10_Years_Rate_Fixation",
            "over_10_years_rate_fixation": "Over_10_Years_Rate_Fixation"
        }
        out.rename(columns=rev_map, inplace=True)
        if "id" in out.columns:
            out["id"] = pd.to_numeric(out["id"], errors="coerce").astype("Int64")
        for c in target_cols:
            if c not in out.columns:
                out[c] = pd.NA
        sort_cols = ["Year", "Month", "Group", "Loan_Type"]
        out = out.sort_values(sort_cols).reset_index(drop=True)
        return out[target_cols]

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        # Reuse path and hash from the master download pipeline
        try:
            xls_path = get_latest_pdf_path(self.SOURCE_PIPELINE_ID)
            src_hash, _ = get_source_fingerprint(self.SOURCE_PIPELINE_ID)
        except Exception as e:
            return {"status": "error", "message": f"Dependency error: {str(e)}", "state": state}

        new_state = dict(state)
        new_state.update({
            "file_sha256": src_hash,
            "last_download_path": str(xls_path),
        })

        # 1. Extraction
        print(f"Extracting Loan Amounts from {xls_path}...")
        try:
            df_new = extract_loan_amounts(xls_path)
        except Exception as e:
            return {
                "status": "error",
                "message": f"Extraction failed: {str(e)}",
                "state": new_state,
            }

        # 2. Sync with Baseline DB (Local Reference)
        db_path = pp.baseline
        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = pp.output / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Month", "Group", "Loan Type"]
        )

        # 3. DB Comparison (READ-ONLY)
        print("Comparing extraction with live Postgres DB...")
        df_for_db = df_new.copy()
        col_map = {
            "Year": "year",
            "Month": "month",
            "Group": "group",
            "Loan Type": "loan_type",
            "Total Loan Amount": "total_loan_amount",
            "Total Collateral Guarantees Loans": "total_collateral_guarantees_loans",
            "Total Small Medium Enterprises Loans": "total_small_medium_enterprises_loans",
            "Floating Rate 1 Year Fixation": "floating_rate_1_year_fixation",
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": "floating_rate_1_year_rate_fixation_collateral_guarantees",
            "Floating Rate 1 Year Rate Fixation Floating Rate": "floating_rate_1_year_rate_fixation_floating_rate",
            "Over 1 To 5 Years Rate Fixation": "over_1_to_5_years_rate_fixation",
            "Over 5 Years Rate Fixation": "over_5_years_rate_fixation",
            "Over 5 To 10 Years Rate Fixation": "over_5_to_10_years_rate_fixation",
            "Over 10 Years Rate Fixation": "over_10_years_rate_fixation"
        }
        df_for_db.rename(columns=col_map, inplace=True)
        if "_sort_order" in df_for_db.columns:
            df_for_db.drop(columns=["_sort_order"], inplace=True)
            
        sql_path = pp.sql("ed_loan_amounts_millions.sql")
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "month", "group", "loan_type"],
            sync_cols=[c for c in col_map.values() if c not in ["year", "month", "group", "loan_type"]],
            tolerance=0.05,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        full_replace_df = self._build_full_replace_df(df_for_db=df_for_db, sql_path=sql_path)
        db_diff_only_file = output_dir / "db_differences_only.csv"

        # 4. Create Deliverables
        output_file = output_dir / "new_entries.csv"

        enriched_snapshot_df = self._attach_ids_to_local_output(res.updated_df, full_replace_df)
        enriched_diff_df = self._attach_ids_to_local_output(res.diff_df, full_replace_df)

        for df_local_output in (enriched_snapshot_df, enriched_diff_df):
            if "_sort_order" in df_local_output.columns:
                df_local_output.drop(columns=["_sort_order"], inplace=True)

        # Save snapshot
        enriched_snapshot_df.to_csv(out_csv_full, index=False)

        # New Entries (Local delta)
        enriched_diff_df.to_csv(output_file, index=False)

        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        db_diff_only_df = self._format_db_compare_output(updated_df)
        db_diff_only_df.to_csv(db_diff_only_file, index=False)

        # 5. Timestamped Deliverable (DB delta only: missing + different)
        import datetime
        now = datetime.datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        delta_db_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        deliverable_df = self._format_db_compare_output(delta_db_df)
        write_deliverable_csv(deliverable_df, deliverable_path)
        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated")
        }

        new_state.update({
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "db_comparison": db_summary,
            "deliverable_path": str(deliverable_path),
            "delta_path": str(output_file),
            "db_differences_only_path": str(db_diff_only_file),
            "mock_db_snapshot_path": str(out_csv_full),
        })

        if not is_new_by_hash(state.get("file_sha256"), src_hash) and res.new_rows == 0 and res.updated_cells == 0 and db_comp_res.get("inserted") == 0 and db_comp_res.get("updated") == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}",
            "state": new_state,
        }
