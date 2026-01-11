from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import pandas as pd

from etl.core.download import is_new_by_hash
from etl.core.migration_source import get_latest_pdf_path, get_source_fingerprint
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from .extract import extract_loan_amounts


class Pipeline:
    pipeline_id = "ed_loan_amounts_millions"
    display_name = "Housing & Consumer Loans (Amounts) - New Business"

    SOURCE_PIPELINE_ID = "ed_loan_interest_rates"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "20"
        
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
        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"

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
            
        sql_path = Path(__file__).parent / "ed_loan_amounts_millions.sql"
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.pipeline_id,
            db_name="athena",
            match_cols=["year", "month", "group", "loan_type"],
            sync_cols=[c for c in col_map.values() if c not in ["year", "month", "group", "loan_type"]],
            tolerance=0.05,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Save snapshot
        res.updated_df.to_csv(out_csv_full, index=False)
        
        # New Entries (Local delta)
        res.diff_df.to_csv(output_file, index=False)

        # 5. Timestamped Deliverable (DB Delta ONLY)
        import datetime
        now = datetime.datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        # Use ONLY the rows that are actually missing or different in Postgres
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Year', 'Month', 'Group', 'Loan_Type', 'Total_Loan_Amount', 
            'Total_Collateral_Guarantees_Loans', 'Total_Small_Medium_Enterprises_Loans', 
            'Floating_Rate_1_Year_Fixation', 'Floating_Rate_1_Year_Rate_Fixation_Collateral_Guarantees', 
            'Floating_Rate_1_Year_Rate_Fixation_Floating_Rate', 'Over_1_To_5_Years_Rate_Fixation', 
            'Over_5_Years_Rate_Fixation', 'Over_5_To_10_Years_Rate_Fixation', 'Over_10_Years_Rate_Fixation'
        ]
        
        if not delta_df.empty:
            rev_map = {
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
            delta_df.rename(columns=rev_map, inplace=True)
            for c in target_cols:
                if c not in delta_df.columns: delta_df[c] = pd.NA
            
            delta_df[target_cols].to_csv(deliverable_path, index=False)
        else:
            pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)

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
            "mock_db_snapshot_path": str(out_csv_full),
        })

        if not is_new_by_hash(state.get("file_sha256"), src_hash) and res.new_rows == 0 and res.updated_cells == 0 and db_comp_res.get("inserted") == 0 and db_comp_res.get("updated") == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}",
            "state": new_state,
        }
