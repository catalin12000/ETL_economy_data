from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from .extract import extract_loan_interest_rates


class Pipeline:
    pipeline_id = "ed_loan_interest_rates"
    display_name = "Housing & Consumer Loans (Interest Rates)"

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/financial-markets-and-interest-rates/"
        "bank-deposit-and-loan-interest-rates"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/Rates_TABLE_1+1a.xls"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "21"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "Rates_TABLE_1+1a_v2.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta = download_file(self.FILE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_page": self.SOURCE_PAGE,
            "source_url_used": self.FILE_URL,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 1. Extraction
        print(f"Extracting Loan Rates from {out_path}...")
        try:
            df_new = extract_loan_interest_rates(out_path)
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
            "Total Consumer Loans Aprc": "total_consumer_loans_aprc",
            "Total Housing Loans Aprc": "total_housing_loans_aprc",
            "Delta Interest Rate Deposits": "delta_interest_rate_deposits",
            "Weighted Average Interest Rate New Loans In Euro": "weighted_average_interest_rate_new_loans_in_euro",
            "Weighted Average Interest Rate": "weighted_average_interest_rate",
            "Credit Cards": "credit_cards",
            "Open Account Loans": "open_account_loans",
            "Debit Balances On Current Accounts": "debit_balances_on_current_accounts",
            "Total Interest Rate": "total_interest_rate",
            "Total Collateral Guarantees Interest Rates": "total_collateral_guarantees_interest_rates",
            "Total Small Medium Enterprises Interest Rates": "total_small_medium_enterprises_interest_rates",
            "Floating Rate 1 Year Fixation": "floating_rate_1_year_fixation",
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": "floating_rate_1_year_rate_fixation_collateral_guarantees",
            "Floating Rate 1 Year Rate Fixation Floating Rate": "floating_rate_1_year_rate_fixation_floating_rate",
            "Over 1 To 5 Years Rate Fixation": "over_1_to_5_years_rate_fixation",
            "Over 5 Years Rate Fixation": "over_5_years_rate_fixation",
            "Over 5 To 10 Years Rate Fixation": "over_5_to_10_years_rate_fixation",
            "Over 10 Years Rate Fixation": "over_10_years_rate_fixation",
            "Credit Lines": "credit_lines",
            "Debit Balances Sight Deposits": "debit_balances_sight_deposits"
        }
        df_for_db.rename(columns=col_map, inplace=True)
        if "_sort_order" in df_for_db.columns:
            df_for_db.drop(columns=["_sort_order"], inplace=True)
            
        sql_path = Path(__file__).parent / "ed_loan_interest_rates.sql"
        
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
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Year', 'Month', 'Group', 'Loan_Type', 'Total_Consumer_Loans_Aprc', 
            'Total_Housing_Loans_Aprc', 'Delta_Interest_Rate_Deposits', 
            'Weighted_Average_Interest_Rate_New_Loans_In_Euro', 'Weighted_Average_Interest_Rate', 
            'Credit_Cards', 'Open_Account_Loans', 'Debit_Balances_On_Current_Accounts', 
            'Total_Interest_Rate', 'Total_Collateral_Guarantees_Interest_Rates', 
            'Total_Small_Medium_Enterprises_Interest_Rates', 'Floating_Rate_1_Year_Fixation', 
            'Floating_Rate_1_Year_Rate_Fixation_Collateral_Guarantees', 
            'Floating_Rate_1_Year_Rate_Fixation_Floating_Rate', 'Over_1_To_5_Years_Rate_Fixation', 
            'Over_5_Years_Rate_Fixation', 'Over_5_To_10_Years_Rate_Fixation', 'Over_10_Years_Rate_Fixation', 
            'Credit_Lines', 'Debit_Balances_Sight_Deposits'
        ]
        
        if not delta_df.empty:
            rev_map = {k: k.replace(" ", "_").title() for k in col_map.values()}
            # Specific fixes for case consistency
            rev_map["year"] = "Year"
            rev_map["month"] = "Month"
            rev_map["group"] = "Group"
            rev_map["loan_type"] = "Loan_Type"
            
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

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0 and db_comp_res.get("inserted") == 0 and db_comp_res.get("updated") == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}",
            "state": new_state,
        }