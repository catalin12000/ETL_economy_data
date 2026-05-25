from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres, get_engine, _normalize_match_value
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_loan_interest_rates


class Pipeline:
    pipeline_id = "ed_loan_interest_rates"
    country = "gr"
    source = "bank_of_greece"
    db_table_name = "ed_loan_interest_rates"
    display_name = "Housing & Consumer Loans (Interest Rates)"

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/financial-markets-and-interest-rates/"
        "bank-deposit-and-loan-interest-rates"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/Rates_TABLE_1+1a.xls"

    @staticmethod
    def _format_db_compare_output(df: pd.DataFrame) -> pd.DataFrame:
        target_cols = [
            "id", "year", "month", "group", "loan_type",
            "total_consumer_loans_aprc", "total_housing_loans_aprc",
            "delta_interest_rate_deposits",
            "weighted_average_interest_rate_new_loans_in_euro",
            "weighted_average_interest_rate", "credit_cards", "open_account_loans",
            "debit_balances_on_current_accounts", "total_interest_rate",
            "total_collateral_guarantees_interest_rates",
            "total_small_medium_enterprises_interest_rates",
            "floating_rate_1_year_fixation",
            "floating_rate_1_year_rate_fixation_collateral_guarantees",
            "floating_rate_1_year_rate_fixation_floating_rate",
            "over_1_to_5_years_rate_fixation", "over_5_years_rate_fixation",
            "over_5_to_10_years_rate_fixation", "over_10_years_rate_fixation",
            "credit_lines", "debit_balances_sight_deposits",
        ]

        if df.empty:
            return pd.DataFrame(columns=target_cols)

        out = df.copy()
        if "id" in out.columns:
            out["id"] = pd.to_numeric(out["id"], errors="coerce").astype("Int64")
        out["year"] = pd.to_numeric(out["year"], errors="coerce")
        out["month"] = pd.to_numeric(out["month"], errors="coerce")
        out = out.dropna(subset=["year", "month"]).copy()
        out["year"] = out["year"].astype(int)
        out["month"] = out["month"].astype(int)
        out = out.sort_values(["year", "month", "group", "loan_type"]).reset_index(drop=True)

        for c in target_cols:
            if c not in out.columns:
                out[c] = pd.NA

        return out[target_cols]

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
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

        # Internal ordering helper: keep stable ordering, but do not persist helper column.
        if "_sort_order" in df_new.columns:
            df_new = df_new.sort_values(
                ["Year", "Month", "_sort_order", "Group", "Loan Type"],
                kind="stable"
            ).drop(columns=["_sort_order"]).reset_index(drop=True)

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
            
        sql_path = pp.sql("ed_loan_interest_rates.sql")
        
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

        # 4. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Save snapshot
        res.updated_df.to_csv(out_csv_full, index=False)
        
        # New Entries (Local delta)
        res.diff_df.to_csv(output_file, index=False)

        db_diff_only_file = output_dir / "db_differences_only.csv"

        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        db_diff_only_df = self._format_db_compare_output(updated_df)
        db_diff_only_df.to_csv(db_diff_only_file, index=False)

        # 5. Timestamped Deliverable (DB delta only: missing + different)
        now = datetime.now()
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

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0 and db_comp_res.get("inserted") == 0 and db_comp_res.get("updated") == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}",
            "state": new_state,
        }
