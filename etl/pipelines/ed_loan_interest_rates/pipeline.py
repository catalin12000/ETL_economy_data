from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres, get_engine, _normalize_match_value
from etl.core.output import write_deliverable_csv
from .extract import extract_loan_interest_rates


class Pipeline:
    pipeline_id = "ed_loan_interest_rates"
    display_name = "Housing & Consumer Loans (Interest Rates)"

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/financial-markets-and-interest-rates/"
        "bank-deposit-and-loan-interest-rates"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/Rates_TABLE_1+1a.xls"

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
    def _format_db_compare_output(df: pd.DataFrame) -> pd.DataFrame:
        target_cols = [
            'ID', 'Year', 'Month', 'Group', 'Loan_Type', 'Total_Consumer_Loans_Aprc',
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

        if df.empty:
            return pd.DataFrame(columns=target_cols)

        out = df.copy()
        rev_map = {
            "id": "ID",
            "year": "Year",
            "month": "Month",
            "group": "Group",
            "loan_type": "Loan_Type",
            "total_consumer_loans_aprc": "Total_Consumer_Loans_Aprc",
            "total_housing_loans_aprc": "Total_Housing_Loans_Aprc",
            "delta_interest_rate_deposits": "Delta_Interest_Rate_Deposits",
            "weighted_average_interest_rate_new_loans_in_euro": "Weighted_Average_Interest_Rate_New_Loans_In_Euro",
            "weighted_average_interest_rate": "Weighted_Average_Interest_Rate",
            "credit_cards": "Credit_Cards",
            "open_account_loans": "Open_Account_Loans",
            "debit_balances_on_current_accounts": "Debit_Balances_On_Current_Accounts",
            "total_interest_rate": "Total_Interest_Rate",
            "total_collateral_guarantees_interest_rates": "Total_Collateral_Guarantees_Interest_Rates",
            "total_small_medium_enterprises_interest_rates": "Total_Small_Medium_Enterprises_Interest_Rates",
            "floating_rate_1_year_fixation": "Floating_Rate_1_Year_Fixation",
            "floating_rate_1_year_rate_fixation_collateral_guarantees": "Floating_Rate_1_Year_Rate_Fixation_Collateral_Guarantees",
            "floating_rate_1_year_rate_fixation_floating_rate": "Floating_Rate_1_Year_Rate_Fixation_Floating_Rate",
            "over_1_to_5_years_rate_fixation": "Over_1_To_5_Years_Rate_Fixation",
            "over_5_years_rate_fixation": "Over_5_Years_Rate_Fixation",
            "over_5_to_10_years_rate_fixation": "Over_5_To_10_Years_Rate_Fixation",
            "over_10_years_rate_fixation": "Over_10_Years_Rate_Fixation",
            "credit_lines": "Credit_Lines",
            "debit_balances_sight_deposits": "Debit_Balances_Sight_Deposits",
        }
        out.rename(columns=rev_map, inplace=True)
        if "ID" in out.columns:
            out["ID"] = pd.to_numeric(out["ID"], errors="coerce").astype("Int64")
        for c in target_cols:
            if c not in out.columns:
                out[c] = pd.NA

        sort_cols = ["Year", "Month", "Group", "Loan_Type"]
        out = out.sort_values(sort_cols).reset_index(drop=True)
        return out[target_cols]

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

        # Internal ordering helper: keep stable ordering, but do not persist helper column.
        if "_sort_order" in df_new.columns:
            df_new = df_new.sort_values(
                ["Year", "Month", "_sort_order", "Group", "Loan Type"],
                kind="stable"
            ).drop(columns=["_sort_order"]).reset_index(drop=True)

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

        db_diff_only_file = output_dir / "db_differences_only.csv"

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

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0 and db_comp_res.get("inserted") == 0 and db_comp_res.get("updated") == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}",
            "state": new_state,
        }
