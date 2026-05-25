from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_households_loans


class Pipeline:
    pipeline_id = "cy_16_total_households_loans_millions"
    country = "cy"
    source = "central_bank_cy"
    db_table_name = "ed_total_households_loans_millions"
    source_type = "static_file"
    display_name = "Cyprus: Total Households Loans Millions (NPLs)"

    # Page listing the aggregate banking sector data
    SOURCE_PAGE = "https://www.centralbank.cy/en/licensing-supervision/banks/aggregate-cyprus-banking-sector-data"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get page to find the latest "non-performing loans" link
        print(f"Finding latest NPLs file on CBC...")
        r_page = requests.get(self.SOURCE_PAGE, headers=headers, timeout=30)
        r_page.raise_for_status()
        soup = BeautifulSoup(r_page.text, "html.parser")
        
        target_xls_url = None
        for a in soup.find_all("a", href=True):
            text_val = a.get_text().strip().lower()
            href = a["href"].lower()
            if "non-performing" in text_val and (".xls" in href or ".xlsx" in href):
                if a["href"].startswith("http"):
                    target_xls_url = a["href"]
                else:
                    target_xls_url = "https://www.centralbank.cy" + a["href"]
                break
        
        if not target_xls_url:
            return {"status": "error", "message": "Could not find 'non-performing loans' Excel link on CBC page.", "state": state}

        print(f"Found NPL file: {target_xls_url}")
        
        # Determine extension
        ext = ".xlsx" if ".xlsx" in target_xls_url.lower() else ".xls"
        out_path = out_dir / f"cbc_aggregate_npls{ext}"

        # 2) Download + hash
        meta = download_file(target_xls_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": target_xls_url,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        # 3. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_households_loans(out_path)
        
        # 4. Sync with Baseline DB (Local Reference)
        db_path = pp.baseline
        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = pp.output / "update_report.csv"
        
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path, 
            df_new, 
            out_csv_full, 
            report_csv, 
            key_cols=["Year", "Month"]
        )

        # 5. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()
        
        # Standardize columns for DB sync result return (lowercase)
        col_map = {
            "Year": "year",
            "Month": "month",
            "Total_Loans": "total_loans",
            "Total_Households_Loans": "total_households_loans",
            "Non_Performing_Loans": "non_performing_loans",
            "Loan_Amounts_Past_90_Days": "loan_amounts_past_90_days",
            "Restructured_Loans_Forbearance": "restructured_loans_forbearance",
            "Non_Performing_Restructured_Loans": "non_performing_restructured_loans"
        }
        df_for_db.rename(columns=col_map, inplace=True)
        
        sql_path = pp.sql("ed_total_households_loans_millions.sql")
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus", # CYPRUS
            match_cols=["year", "month"],
            sync_cols=[c for c in col_map.values() if c not in ["year", "month"]],
            tolerance=0.11, # Same as others
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 6. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        # 7. Timestamped Deliverable (DB Delta ONLY, 2024+)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Year', 'Month', 'Total_Loans', 'Total_Households_Loans', 
            'Non_Performing_Loans', 'Loan_Amounts_Past_90_Days', 
            'Restructured_Loans_Forbearance', 'Non_Performing_Restructured_Loans'
        ]
        
        if not delta_df.empty:
            rev_map = {v: k for k, v in col_map.items()}
            delta_df.rename(columns=rev_map, inplace=True)
            
            # Apply Filter: Only 2023 onwards
            if "Year" in delta_df.columns:
                delta_df = delta_df[delta_df["Year"] >= 2023].copy()
            
            if not delta_df.empty:
                delta_df = delta_df.sort_values(["Year", "Month"]).reset_index(drop=True)
                for c in target_cols:
                    if c not in delta_df.columns: delta_df[c] = pd.NA
                write_deliverable_csv(delta_df[target_cols], deliverable_path)
                print(f"Created filtered deliverable with {len(delta_df)} rows: {deliverable_name}")
            else:
                write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
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
            "message": f"Extracted {len(df_new)} rows. DB (zeus) Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}", 
            "state": new_state
        }
