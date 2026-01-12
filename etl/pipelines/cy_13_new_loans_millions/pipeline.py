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
from .extract import extract_new_loans


class Pipeline:
    pipeline_id = "cy_13_new_loans_millions"
    display_name = "Cyprus: New Loans Millions (MFS Spreadsheet)"

    # Base URL for statistics
    ROOT_URL = "https://www.centralbank.cy/en/publications/monetary-and-financial-statistics/"
    
    # Specific DB file provided by user (Local baseline)
    DB_FILENAME = "cy_13_new_loans_millions.csv"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "13"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get root page to find the latest "Year-XXXX" link
        r_root = requests.get(self.ROOT_URL, headers=headers, timeout=30)
        r_root.raise_for_status()
        soup_root = BeautifulSoup(r_root.text, "html.parser")
        
        year_links = []
        for a in soup_root.find_all("a", href=True):
            href = a["href"]
            if "year-" in href:
                m = re.search(r"year-(\d{4})", href)
                if m:
                    year_links.append((int(m.group(1)), href))
        
        if not year_links:
            return {"status": "error", "message": "Could not find any Year pages on CBC root.", "state": state}

        latest_year, year_path = max(year_links, key=lambda x: x[0])
        latest_year_url = "https://www.centralbank.cy" + year_path
        
        # 2) Get the year page to find the latest MFS file
        r_year = requests.get(latest_year_url, headers=headers, timeout=30)
        r_year.raise_for_status()
        soup_year = BeautifulSoup(r_year.text, "html.parser")
        
        mfs_links = []
        for a in soup_year.find_all("a", href=True):
            href = a["href"]
            if "MFS_" in href and ".xls" in href:
                mfs_links.append(href)
        
        if not mfs_links:
            return {"status": "error", "message": f"Could not find any MFS Excel files on {latest_year_url}", "state": state}

        target_mfs_url = "https://www.centralbank.cy" + mfs_links[0]
        
        out_path = out_dir / "cbc_mfs_monetary_statistics.xls"

        # 3) Download + hash
        meta = download_file(target_mfs_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": target_mfs_url,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        # 4. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_new_loans(out_path)
        
        # 5. Sync with baseline DB (Local Reference)
        db_path = Path("data/db") / self.DB_FILENAME
        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"cy_{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path, 
            df_new, 
            out_csv_full, 
            report_csv, 
            key_cols=["Year", "Month"]
        )

        # 6. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()
        
        # Mapping extract columns to DB schema
        col_map = {
            "Year": "year",
            "Month": "month",
            "Housing_Pure_New_Loans": "housing_pure_new_loans",
            "Housing_Renegotiated_Loans": "housing_renegotiated_loans",
            "Housing_Floating_Rate_Up_to_1_Year_Initial_Fixation_Rate": "housing_floating_rate_up_to_1_year_initial_fixation_rate",
            "Housing_Annual_Percentage_Rate_Of_Charge": "housing_annual_percentage_rate_of_charge",
            "Outstanding_Housing_Loans_Locals": "outstanding_housing_loans_locals",
            "Outstanding_Housing_Loans_Eu": "outstanding_housing_loans_eu",
            "Outstanding_Housing_Loans_Non_Eu_Rates": "outstanding_housing_loans_non_eu",
            "Consumer_Annual_Percentage_Rate_Of_Charge": "consumer_annual_percentage_rate_of_charge",
            "Consumer_Floating_Rate_Up_to_1_Year_Initial_Fixation_Rate": "consumer_floating_rate_up_to_1_year_initial_fixation_rate",
            "Consumer_Pure_New_Loans": "consumer_pure_new_loans",
            "Consumer_Renegotiated_Loans": "consumer_renegotiated_loans",
            "Outstanding_Consumer_Loans_Locals": "outstanding_consumer_loans_locals",
            "Outstanding_Consumer_Loans_Eu": "outstanding_consumer_loans_eu",
            "Outstanding_Consumer_Loans_Non_Eu_Rates": "outstanding_consumer_loans_non_eu"
        }
        df_for_db.rename(columns=col_map, inplace=True)
        
        sql_path = Path(__file__).parent / "ed_new_loans_millions.sql"
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_new_loans_millions",
            db_name="zeus", # CYPRUS
            match_cols=["year", "month"],
            sync_cols=[c for c in col_map.values() if c not in ["year", "month"]],
            tolerance=0.05,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 7. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        # 8. Timestamped Deliverable (DB Delta ONLY)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Year', 'Month', 'Housing_Pure_New_Loans', 'Housing_Renegotiated_Loans',
            'Housing_Floating_Rate_Up_to_1_Year_Initial_Fixation_Rate',
            'Housing_Annual_Percentage_Rate_Of_Charge', 'Outstanding_Housing_Loans_Locals',
            'Outstanding_Housing_Loans_Eu', 'Outstanding_Housing_Loans_Non_Eu',
            'Consumer_Annual_Percentage_Rate_Of_Charge',
            'Consumer_Floating_Rate_Up_to_1_Year_Initial_Fixation_Rate',
            'Consumer_Pure_New_Loans', 'Consumer_Renegotiated_Loans',
            'Outstanding_Consumer_Loans_Eu', 'Outstanding_Consumer_Loans_Locals',
            'Outstanding_Consumer_Loans_Non_Eu'
        ]
        
        if not delta_df.empty:
            # Map back to Capitalized for deliverable
            rev_map = {v: k for k, v in col_map.items()}
            # Specific fixes for casing
            rev_map["year"] = "Year"
            rev_map["month"] = "Month"
            rev_map["outstanding_housing_loans_non_eu"] = "Outstanding_Housing_Loans_Non_Eu"
            rev_map["outstanding_consumer_loans_non_eu"] = "Outstanding_Consumer_Loans_Non_Eu"
            
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
            "message": f"Extracted {len(df_new)} rows. DB (zeus) Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}", 
            "state": new_state
        }
