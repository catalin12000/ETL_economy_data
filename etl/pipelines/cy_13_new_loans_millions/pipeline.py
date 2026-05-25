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
from .extract import extract_new_loans


class Pipeline:
    pipeline_id = "cy_13_new_loans_millions"
    country = "cy"
    source = "central_bank_cy"
    db_table_name = "ed_new_loans_millions"
    source_type = "static_file"
    display_name = "Cyprus: New Loans Millions (MFS Spreadsheet)"

    # Base URL for statistics
    ROOT_URL = "https://www.centralbank.cy/en/publications/monetary-and-financial-statistics/"
    
    # Specific DB file provided by user (Local baseline)
    DB_FILENAME = "cy_13_new_loans_millions.csv"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
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

        MONTH_ORDER = [
            "december", "november", "october", "september", "august", "july",
            "june", "may", "april", "march", "february", "january"
        ]

        year_links = sorted(year_links, key=lambda x: x[0], reverse=True)
        
        target_mfs_url = None
        for year_val, year_path in year_links:
            year_url = "https://www.centralbank.cy" + year_path
            print(f"Checking year page: {year_url}")
            
            r_year = requests.get(year_url, headers=headers, timeout=30)
            r_year.raise_for_status()
            soup_year = BeautifulSoup(r_year.text, "html.parser")
            
            # 2.1) Check for intermediate month pages ("Learn More" buttons)
            month_links = []
            for a in soup_year.find_all("a", href=True):
                href = a["href"]
                # Look for links ending in month names
                for m in MONTH_ORDER:
                    if href.lower().endswith(f"/{m}"):
                        month_links.append((m, href))
                        break
            
            if month_links:
                # Sort month links by our MONTH_ORDER
                month_links.sort(key=lambda x: MONTH_ORDER.index(x[0].lower()))
                
                for m_name, m_path in month_links:
                    month_url = "https://www.centralbank.cy" + m_path
                    print(f"  Checking month page: {month_url}")
                    
                    r_month = requests.get(month_url, headers=headers, timeout=30)
                    r_month.raise_for_status()
                    soup_month = BeautifulSoup(r_month.text, "html.parser")
                    
                    mfs_links = []
                    for a in soup_month.find_all("a", href=True):
                        href = a["href"]
                        if "MFS" in href.upper() and (".xls" in href.lower() or ".xlsx" in href.lower()):
                            mfs_links.append(href)
                    
                    if mfs_links:
                        target_mfs_url = "https://www.centralbank.cy" + mfs_links[0]
                        print(f"Found latest MFS file on month page: {target_mfs_url}")
                        break
            
            # 2.2) Fallback: Check if file links are directly on the Year page (old structure)
            if not target_mfs_url:
                mfs_links = []
                for a in soup_year.find_all("a", href=True):
                    href = a["href"]
                    if "MFS" in href.upper() and (".xls" in href.lower() or ".xlsx" in href.lower()):
                        mfs_links.append(href)
                
                if mfs_links:
                    target_mfs_url = "https://www.centralbank.cy" + mfs_links[0]
                    print(f"Found latest MFS file on year page: {target_mfs_url}")
            
            if target_mfs_url:
                break
        
        if not target_mfs_url:
            return {"status": "error", "message": "Could not find any MFS Excel files on any Year or Month pages.", "state": state}
        
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
            key_cols=["year", "month"]
        )

        # 6. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()
        df_for_db = df_for_db.rename(columns={
            "outstanding_housing_loans_non_eu_rates": "outstanding_housing_loans_non_eu",
            "outstanding_consumer_loans_non_eu_rates": "outstanding_consumer_loans_non_eu",
        })

        sync_cols = [
            "housing_pure_new_loans",
            "housing_renegotiated_loans",
            "housing_floating_rate_up_to_1_year_initial_fixation_rate",
            "housing_annual_percentage_rate_of_charge",
            "outstanding_housing_loans_locals",
            "outstanding_housing_loans_eu",
            "outstanding_housing_loans_non_eu",
            "consumer_annual_percentage_rate_of_charge",
            "consumer_floating_rate_up_to_1_year_initial_fixation_rate",
            "consumer_pure_new_loans",
            "consumer_renegotiated_loans",
            "outstanding_consumer_loans_locals",
            "outstanding_consumer_loans_eu",
            "outstanding_consumer_loans_non_eu",
        ]

        sql_path = pp.sql("ed_new_loans_millions.sql")

        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year", "month"],
            sync_cols=sync_cols,
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
        
        # Use ONLY the rows that were actually pushed to Postgres
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'id', 'year', 'month',
            'consumer_floating_rate_up_to_1_year_initial_fixation_rate',
            'housing_floating_rate_up_to_1_year_initial_fixation_rate',
            'consumer_annual_percentage_rate_of_charge',
            'housing_annual_percentage_rate_of_charge',
            'consumer_pure_new_loans',
            'consumer_renegotiated_loans',
            'housing_pure_new_loans',
            'housing_renegotiated_loans',
            'outstanding_consumer_loans_locals',
            'outstanding_housing_loans_locals',
            'outstanding_consumer_loans_eu',
            'outstanding_housing_loans_eu',
            'outstanding_consumer_loans_non_eu',
            'outstanding_housing_loans_non_eu',
        ]

        if not delta_df.empty:
            if "id" in delta_df.columns:
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce").astype("Int64")

            # Apply Filter: Only 2024 onwards
            if "year" in delta_df.columns:
                delta_df = delta_df[delta_df["year"] >= 2024].copy()

            if not delta_df.empty:
                delta_df = delta_df.sort_values(["year", "month"]).reset_index(drop=True)

                for c in target_cols:
                    if c not in delta_df.columns: delta_df[c] = pd.NA
                write_deliverable_csv(delta_df[target_cols], deliverable_path)
                print(f"Created filtered deliverable with {len(delta_df)} rows: {deliverable_name}")
            else:
                write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
                print("No data from 2024+ found in delta. Deliverable is empty.")
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
            print("No changes detected in DB. Deliverable is empty.")

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
