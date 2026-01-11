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
from .extract import extract_new_loans


class Pipeline:
    pipeline_id = "cy_13_new_loans_millions"
    display_name = "Cyprus: New Loans Millions (MFS Spreadsheet)"

    # Base URL for statistics
    ROOT_URL = "https://www.centralbank.cy/en/publications/monetary-and-financial-statistics/"
    
    # Specific DB file provided by user
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
        
        # 5. Sync with baseline DB (Reference)
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

        # 6. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Snapshot (Full)
        res.updated_df.to_csv(out_csv_full, index=False)
        
        # New Entries
        res.diff_df.to_csv(output_file, index=False)

        new_state.update({
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "deliverable_path": str(output_file),
            "mock_db_snapshot_path": str(out_csv_full),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered", 
            "message": f"Extracted {len(df_new)} rows. {res.new_rows} new, {res.updated_cells} updates. Deliverables generated.", 
            "state": new_state
        }