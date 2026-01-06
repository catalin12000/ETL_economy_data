from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_eu_hicp


class Pipeline:
    pipeline_id = "ed_eu_hicp"
    display_name = "Ed EU Harmonized Index Of Consumer Prices (Eurostat)"

    DATASET_CODE = "prc_hicp_manr"
    FILTER = "M.RCH_A.CP00.EL+RO+CY+EU27_2020+EA20"
    FILE_URL = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{DATASET_CODE}/{FILTER}/?format=SDMX-CSV&compressed=false&startPeriod=2025-01"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "48"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"eurostat_{self.DATASET_CODE}.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta = download_file(self.FILE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": self.FILE_URL,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 1. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_eu_hicp(out_path)
        
        # 2. Sync with baseline DB (Reference)
        db_path = Path("data/db") / f"{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path, 
            df_new, 
            out_csv_full, 
            report_csv, 
            key_cols=["Year", "Month", "Geopolitical Entity"]
        )

        # 3. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Requested Format: Geopolitic,Year,Month,Annual_rate_of_change
        cols_map = {
            "Geopolitical Entity": "Geopolitic",
            "Year": "Year",
            "Month": "Month",
            "Annual Rate Of Change": "Annual_rate_of_change"
        }
        
        # Snapshot (Full)
        res.updated_df[list(cols_map.keys())].rename(columns=cols_map).to_csv(out_csv_full, index=False)
        
        # New Entries
        if not res.diff_df.empty:
            df_deliverable = res.diff_df[list(cols_map.keys())].rename(columns=cols_map)
        else:
            df_deliverable = pd.DataFrame(columns=list(cols_map.values()))
            
        df_deliverable.to_csv(output_file, index=False)

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