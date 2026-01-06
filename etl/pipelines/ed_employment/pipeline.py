from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_employment

class Pipeline:
    pipeline_id = "ed_employment"
    display_name = "Employment Status & Unemployment Rate"

    TARGET_TITLE_SUBSTRING = "Κατάσταση απασχόλησης και ποσοστό ανεργίας"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "08"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "ed_employment.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url(publication_code="SJO02", locale="el", frequency="monthly", headers=headers)
        download_url = get_download_url_by_title(publication_url=pub_url, target_title=self.TARGET_TITLE_SUBSTRING, headers=headers)

        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 1. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_employment(out_path)
        
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
            key_cols=["Year", "Month", "Seasonally"]
        )

        # 3. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Format: Year,Month,Seasonally,Employed 000s,Unemployed 000s,Inactives 000s,Adjusted_Unemployment_Rate,Unadjusted_Unemployment_Rate
        cols = ["Year", "Month", "Seasonally", "Employed 000s", "Unemployed 000s", "Inactives 000s", "Adjusted_Unemployment_Rate", "Unadjusted_Unemployment_Rate"]
        
        # Snapshot (Full updated DB)
        res.updated_df[cols].to_csv(out_csv_full, index=False)
        
        # Additions/Updates only
        df_deliverable = res.diff_df[cols] if not res.diff_df.empty else pd.DataFrame(columns=cols)
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
            "state": new_state,
        }