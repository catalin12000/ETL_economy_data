from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_employment

class Pipeline:
    pipeline_id = "ed_employment"
    display_name = "Employment Status & Unemployment Rate"

    # Match substring to be resilient to changes
    TARGET_TITLE_SUBSTRING = "Κατάσταση απασχόλησης και ποσοστό ανεργίας"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "08"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Keep original file format (ELSTAT usually serves .xls)
        out_path = out_dir / "ed_employment.xls"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }

        # 1) Resolve latest month page dynamically
        pub_url = get_latest_publication_url(
            publication_code="SJO02",
            locale="el",
            frequency="monthly",
            headers=headers,
        )

        # 2) Find the correct downloadable file by title
        download_url = get_download_url_by_title(
            publication_url=pub_url,
            target_title=self.TARGET_TITLE_SUBSTRING,
            headers=headers,
        )

        # 3) Download
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        # 4) Update state
        new_state = dict(state)
        new_state.update({
            "publication_url_used": pub_url,
            "download_url_used": download_url,
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 5. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_employment(out_path)
        
        # 6. Sync with master DB
        db_path = Path("data/db") / f"{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with master DB {db_path}...")
        res = compare_and_update_csv(
            db_path, 
            df_new, 
            out_csv_full, 
            report_csv, 
            key_cols=["Year", "Month", "Seasonally"]
        )

        # 7. Create "New Entries" deliverable (Latest Month)
        output_file = output_dir / "new_entries.csv"
        
        # Identify latest period
        max_year = df_new["Year"].max()
        max_month = df_new[df_new["Year"] == max_year]["Month"].max()
        
        df_deliverable = df_new[(df_new["Year"] == max_year) & (df_new["Month"] == max_month)].copy()
        df_deliverable.to_csv(output_file, index=False)

        new_state.update({
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "deliverable_path": str(output_file),
            "mock_db_path": str(out_csv_full),
        })

        print(f"Deliverables created in: {output_dir}")

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0:
            return {
                "status": "skipped",
                "message": "No new file and no data changes.",
                "state": new_state,
            }

        status = "delivered" if (res.new_rows > 0 or res.updated_cells > 0) else "verified"
        msg = f"Extracted {len(df_new)} rows. New entries: {len(df_deliverable)}."

        return {
            "status": status,
            "message": msg,
            "state": new_state,
        }