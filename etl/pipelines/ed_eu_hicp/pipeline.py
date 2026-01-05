from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_eu_hicp


class Pipeline:
    pipeline_id = "ed_eu_hicp"
    display_name = "Ed EU Harmonized Index Of Consumer Prices (Eurostat)"

    # Filter: Greece (EL), Romania (RO), Cyprus (CY), EU27 (EU27_2020), EA20
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
        
        # 2. Sync with master DB
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
            key_cols=["Year", "Month", "Geopolitical Entity"]
        )

        # 3. Create "New Entries" deliverable (Latest Month)
        output_file = output_dir / "new_entries.csv"
        
        max_year = df_new["Year"].max()
        max_month = df_new[df_new["Year"] == max_year]["Month"].max()
        
        df_deliverable = df_new[(df_new["Year"] == max_year) & (df_new["Month"] == max_month)].copy()
        
        # Format for output headers
        df_out = df_deliverable.rename(columns={
            "Geopolitical Entity": "Geopolitic",
            "Annual Rate Of Change": "Annual_rate_of_change"
        })
        
        df_out.to_csv(output_file, index=False)

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
            return {"status": "skipped", "message": "No new data.", "state": new_state}

        return {
            "status": "delivered", 
            "message": f"Extracted {len(df_new)} rows. {res.new_rows} new, {res.updated_cells} updated. Deliverable has {len(df_out)} rows.", 
            "state": new_state
        }