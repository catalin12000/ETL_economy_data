from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_cpi


class Pipeline:
    pipeline_id = "ed_consumer_price_index"
    display_name = "Ed Consumer Price Index"

    # Match substring to be resilient to period changes
    TARGET_TITLE_SUBSTRING = "Συγκρίσεις Γενικού Δείκτη Τιμών Καταναλωτή"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "05"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        xls_path = out_dir / "elstat_consumer_price_index.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # DKT87 is monthly
        pub_url = get_latest_publication_url(
            "DKT87",
            locale="el",
            frequency="monthly",
            headers=headers,
        )

        download_url = get_download_url_by_title(
            pub_url,
            self.TARGET_TITLE_SUBSTRING,
            headers=headers,
        )

        meta = download_file(download_url, xls_path, headers=headers)
        file_hash = sha256_file(xls_path)

        new_state = dict(state)
        new_state.update({
            "publication_url_used": pub_url,
            "download_url_used": download_url,
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": xls_path.name,
            "last_download_path": str(xls_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # Extraction and Sync
        print(f"Extracting data from {xls_path}...")
        df_new = extract_cpi(xls_path)
        
        db_path = Path("data/db") / f"{self.pipeline_id}.csv"
        out_csv = Path("data/outputs") / f"{prefix}_{self.pipeline_id}" / f"{self.pipeline_id}_updated.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with master DB {db_path}...")
        res = compare_and_update_csv(db_path, df_new, out_csv, report_csv)

        new_state.update({
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "output_csv": str(out_csv),
            "report_csv": str(report_csv),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0:
            return {"status": "skipped", "message": "No new file and no data changes.", "state": new_state}

        status = "delivered" if (res.new_rows > 0 or res.updated_cells > 0) else "verified"
        msg = f"Extracted {len(df_new)} rows. {res.new_rows} new rows, {res.updated_cells} cells updated."

        return {"status": status, "message": msg, "state": new_state}