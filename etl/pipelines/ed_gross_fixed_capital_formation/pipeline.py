from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.compare_csv import compare_and_update_csv
from .extract import extract_gfcf


class Pipeline:
    pipeline_id = "ed_gross_fixed_capital_formation"
    display_name = "Ed Gross Fixed Capital Formation (SEL81) - Quarterly"

    # SEL81: Gross Fixed Capital Formation (Chain-linked volumes)
    PUBLICATION_CODE = "SEL81"

    # Match the core title while ignoring changing periods in parentheses
    TARGET_TITLE_SUBSTRING = "Quarterly Gross fixed capital formation by Asset"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "13"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "elstat_gross_fixed_capital_formation.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest quarterly page dynamically
        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="quarterly",
            headers=headers,
        )

        # 2) Find download link by title
        download_url = get_download_url_by_title(
            pub_url,
            self.TARGET_TITLE_SUBSTRING,
            headers=headers,
        )

        # 3) Download + hash
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "publication_code": self.PUBLICATION_CODE,
            "publication_url_used": pub_url,
            "download_url_used": download_url,
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # Extraction and Sync
        print(f"Extracting data from {out_path}...")
        df_new = extract_gfcf(out_path)
        
        db_path = Path("data/db") / f"{self.pipeline_id}.csv"
        out_csv = Path("data/outputs") / f"{prefix}_{self.pipeline_id}" / f"{self.pipeline_id}_updated.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with master DB {db_path}...")
        res = compare_and_update_csv(db_path, df_new, out_csv, report_csv, key_cols=["Year", "Quarter"])

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
