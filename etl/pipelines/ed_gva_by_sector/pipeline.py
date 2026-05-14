from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import (
    BASE_URL,
    get_download_url_by_title,
    list_publication_years,
)
from etl.core.compare_csv import compare_and_update_csv
from etl.core.paths import PipelinePaths
from .extract import extract_gva


class Pipeline:
    pipeline_id = "ed_gva_by_sector"
    display_name = "Ed GVA By Sector - Annual"

    PUBLICATION_CODE = "SEL12"
    TARGET_TITLE_SUBSTRING = "Ακαθάριστη προστιθέμενη αξία κατά κλάδο (A64)"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "ed_gva_by_sector.xls"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }

        # 1) Walk back through years until the target file is found.
        #    The newest year may be listed in the index before the GVA workbook
        #    has been published (e.g. SEL12/2025 had no A64 file as of April 2026).
        years = list_publication_years(
            publication_code=self.PUBLICATION_CODE,
            locale="el",
            headers=headers,
        )
        if not years:
            raise RuntimeError(f"No years found for publication {self.PUBLICATION_CODE}")

        pub_url = None
        download_url = None
        last_err: Exception | None = None
        for y in years:
            candidate_url = f"{BASE_URL}/el/statistics/-/publication/{self.PUBLICATION_CODE}/{y}"
            try:
                download_url = get_download_url_by_title(
                    publication_url=candidate_url,
                    target_title=self.TARGET_TITLE_SUBSTRING,
                    headers=headers,
                )
                pub_url = candidate_url
                break
            except RuntimeError as e:
                last_err = e
                continue

        if download_url is None:
            raise RuntimeError(
                f"Could not find '{self.TARGET_TITLE_SUBSTRING}' on any year of "
                f"{self.PUBLICATION_CODE}. Last error: {last_err}"
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

        # Extraction and Sync
        print(f"Extracting data from {out_path}...")
        df_new = extract_gva(out_path)
        
        db_path = pp.baseline
        out_csv = pp.output / f"{self.pipeline_id}_updated.csv"
        report_csv = pp.output / "update_report.csv"
        
        print(f"Comparing with master DB {db_path}...")
        res = compare_and_update_csv(db_path, df_new, out_csv, report_csv, key_cols=["Year", "Industry_Code"])

        new_state.update({
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "output_csv": str(out_csv),
            "report_csv": str(report_csv),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0:
            return {
                "status": "skipped",
                "message": f"No new file and no data changes.",
                "state": new_state,
            }

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. {res.new_rows} new rows.",
            "state": new_state,
        }
