from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.fingerprint import dataframe_sha256
from etl.core.compare_csv import compare_and_update_csv
from etl.pipelines.ed_building_permits_table.extract import extract_building_permits


class Pipeline:
    pipeline_id = "ed_building_permits_table"
    display_name = "Ed Building Permits Table"

    XLS_URL = "https://www.statistics.gr/en/statistics?p_p_id=documents_WAR_publicationsportlet_INSTANCE_Mr0GiQJSgPHd&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_cacheability=cacheLevelPage&p_p_col_id=column-2&p_p_col_count=4&p_p_col_pos=3&_documents_WAR_publicationsportlet_INSTANCE_Mr0GiQJSgPHd_javax.faces.resource=document&_documents_WAR_publicationsportlet_INSTANCE_Mr0GiQJSgPHd_ln=downloadResources&_documents_WAR_publicationsportlet_INSTANCE_Mr0GiQJSgPHd_documentID=243344&_documents_WAR_publicationsportlet_INSTANCE_Mr0GiQJSgPHd_locale=en"

    DB_CSV = Path("data") / "db" / "ed_building_permits_table.csv"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        # 1) Download
        out_dir = Path("data/downloads") / self.pipeline_id
        out_dir.mkdir(parents=True, exist_ok=True)
        xls_path = out_dir / "elstat_building_permits.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        meta = download_file(self.XLS_URL, xls_path, headers=headers)

        file_hash = sha256_file(xls_path)
        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new file detected (same file SHA256).", "state": state}

        # 2) Extract
        df = extract_building_permits(xls_path)

        latest_year = int(df["Year"].max())
        latest_month = int(df[df["Year"] == latest_year]["Month"].max())
        latest_period = f"{latest_year}-{latest_month:02d}"

        # 3) Data hash
        data_hash = dataframe_sha256(df, sort_cols=["Year", "Month"])
        if state.get("data_sha256") == data_hash:
            new_state = dict(state)
            new_state.update({
                "file_sha256": file_hash,
                "data_sha256": data_hash,
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "content_length": meta.get("content_length"),
                "final_url": meta.get("final_url"),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
                "last_download_path": str(xls_path),
                "latest_period_seen": latest_period,
            })
            return {
                "status": "skipped",
                "message": "File changed, but extracted data is identical (same data SHA256).",
                "state": new_state,
            }

        # 4) Compare + update DB CSV -> deliverable + report
        out_csv = Path("data/outputs") / self.pipeline_id / "Ed Building Permits Table.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        out_report = Path("data/reports") / self.pipeline_id / "update_report.csv"
        out_report.parent.mkdir(parents=True, exist_ok=True)

        result = compare_and_update_csv(
            db_csv_path=self.DB_CSV,
            extracted_df=df,
            out_csv_path=out_csv,
            report_csv_path=out_report,
            prevent_older_than_db=True,
        )

        # 5) Update state
        new_state = dict(state)
        new_state.update({
            "file_sha256": file_hash,
            "data_sha256": data_hash,
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
            "last_download_path": str(xls_path),
            "latest_period_seen": latest_period,
            "deliverable_path": str(out_csv),
            "update_report_csv": str(out_report),
        })

        msg = (
            f"Extracted {len(df)} rows. "
            f"CSV rows {result.rows_before} -> {result.rows_after}. "
            f"Updated cells={result.updated_cells}, New rows={result.new_rows}. "
            f"Deliverable={out_csv}"
        )
        return {"status": "delivered", "message": msg, "state": new_state}
