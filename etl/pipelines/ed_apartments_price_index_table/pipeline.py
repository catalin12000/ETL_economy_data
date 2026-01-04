from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.fingerprint import dataframe_sha256
from etl.core.compare_excel import compare_and_update_excel
from etl.pipelines.ed_apartments_price_index_table.extract import extract_apartment_indices


class Pipeline:
    pipeline_id = "ed_apartments_price_index_table"
    display_name = "Ed Apartments Price Index Table"

    PDF_URL = "https://www.bankofgreece.gr/RelatedDocuments/Νέοι_Πίνακες_Τιμών_Κατοικιών_full.pdf"

    # Put your manual Excel here:
    DB_EXCEL = Path("data") / "db" / "1503 Ed Apartments Price Index November 2025 (3).xlsx"
    DB_SHEET = "Sheet1"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        out_dir = Path("data/downloads") / f"01_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = out_dir / "Neoi_Pinakes_Timon_Katoikion_full.pdf"

        meta = download_file(self.PDF_URL, pdf_path)
        file_hash = sha256_file(pdf_path)

        # 1) File freshness (bytes)
        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new file detected (same file SHA256).", "state": state}

        # 2) Extract
        df = extract_apartment_indices(pdf_path)

        latest_year = int(df["Year"].max())
        latest_q = int(df[df["Year"] == latest_year]["Quarter"].max())
        latest_period = f"{latest_year}-Q{latest_q}"

        # 3) Data freshness (actual extracted data)
        data_hash = dataframe_sha256(df, sort_cols=["Year", "Quarter", "Region"])
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
                "last_download_path": str(pdf_path),
                "latest_period_seen": latest_period,
            })
            return {
                "status": "skipped",
                "message": "File changed, but extracted data is identical (same data SHA256).",
                "state": new_state,
            }

        # 4) Compare/update -> deliverable + report
        prefix = "01"
        out_excel = Path("data/outputs") / f"{prefix}_{self.pipeline_id}" / "Ed Apartments Price Index Table.xlsx"
        out_excel.parent.mkdir(parents=True, exist_ok=True)
        out_report = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        out_report.parent.mkdir(parents=True, exist_ok=True)

        result = compare_and_update_excel(
            db_excel_path=self.DB_EXCEL,
            sheet=self.DB_SHEET,
            extracted_df=df,
            out_excel_path=out_excel,
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
            "last_download_path": str(pdf_path),
            "latest_period_seen": latest_period,
            "deliverable_path": str(out_excel),
            "update_report_csv": str(out_report),
        })

        msg = (
            f"Extracted {len(df)} rows. "
            f"Excel rows {result.rows_before} -> {result.rows_after}. "
            f"Updated cells={result.updated_cells}, New rows={result.new_rows}. "
            f"Deliverable={out_excel}"
        )
        return {"status": "delivered", "message": msg, "state": new_state}
