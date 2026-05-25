from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_services_sector_turnover


class Pipeline:
    pipeline_id = "ed_services_sector_turnover_monthly_index"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_services_sector_turnover_monthly_index"
    display_name = "Ed Services Sector Turnover Monthly Index (DKT54)"

    PUBLICATION_CODE = "DKT54"
    TARGET_TITLE_SUBSTRING = "01.Τurnover Indices in Services Sections"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_services_turnover_index.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="quarterly",
            headers=headers,
        )
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE_SUBSTRING, headers=headers)

        download_note = None
        try:
            meta = download_file(download_url, out_path, headers=headers)
        except PermissionError:
            if not out_path.exists():
                raise
            meta = {"downloaded_at_utc": state.get("downloaded_at_utc")}
            download_note = "Used existing local workbook because the source file was locked."

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "publication_code": self.PUBLICATION_CODE,
                "publication_url_used": pub_url,
                "download_url_used": download_url,
                "file_sha256": file_hash,
                "last_download_path": str(out_path),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
            }
        )
        if download_note:
            new_state["download_note"] = download_note

        # QA: extractor written without seeing the actual DKT54 file — validate column
        # positions and NACE code row detection on first successful run.
        print("Extracting services sector turnover index data...")
        df_new = extract_services_sector_turnover(out_path)

        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        report_csv = pp.output / "update_report.csv"
        db_path = pp.baseline

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Month", "Code"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Month": "month",
                "Code": "code",
                "Economic_Activity": "economic_activity",
                "Index": "index",
            }
        )
        sql_path = pp.sql("ed_services_sector_turnover_monthly_index.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "month", "economic_activity", "code"],
            sync_cols=["index"],
            tolerance=0.05,
            sql_file_path=str(sql_path),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (athena) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        db_diff_only_path = output_dir / "db_differences_only.csv"
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = ["id", "year", "month", "economic_activity", "code", "index"]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(columns={"id": "id"}).copy()
            shaped["id"] = pd.to_numeric(shaped["id"], errors="coerce")
            shaped["year"] = pd.to_numeric(shaped["year"], errors="coerce")
            shaped["month"] = pd.to_numeric(shaped["month"], errors="coerce")
            shaped = shaped.dropna(subset=["year", "month"]).copy()
            shaped["year"] = shaped["year"].astype(int)
            shaped["month"] = shaped["month"].astype(int)
            shaped = shaped.sort_values(["year", "month", "code"]).reset_index(drop=True)
            shaped["id"] = shaped["id"].map(lambda x: "" if pd.isna(x) else str(int(x)))

            if "index" in shaped.columns:
                shaped["index"] = pd.to_numeric(shaped["index"], errors="coerce").map(
                    lambda x: "" if pd.isna(x) else f"{float(x):.3f}"
                )

            for column in target_cols:
                if column not in shaped.columns:
                    shaped[column] = pd.NA

            return shaped[target_cols]

        write_deliverable_csv(shape_output(delta_df), deliverable_path)
        shape_output(updated_df).to_csv(db_diff_only_path, index=False)

        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
        }
        new_state.update(
            {
                "rows_before": res.rows_before,
                "rows_after": res.rows_after,
                "new_rows": res.new_rows,
                "updated_cells": res.updated_cells,
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
                "delta_path": str(output_file),
                "db_differences_only_path": str(db_diff_only_path),
                "mock_db_snapshot_path": str(out_csv_full),
            }
        )

        if (
            not is_new_by_hash(state.get("file_sha256"), file_hash)
            and res.new_rows == 0
            and res.updated_cells == 0
            and db_comp_res.get("inserted") == 0
            and db_comp_res.get("updated") == 0
        ):
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows. DB (athena) Comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
                + (f" {download_note}" if download_note else "")
            ),
            "state": new_state,
        }
