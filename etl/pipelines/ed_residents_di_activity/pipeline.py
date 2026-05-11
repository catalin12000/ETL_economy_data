from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.output import write_deliverable_csv
from .extract import extract_residents_di_activity


class Pipeline:
    pipeline_id = "ed_residents_di_activity"
    display_name = "BoG FDI Flows - Residents by Activity"
    MIN_DB_YEAR = 2020

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/external-sector/"
        "direct-investment/direct-investment---flows"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/BPM6_FDI_ABROAD_BY_ACTIVITY.xls"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "36"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "BPM6_FDI_ABROAD_BY_ACTIVITY.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        download_note = None
        try:
            meta = download_file(self.FILE_URL, out_path, headers=headers)
        except PermissionError:
            if not out_path.exists():
                raise
            meta = {
                "last_modified": state.get("last_modified"),
                "etag": state.get("etag"),
                "content_length": state.get("content_length"),
                "final_url": state.get("final_url") or self.FILE_URL,
                "downloaded_at_utc": state.get("downloaded_at_utc"),
            }
            download_note = "Used existing local workbook because the source file was locked."
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "source_page": self.SOURCE_PAGE,
                "source_url_used": self.FILE_URL,
                "file_sha256": file_hash,
                "downloaded_filename": out_path.name,
                "last_download_path": str(out_path),
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "content_length": meta.get("content_length"),
                "final_url": meta.get("final_url"),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
            }
        )
        if download_note:
            new_state["download_note"] = download_note

        print("Extracting residents DI by activity data...")
        df_all = extract_residents_di_activity(out_path)
        df_new = df_all[pd.to_numeric(df_all["Year"], errors="coerce") >= self.MIN_DB_YEAR].copy()
        if df_new.empty:
            return {
                "status": "error",
                "message": f"No rows found for Year >= {self.MIN_DB_YEAR}.",
                "state": new_state,
            }

        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path,
            df_new,
            out_csv_full,
            report_csv,
            key_cols=["Year", "Subsection Code"],
        )

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Section Code": "section_code",
                "Section Name": "section_name",
                "Subsection Code": "subsection_code",
                "Subsection Name": "subsection_name",
                "Amount Millions": "amount_millions",
            }
        )
        sql_path = Path(__file__).parent / "ed_residents_di_by_activity.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_residents_di_by_activity",
            db_name="athena",
            match_cols=["year", "subsection_code"],
            sync_cols=["section_code", "section_name", "subsection_name", "amount_millions"],
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )

        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (athena) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        output_file = output_dir / "new_entries.csv"
        db_diff_only_path = output_dir / "db_differences_only.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "ID",
            "Year",
            "Section Code",
            "Section Name",
            "Subsection Code",
            "Subsection Name",
            "Amount Millions",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(
                columns={
                    "id": "ID",
                    "year": "Year",
                    "section_code": "Section Code",
                    "section_name": "Section Name",
                    "subsection_code": "Subsection Code",
                    "subsection_name": "Subsection Name",
                    "amount_millions": "Amount Millions",
                }
            ).copy()
            shaped["Year"] = pd.to_numeric(shaped["Year"], errors="coerce")
            shaped["Amount Millions"] = pd.to_numeric(shaped["Amount Millions"], errors="coerce")
            shaped["ID"] = pd.to_numeric(shaped["ID"], errors="coerce")
            shaped = shaped.dropna(subset=["Year", "Subsection Code"]).copy()
            shaped["Year"] = shaped["Year"].astype(int)
            shaped = shaped.sort_values(
                ["Year", "Section Code", "Subsection Code"],
                ascending=[False, True, True],
            ).reset_index(drop=True)
            shaped["ID"] = shaped["ID"].map(lambda x: "" if pd.isna(x) else str(int(x)))
            shaped["Amount Millions"] = shaped["Amount Millions"].map(
                lambda x: "" if pd.isna(x) else f"{float(x):.2f}"
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
