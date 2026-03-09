from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from .extract import extract_fdi_activity


class Pipeline:
    pipeline_id = "ed_fdi_activity"
    display_name = "BoG FDI Flows - Home by Activity"
    MIN_DB_YEAR = 2020

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/external-sector/"
        "direct-investment/direct-investment---flows"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/BPM6_FDI_HOME_BY_ACTIVITY.xls"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "09"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "BPM6_FDI_HOME_BY_ACTIVITY.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        meta = download_file(self.FILE_URL, out_path, headers=headers)
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

        print("Extracting FDI activity data...")
        df_all = extract_fdi_activity(out_path)
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
                "Amount": "amount",
            }
        )
        sql_path = Path(__file__).parent / "ed_fdi_activity.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_fdi_activity",
            db_name="athena",
            match_cols=["year", "subsection_code"],
            sync_cols=["section_code", "section_name", "subsection_name", "amount"],
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
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "Year",
            "Section Code",
            "Section Name",
            "Subsection Code",
            "Subsection Name",
            "Amount",
        ]

        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "year": "Year",
                    "section_code": "Section Code",
                    "section_name": "Section Name",
                    "subsection_code": "Subsection Code",
                    "subsection_name": "Subsection Name",
                    "amount": "Amount",
                }
            )

            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce")
            delta_df["Amount"] = pd.to_numeric(delta_df["Amount"], errors="coerce")
            delta_df = delta_df.dropna(subset=["Year", "Subsection Code"]).copy()
            delta_df["Year"] = delta_df["Year"].astype(int)
            delta_df = delta_df.sort_values(
                ["Year", "Section Code", "Subsection Code"],
                ascending=[False, True, True],
            ).reset_index(drop=True)

            delta_df["Amount"] = delta_df["Amount"].map(
                lambda x: "" if pd.isna(x) else f"{float(x):.6f}".rstrip("0").rstrip(".")
            )

            for c in target_cols:
                if c not in delta_df.columns:
                    delta_df[c] = pd.NA
            delta_df[target_cols].to_csv(deliverable_path, index=False)
        else:
            pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)

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
            ),
            "state": new_state,
        }
