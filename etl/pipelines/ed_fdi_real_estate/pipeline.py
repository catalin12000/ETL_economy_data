from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_fdi_real_estate


class Pipeline:
    pipeline_id = "ed_fdi_real_estate"
    display_name = "BoG FDI Direct Investment Real Estate"
    MIN_DB_YEAR = 2024

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/external-sector/"
        "direct-investment/direct-investment---flows"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/EN_Direct_Investment_in_Greece_in_Real_Estate.xlsx"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "EN_Direct_Investment_in_Greece_in_Real_Estate.xlsx"

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

        print("Extracting FDI real estate data...")
        df_all = extract_fdi_real_estate(out_path)
        df_new = df_all[pd.to_numeric(df_all["Year"], errors="coerce") >= self.MIN_DB_YEAR].copy()
        if df_new.empty:
            return {
                "status": "error",
                "message": f"No rows found for Year >= {self.MIN_DB_YEAR}.",
                "state": new_state,
            }

        # DB keeps integer amounts in this dataset; align before compare/deliverable.
        df_new["Amount"] = pd.to_numeric(df_new["Amount"], errors="coerce").round(0)

        db_path = pp.baseline
        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = pp.output / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path,
            df_new,
            out_csv_full,
            report_csv,
            key_cols=["Year", "Country"],
        )

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Country": "country",
                "Area": "area",
                "Amount": "amount",
            }
        )
        sql_path = pp.sql("ed_fdi_real_estate.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_fdi_real_estate",
            db_name="athena",
            match_cols=["year", "country"],
            sync_cols=["area", "amount"],
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

        target_cols = ["Year", "Country", "Area", "Amount"]
        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "year": "Year",
                    "country": "Country",
                    "area": "Area",
                    "amount": "Amount",
                }
            )
            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce")
            delta_df["Amount"] = pd.to_numeric(delta_df["Amount"], errors="coerce")
            delta_df = delta_df.dropna(subset=["Year", "Country"]).copy()
            delta_df["Year"] = delta_df["Year"].astype(int)
            delta_df["Amount"] = delta_df["Amount"].round(0).astype("Int64")
            delta_df = delta_df.sort_values(["Year", "Country"], ascending=[False, True]).reset_index(drop=True)
            for c in target_cols:
                if c not in delta_df.columns:
                    delta_df[c] = pd.NA
            write_deliverable_csv(delta_df[target_cols], deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
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
