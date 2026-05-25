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
from .extract import extract_tourists_arrivals_revenue


class Pipeline:
    pipeline_id = "ed_tourists_arrivals_revenue"
    country = "gr"
    source = "bank_of_greece"
    db_table_name = "ed_tourists_arrivals_revenue"
    display_name = "Ed Tourists Arrivals and Revenue (BoG)"

    RECEIPTS_URL = "https://www.bankofgreece.gr/RelatedDocuments/RECEIPTS_BY_COUNTRY_OF_ORIGIN.xls"
    TRAVELLERS_URL = "https://www.bankofgreece.gr/RelatedDocuments/NUMBER_OF_INBOUND_TRAVELLERS_IN_GREECE_BY_COUNTRY_OF_ORIGIN.xls"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        receipts_path = out_dir / "Receipts_By_Country_Of_Origin.xls"
        travellers_path = out_dir / "Number_Of_Inbound_Travellers.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        download_note = None
        try:
            meta_receipts = download_file(self.RECEIPTS_URL, receipts_path, headers=headers)
            meta_travellers = download_file(self.TRAVELLERS_URL, travellers_path, headers=headers)
        except PermissionError:
            if not receipts_path.exists() or not travellers_path.exists():
                raise
            meta_receipts = {
                "downloaded_at_utc": state.get("downloaded_at_utc"),
            }
            meta_travellers = {
                "downloaded_at_utc": state.get("downloaded_at_utc"),
            }
            download_note = "Used existing local workbooks because one of the source files was locked."

        hash_receipts = sha256_file(receipts_path)
        hash_travellers = sha256_file(travellers_path)

        new_state = dict(state)
        new_state.update(
            {
                "file_sha256_receipts": hash_receipts,
                "file_sha256_travellers": hash_travellers,
                "last_download_path_receipts": str(receipts_path),
                "last_download_path_travellers": str(travellers_path),
                "downloaded_at_utc": meta_receipts.get("downloaded_at_utc"),
            }
        )
        if download_note:
            new_state["download_note"] = download_note

        print("Extracting tourists arrivals and revenue data...")
        df_new = extract_tourists_arrivals_revenue(receipts_path, travellers_path)

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
            key_cols=["Year", "Quarter", "Area", "Country of Origin"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Quarter": "quarter",
                "Area": "area",
                "Country of Origin": "country_of_origin",
                "Number of Travellers (000s)": "number_of_travellers_000s",
                "Revenues by Country of Origin (millions)": "revenues_by_country_of_origin_millions",
            }
        )
        sql_path = pp.sql("ed_tourists_arrivals_revenue.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "quarter", "area", "country_of_origin"],
            sync_cols=["number_of_travellers_000s", "revenues_by_country_of_origin_millions"],
            tolerance=0.11,
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

        target_cols = [
            "id",
            "year",
            "quarter",
            "area",
            "country_of_origin",
            "number_of_travellers_000s",
            "revenues_by_country_of_origin_millions",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(columns={"id": "id"}).copy()
            shaped["id"] = pd.to_numeric(shaped["id"], errors="coerce")
            shaped["year"] = pd.to_numeric(shaped["year"], errors="coerce")
            shaped["quarter"] = pd.to_numeric(shaped["quarter"], errors="coerce")
            shaped["number_of_travellers_000s"] = pd.to_numeric(shaped["number_of_travellers_000s"], errors="coerce")
            shaped["revenues_by_country_of_origin_millions"] = pd.to_numeric(shaped["revenues_by_country_of_origin_millions"], errors="coerce")
            shaped = shaped.dropna(subset=["year", "quarter", "area", "country_of_origin"]).copy()
            shaped["year"] = shaped["year"].astype(int)
            shaped["quarter"] = shaped["quarter"].astype(int)
            shaped = shaped.sort_values(["year", "quarter", "area", "country_of_origin"]).reset_index(drop=True)
            shaped["id"] = shaped["id"].map(lambda x: "" if pd.isna(x) else str(int(x)))
            shaped["number_of_travellers_000s"] = shaped["number_of_travellers_000s"].map(
                lambda x: "" if pd.isna(x) else f"{float(x):.1f}"
            )
            shaped["revenues_by_country_of_origin_millions"] = shaped["revenues_by_country_of_origin_millions"].map(
                lambda x: "" if pd.isna(x) else f"{float(x):.1f}"
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
            not is_new_by_hash(state.get("file_sha256_receipts"), hash_receipts)
            and not is_new_by_hash(state.get("file_sha256_travellers"), hash_travellers)
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
