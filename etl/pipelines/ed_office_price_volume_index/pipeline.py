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
from .extract import extract_office_price_volume_index


class Pipeline:
    pipeline_id = "ed_office_price_volume_index"
    country = "gr"
    source = "bank_of_greece"
    db_table_name = "ed_office_price_volume_index"
    source_type = "static_file"
    display_name = "Ed Office Price and Rent Indices (BoG)"

    PRICE_INDEX_URL = "https://www.bankofgreece.gr/RelatedDocuments/OFFICE_PRICE_INDEX.pdf"
    RENT_INDEX_URL = "https://www.bankofgreece.gr/RelatedDocuments/OFFICE_RENT_INDEX.pdf"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        price_path = out_dir / "Office_Price_Index.pdf"
        rent_path = out_dir / "Office_Rent_Index.pdf"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta_price = download_file(self.PRICE_INDEX_URL, price_path, headers=headers)
        hash_price = sha256_file(price_path)

        meta_rent = download_file(self.RENT_INDEX_URL, rent_path, headers=headers)
        hash_rent = sha256_file(rent_path)

        new_state = dict(state)
        new_state.update(
            {
                "source_url_price": self.PRICE_INDEX_URL,
                "source_url_rent": self.RENT_INDEX_URL,
                "file_sha256_price": hash_price,
                "file_sha256_rent": hash_rent,
                "last_download_path_price": str(price_path),
                "last_download_path_rent": str(rent_path),
                "last_modified_price": meta_price.get("last_modified"),
                "last_modified_rent": meta_rent.get("last_modified"),
                "etag_price": meta_price.get("etag"),
                "etag_rent": meta_rent.get("etag"),
                "content_length_price": meta_price.get("content_length"),
                "content_length_rent": meta_rent.get("content_length"),
                "final_url_price": meta_price.get("final_url"),
                "final_url_rent": meta_rent.get("final_url"),
                "downloaded_at_utc_price": meta_price.get("downloaded_at_utc"),
                "downloaded_at_utc_rent": meta_rent.get("downloaded_at_utc"),
            }
        )

        print(f"Extracting data from {price_path} and {rent_path}...")
        df_new = extract_office_price_volume_index(price_path, rent_path)

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
            key_cols=["year", "year_half"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Year Half": "year_half",
                "Total Price Index": "total_price_index",
                "Total Rent Index": "total_rent_index",
                "Athens Price Index": "athens_price_index",
                "Athens Rent Index": "athens_rent_index",
                "Thessaloniki Price Index": "thessaloniki_price_index",
                "Thessaloniki Rent Index": "thessaloniki_rent_index",
                "Rest Of Greece Price Index": "rest_of_greece_price_index",
                "Rest Of Greece Rent Index": "rest_of_greece_rent_index",
            }
        )
        sql_path = pp.sql("ed_office_price_volume_index.sql")
        sync_cols = [
            "total_price_index",
            "total_rent_index",
            "athens_price_index",
            "athens_rent_index",
            "thessaloniki_price_index",
            "thessaloniki_rent_index",
            "rest_of_greece_price_index",
            "rest_of_greece_rent_index",
        ]
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "year_half"],
            sync_cols=sync_cols,
            tolerance=0.05,
            sql_file_path=str(sql_path),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (athena) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "id",
            "Year",
            "Year Half",
            "Total Price Index",
            "Total Rent Index",
            "Athens Price Index",
            "Athens Rent Index",
            "Thessaloniki Price Index",
            "Thessaloniki Rent Index",
            "Rest Of Greece Price Index",
            "Rest Of Greece Rent Index",
        ]

        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "id": "id",
                    "year": "Year",
                    "year_half": "Year Half",
                    "total_price_index": "Total Price Index",
                    "total_rent_index": "Total Rent Index",
                    "athens_price_index": "Athens Price Index",
                    "athens_rent_index": "Athens Rent Index",
                    "thessaloniki_price_index": "Thessaloniki Price Index",
                    "thessaloniki_rent_index": "Thessaloniki Rent Index",
                    "rest_of_greece_price_index": "Rest Of Greece Price Index",
                    "rest_of_greece_rent_index": "Rest Of Greece Rent Index",
                }
            )
            if "id" in delta_df.columns:
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce").astype("Int64")
            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce").astype("Int64")
            delta_df["Year Half"] = pd.to_numeric(delta_df["Year Half"], errors="coerce").astype("Int64")
            for c in target_cols[3:]:
                delta_df[c] = pd.to_numeric(delta_df[c], errors="coerce")

            delta_df = delta_df.sort_values(["Year", "Year Half"]).reset_index(drop=True)
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
            not is_new_by_hash(state.get("file_sha256_price"), hash_price)
            and not is_new_by_hash(state.get("file_sha256_rent"), hash_rent)
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
