from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.elstat import get_download_url_by_title, get_latest_publication_url
from .extract import extract_retail_trade_volume


class Pipeline:
    pipeline_id = "ed_retail_trade_volume_index"
    display_name = "Retail Trade Volume Index"

    PUBLICATION_CODE = "DKT39"
    TARGET_TITLE_SUBSTRING = "02. Volume Index in Retail Trade"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "40"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "elstat_retail_volume.xls"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="monthly",
            headers=headers,
        )
        download_url = get_download_url_by_title(
            pub_url,
            self.TARGET_TITLE_SUBSTRING,
            headers=headers,
        )

        download_note = None
        try:
            meta = download_file(download_url, out_path, headers=headers)
        except PermissionError:
            if not out_path.exists():
                raise
            meta = {
                "last_modified": state.get("last_modified"),
                "etag": state.get("etag"),
                "content_length": state.get("content_length"),
                "final_url": state.get("final_url") or download_url,
                "downloaded_at_utc": state.get("downloaded_at_utc"),
            }
            download_note = "Used existing local workbook because the source file was locked."
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "publication_code": self.PUBLICATION_CODE,
                "publication_url_used": pub_url,
                "download_url_used": download_url,
                "source_url_used": download_url,
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

        print("Extracting retail trade volume data...")
        df_new = extract_retail_trade_volume(out_path)

        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Month"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Month": "month",
                "Overall Index": "overall_index",
                "Overall index except automotive fuel": "overall_index_excl_automotive",
                "Food sector (supermarkets, food,beverages, tobacco)": "food_sector_index",
                "Overall index except Food sector and Automotive fuel": "overall_index_excl_food_sector",
                "Super Markets": "supermarkets_index",
                "Department stores": "department_stores_index",
                "Automotive fuel": "automotive_fuel_index",
                "Food beverages, tobacco": "food_beverages_tobacco_index",
                "Pharmaceutical products, cosmetics": "pharmaceutical_cosmetics_index",
                "Clothing and footwear": "clothing_footwear_index",
                "Furniture, electrical and household equipment": "furniture_electrical_household_equipment_index",
                "Books stationery, other goods": "books_stationary_other_goods_index",
            }
        )
        sql_path = Path(__file__).parent / "ed_retail_trade_volume_index.sql"
        sync_cols = [
            "overall_index",
            "overall_index_excl_automotive",
            "food_sector_index",
            "overall_index_excl_food_sector",
            "supermarkets_index",
            "department_stores_index",
            "automotive_fuel_index",
            "food_beverages_tobacco_index",
            "pharmaceutical_cosmetics_index",
            "clothing_footwear_index",
            "furniture_electrical_household_equipment_index",
            "books_stationary_other_goods_index",
        ]
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.pipeline_id,
            db_name="athena",
            match_cols=["year", "month"],
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

        db_diff_only_path = output_dir / "db_differences_only.csv"
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "ID",
            "Year",
            "Month",
            "Overall Index",
            "Overall index except automotive fuel",
            "Food sector (supermarkets, food,beverages, tobacco)",
            "Overall index except Food sector and Automotive fuel",
            "Super Markets",
            "Department stores",
            "Automotive fuel",
            "Food beverages, tobacco",
            "Pharmaceutical products, cosmetics",
            "Clothing and footwear",
            "Furniture, electrical and household equipment",
            "Books stationery, other goods",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(
                columns={
                    "id": "ID",
                    "year": "Year",
                    "month": "Month",
                    "overall_index": "Overall Index",
                    "overall_index_excl_automotive": "Overall index except automotive fuel",
                    "food_sector_index": "Food sector (supermarkets, food,beverages, tobacco)",
                    "overall_index_excl_food_sector": "Overall index except Food sector and Automotive fuel",
                    "supermarkets_index": "Super Markets",
                    "department_stores_index": "Department stores",
                    "automotive_fuel_index": "Automotive fuel",
                    "food_beverages_tobacco_index": "Food beverages, tobacco",
                    "pharmaceutical_cosmetics_index": "Pharmaceutical products, cosmetics",
                    "clothing_footwear_index": "Clothing and footwear",
                    "furniture_electrical_household_equipment_index": "Furniture, electrical and household equipment",
                    "books_stationary_other_goods_index": "Books stationery, other goods",
                }
            ).copy()

            shaped["ID"] = pd.to_numeric(shaped["ID"], errors="coerce")
            shaped["Year"] = pd.to_numeric(shaped["Year"], errors="coerce")
            shaped["Month"] = pd.to_numeric(shaped["Month"], errors="coerce")
            shaped = shaped.dropna(subset=["Year", "Month"]).copy()
            shaped["Year"] = shaped["Year"].astype(int)
            shaped["Month"] = shaped["Month"].astype(int)
            for column in target_cols[3:]:
                shaped[column] = pd.to_numeric(shaped[column], errors="coerce")

            shaped = shaped.sort_values(["Year", "Month"]).reset_index(drop=True)
            if "ID" in shaped.columns:
                shaped["ID"] = shaped["ID"].map(lambda x: "" if pd.isna(x) else str(int(x)))

            for column in target_cols:
                if column not in shaped.columns:
                    shaped[column] = pd.NA

            return shaped[target_cols]

        shape_output(delta_df).to_csv(deliverable_path, index=False)
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
