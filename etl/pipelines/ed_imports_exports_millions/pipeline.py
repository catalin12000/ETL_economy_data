from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.database import compare_with_postgres
from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_download_url_by_title, get_latest_publication_year_url
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_imports_exports_millions


DISPLAY_MAP = {
    # Headers chosen so they snake_case directly to the DB column names,
    # so the deliverable maps 1:1 onto ed_imports_exports_millions.
    "current_prices_goods": "Current prices goods",
    "current_prices_services": "Current prices services",
    "current_prices_imports": "Current prices imports",
    "current_prices_expenditures_of_residents_in_rest_of_the_world": "Current prices expenditures of residents in rest of the world",
    "current_prices_goods_exports": "Current prices goods exports",
    "current_prices_services_exports": "Current prices services exports",
    "current_prices_exports": "Current prices exports",
    "current_prices_expenditures_of_residents_on_economic_territory": "Current prices expenditures of residents on economic territory",
    "current_prices_exports_imports_balance": "Current prices exports imports balance",
    "constant_prices_goods": "Constant prices goods",
    "constant_prices_services": "Constant prices services",
    "constant_prices_imports": "Constant prices imports",
    "constant_prices_expenditures_of_residents_in_rest_of_the_world": "Constant prices expenditures of residents in rest of the world",
    "constant_prices_goods_exports": "Constant prices goods exports",
    "constant_prices_services_exports": "Constant prices services exports",
    "constant_prices_exports": "Constant prices exports",
    "constant_prices_expenditures_of_residents_on_economic_territory": "Constant prices expenditures of residents on economic territory",
    "constant_prices_exports_imports_balance": "Constant prices exports imports balance",
}


DB_NUMERIC_COLS = [
    "current_prices_goods",
    "current_prices_services",
    "current_prices_imports",
    "current_prices_expenditures_of_residents_in_rest_of_the_world",
    "current_prices_goods_exports",
    "current_prices_services_exports",
    "current_prices_exports",
    "current_prices_expenditures_of_residents_on_economic_territory",
    "current_prices_exports_imports_balance",
    "constant_prices_goods",
    "constant_prices_services",
    "constant_prices_imports",
    "constant_prices_expenditures_of_residents_in_rest_of_the_world",
    "constant_prices_goods_exports",
    "constant_prices_services_exports",
    "constant_prices_exports",
    "constant_prices_expenditures_of_residents_on_economic_territory",
    "constant_prices_exports_imports_balance",
]


class Pipeline:
    pipeline_id = "ed_imports_exports_millions"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_imports_exports_millions"
    source_type = "dynamic_file"
    display_name = "Imports-Exports of Goods and Services (Millions) - Annual"

    PUBLICATION_CODE = "SEL30"
    TARGET_TITLE_SUBSTRING = "Imports-Exports of Goods and Services"
    MIN_DB_YEAR = 2015

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_imports_exports_goods_services.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest year page dynamically
        pub_url = get_latest_publication_year_url(
            publication_code=self.PUBLICATION_CODE,
            locale="en",
            headers=headers,
        )

        # 2) Find download link by title (substring match)
        download_url = get_download_url_by_title(
            publication_url=pub_url,
            target_title=self.TARGET_TITLE_SUBSTRING,
            headers=headers,
        )

        # 3) Download + hash
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
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
        })

        print(f"Extracting data from {out_path}...")
        df_new = extract_imports_exports_millions(out_path)
        df_new = df_new[pd.to_numeric(df_new["Year"], errors="coerce") >= self.MIN_DB_YEAR].copy()
        if df_new.empty:
            return {
                "status": "error",
                "message": f"No rows found for Year >= {self.MIN_DB_YEAR}.",
                "state": new_state,
            }

        output_dir = pp.output

        # Local baseline compare (deliverable shape, without ID)
        df_local = pd.DataFrame({"Year": df_new["Year"]})
        for db_col, out_col in DISPLAY_MAP.items():
            df_local[out_col] = df_new[db_col]


        # DB compare (read-only)
        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = pd.DataFrame({
            "year": pd.to_numeric(df_new["Year"], errors="coerce"),
            "current_prices_goods": pd.to_numeric(df_new["current_prices_goods"], errors="coerce"),
            "current_prices_services": pd.to_numeric(df_new["current_prices_services"], errors="coerce"),
            "current_prices_imports": pd.to_numeric(df_new["current_prices_imports"], errors="coerce"),
            "current_prices_expenditures_of_residents_in_rest_of_the_world": pd.to_numeric(
                df_new["current_prices_expenditures_of_residents_in_rest_of_the_world"], errors="coerce"
            ),
            "current_prices_goods_exports": pd.to_numeric(df_new["current_prices_goods_exports"], errors="coerce"),
            "current_prices_services_exports": pd.to_numeric(df_new["current_prices_services_exports"], errors="coerce"),
            "current_prices_exports": pd.to_numeric(df_new["current_prices_exports"], errors="coerce"),
            "current_prices_expenditures_of_residents_on_economic_territory": pd.to_numeric(
                df_new["current_prices_expenditures_of_residents_on_economic_territory"], errors="coerce"
            ),
            "current_prices_exports_imports_balance": pd.to_numeric(
                df_new["current_prices_exports_imports_balance"], errors="coerce"
            ),
            "constant_prices_goods": pd.to_numeric(df_new["constant_prices_goods"], errors="coerce"),
            "constant_prices_services": pd.to_numeric(df_new["constant_prices_services"], errors="coerce"),
            "constant_prices_imports": pd.to_numeric(df_new["constant_prices_imports"], errors="coerce"),
            "constant_prices_expenditures_of_residents_in_rest_of_the_world": pd.to_numeric(
                df_new["constant_prices_expenditures_of_residents_in_rest_of_the_world"], errors="coerce"
            ),
            "constant_prices_goods_exports": pd.to_numeric(df_new["constant_prices_goods_exports"], errors="coerce"),
            "constant_prices_services_exports": pd.to_numeric(df_new["constant_prices_services_exports"], errors="coerce"),
            "constant_prices_exports": pd.to_numeric(df_new["constant_prices_exports"], errors="coerce"),
            "constant_prices_expenditures_of_residents_on_economic_territory": pd.to_numeric(
                df_new["constant_prices_expenditures_of_residents_on_economic_territory"], errors="coerce"
            ),
            "constant_prices_exports_imports_balance": pd.to_numeric(
                df_new["constant_prices_exports_imports_balance"], errors="coerce"
            ),
        }).dropna(subset=["year"]).copy()

        sql_path = pp.sql("ed_imports_exports_millions.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year"],
            sync_cols=DB_NUMERIC_COLS,
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}
        print(
            f"Postgres (athena) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        # Deliverable: DB delta only + ID in requested format
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = ["id", "year"] + list(DISPLAY_MAP.keys())

        if not delta_df.empty:
            if "id" in delta_df.columns:
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce").astype("Int64")
            delta_df["year"] = pd.to_numeric(delta_df["year"], errors="coerce").astype("Int64")

            for db_col in DISPLAY_MAP:
                delta_df[db_col] = pd.to_numeric(delta_df.get(db_col), errors="coerce")

            delta_df = delta_df.sort_values(["year"]).reset_index(drop=True)

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
        new_state.update({
            "db_comparison": db_summary,
            "deliverable_path": str(deliverable_path),
        })

        if (
            not is_new_by_hash(state.get("file_sha256"), file_hash)
            and db_comp_res.get("inserted") == 0
            and db_comp_res.get("updated") == 0
        ):
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows. DB (athena) comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
