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
from .extract import extract_industrial_production


class Pipeline:
    pipeline_id = "ed_industrial_production_index"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_industrial_production_index"
    source_type = "dynamic_file"
    display_name = "Industrial Production Index (Overall + Seasonally Adjusted)"

    PUBLICATION_CODE = "DKT21"

    # Use stable substrings (titles contain changing base years like "... 2021=100.0")
    TITLE_03 = "Evolution of the Overall Industrial Production Index"
    TITLE_04 = "Seasonally Adjusted Industrial Production Index"
    MIN_DB_YEAR = 2015

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest month dynamically
        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="monthly",
            headers=headers,
        )

        # 2) Resolve the 2 download URLs by title
        url_03 = get_download_url_by_title(pub_url, self.TITLE_03, headers=headers)
        url_04 = get_download_url_by_title(pub_url, self.TITLE_04, headers=headers)

        # 3) Download both files
        path_03 = out_dir / "industrial_production_index_overall_03.xls"
        path_04 = out_dir / "industrial_production_index_sa_04.xls"

        meta_03 = download_file(url_03, path_03, headers=headers)
        meta_04 = download_file(url_04, path_04, headers=headers)

        hash_03 = sha256_file(path_03)
        hash_04 = sha256_file(path_04)

        # 4) Update state
        new_state = dict(state)
        new_state.update({
            "publication_code": self.PUBLICATION_CODE,
            "publication_url_used": pub_url,

            "download_url_03": url_03,
            "download_url_04": url_04,

            "file_sha256_03": hash_03,
            "file_sha256_04": hash_04,

            "downloaded_filename_03": path_03.name,
            "downloaded_filename_04": path_04.name,

            "last_download_path_03": str(path_03),
            "last_download_path_04": str(path_04),

            # keep meta (separately) for debugging
            "meta_03": {
                "last_modified": meta_03.get("last_modified"),
                "etag": meta_03.get("etag"),
                "content_length": meta_03.get("content_length"),
                "final_url": meta_03.get("final_url"),
                "downloaded_at_utc": meta_03.get("downloaded_at_utc"),
            },
            "meta_04": {
                "last_modified": meta_04.get("last_modified"),
                "etag": meta_04.get("etag"),
                "content_length": meta_04.get("content_length"),
                "final_url": meta_04.get("final_url"),
                "downloaded_at_utc": meta_04.get("downloaded_at_utc"),
            },
        })

        print(f"Extracting and merging data from {path_03.name} + {path_04.name}...")
        df_new = extract_industrial_production(path_03, path_04)
        df_new = df_new[pd.to_numeric(df_new["Year"], errors="coerce") >= self.MIN_DB_YEAR].copy()
        if df_new.empty:
            return {
                "status": "error",
                "message": f"No rows found for Year >= {self.MIN_DB_YEAR}.",
                "state": new_state,
            }

        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        report_csv = pp.output / "update_report.csv"
        db_path = pp.baseline

        df_local = pd.DataFrame({
            "Year": df_new["Year"],
            "Month": df_new["Month"],
            "Overall Index": df_new["overall_index"],
            "Seasonally Adjusted Overall Index": df_new["seasonally_adjusted_overall_index"],
            "Mining Quarrying": df_new["mining_quarrying"],
            "Manufacturing": df_new["manufacturing"],
            "Electricity": df_new["electricity"],
            "Water Supply": df_new["water_supply"],
        })

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_local,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Month"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = pd.DataFrame({
            "year": pd.to_numeric(df_new["Year"], errors="coerce"),
            "month": pd.to_numeric(df_new["Month"], errors="coerce"),
            "overall_index": pd.to_numeric(df_new["overall_index"], errors="coerce"),
            "seasonally_adjusted_overall_index": pd.to_numeric(df_new["seasonally_adjusted_overall_index"], errors="coerce"),
            "mining_quarrying": pd.to_numeric(df_new["mining_quarrying"], errors="coerce"),
            "manufacturing": pd.to_numeric(df_new["manufacturing"], errors="coerce"),
            "electricity": pd.to_numeric(df_new["electricity"], errors="coerce"),
            "water_supply": pd.to_numeric(df_new["water_supply"], errors="coerce"),
        }).dropna(subset=["year", "month"]).copy()

        sql_path = pp.sql("ed_industrial_production_index.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "month"],
            sync_cols=[
                "overall_index",
                "seasonally_adjusted_overall_index",
                "mining_quarrying",
                "manufacturing",
                "electricity",
                "water_supply",
            ],
            tolerance=0.11,
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
            "Month",
            "Overall Index",
            "Seasonally Adjusted Overall Index",
            "Mining Quarrying",
            "Manufacturing",
            "Electricity",
            "Water Supply",
        ]

        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "id": "id",
                    "year": "Year",
                    "month": "Month",
                    "overall_index": "Overall Index",
                    "seasonally_adjusted_overall_index": "Seasonally Adjusted Overall Index",
                    "mining_quarrying": "Mining Quarrying",
                    "manufacturing": "Manufacturing",
                    "electricity": "Electricity",
                    "water_supply": "Water Supply",
                }
            )
            if "id" in delta_df.columns:
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce").astype("Int64")
            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce").astype("Int64")
            delta_df["Month"] = pd.to_numeric(delta_df["Month"], errors="coerce").astype("Int64")
            for c in target_cols[3:]:
                delta_df[c] = pd.to_numeric(delta_df[c], errors="coerce").round(2)

            delta_df = delta_df.sort_values(["Year", "Month"]).reset_index(drop=True)
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
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "db_comparison": db_summary,
            "deliverable_path": str(deliverable_path),
            "delta_path": str(output_file),
            "mock_db_snapshot_path": str(out_csv_full),
        })

        unchanged_03 = not is_new_by_hash(state.get("file_sha256_03"), hash_03)
        unchanged_04 = not is_new_by_hash(state.get("file_sha256_04"), hash_04)
        if (
            unchanged_03
            and unchanged_04
            and res.new_rows == 0
            and res.updated_cells == 0
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
