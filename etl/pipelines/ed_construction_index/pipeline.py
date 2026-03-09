from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.elstat import get_download_url_by_title, get_latest_publication_url
from .extract import extract_construction_index_quarterly


class Pipeline:
    pipeline_id = "ed_construction_index"
    display_name = "Ed Construction Index"

    TARGET_TITLE = "02. Evolution of the Production Index in Construction (working day adjusted data)"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "04"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        xls_path = out_dir / "elstat_construction_index.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        pub_url = get_latest_publication_url("DKT66", locale="en", frequency="quarterly", headers=headers)
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE, headers=headers)

        meta = download_file(download_url, xls_path, headers=headers)
        file_hash = sha256_file(xls_path)

        new_state = dict(state)
        new_state.update(
            {
                "publication_url_used": pub_url,
                "download_url_used": download_url,
                "source_url_used": download_url,
                "file_sha256": file_hash,
                "downloaded_filename": xls_path.name,
                "last_download_path": str(xls_path),
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "content_length": meta.get("content_length"),
                "final_url": meta.get("final_url"),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
            }
        )

        print("Extracting quarterly construction index data...")
        df_new = extract_construction_index_quarterly(xls_path)

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
            key_cols=["Year", "Quarter"],
        )

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Quarter": "quarter",
                "Production Index in Construction": "production_index_in_construction",
                "Production Index Building Construction": "production_index_building_construction",
                "Production Index Civil Engineering": "production_index_civil_engineering",
            }
        )
        sql_path = Path(__file__).parent / "ed_construction_index.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_construction_index",
            db_name="athena",
            match_cols=["year", "quarter"],
            sync_cols=[
                "production_index_in_construction",
                "production_index_building_construction",
                "production_index_civil_engineering",
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
            "Quarter",
            "Production Index in Construction",
            "Production Index Building Construction",
            "Production Index Civil Engineering",
        ]
        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "year": "Year",
                    "quarter": "Quarter",
                    "production_index_in_construction": "Production Index in Construction",
                    "production_index_building_construction": "Production Index Building Construction",
                    "production_index_civil_engineering": "Production Index Civil Engineering",
                }
            )
            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce")
            delta_df["Quarter"] = pd.to_numeric(delta_df["Quarter"], errors="coerce")
            for c in target_cols[2:]:
                delta_df[c] = pd.to_numeric(delta_df[c], errors="coerce")

            delta_df = delta_df.dropna(subset=["Year", "Quarter"]).copy()
            delta_df["Year"] = delta_df["Year"].astype(int)
            delta_df["Quarter"] = delta_df["Quarter"].astype(int)
            delta_df = delta_df.sort_values(["Year", "Quarter"]).reset_index(drop=True)
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
