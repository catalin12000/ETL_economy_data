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
from .extract import extract_motor_trade_turnover


class Pipeline:
    pipeline_id = "ed_motor_trade_turnover_index"
    display_name = "Motor Trade Turnover Index"

    PUBLICATION_CODE = "DKT45"
    TARGET_TITLE_SUBSTRING = "03. Turnover Index for Motor Trade"
    MIN_DB_YEAR = 2015

    @staticmethod
    def _format_db_compare_output(df: pd.DataFrame) -> pd.DataFrame:
        target_cols = [
            "ID",
            "Year",
            "Month",
            "Motor Trade Turnover Index",
            "Vehicle Sale Turnover Index",
        ]
        if df.empty:
            return pd.DataFrame(columns=target_cols)

        out = df.rename(
            columns={
                "id": "ID",
                "year": "Year",
                "month": "Month",
                "motor_trade_turnover_index": "Motor Trade Turnover Index",
                "vehicle_sale_turnover_index": "Vehicle Sale Turnover Index",
            }
        ).copy()

        if "ID" in out.columns:
            out["ID"] = pd.to_numeric(out["ID"], errors="coerce").astype("Int64")
        out["Year"] = pd.to_numeric(out["Year"], errors="coerce").astype("Int64")
        out["Month"] = pd.to_numeric(out["Month"], errors="coerce").astype("Int64")
        out["Motor Trade Turnover Index"] = pd.to_numeric(
            out["Motor Trade Turnover Index"], errors="coerce"
        ).round(2)
        out["Vehicle Sale Turnover Index"] = pd.to_numeric(
            out["Vehicle Sale Turnover Index"], errors="coerce"
        ).round(2)

        out = out.sort_values(["Year", "Month"]).reset_index(drop=True)
        for col in target_cols:
            if col not in out.columns:
                out[col] = pd.NA
        return out[target_cols]

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "22"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "elstat_motor_trade_turnover.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest quarterly page dynamically
        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="quarterly",
            headers=headers,
        )

        # 2) Find download link by title
        download_url = get_download_url_by_title(
            pub_url,
            self.TARGET_TITLE_SUBSTRING,
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
        df_new = extract_motor_trade_turnover(out_path)
        df_new = df_new[pd.to_numeric(df_new["Year"], errors="coerce") >= self.MIN_DB_YEAR].copy()
        if df_new.empty:
            return {
                "status": "error",
                "message": f"No rows found for Year >= {self.MIN_DB_YEAR}.",
                "state": new_state,
            }

        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        db_differences_only_path = output_dir / "db_differences_only.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"

        df_local = df_new[
            ["Year", "Month", "Motor Trade Turnover Index", "Vehicle Sale Turnover Index"]
        ].copy()

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
        df_for_db = pd.DataFrame(
            {
                "year": pd.to_numeric(df_new["Year"], errors="coerce"),
                "month": pd.to_numeric(df_new["Month"], errors="coerce"),
                "motor_trade_turnover_index": pd.to_numeric(
                    df_new["Motor Trade Turnover Index"], errors="coerce"
                ),
                "vehicle_sale_turnover_index": pd.to_numeric(
                    df_new["Vehicle Sale Turnover Index"], errors="coerce"
                ),
            }
        ).dropna(subset=["year", "month"]).copy()

        sql_path = Path(__file__).parent / "ed_motor_trade_turnover_index.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.pipeline_id,
            db_name="athena",
            match_cols=["year", "month"],
            sync_cols=["motor_trade_turnover_index", "vehicle_sale_turnover_index"],
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
        delta_db_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        write_deliverable_csv(self._format_db_compare_output(delta_db_df), deliverable_path)
        self._format_db_compare_output(updated_df).to_csv(db_differences_only_path, index=False)

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
                "db_differences_only_path": str(db_differences_only_path),
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
                f"Extracted {len(df_new)} rows. DB (athena) comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
