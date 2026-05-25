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
from .extract import extract_motor_trade_turnover, extract_motor_trade_volume


class Pipeline:
    pipeline_id = "ed_motor_trade_turnover_index"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_motor_trade_turnover_index"
    source_type = "dynamic_file"
    display_name = "Motor Trade Turnover and Volume Index"

    PUBLICATION_CODE = "DKT45"
    TURNOVER_TITLE = "03. Turnover Index for Motor Trade"
    VOLUME_TITLE   = "04. Volume Index for Motor Trade"
    MIN_DB_YEAR = 2015

    DB_SYNC_COLS = [
        "motor_trade_turnover_index",
        "vehicle_sale_turnover_index",
    ]
    OUTPUT_COLS = [
        "motor_trade_turnover_index",
        "vehicle_sale_turnover_index",
        "motor_trade_volume_index",
        "vehicle_sale_volume_index",
    ]

    def _format_db_compare_output(self, df: pd.DataFrame) -> pd.DataFrame:
        target_cols = ["id", "year", "month"] + self.OUTPUT_COLS
        if df.empty:
            return pd.DataFrame(columns=target_cols)

        out = df.rename(columns={"id": "id"}).copy()
        if "id" in out.columns:
            out["id"] = pd.to_numeric(out["id"], errors="coerce").astype("Int64")
        out["year"]  = pd.to_numeric(out["year"],  errors="coerce").astype("Int64")
        out["month"] = pd.to_numeric(out["month"], errors="coerce").astype("Int64")
        for col in target_cols[3:]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce").round(2)

        out = out.sort_values(["year", "month"]).reset_index(drop=True)
        for col in target_cols:
            if col not in out.columns:
                out[col] = pd.NA
        return out[target_cols]

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest quarterly publication page (shared by both files)
        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE, locale="en", frequency="quarterly", headers=headers,
        )

        # 2) Download file 03 (turnover)
        turnover_url = get_download_url_by_title(pub_url, self.TURNOVER_TITLE, headers=headers)
        turnover_path = out_dir / "elstat_motor_trade_turnover.xls"
        meta_t = download_file(turnover_url, turnover_path, headers=headers)
        hash_t = sha256_file(turnover_path)

        # 3) Download file 04 (volume)
        volume_url = get_download_url_by_title(pub_url, self.VOLUME_TITLE, headers=headers)
        volume_path = out_dir / "elstat_motor_trade_volume.xls"
        meta_v = download_file(volume_url, volume_path, headers=headers)
        hash_v = sha256_file(volume_path)

        new_state = dict(state)
        new_state.update({
            "publication_code": self.PUBLICATION_CODE,
            "publication_url_used": pub_url,
            "turnover_url": turnover_url,
            "volume_url": volume_url,
            "file_sha256_turnover": hash_t,
            "file_sha256_volume": hash_v,
            "downloaded_at_utc": meta_t.get("downloaded_at_utc"),
        })

        # 4) Extract + merge on Year/Month
        print("Extracting turnover data (file 03)...")
        df_turnover = extract_motor_trade_turnover(turnover_path)

        print("Extracting volume data (file 04)...")
        df_volume = extract_motor_trade_volume(volume_path)

        df_new = pd.merge(df_turnover, df_volume, on=["year", "month"], how="outer")
        df_new = df_new[pd.to_numeric(df_new["year"], errors="coerce") >= self.MIN_DB_YEAR].copy()
        if df_new.empty:
            return {
                "status": "error",
                "message": f"No rows found for year >= {self.MIN_DB_YEAR}.",
                "state": new_state,
            }
        df_new = df_new.sort_values(["year", "month"]).reset_index(drop=True)

        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file  = output_dir / "new_entries.csv"
        db_diff_path = output_dir / "db_differences_only.csv"
        report_csv   = pp.output / "update_report.csv"
        db_path      = pp.baseline

        # 5) Local baseline compare
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["year", "month"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        # 6) DB compare — all 4 sync cols
        print("Comparing with live Postgres DB (athena)...")
        df_for_db = df_new.copy()
        df_for_db["year"] = pd.to_numeric(df_for_db["year"], errors="coerce")
        df_for_db["month"] = pd.to_numeric(df_for_db["month"], errors="coerce")
        for c in self.OUTPUT_COLS:
            df_for_db[c] = pd.to_numeric(df_for_db[c], errors="coerce")
        df_for_db = df_for_db.dropna(subset=["year", "month"]).copy()

        sql_path = pp.sql("ed_motor_trade_turnover_index.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "month"],
            sync_cols=self.DB_SYNC_COLS,
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (athena) comparison: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        # 7) Deliverable
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df  = db_comp_res.get("updated_df",  pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        def enrich_with_volume_columns(delta_like: pd.DataFrame) -> pd.DataFrame:
            if delta_like.empty:
                return pd.DataFrame(columns=["id", "year", "month"] + self.OUTPUT_COLS)
            keys = delta_like[["id", "year", "month"]].copy()
            keys["year"] = pd.to_numeric(keys["year"], errors="coerce")
            keys["month"] = pd.to_numeric(keys["month"], errors="coerce")
            full_values = df_for_db[["year", "month"] + self.OUTPUT_COLS].drop_duplicates(subset=["year", "month"])
            return keys.merge(full_values, on=["year", "month"], how="left")

        deliverable_df = enrich_with_volume_columns(delta_df)
        updated_only_df = enrich_with_volume_columns(updated_df)

        write_deliverable_csv(self._format_db_compare_output(deliverable_df), deliverable_path)
        self._format_db_compare_output(updated_only_df).to_csv(db_diff_path, index=False)

        both_unchanged = (
            not is_new_by_hash(state.get("file_sha256_turnover"), hash_t)
            and not is_new_by_hash(state.get("file_sha256_volume"),   hash_v)
        )

        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
        }
        new_state.update({
            "rows_before": res.rows_before,
            "rows_after":  res.rows_after,
            "new_rows":    res.new_rows,
            "updated_cells": res.updated_cells,
            "db_comparison": db_summary,
            "deliverable_path": str(deliverable_path),
            "delta_path": str(output_file),
            "mock_db_snapshot_path": str(out_csv_full),
            "db_differences_only_path": str(db_diff_path),
        })

        if (
            both_unchanged
            and res.new_rows == 0
            and res.updated_cells == 0
            and db_comp_res.get("inserted") == 0
            and db_comp_res.get("updated") == 0
        ):
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows (turnover + volume merged). "
                f"DB (athena): {db_comp_res.get('inserted')} missing, "
                f"{db_comp_res.get('updated')} diff. File: {deliverable_name}"
            ),
            "state": new_state,
        }
