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
from .extract import extract_wholesale_trade_indices


class Pipeline:
    pipeline_id = "ed_wholesale_trade_turnover_index"
    display_name = "Wholesale Trade Turnover Index"

    PUBLICATION_CODE = "DKT42"
    TURNOVER_TITLE = "03. Turnover Index in Wholesale Trade"
    VOLUME_TITLE = "04. Volume Index in Wholesale Trade"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "44"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        turnover_path = out_dir / "elstat_wholesale_turnover.xls"
        volume_path = out_dir / "elstat_wholesale_volume.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="quarterly",
            headers=headers,
        )
        turnover_url = get_download_url_by_title(pub_url, self.TURNOVER_TITLE, headers=headers)
        volume_url = get_download_url_by_title(pub_url, self.VOLUME_TITLE, headers=headers)

        download_note = None
        try:
            meta_turnover = download_file(turnover_url, turnover_path, headers=headers)
            meta_volume = download_file(volume_url, volume_path, headers=headers)
        except PermissionError:
            if not turnover_path.exists() or not volume_path.exists():
                raise
            meta_turnover = {"downloaded_at_utc": state.get("downloaded_at_utc")}
            meta_volume = {"downloaded_at_utc": state.get("downloaded_at_utc")}
            download_note = "Used existing local workbooks because one of the source files was locked."

        turnover_hash = sha256_file(turnover_path)
        volume_hash = sha256_file(volume_path)

        new_state = dict(state)
        new_state.update(
            {
                "publication_code": self.PUBLICATION_CODE,
                "publication_url_used": pub_url,
                "download_url_used_turnover": turnover_url,
                "download_url_used_volume": volume_url,
                "source_url_used": turnover_url,
                "file_sha256_turnover": turnover_hash,
                "file_sha256_volume": volume_hash,
                "downloaded_filename_turnover": turnover_path.name,
                "downloaded_filename_volume": volume_path.name,
                "last_download_path_turnover": str(turnover_path),
                "last_download_path_volume": str(volume_path),
                "downloaded_at_utc": meta_turnover.get("downloaded_at_utc") or meta_volume.get("downloaded_at_utc"),
            }
        )
        if download_note:
            new_state["download_note"] = download_note

        print("Extracting wholesale trade turnover/volume indices...")
        df_new = extract_wholesale_trade_indices(turnover_path, volume_path)

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
                "Turnover Index": "turnover_index",
                "Volume Index": "volume_index",
            }
        )
        sql_path = Path(__file__).parent / "ed_wholesales_turnover_index.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_wholesales_turnover_index",
            db_name="athena",
            match_cols=["year", "month"],
            sync_cols=["turnover_index", "volume_index"],
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

        target_cols = ["ID", "Year", "Month", "Turnover Index", "Volume Index"]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(
                columns={
                    "id": "ID",
                    "year": "Year",
                    "month": "Month",
                    "turnover_index": "Turnover Index",
                    "volume_index": "Volume Index",
                }
            ).copy()
            shaped["ID"] = pd.to_numeric(shaped["ID"], errors="coerce")
            shaped["Year"] = pd.to_numeric(shaped["Year"], errors="coerce")
            shaped["Month"] = pd.to_numeric(shaped["Month"], errors="coerce")
            shaped = shaped.dropna(subset=["Year", "Month"]).copy()
            shaped["Year"] = shaped["Year"].astype(int)
            shaped["Month"] = shaped["Month"].astype(int)
            shaped = shaped.sort_values(["Year", "Month"]).reset_index(drop=True)
            shaped["ID"] = shaped["ID"].map(lambda x: "" if pd.isna(x) else str(int(x)))
            for column in ["Turnover Index", "Volume Index"]:
                if column in shaped.columns:
                    shaped[column] = pd.to_numeric(shaped[column], errors="coerce").map(
                        lambda x: "" if pd.isna(x) else f"{float(x):.6f}"
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
            not is_new_by_hash(state.get("file_sha256_turnover"), turnover_hash)
            and not is_new_by_hash(state.get("file_sha256_volume"), volume_hash)
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
