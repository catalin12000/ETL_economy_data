from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import BASE_URL, get_download_url_by_title, list_publication_years
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_gva, DB_COLS


class Pipeline:
    pipeline_id = "ed_gva_by_sector"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_gva_by_sector"
    display_name = "Ed GVA By Sector - Annual"

    PUBLICATION_CODE = "SEL12"
    TARGET_TITLE_SUBSTRING = "Ακαθάριστη προστιθέμενη αξία κατά κλάδο (A64)"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "ed_gva_by_sector.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # Walk back through years until the A64 file is found —
        # ELSTAT sometimes publishes the year index before the workbook is ready.
        years = list_publication_years(
            publication_code=self.PUBLICATION_CODE,
            locale="el",
            headers=headers,
        )
        if not years:
            raise RuntimeError(f"No years found for publication {self.PUBLICATION_CODE}")

        pub_url = None
        download_url = None
        last_err: Exception | None = None
        for y in years:
            candidate_url = f"{BASE_URL}/el/statistics/-/publication/{self.PUBLICATION_CODE}/{y}"
            try:
                download_url = get_download_url_by_title(
                    publication_url=candidate_url,
                    target_title=self.TARGET_TITLE_SUBSTRING,
                    headers=headers,
                )
                pub_url = candidate_url
                break
            except RuntimeError as e:
                last_err = e
                continue

        if download_url is None:
            raise RuntimeError(
                f"Could not find '{self.TARGET_TITLE_SUBSTRING}' on any year of "
                f"{self.PUBLICATION_CODE}. Last error: {last_err}"
            )

        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
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

        # QA: run once and check for any unmapped NACE codes printed to console.
        # Adjust NACE_TO_DB_COL in extract.py if ELSTAT uses variant code formats.
        print(f"Extracting GVA by sector data from {out_path}...")
        df_new = extract_gva(out_path)

        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        report_csv = output_dir / "update_report.csv"
        db_path = pp.baseline

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["year"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        sql_path = pp.sql("ed_gva_by_sector.sql")
        db_comp_res = compare_with_postgres(
            df=df_new,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year"],
            sync_cols=DB_COLS,
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

        target_cols = ["id", "year"] + DB_COLS

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)
            out = df.copy()
            out["id"] = pd.to_numeric(out.get("id"), errors="coerce").map(
                lambda x: "" if pd.isna(x) else str(int(x))
            )
            out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
            out = out.sort_values("year").reset_index(drop=True)
            for c in target_cols:
                if c not in out.columns:
                    out[c] = pd.NA
            return out[target_cols]

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
