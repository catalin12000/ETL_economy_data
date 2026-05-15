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
from .extract import extract_key_partners_primary_goods


class Pipeline:
    pipeline_id = "ed_key_partners_primary_goods"
    display_name = "Key Partners - Primary Goods (SFC02) - Trade Balance Time Period"

    PUBLICATION_CODE = "SFC02"
    TARGET_TITLE_SUBSTRING = (
        "Imports - Arrivals, Exports - Dispatches in Value per Country by Standard International Trade Classification (SITC 1)"
    )
    LOOKBACK_MONTHS = 36

    @staticmethod
    def _prev_period(year: int, month: int) -> tuple[int, int]:
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        return year, month

    @staticmethod
    def _extract_period(pub_url: str) -> tuple[int, int]:
        period = pub_url.rstrip("/").split("/")[-1]
        y, m = period.split("-M")
        return int(y), int(m)

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_sfc02_sitc1_value_per_country.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        latest_pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="monthly",
            headers=headers,
        )

        year, month = self._extract_period(latest_pub_url)
        pub_url_used = latest_pub_url
        download_url = None
        lookback_used = 0

        from etl.core.elstat import BASE_URL
        for step in range(self.LOOKBACK_MONTHS + 1):
            candidate = f"{BASE_URL}/en/statistics/-/publication/{self.PUBLICATION_CODE}/{year}-M{month:02d}"
            try:
                found = get_download_url_by_title(
                    publication_url=candidate,
                    target_title=self.TARGET_TITLE_SUBSTRING,
                    headers=headers,
                )
                download_url = found
                pub_url_used = candidate
                lookback_used = step
                break
            except Exception:
                year, month = self._prev_period(year, month)

        if not download_url:
            return {
                "status": "error",
                "message": (
                    "Could not find SITC-1 table link within lookback window. "
                    f"Title contains: {self.TARGET_TITLE_SUBSTRING}"
                ),
                "state": dict(state),
            }

        download_note = None
        try:
            meta = download_file(download_url, out_path, headers=headers)
        except PermissionError:
            if not out_path.exists():
                raise
            meta = {"downloaded_at_utc": state.get("downloaded_at_utc")}
            download_note = "Used existing local workbook because the source file was locked."

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "publication_code": self.PUBLICATION_CODE,
                "latest_publication_url": latest_pub_url,
                "publication_url_used": pub_url_used,
                "lookback_months_used": lookback_used,
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

        print("Extracting key partners primary goods data...")
        df_new = extract_key_partners_primary_goods(out_path)

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
            key_cols=["year", "country", "categories"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        sql_path = pp.sql("ed_key_partners_primary_goods.sql")
        db_comp_res = compare_with_postgres(
            df=df_new,
            table_name="ed_key_partners_primary_goods",
            db_name="athena",
            match_cols=["year", "country", "categories"],
            sync_cols=["imports_value", "exports_value", "codes"],
            tolerance=1.0,
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
            "year",
            "imports_value",
            "exports_value",
            "categories",
            "country",
            "codes",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(columns={"id": "ID"}).copy()
            shaped["ID"] = pd.to_numeric(shaped["ID"], errors="coerce")
            shaped["year"] = pd.to_numeric(shaped["year"], errors="coerce")
            shaped = shaped.dropna(subset=["year", "country"]).copy()
            shaped["year"] = shaped["year"].astype(int)
            shaped = shaped.sort_values(["year", "country", "categories"]).reset_index(drop=True)
            shaped["ID"] = shaped["ID"].map(lambda x: "" if pd.isna(x) else str(int(x)))

            for column in ["imports_value", "exports_value"]:
                if column in shaped.columns:
                    shaped[column] = pd.to_numeric(shaped[column], errors="coerce").map(
                        lambda x: "" if pd.isna(x) else f"{float(x):.3f}"
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
