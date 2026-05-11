from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.output import write_deliverable_csv
from .extract import extract_economic_sentiment_indicator


class Pipeline:
    pipeline_id = "ed_economic_sentiment_indicator"
    display_name = "Ed Economic Sentiment Indicator (Eurostat)"

    DATASET_CODE = "teibs010"
    FILTER = "M.BS-ESI-I.SA.EL+RO+CY+EU27_2020+EA20"
    FILE_URL = (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
        f"{DATASET_CODE}/{FILTER}/?format=SDMX-CSV&compressed=false&startPeriod=2025-01"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "53"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"eurostat_{self.DATASET_CODE}.csv"
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta = download_file(self.FILE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "source_url_used": self.FILE_URL,
                "file_sha256": file_hash,
                "downloaded_filename": out_path.name,
                "last_download_path": str(out_path),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "content_length": meta.get("content_length"),
                "final_url": meta.get("final_url"),
            }
        )

        print(f"Extracting data from {out_path}...")
        df_new = extract_economic_sentiment_indicator(out_path)

        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        db_diff_only_path = output_dir / "db_differences_only.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_csv_path=db_path,
            extracted_df=df_new,
            out_csv_path=out_csv_full,
            report_csv_path=report_csv,
            key_cols=["Year", "Month", "Geopolitical Entity"],
        )
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        print("Comparing extraction with live Postgres DB (athena)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Month": "month",
                "Geopolitical Entity": "geopolitical_entity",
                "Economic Sentiment Indicator": "economic_sentiment_indicator",
            }
        )
        sql_path = Path(__file__).parent / "ed_economic_sentiment_indicator.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_economic_sentiment_indicator",
            db_name="athena",
            match_cols=["year", "month", "geopolitical_entity"],
            sync_cols=["economic_sentiment_indicator"],
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
            "ID",
            "Year",
            "Month",
            "Geopolitical_Entity",
            "Economic_Sentiment_Indicator",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)

            shaped = df.rename(
                columns={
                    "id": "ID",
                    "year": "Year",
                    "month": "Month",
                    "geopolitical_entity": "Geopolitical_Entity",
                    "economic_sentiment_indicator": "Economic_Sentiment_Indicator",
                }
            ).copy()
            shaped["ID"] = pd.to_numeric(shaped["ID"], errors="coerce")
            shaped["Year"] = pd.to_numeric(shaped["Year"], errors="coerce")
            shaped["Month"] = pd.to_numeric(shaped["Month"], errors="coerce")
            shaped["Economic_Sentiment_Indicator"] = pd.to_numeric(
                shaped["Economic_Sentiment_Indicator"], errors="coerce"
            )
            shaped = shaped.dropna(subset=["Year", "Month", "Geopolitical_Entity"]).copy()
            shaped["Year"] = shaped["Year"].astype(int)
            shaped["Month"] = shaped["Month"].astype(int)
            shaped = shaped.sort_values(["Year", "Month", "Geopolitical_Entity"]).reset_index(drop=True)
            shaped["ID"] = shaped["ID"].map(lambda x: "" if pd.isna(x) else str(int(x)))
            shaped["Economic_Sentiment_Indicator"] = shaped["Economic_Sentiment_Indicator"].map(
                lambda x: "" if pd.isna(x) else f"{float(x):.1f}"
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
            ),
            "state": new_state,
        }
