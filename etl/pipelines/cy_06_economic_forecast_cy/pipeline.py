from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime

import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.fingerprint import dataframe_sha256
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from etl.pipelines.cy_06_economic_forecast_cy.extract import extract_forecast_deliverable
from etl.core.paths import PipelinePaths


class Pipeline:
    pipeline_id = "cy_06_economic_forecast_cy"
    country = "cy"
    source = "eurostat"
    db_table_name = "ed_economic_forecast_cy"
    source_type = "static_file"
    display_name = "Cyprus: Economic Forecast (EU Commission)"

    SOURCE_URL = (
        "https://economy-finance.ec.europa.eu/"
        "economic-surveillance-eu-member-states/"
        "country-pages/cyprus/economic-forecast-cyprus_en"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "economic_forecast_cyprus.html"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"}
        
        meta = download_file(self.SOURCE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": self.SOURCE_URL,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 2) Extract deliverable-shaped table
        print(f"Extracting forecast data from {out_path}...")
        df_new = extract_forecast_deliverable(out_path)

        # 3) Data hash
        data_hash = dataframe_sha256(df_new, sort_cols=["Year"])

        # 4) Sync with local baseline
        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        db_path = pp.baseline
        report_csv = pp.output / "update_report.csv"

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

        # 5) Compare with live Postgres (READ-ONLY)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Gdp Growth": "gdp_growth",
                "Inflation": "inflation",
                "Unemployment": "unemployment",
                "General Government Balance": "general_government_balance",
                "Gross Public Debt": "gross_public_debt",
                "Current Account Balance": "current_account_balance",
            }
        ).copy()

        sql_path = pp.sql("ed_economic_forecast_cy.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year"],
            sync_cols=[
                "gdp_growth",
                "inflation",
                "unemployment",
                "general_government_balance",
                "gross_public_debt",
                "current_account_balance",
            ],
            tolerance=0.05,
            sql_file_path=str(sql_path),
        )
        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            "Postgres comparison result: "
            f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different."
        )

        # 6) Write DB-delta deliverable with ID
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "id",
            "Year",
            "Gdp Growth",
            "Inflation",
            "Unemployment",
            "General Government Balance",
            "Gross Public Debt",
            "Current Account Balance",
        ]
        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "id": "id",
                    "year": "Year",
                    "gdp_growth": "Gdp Growth",
                    "inflation": "Inflation",
                    "unemployment": "Unemployment",
                    "general_government_balance": "General Government Balance",
                    "gross_public_debt": "Gross Public Debt",
                    "current_account_balance": "Current Account Balance",
                }
            )
            if "id" in delta_df.columns:
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce").astype("Int64")
            for c in target_cols:
                if c not in delta_df.columns:
                    delta_df[c] = pd.NA
            write_deliverable_csv(delta_df[target_cols].sort_values(["Year"]), deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
        }

        new_state.update(
            {
                "data_sha256": data_hash,
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

        no_source_change = not is_new_by_hash(state.get("file_sha256"), file_hash)
        no_data_change = state.get("data_sha256") == data_hash
        no_local_change = res.new_rows == 0 and res.updated_cells == 0
        no_db_change = db_comp_res.get("inserted", 0) == 0 and db_comp_res.get("updated", 0) == 0

        if no_source_change and no_data_change and no_local_change and no_db_change:
            return {
                "status": "skipped",
                "message": f"No new data detected. Deliverable refreshed: {deliverable_name}",
                "state": new_state,
            }

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows. "
                f"DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
