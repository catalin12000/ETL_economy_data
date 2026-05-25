from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import datetime

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.database import compare_with_postgres
from etl.core.fingerprint import dataframe_sha256
from etl.core.output import write_deliverable_csv
from etl.pipelines.ed_economic_forecast.extract import extract_forecast_deliverable
from etl.core.paths import PipelinePaths


class Pipeline:
    pipeline_id = "ed_economic_forecast"
    country = "gr"
    source = "eurostat"
    db_table_name = "ed_economic_forecast"
    source_type = "static_file"
    display_name = "EU Economic Forecast - Greece"

    SOURCE_URL = (
        "https://economy-finance.ec.europa.eu/"
        "economic-surveillance-eu-member-states/"
        "country-pages/greece/economic-forecast-greece_en"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        # 1) Download HTML
        out_dir = pp.downloaded
        html_path = out_dir / "economic_forecast_greece.html"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"}
        meta = download_file(self.SOURCE_URL, html_path, headers=headers)
        file_hash = sha256_file(html_path)

        new_state = dict(state)
        new_state.update(
            {
                "source_url_used": self.SOURCE_URL,
                "file_sha256": file_hash,
                "downloaded_filename": html_path.name,
                "last_download_path": str(html_path),
                "last_modified": meta.get("last_modified"),
                "etag": meta.get("etag"),
                "content_length": meta.get("content_length"),
                "final_url": meta.get("final_url"),
                "downloaded_at_utc": meta.get("downloaded_at_utc"),
            }
        )

        # 2) Extract deliverable-shaped table
        print(f"Extracting forecast data from {html_path}...")
        df_new = extract_forecast_deliverable(html_path)

        # 3) Data hash
        data_hash = dataframe_sha256(df_new, sort_cols=["Year"])

        # 4) Sync with local baseline
        output_dir = pp.output

        # 5) Compare with live Postgres (READ-ONLY)
        print("Comparing extraction with live Postgres DB...")
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

        sql_path = pp.sql("ed_economic_forecast.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
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
        print(
            "Postgres comparison result: "
            f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different."
        )

        # 6) Write deliverable in required format (full table)
        now = datetime.datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        write_deliverable_csv(df_new, deliverable_path)
        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
        }

        new_state.update(
            {
                "data_sha256": data_hash,
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
            }
        )

        no_source_change = not is_new_by_hash(state.get("file_sha256"), file_hash)
        no_data_change = state.get("data_sha256") == data_hash
        no_db_change = db_comp_res.get("inserted", 0) == 0 and db_comp_res.get("updated", 0) == 0

        if no_source_change and no_data_change and no_db_change:
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
