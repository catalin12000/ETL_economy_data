from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

from etl.core.download import sha256_file
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_cpi

class Pipeline:
    pipeline_id = "cy_05_consumer_price_index"
    country = "cy"
    source = "cystat"
    db_table_name = "ed_consumer_price_index"
    display_name = "Cyprus: Consumer Price Index (Monthly)"
    DB_TABLE_NAME = "ed_consumer_price_index"
    DB_BASE_YEAR = 2025
    DELIVERABLE_FROM_YEAR = 2024

    # New dataset including Base Year 2025
    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Price%20Indices/Consumer%20Price%20Index/0410055E.px"

    @staticmethod
    def _prepare_primary_series(df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep CPI series for configured base year and derive YoY.
        """
        if df is None or df.empty:
            return pd.DataFrame(columns=["year", "month", "index", "year_over_year"])

        out = (
            df[df["base_year"] == Pipeline.DB_BASE_YEAR][["year", "month", "index"]]
            .copy()
            .sort_values(["year", "month"])
            .reset_index(drop=True)
        )
        out["year_over_year"] = out["index"].pct_change(12) * 100.0
        out["year_over_year"] = pd.to_numeric(out["year_over_year"], errors="coerce").round(2)
        return out

    @staticmethod
    def _build_deliverable_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["id", "Year", "Month", "Index", "Year_Over_Year"])

        out = df.copy()
        out = out[out["year"] >= Pipeline.DELIVERABLE_FROM_YEAR].copy()
        col_map = {
            "id": "id",
            "year": "Year",
            "month": "Month",
            "index": "Index",
            "year_over_year": "Year_Over_Year",
        }
        out.rename(columns=col_map, inplace=True)
        if "id" not in out.columns:
            out["id"] = pd.NA
        else:
            out["id"] = pd.to_numeric(out["id"], errors="coerce").astype("Int64")
        if "Year_Over_Year" not in out.columns:
            out["Year_Over_Year"] = pd.NA
        out = out[["id", "Year", "Month", "Index", "Year_Over_Year"]]
        out.sort_values(["Year", "Month"], inplace=True)
        return out

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "cystat_cpi_raw.csv"

        # Query all months and all base years
        query = {
            "query": [
                {
                    "code": "MONTH",
                    "selection": {
                        "filter": "all",
                        "values": ["*"]
                    }
                },
                {
                    "code": "BASE YEAR",
                    "selection": {
                        "filter": "all",
                        "values": ["*"]
                    }
                }
            ],
            "response": {
                "format": "csv"
            }
        }

        headers = {"User-Agent": "Mozilla/5.0"}
        
        print(f"Requesting CPI data from CYSTAT API...")
        response = requests.post(self.API_URL, json=query, headers=headers, timeout=60)
        response.raise_for_status()

        # Save the raw CSV
        with open(out_path, "wb") as f:
            f.write(response.content)

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "api_url_used": self.API_URL,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        # 1. Extraction
        print(f"Extracting CPI data from {out_path}...")
        df_new = extract_cpi(out_path)
        df_primary = self._prepare_primary_series(df_new)

        # 1b. Local baseline comparison (for mock snapshot + new entries files)
        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        output_file = output_dir / "new_entries.csv"
        db_path = pp.baseline
        report_csv = pp.output / "update_report.csv"

        df_local = (
            df_primary.rename(
                columns={
                    "year": "Year",
                    "month": "Month",
                    "index": "Index",
                    "year_over_year": "Year Over Year",
                }
            )[["Year", "Month", "Index", "Year Over Year"]]
            .copy()
        )

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
        
        # 2. Sync with live Cyprus Postgres DB (zeus)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        sql_path = pp.sql("cy_05_consumer_price_index.sql")
        db_comp_res = compare_with_postgres(
            df=df_primary,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year", "month"],
            sync_cols=["index", "year_over_year"],
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )
        
        # 3. Create Deliverable (2024 onwards)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        # If DB compare fails (schema/table mismatch, connection issue), fall back to extracted data.
        db_error = db_comp_res.get("error")
        if db_error:
            deliverable_df = self._build_deliverable_df(df_primary)
            write_deliverable_csv(deliverable_df, deliverable_path)
            print(f"DB compare failed; wrote extracted fallback with {len(deliverable_df)} rows: {deliverable_name}")
        else:
            inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
            updated_df = db_comp_res.get("updated_df", pd.DataFrame())
            delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
            deliverable_df = self._build_deliverable_df(delta_df)
            write_deliverable_csv(deliverable_df, deliverable_path)
            print(f"Created DB-delta deliverable with {len(deliverable_df)} rows: {deliverable_name}")

        db_summary = {
            "status": db_comp_res.get("status", "error" if db_error else "success"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
            "compare_base_year": self.DB_BASE_YEAR,
            "error": db_error,
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

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff.",
            "state": new_state
        }
