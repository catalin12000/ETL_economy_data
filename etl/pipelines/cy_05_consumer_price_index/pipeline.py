from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

from etl.core.download import sha256_file
from etl.core.database import compare_with_postgres
from .extract import extract_cpi

class Pipeline:
    pipeline_id = "cy_05_consumer_price_index"
    display_name = "Cyprus: Consumer Price Index (Monthly)"

    # New dataset including Base Year 2025
    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Price%20Indices/Consumer%20Price%20Index/0410055E.px"

    @staticmethod
    def _build_deliverable_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["Year", "Month", "Base_Year", "Index"])

        out = df.copy()
        out = out[out["year"] >= 2024].copy()
        col_map = {
            "year": "Year",
            "month": "Month",
            "base_year": "Base_Year",
            "index": "Index",
        }
        out.rename(columns=col_map, inplace=True)
        out = out[["Year", "Month", "Base_Year", "Index"]]
        out.sort_values(["Year", "Month", "Base_Year"], inplace=True)
        return out

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "05"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

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
        
        # 2. Sync with live Cyprus Postgres DB (zeus)
        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        db_comp_res = compare_with_postgres(
            df=df_new,
            table_name="ed_consumer_price_index",
            db_name="zeus",
            match_cols=["year", "month", "base_year"],
            sync_cols=["index"],
            tolerance=0.11
        )
        
        # 3. Create Deliverable (2024 onwards)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        # If DB compare fails (schema/table mismatch, connection issue), fall back to extracted data.
        db_error = db_comp_res.get("error")
        if db_error:
            deliverable_df = self._build_deliverable_df(df_new)
            deliverable_df.to_csv(deliverable_path, index=False)
            print(f"DB compare failed; wrote extracted fallback with {len(deliverable_df)} rows: {deliverable_name}")
        else:
            inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
            updated_df = db_comp_res.get("updated_df", pd.DataFrame())
            delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
            deliverable_df = self._build_deliverable_df(delta_df)
            deliverable_df.to_csv(deliverable_path, index=False)
            print(f"Created DB-delta deliverable with {len(deliverable_df)} rows: {deliverable_name}")

        db_summary = {
            "status": db_comp_res.get("status", "error" if db_error else "success"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
            "error": db_error,
        }

        new_state.update({
            "db_comparison": db_summary,
            "deliverable_path": str(deliverable_path)
        })

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff.",
            "state": new_state
        }
