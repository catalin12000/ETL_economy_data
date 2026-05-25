from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

from etl.core.download import sha256_file, is_new_by_hash
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_building_permits_district


class Pipeline:
    pipeline_id = "cy_02_building_permits_by_district"
    country = "cy"
    source = "cystat"
    db_table_name = "ed_building_permits_by_district"
    source_type = "api"
    display_name = "Cyprus: Building Permits by District (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Building%20Permits/1440010E.px"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "cystat_data.csv"

        # PxWeb API Query: Using CSV format for easier extraction
        query = {
            "query": [
                { "code": "MEASUREMENT", "selection": { "filter": "item", "values": ["0"] } },
                { "code": "REFERENCE PERIOD", "selection": { "filter": "item", "values": ["0"] } }
            ],
            "response": { "format": "csv" }
        }

        headers = {"User-Agent": "Mozilla/5.0"}
        print(f"Requesting data from CYSTAT API (CSV)...")
        response = requests.post(self.API_URL, json=query, headers=headers, timeout=60)
        response.raise_for_status()

        with open(out_path, "wb") as f:
            f.write(response.content)

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": self.API_URL,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        # 1. Extraction
        print(f"Extracting data from CSV...")
        df_new = extract_building_permits_district(out_path)
        
        # 2. Sync with baseline DB (Local Reference)
        output_dir = pp.output
        

        # 3. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()

        sql_path = Path(__file__).parent / f"{self.pipeline_id}.sql"
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year", "month", "district", "urban_rural"],
            sync_cols=["area_m2", "dwelling_units", "number", "value_000s"],
            tolerance=0.11,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create Deliverables

        # 5. Timestamped Deliverable (DB Delta ONLY, 2024+)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = ['id', 'year', 'month', 'district', 'urban_rural', 'area_m2', 'dwelling_units', 'number', 'value_000s']

        if not delta_df.empty:
            if "year" in delta_df.columns:
                delta_df = delta_df[delta_df["year"] >= 2023].copy()

            if not delta_df.empty:
                delta_df = delta_df.sort_values(["year", "month", "district", "urban_rural"]).reset_index(drop=True)
                for c in target_cols:
                    if c not in delta_df.columns: delta_df[c] = pd.NA
                delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce")
                delta_df["id"] = delta_df["id"].map(lambda x: "" if pd.isna(x) else str(int(x)))
                write_deliverable_csv(delta_df[target_cols], deliverable_path)
                print(f"Created filtered deliverable: {deliverable_name}")
            else:
                write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated")
        }

        new_state.update({
            "db_comparison": db_summary,
            "deliverable_path": str(deliverable_path),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and db_comp_res.get("inserted") == 0 and db_comp_res.get("updated") == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered", 
            "message": f"Extracted {len(df_new)} rows. DB (zeus) Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}", 
            "state": new_state
        }
