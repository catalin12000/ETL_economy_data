from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

from etl.core.download import sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_building_permits_district


class Pipeline:
    pipeline_id = "cy_02_building_permits_by_district"
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
        db_path = pp.baseline
        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = pp.output / "update_report.csv"
        
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path, 
            df_new, 
            out_csv_full, 
            report_csv, 
            key_cols=["Year", "Month", "District", "Urban_Rural"]
        )

        # 3. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()
        col_map = {
            "Year": "year",
            "Month": "month",
            "District": "district",
            "Urban_Rural": "urban_rural",
            "Area_m2": "area_m2",
            "Dwelling_Units": "dwelling_units",
            "Number": "number",
            "Value_000s": "value_000s"
        }
        df_for_db.rename(columns=col_map, inplace=True)
        
        sql_path = Path(__file__).parent / f"{self.pipeline_id}.sql"
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_building_permits_by_district",
            db_name="zeus",
            match_cols=["year", "month", "district", "urban_rural"],
            sync_cols=["area_m2", "dwelling_units", "number", "value_000s"],
            tolerance=0.11,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        # 5. Timestamped Deliverable (DB Delta ONLY, 2024+)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = ['ID', 'Year', 'Month', 'District', 'Urban_Rural', 'Area_m2', 'Dwelling_Units', 'Number', 'Value_000s']
        
        if not delta_df.empty:
            rev_map = {v: k for k, v in col_map.items()}
            delta_df.rename(columns=rev_map, inplace=True)
            if "id" in delta_df.columns:
                delta_df.rename(columns={"id": "ID"}, inplace=True)
            
            if "Year" in delta_df.columns:
                delta_df = delta_df[delta_df["Year"] >= 2023].copy()
            
            if not delta_df.empty:
                delta_df = delta_df.sort_values(["Year", "Month", "District", "Urban_Rural"]).reset_index(drop=True)
                for c in target_cols:
                    if c not in delta_df.columns: delta_df[c] = pd.NA
                delta_df["ID"] = pd.to_numeric(delta_df["ID"], errors="coerce")
                delta_df["ID"] = delta_df["ID"].map(lambda x: "" if pd.isna(x) else str(int(x)))
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
            "rows_before": res.rows_before,
            "rows_after": res.rows_after,
            "new_rows": res.new_rows,
            "updated_cells": res.updated_cells,
            "db_comparison": db_summary,
            "deliverable_path": str(deliverable_path),
            "delta_path": str(output_file),
            "mock_db_snapshot_path": str(out_csv_full),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0 and db_comp_res.get("inserted") == 0 and db_comp_res.get("updated") == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered", 
            "message": f"Extracted {len(df_new)} rows. DB (zeus) Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}", 
            "state": new_state
        }
