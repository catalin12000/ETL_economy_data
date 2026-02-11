from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests
import pandas as pd

from etl.core.download import sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from .extract import extract_building_permits_type


class Pipeline:
    pipeline_id = "cy_03_building_permits_by_property_type"
    display_name = "Cyprus: Building Permits by Property Type (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Building%20Permits/1440005E.px"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "03"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "cystat_data.csv"

        # PxWeb API Query: Focus on Monthly Number of permits
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
        df_new = extract_building_permits_type(out_path)
        
        # 2. Sync with baseline DB (Local Reference)
        db_path = Path("data/db") / f"cy_{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"cy_{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path, 
            df_new, 
            out_csv_full, 
            report_csv, 
            key_cols=["Year", "Month"]
        )

        # 3. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()
        
        # Map columns to lowercase for DB comparison
        col_map = {c: c.lower() for c in df_for_db.columns}
        df_for_db.rename(columns=col_map, inplace=True)
        
        sql_path = Path(__file__).parent / "ed_building_permits_by_property_type.sql"
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_building_permits_by_property_type",
            db_name="zeus",
            match_cols=["year", "month"],
            sync_cols=[c for c in col_map.values() if c not in ["year", "month"]],
            tolerance=0.11,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        # 5. Timestamped Deliverable (DB Delta ONLY, 2023+)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Year', 'Month', 'single_houses', 'buildings_with_two_housing_units',
            'residential_apartment_blocks', 'residential_commercial_apartment_blocks',
            'cottage_apartment_complexes', 'residencies_for_communities', 'hotels',
            'tourist_apartments_and_villages', 'restaurants_coffee_bars',
            'other_tourist_accommodation', 'office_buildings',
            'wholesale_retail_buildings', 'transport_communication_buildings',
            'industrial_buildings_and_warehouses',
            'public_entertainment_educational_medical',
            'other_non_residential_buildings', 'civil_engineering',
            'division_of_plots', 'road_construction', 'permits'
        ]
        
        if not delta_df.empty:
            # Map back to specific target names if needed, but here we use the lowercase ones from DB
            rev_map = { "year": "Year", "month": "Month" }
            delta_df.rename(columns=rev_map, inplace=True)
            
            if "Year" in delta_df.columns:
                delta_df = delta_df[delta_df["Year"] >= 2023].copy()
            
            if not delta_df.empty:
                delta_df = delta_df.sort_values(["Year", "Month"]).reset_index(drop=True)
                # Filter to target_cols
                # Note: target_cols uses lowercase except Year/Month
                actual_targets = ["Year", "Month"] + [c for c in target_cols if c not in ["Year", "Month"]]
                for c in actual_targets:
                    if c not in delta_df.columns: delta_df[c] = pd.NA
                delta_df[actual_targets].to_csv(deliverable_path, index=False)
                print(f"Created filtered deliverable: {deliverable_name}")
            else:
                pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)
        else:
            pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)

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
