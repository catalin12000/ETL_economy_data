from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from .extract import extract_eu_gdp


class Pipeline:
    pipeline_id = "ed_eu_gdp"
    display_name = "Ed EU GDP (Eurostat)"

    DATASET_CODE = "namq_10_gdp"
    FILTER = "Q.CP_MEUR+CLV20_MEUR+CLV_PCH_PRE+CLV_PCH_SM.SCA.B1GQ.EL+RO+CY+EU27_2020+EA20+EA+EA19+EA12"
    FILE_URL = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{DATASET_CODE}/{FILTER}/?format=SDMX-CSV&compressed=false&startPeriod=2024-Q1"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "47"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"eurostat_{self.DATASET_CODE}.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta = download_file(self.FILE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": self.FILE_URL,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 1. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_eu_gdp(out_path)
        
        # 2. Sync with baseline DB (Reference)
        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path, 
            df_new, 
            out_csv_full, 
            report_csv, 
            key_cols=["Year", "Quarter", "Geopolitical Entity"]
        )

        # 3. DB Comparison (READ-ONLY)
        print("Comparing extraction with live Postgres DB...")
        df_for_db = df_new.copy()
        col_map = {
            "Geopolitical Entity": "geopolitical_entity",
            "Year": "year",
            "Quarter": "quarter",
            "Chain Linked Volumes": "chain_linked_volumes",
            "Quarter Over Quarter": "quarter_over_quarter",
            "Year Over Year": "year_over_year",
            "Current Prices": "current_prices"
        }
        df_for_db.rename(columns=col_map, inplace=True)
        
        sql_path = Path(__file__).parent / "ed_eu_gdp.sql"
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.pipeline_id,
            db_name="athena",
            match_cols=["geopolitical_entity", "year", "quarter"],
            sync_cols=[
                "chain_linked_volumes", "quarter_over_quarter", 
                "year_over_year", "current_prices"
            ],
            tolerance=0.05,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Save snapshot
        res.updated_df.to_csv(out_csv_full, index=False)
        
        # New Entries (Local delta)
        res.diff_df.to_csv(output_file, index=False)

        # 5. Timestamped Deliverable (DB Delta ONLY)
        import datetime
        now = datetime.datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        # Use ONLY the rows that are actually missing or different in Postgres
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Geopolitical_Entity', 'Year', 'Quarter', 'Chain_Linked_Volumes', 
            'Quarter_Over_Quarter', 'Year_Over_Year', 'Current_Prices'
        ]
        
        if not delta_df.empty:
            # Map back to Capitalized for deliverable
            rev_map = {
                "geopolitical_entity": "Geopolitical_Entity",
                "year": "Year",
                "quarter": "Quarter",
                "chain_linked_volumes": "Chain_Linked_Volumes",
                "quarter_over_quarter": "Quarter_Over_Quarter",
                "year_over_year": "Year_Over_Year",
                "current_prices": "Current_Prices"
            }
            delta_df.rename(columns=rev_map, inplace=True)
            for c in target_cols:
                if c not in delta_df.columns: delta_df[c] = pd.NA
            delta_df[target_cols].to_csv(deliverable_path, index=False)
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
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}", 
            "state": new_state
        }
