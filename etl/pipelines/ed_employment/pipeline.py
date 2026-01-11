from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from .extract import extract_employment

class Pipeline:
    pipeline_id = "ed_employment"
    display_name = "Employment Status & Unemployment Rate"

    TARGET_TITLE_SUBSTRING = "Κατάσταση απασχόλησης και ποσοστό ανεργίας"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "08"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "ed_employment.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url(publication_code="SJO02", locale="el", frequency="monthly", headers=headers)
        download_url = get_download_url_by_title(publication_url=pub_url, target_title=self.TARGET_TITLE_SUBSTRING, headers=headers)

        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 1. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_employment(out_path)
        
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
            key_cols=["Year", "Month", "Seasonally"]
        )

        # 3. DB Comparison (READ-ONLY)
        print("Comparing extraction with live Postgres DB...")
        # Prepare DF for DB comparison (standardize column names)
        df_for_db = df_new.copy()
        col_map = {
            "Year": "year",
            "Month": "month",
            "Seasonally": "seasonally",
            "Employed 000s": "employed_000s",
            "Unemployed 000s": "unemployed_000s",
            "Inactives 000s": "inactives_000s",
            "Adjusted_Unemployment_Rate": "adjusted_unemployment_rate",
            "Unadjusted_Unemployment_Rate": "unadjusted_unemployment_rate"
        }
        df_for_db.rename(columns=col_map, inplace=True)
        
        sql_path = Path(__file__).parent / "ed_employment.sql"
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.pipeline_id,
            db_name="athena",
            match_cols=["year", "month", "seasonally"],
            sync_cols=[
                "employed_000s", "unemployed_000s", "inactives_000s",
                "adjusted_unemployment_rate", "unadjusted_unemployment_rate"
            ],
            tolerance=0.05,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        
        # Snapshot (Full updated DB)
        res.updated_df.to_csv(out_csv_full, index=False)
        
        # Additions/Updates only (Delta)
        res.diff_df.to_csv(output_file, index=False)

        # 5. Timestamped Deliverable (DB Delta ONLY)
        import datetime
        now = datetime.datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Year', 'Month', 'Seasonally', 'Employed_000s', 'Unemployed_000s', 
            'Inactives_000s', 'Adjusted_Unemployment_Rate', 'Unadjusted_Unemployment_Rate'
        ]
        
        if not delta_df.empty:
            # Map database columns (lowercase) back to Capitalized for deliverable
            rev_col_map = {
                "year": "Year",
                "month": "Month",
                "seasonally": "Seasonally",
                "employed_000s": "Employed_000s",
                "unemployed_000s": "Unemployed_000s",
                "inactives_000s": "Inactives_000s",
                "adjusted_unemployment_rate": "Adjusted_Unemployment_Rate",
                "unadjusted_unemployment_rate": "Unadjusted_Unemployment_Rate"
            }
            delta_df.rename(columns=rev_col_map, inplace=True)
            
            for c in target_cols:
                if c not in delta_df.columns:
                    delta_df[c] = pd.NA
            
            delta_df[target_cols].to_csv(deliverable_path, index=False)
        else:
            # If no changes were made to the DB, create an empty file with headers
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
            "state": new_state,
        }