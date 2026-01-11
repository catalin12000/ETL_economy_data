from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from .extract import extract_cpi


class Pipeline:
    pipeline_id = "ed_consumer_price_index"
    display_name = "Ed Consumer Price Index"

    TARGET_TITLE_SUBSTRING = "Συγκρίσεις Γενικού Δείκτη Τιμών Καταναλωτή"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "05"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        xls_path = out_dir / "elstat_consumer_price_index.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url("DKT87", locale="el", frequency="monthly", headers=headers)
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE_SUBSTRING, headers=headers)

        meta = download_file(download_url, xls_path, headers=headers)
        file_hash = sha256_file(xls_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": xls_path.name,
            "last_download_path": str(xls_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        # 1. Extraction
        print(f"Extracting data from {xls_path}...")
        df_new = extract_cpi(xls_path)
        
        # 2. Sync with master DB (Read-Only Reference)
        db_path = Path("data/db") / f"{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"{prefix}_{self.pipeline_id}" / "update_report.csv"
        
        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(db_path, df_new, out_csv_full, report_csv)

        # 3. DB Comparison (READ-ONLY)
        print("Comparing extraction with live Postgres DB...")
        # Prepare DF for DB sync (needs 'year_over_year' match)
        df_for_db = df_new.copy()
        if "Year Over Year" in df_for_db.columns:
            df_for_db.rename(columns={"Year Over Year": "Year_over_Year"}, inplace=True)
            
        # Locate the SQL file for fetching DB state
        sql_path = Path(__file__).parent / "ed_consumer_price_index.sql"
            
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.pipeline_id,
            db_name="athena",
            match_cols=["year", "month"],
            sync_cols=["index", "year_over_year"],
            tolerance=0.05,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create "New Entries" deliverable
        output_file = output_dir / "new_entries.csv"

        # Snapshot (full updated DB)
        res.updated_df.to_csv(out_csv_full, index=False)

        # Deliverable (additions/updates only)
        res.diff_df.to_csv(output_file, index=False)
        
        # 5. User-Requested Timestamped Deliverable (DB Delta ONLY)
        import datetime
        now = datetime.datetime.now()
        month_name = now.strftime("%B") # e.g., "January"
        year_str = now.strftime("%Y")   # e.g., "2026"
        
        deliverable_name = f"deliverable_{self.pipeline_id}_{month_name}_{year_str}.csv"
        deliverable_path = output_dir / deliverable_name
        
        # Use ONLY the rows that are actually missing or different in Postgres
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = ['Year', 'Month', 'Index', 'Year_over_Year']
        
        if not delta_df.empty:
            # Map columns to the expected deliverable format
            col_map = {
                "year": "Year",
                "month": "Month",
                "index": "Index",
                "year_over_year": "Year_over_Year"
            }
            delta_df.rename(columns=col_map, inplace=True)
            
            for c in target_cols:
                if c not in delta_df.columns: delta_df[c] = pd.NA
            
            delta_df[target_cols].to_csv(deliverable_path, index=False)
        else:
            # If no changes are needed, create an empty file with headers
            pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)

        # Prepare state
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
