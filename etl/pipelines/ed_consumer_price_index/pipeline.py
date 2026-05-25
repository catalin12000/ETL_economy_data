from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_cpi


class Pipeline:
    pipeline_id = "ed_consumer_price_index"
    country = "gr"
    source = "elstat"
    db_table_name = "ed_consumer_price_index"
    source_type = "dynamic_file"
    display_name = "Ed Consumer Price Index"

    TARGET_TITLE_SUBSTRING = "Συγκρίσεις Γενικού Δείκτη Τιμών Καταναλωτή"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
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
        output_dir = pp.output
        

        # 3. DB Comparison (READ-ONLY)
        print("Comparing extraction with live Postgres DB...")
        # Prepare DF for DB sync (needs 'year_over_year' match)
        df_for_db = df_new.copy()
            
        # Locate the SQL file for fetching DB state
        sql_path = pp.sql("ed_consumer_price_index.sql")
            
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="athena",
            match_cols=["year", "month"],
            sync_cols=["index", "year_over_year"],
            tolerance=0.05,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 4. Create "New Entries" deliverable

        # Snapshot (full updated DB)

        # Deliverable (additions/updates only)
        
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
        
        target_cols = ['year', 'month', 'index', 'year_over_year']

        if not delta_df.empty:
            for c in target_cols:
                if c not in delta_df.columns: delta_df[c] = pd.NA

            write_deliverable_csv(delta_df[target_cols], deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
        # Prepare state
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
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. File: {deliverable_name}", 
            "state": new_state
        }
