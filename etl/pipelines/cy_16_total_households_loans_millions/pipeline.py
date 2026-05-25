from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.database import compare_with_postgres
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_households_loans


class Pipeline:
    pipeline_id = "cy_16_total_households_loans_millions"
    country = "cy"
    source = "central_bank_cy"
    db_table_name = "ed_total_households_loans_millions"
    source_type = "static_file"
    display_name = "Cyprus: Total Households Loans Millions (NPLs)"

    # Page listing the aggregate banking sector data
    SOURCE_PAGE = "https://www.centralbank.cy/en/licensing-supervision/banks/aggregate-cyprus-banking-sector-data"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get page to find the latest "non-performing loans" link
        print(f"Finding latest NPLs file on CBC...")
        r_page = requests.get(self.SOURCE_PAGE, headers=headers, timeout=30)
        r_page.raise_for_status()
        soup = BeautifulSoup(r_page.text, "html.parser")
        
        target_xls_url = None
        for a in soup.find_all("a", href=True):
            text_val = a.get_text().strip().lower()
            href = a["href"].lower()
            if "non-performing" in text_val and (".xls" in href or ".xlsx" in href):
                if a["href"].startswith("http"):
                    target_xls_url = a["href"]
                else:
                    target_xls_url = "https://www.centralbank.cy" + a["href"]
                break
        
        if not target_xls_url:
            return {"status": "error", "message": "Could not find 'non-performing loans' Excel link on CBC page.", "state": state}

        print(f"Found NPL file: {target_xls_url}")
        
        # Determine extension
        ext = ".xlsx" if ".xlsx" in target_xls_url.lower() else ".xls"
        out_path = out_dir / f"cbc_aggregate_npls{ext}"

        # 2) Download + hash
        meta = download_file(target_xls_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": target_xls_url,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        # 3. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_households_loans(out_path)
        
        # 4. Sync with Baseline DB (Local Reference)
        output_dir = pp.output
        

        # 5. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()

        sync_cols = [
            "total_loans",
            "total_households_loans",
            "non_performing_loans",
            "loan_amounts_past_90_days",
            "restructured_loans_forbearance",
            "non_performing_restructured_loans",
        ]

        sql_path = pp.sql("ed_total_households_loans_millions.sql")

        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year", "month"],
            sync_cols=sync_cols,
            tolerance=0.11,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 6. Create Deliverables

        # 7. Timestamped Deliverable (DB Delta ONLY, 2024+)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'year', 'month', 'total_loans', 'total_households_loans',
            'non_performing_loans', 'loan_amounts_past_90_days',
            'restructured_loans_forbearance', 'non_performing_restructured_loans',
        ]

        if not delta_df.empty:
            # Apply Filter: Only 2023 onwards
            if "year" in delta_df.columns:
                delta_df = delta_df[delta_df["year"] >= 2023].copy()

            if not delta_df.empty:
                delta_df = delta_df.sort_values(["year", "month"]).reset_index(drop=True)
                for c in target_cols:
                    if c not in delta_df.columns: delta_df[c] = pd.NA
                write_deliverable_csv(delta_df[target_cols], deliverable_path)
                print(f"Created filtered deliverable with {len(delta_df)} rows: {deliverable_name}")
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
