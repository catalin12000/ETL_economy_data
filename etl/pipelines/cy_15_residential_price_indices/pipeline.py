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
from .extract import extract_rppi


class Pipeline:
    pipeline_id = "cy_15_residential_price_indices"
    country = "cy"
    source = "central_bank_cy"
    db_table_name = "ed_residential_price_indices"
    source_type = "static_file"
    display_name = "Cyprus: Residential Property Price Indices (RPPI)"

    # Page listing the RPPI data
    SOURCE_PAGE = "https://www.centralbank.cy/en/publications/residential-property-price-indices"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get page to find the latest "Data series" link
        print(f"Finding latest RPPI Data series on CBC...")
        r_page = requests.get(self.SOURCE_PAGE, headers=headers, timeout=30)
        r_page.raise_for_status()
        soup = BeautifulSoup(r_page.text, "html.parser")
        
        target_xls_url = None
        for a in soup.find_all("a", href=True):
            text_val = a.get_text().strip().lower()
            if "data series" in text_val and (".xls" in a["href"].lower() or ".xlsx" in a["href"].lower()):
                if a["href"].startswith("http"):
                    target_xls_url = a["href"]
                else:
                    target_xls_url = "https://www.centralbank.cy" + a["href"]
                break
        
        if not target_xls_url:
            return {"status": "error", "message": "Could not find RPPI Data series Excel link on CBC page.", "state": state}

        print(f"Found RPPI file: {target_xls_url}")
        
        # Determine extension
        ext = ".xlsx" if ".xlsx" in target_xls_url.lower() else ".xls"
        out_path = out_dir / f"cbc_rppi_data_series{ext}"

        # 2) Download + hash
        meta = download_file(target_xls_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": target_xls_url,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        # 3. Extraction
        print(f"Extracting data from {out_path}...")
        df_new = extract_rppi(out_path)
        
        # 4. Sync with baseline DB (Local Reference)
        output_dir = pp.output
        

        # 5. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()

        sql_path = pp.sql("ed_residential_price_indices.sql")

        sync_cols = [c for c in df_for_db.columns if c not in ["year", "quarter"]]
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year", "quarter"],
            sync_cols=sync_cols,
            tolerance=0.11,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 6. Create Deliverables

        # 7. Timestamped Deliverable (DB Delta ONLY, 2023+, Long Format)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_db_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        # DB-shaped deliverable: one row per (Year, Quarter), one column per (Location, Residence_type).
        index_cols = [
            "residential_price_property_price_index",
            "apartments_cy",
            "houses_cy",
            "nicosia_residential",
            "limassol_residential",
            "larnaca_residential",
            "paphos_residential",
            "famagusta_residential",
            "nicosia_apartments",
            "limassol_apartments",
            "larnaca_apartments",
            "paphos_apartments",
            "famagusta_apartments",
            "nicosia_houses",
            "limassol_houses",
            "larnaca_houses",
            "paphos_houses",
            "famagusta_houses",
        ]
        target_cols = ["id", "year", "quarter"] + index_cols

        if not delta_db_df.empty:
            delta_db_df = delta_db_df[delta_db_df["year"] >= 2023].copy()

        if not delta_db_df.empty:
            if "id" in delta_db_df.columns:
                delta_db_df["id"] = pd.to_numeric(delta_db_df["id"], errors="coerce").astype("Int64")
            else:
                delta_db_df["id"] = pd.NA
            delta_db_df["year"] = pd.to_numeric(delta_db_df["year"], errors="coerce").astype("Int64")
            delta_db_df["quarter"] = pd.to_numeric(delta_db_df["quarter"], errors="coerce").astype("Int64")

            for c in index_cols:
                if c in delta_db_df.columns:
                    delta_db_df[c] = pd.to_numeric(delta_db_df[c], errors="coerce")

            delta_db_df = delta_db_df.sort_values(["year", "quarter"]).reset_index(drop=True)

            for c in target_cols:
                if c not in delta_db_df.columns:
                    delta_db_df[c] = pd.NA
            write_deliverable_csv(delta_db_df[target_cols], deliverable_path)
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
            "message": f"Extracted {len(df_new)} rows. DB (zeus) Comparison ready. File: {deliverable_name}", 
            "state": new_state
        }
