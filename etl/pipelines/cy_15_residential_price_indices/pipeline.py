from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from .extract import extract_rppi


class Pipeline:
    pipeline_id = "cy_15_residential_price_indices"
    display_name = "Cyprus: Residential Property Price Indices (RPPI)"

    # Page listing the RPPI data
    SOURCE_PAGE = "https://www.centralbank.cy/en/publications/residential-property-price-indices"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "15"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

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
            key_cols=["Year", "Quarter"]
        )

        # 5. DB Comparison (READ-ONLY - ZEUS DB)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.copy()
        
        # Map columns to lowercase for DB comparison (Already lowercase in extract.py except Year/Quarter)
        df_for_db.columns = [c.lower() for c in df_for_db.columns]
        
        sql_path = Path(__file__).parent / "ed_residential_price_indices.sql"
        
        sync_cols = [c for c in df_for_db.columns if c not in ["year", "quarter"]]
        
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_residential_price_indices",
            db_name="zeus",
            match_cols=["year", "quarter"],
            sync_cols=sync_cols,
            tolerance=0.11,
            sql_file_path=str(sql_path)
        )
        print(f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} different.")

        # 6. Create Deliverables
        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        # 7. Timestamped Deliverable (DB Delta ONLY, 2023+, Long Format)
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_db_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        if not delta_db_df.empty:
            # Filtering for 2023+
            delta_db_df = delta_db_df[delta_db_df["year"] >= 2023].copy()
            
            if not delta_db_df.empty:
                print("Transforming delta to long deliverable format...")
                
                # Unpivot Logic
                # DB columns map to (Location, Residence_type)
                VAL_MAP = {
                    "residential_price_property_price_index": ("Cyprus", "Residential"),
                    "apartments_cy": ("Cyprus", "Apartments"),
                    "houses_cy": ("Cyprus", "Houses"),
                    "nicosia_residential": ("Nicosia", "Residential"),
                    "limassol_residential": ("Limassol", "Residential"),
                    "larnaca_residential": ("Larnaca", "Residential"),
                    "paphos_residential": ("Paphos", "Residential"),
                    "famagusta_residential": ("Famagusta", "Residential"),
                    "nicosia_apartments": ("Nicosia", "Apartments"),
                    "limassol_apartments": ("Limassol", "Apartments"),
                    "larnaca_apartments": ("Larnaca", "Apartments"),
                    "paphos_apartments": ("Paphos", "Apartments"),
                    "famagusta_apartments": ("Famagusta", "Apartments"),
                    "nicosia_houses": ("Nicosia", "Houses"),
                    "limassol_houses": ("Limassol", "Houses"),
                    "larnaca_houses": ("Larnaca", "Houses"),
                    "paphos_houses": ("Paphos", "Houses"),
                    "famagusta_houses": ("Famagusta", "Houses")
                }
                
                long_rows = []
                for _, row in delta_db_df.iterrows():
                    for col, (loc, res_type) in VAL_MAP.items():
                        if col in row and pd.notna(row[col]):
                            long_rows.append({
                                "Year": int(row["year"]),
                                "Quarter": int(row["quarter"]),
                                "Location": loc,
                                "Residence_type": res_type,
                                "Price_Index": row[col]
                            })
                
                final_deliv = pd.DataFrame(long_rows)
                final_deliv = final_deliv.sort_values(["Year", "Quarter", "Location", "Residence_type"]).reset_index(drop=True)
                
                # Header Names from User Example: Year,Quarter,Location,Residence_type,Price_Index,,Price_Index
                # Sample shows 5 columns data, but header has empty and duplicate Price_Index.
                # We will stick to the 5 columns data structure.
                
                final_deliv.to_csv(deliverable_path, index=False)
                print(f"Created correctly formatted deliverable: {deliverable_name}")
            else:
                pd.DataFrame(columns=['Year', 'Quarter', 'Location', 'Residence_type', 'Price_Index']).to_csv(deliverable_path, index=False)
        else:
            pd.DataFrame(columns=['Year', 'Quarter', 'Location', 'Residence_type', 'Price_Index']).to_csv(deliverable_path, index=False)

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
            "message": f"Extracted {len(df_new)} rows. DB (zeus) Comparison ready. File: {deliverable_name}", 
            "state": new_state
        }
