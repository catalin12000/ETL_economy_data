from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone
import requests
import pandas as pd

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.database import compare_with_postgres
from .extract import extract_lro_transfers


class Pipeline:
    pipeline_id = "cy_11_lro_transfers"
    display_name = "Cyprus: LRO Transfers (DLS Portal)"

    # Hardcoded verified links for 2026
    URL_TOTALS = "https://portal.dls.moi.gov.cy/wp-content/uploads/2026/02/Παγκύπρια-Στατιστικά-Πωλήσεων-Μεταβιβάσεων-2026.pdf"
    URL_FOREIGNERS = "https://portal.dls.moi.gov.cy/wp-content/uploads/2026/02/ΣΤΑΤΙΣΤΙΚΑ-ΠΩΛΗΣΕΩΝ-ΣΕ-ΑΛΛΟΔΑΠΟΥΣ-2026.pdf"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "11"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Download Verified Files
        print("Downloading verified LRO PDFs...")
        path_t = out_dir / "lro_transfers_totals.pdf"
        path_f = out_dir / "lro_transfers_foreigners.pdf"
        
        download_file(self.URL_TOTALS, path_t, headers=headers)
        download_file(self.URL_FOREIGNERS, path_f, headers=headers)
        
        hash_t = sha256_file(path_t)
        hash_f = sha256_file(path_f)

        new_state = dict(state)
        new_state.update({
            "file_sha256_totals": hash_t,
            "file_sha256_foreigners": hash_f,
            "last_download_at": datetime.now(timezone.utc).isoformat(),
        })

        # 2. Extraction using Fitz (Spatial Parser)
        print(f"Extracting data from PDFs using PyMuPDF...")
        df_new = extract_lro_transfers(path_t, path_f)
        
        if df_new.empty:
            return {"status": "error", "message": "Extraction logic failed to find any data in the PDFs.", "state": new_state}

        # 3. DB Sync (zeus)
        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print("Comparing with live Cyprus Postgres DB (zeus)...")
        db_comp_res = compare_with_postgres(
            df=df_new,
            table_name="ed_lro_transfers",
            db_name="zeus",
            match_cols=["year", "month", "district"],
            sync_cols=[
                "declared_price", "accepted_price", "number_parcels_total",
                "number_parcels_locals", "number_parcels_eu", "number_parcels_non_eu",
                "number_of_buyers_total", "number_of_buyers_locals", 
                "number_of_buyers_eu", "number_of_buyers_noneu"
            ],
            tolerance=0.11
        )
        
        # 4. Create Deliverable
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)
        
        target_cols = [
            'Year', 'Month', 'District', 'Number of Buyers Total', 'Number Parcels Total',
            'Declared Price', 'Accepted Price', 'Number Parcels Locals', 'Number Parcels Eu',
            'Number Parcels Non Eu', 'Number Buyers Locals', 'Number Buyers Eu', 'Number Buyers Non Eu'
        ]
        
        if not delta_df.empty:
            # Map back to User format
            col_map = {
                "year": "Year", "month": "Month", "district": "District",
                "number_of_buyers_total": "Number of Buyers Total",
                "number_parcels_total": "Number Parcels Total",
                "declared_price": "Declared Price",
                "accepted_price": "Accepted Price",
                "number_parcels_locals": "Number Parcels Locals",
                "number_parcels_eu": "Number Parcels Eu",
                "number_parcels_non_eu": "Number Parcels Non Eu",
                "number_of_buyers_locals": "Number Buyers Locals",
                "number_of_buyers_eu": "Number Buyers Eu",
                "number_of_buyers_noneu": "Number Buyers Non Eu"
            }
            delta_df.rename(columns=col_map, inplace=True)
            delta_df.sort_values(["Year", "Month", "District"], inplace=True)
            delta_df[target_cols].to_csv(deliverable_path, index=False)
            print(f"Created deliverable: {deliverable_name}")
        else:
            pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)

        new_state.update({
            "db_comparison": {
                "missing": db_comp_res.get("inserted"),
                "different": db_comp_res.get("updated")
            },
            "deliverable_path": str(deliverable_path)
        })

        return {
            "status": "delivered",
            "message": f"Extracted {len(df_new)} rows. DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff.",
            "state": new_state
        }
