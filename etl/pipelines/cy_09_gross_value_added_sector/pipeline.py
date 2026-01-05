from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests

from etl.core.download import sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_09_gross_value_added_sector"
    display_name = "Cyprus: Gross Value Added By Sector (Annual)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/National%20Accounts/Annual%20National%20Accounts/0610020E.px"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "09"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "cystat_gva_sector_annual.csv"

        # PxWeb API Query
        # Specific indicators requested by user (Total Economy + NACE sectors)
        # Based on metadata mapping for 0610020E.px:
        # 3: Total GVA, 4: Agriculture, 8: Mining, 9: Manufacturing, 
        # 29: Electricity, 30: Water supply, 33: Construction, 
        # 34: Wholesale/Retail, 38: Transport, 44: Accommodation, 
        # 45: Info/Comm, 50: Financial, 54: Real Estate, 55: Professional,
        # 61: Administrative, 66: Public Admin, 67: Education, 68: Health,
        # 71: Arts, 74: Other service, 78: Households as employers
        sector_values = ["3", "4", "8", "9", "29", "30", "33", "34", "38", "44", "45", "50", "54", "55", "61", "66", "67", "68", "71", "74", "78"]

        query = {
            "query": [
                {
                    "code": "MEASURE",
                    "selection": {
                        "filter": "item",
                        "values": ["1"]  # In real terms (Million Euro)
                    }
                },
                {
                    "code": "INDICATORS BY ECONOMIC ACTIVITY NACE (Rev. 2)",
                    "selection": {
                        "filter": "item",
                        "values": sector_values
                    }
                }
            ],
            "response": {
                "format": "csv"
            }
        }

        headers = {"User-Agent": "Mozilla/5.0"}
        
        print(f"Requesting GVA By Sector data from CYSTAT API...")
        response = requests.post(self.API_URL, json=query, headers=headers, timeout=60)
        response.raise_for_status()

        # Save the content
        with open(out_path, "wb") as f:
            f.write(response.content)

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "api_url_used": self.API_URL,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new data from CYSTAT API.", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded to {out_path}", "state": new_state}
