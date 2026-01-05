from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

import requests

from etl.core.download import sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_01_average_monthly_earnings"
    display_name = "Cyprus: Average Monthly Earnings (Quarterly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/el/8.CYSTAT-DB/Labour%20Cost%20and%20Earnings/Earnings/1110010G.px"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "01"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "cystat_earnings.csv"

        # PxWeb API Query
        query = {
            "query": [
                {
                    "code": "ΦΥΛΟ",
                    "selection": {
                        "filter": "item",
                        "values": ["0", "1", "2"]  # Σύνολο, Άντρες, Γυναίκες
                    }
                },
                {
                    "code": "ΔΕΙΚΤΗΣ",
                    "selection": {
                        "filter": "item",
                        "values": ["0"]  # Μέσες μηνιαίες απολαβές - Χωρίς διόρθωση (€)
                    }
                }
            ],
            "response": {
                "format": "csv"
            }
        }

        # For Time (ΤΡΙΜΗΝΟ), we don't specify selection to get ALL history, 
        # or we could select the last few. Let's get everything to be sure.

        headers = {"User-Agent": "Mozilla/5.0"}
        
        print(f"Requesting data from CYSTAT API...")
        response = requests.post(self.API_URL, json=query, headers=headers, timeout=60)
        response.raise_for_status()

        # Save the content
        with open(out_path, "wb") as f:
            f.write(response.content)

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        from datetime import datetime, timezone
        new_state.update({
            "api_url_used": self.API_URL,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new data from CYSTAT API.", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded to {out_path}", "state": new_state}
