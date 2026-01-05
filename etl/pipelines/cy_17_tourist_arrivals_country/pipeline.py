from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests

from etl.core.download import sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_17_tourist_arrivals_country"
    display_name = "Cyprus: Tourist Arrivals By Country (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Tourism/Tourists/Monthly/2021010E.px"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "17"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "cystat_tourist_arrivals.csv"

        # PxWeb API Query
        query = {
            "query": [
                {
                    "code": "COUNTRY OF USUAL RESIDENCE",
                    "selection": {
                        "filter": "item",
                        "values": [str(i) for i in range(68)]  # All 68 countries/regions
                    }
                }
            ],
            "response": {
                "format": "csv"
            }
        }

        headers = {"User-Agent": "Mozilla/5.0"}
        
        print(f"Requesting Tourist Arrivals By Country data from CYSTAT API...")
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
