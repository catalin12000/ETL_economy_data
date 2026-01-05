from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import requests

from etl.core.download import sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_02_building_permits_by_district"
    display_name = "Cyprus: Building Permits By District (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Building%20Permits/1440010E.px"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "02"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "cystat_building_permits_district.csv"

        # PxWeb API Query
        query = {
            "query": [
                {
                    "code": "DISTRICT",
                    "selection": {
                        "filter": "item",
                        "values": ["0", "1", "2", "3", "4"]  # Only specific districts (exclude 5: Total)
                    }
                },
                {
                    "code": "URBAN/RURAL",
                    "selection": {
                        "filter": "item",
                        "values": ["1", "2"]  # Only Urban and Rural (exclude 0: Total)
                    }
                },
                {
                    "code": "MEASUREMENT",
                    "selection": {
                        "filter": "item",
                        "values": ["0"]  # Number
                    }
                },
                {
                    "code": "REFERENCE PERIOD",
                    "selection": {
                        "filter": "item",
                        "values": ["0"]  # Monthly data
                    }
                },
                {
                    "code": "NUMBER/AREA/VALUE/DWELLING UNITS",
                    "selection": {
                        "filter": "item",
                        "values": ["0", "1", "2", "3"]  # All 4 indicators
                    }
                }
            ],
            "response": {
                "format": "csv"
            }
        }

        headers = {"User-Agent": "Mozilla/5.0"}
        
        print(f"Requesting Building Permits data from CYSTAT API...")
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
