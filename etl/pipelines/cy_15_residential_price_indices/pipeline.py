from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_15_residential_price_indices"
    display_name = "Cyprus: Residential Property Price Indices (CBC)"

    # Page containing the data series
    INDICES_URL = "https://www.centralbank.cy/en/publications/residential-property-price-indices"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "15"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get indices page to find the latest "Data series" link
        print(f"Finding latest RPPI Data series on CBC...")
        r_page = requests.get(self.INDICES_URL, headers=headers, timeout=30)
        r_page.raise_for_status()
        soup = BeautifulSoup(r_page.text, "html.parser")
        
        target_xls_url = None
        for a in soup.find_all("a", href=True):
            text = a.get_text().strip().lower()
            href = a["href"].lower()
            if "data series" in text and (".xls" in href or ".xlsx" in href):
                if a["href"].startswith("http"):
                    target_xls_url = a["href"]
                else:
                    target_xls_url = "https://www.centralbank.cy" + a["href"]
                break
        
        if not target_xls_url:
            return {"status": "error", "message": "Could not find 'Data series' Excel link on CBC page.", "state": state}

        print(f"Found RPPI file: {target_xls_url}")
        
        out_path = out_dir / "cbc_rppi_data_series.xls"

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

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new RPPI data detected.", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded latest RPPI file to {out_path}", "state": new_state}
