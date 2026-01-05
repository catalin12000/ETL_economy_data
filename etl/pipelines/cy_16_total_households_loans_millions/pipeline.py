from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_16_total_households_loans_millions"
    display_name = "Cyprus: Banking Sector Data (NPLs)"

    # Page listing the aggregate banking sector data
    SOURCE_PAGE = "https://www.centralbank.cy/en/licensing-supervision/banks/aggregate-cyprus-banking-sector-data"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "16"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get page to find the latest "non-performing loans" link
        print(f"Finding latest NPLs file on CBC...")
        r_page = requests.get(self.SOURCE_PAGE, headers=headers, timeout=30)
        r_page.raise_for_status()
        soup = BeautifulSoup(r_page.text, "html.parser")
        
        target_xls_url = None
        for a in soup.find_all("a", href=True):
            text = a.get_text().strip().lower()
            href = a["href"].lower()
            if "non-performing" in text and (".xls" in href or ".xlsx" in href):
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
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new NPL data detected.", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded latest NPL file to {out_path}", "state": new_state}