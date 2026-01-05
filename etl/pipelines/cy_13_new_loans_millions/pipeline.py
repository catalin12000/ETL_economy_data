from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_13_new_loans_millions"
    display_name = "Cyprus: New Loans Millions (MFS Spreadsheet)"

    # Base URL for statistics
    ROOT_URL = "https://www.centralbank.cy/en/publications/monetary-and-financial-statistics/"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "13"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get root page to find the latest "Year-XXXX" link
        print(f"Finding latest Year page on CBC...")
        r_root = requests.get(self.ROOT_URL, headers=headers, timeout=30)
        r_root.raise_for_status()
        soup_root = BeautifulSoup(r_root.text, "html.parser")
        
        year_links = []
        for a in soup_root.find_all("a", href=True):
            href = a["href"]
            if "year-" in href:
                # Extract year from href or text
                m = re.search(r"year-(\d{4})", href)
                if m:
                    year_links.append((int(m.group(1)), href))
        
        if not year_links:
            return {"status": "error", "message": "Could not find any Year pages on CBC root.", "state": state}

        # Sort by year descending and pick latest
        latest_year, year_path = max(year_links, key=lambda x: x[0])
        latest_year_url = "https://www.centralbank.cy" + year_path
        print(f"Found latest year page: {latest_year_url}")

        # 2) Get the year page to find the latest MFS file
        r_year = requests.get(latest_year_url, headers=headers, timeout=30)
        r_year.raise_for_status()
        soup_year = BeautifulSoup(r_year.text, "html.parser")
        
        # Look for MFS links
        mfs_links = []
        for a in soup_year.find_all("a", href=True):
            href = a["href"]
            if "MFS_" in href and ".xls" in href:
                # Often the month is in the text next to it
                month_text = a.find_parent().get_text().strip()
                mfs_links.append(href)
        
        if not mfs_links:
            return {"status": "error", "message": f"Could not find any MFS Excel files on {latest_year_url}", "state": state}

        # Usually the first MFS link on the page is the latest (December, etc.)
        target_mfs_url = "https://www.centralbank.cy" + mfs_links[0]
        print(f"Found MFS file: {target_mfs_url}")
        
        out_path = out_dir / "cbc_mfs_monetary_statistics.xls"

        # 3) Download + hash
        meta = download_file(target_mfs_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "year_page_used": latest_year_url,
            "source_url_used": target_mfs_url,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new MFS file detected.", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded latest MFS file to {out_path}", "state": new_state}