from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

import re
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_10_lro_contracts_of_sale"
    display_name = "Cyprus: LRO Contracts of Sale (DLS Portal)"

    # Landing pages where the yearly links appear
    CONTRACTS_LANDING_URL = "https://portal.dls.moi.gov.cy/stats_category/enimerosi/statistika/politirion-engrafon/"
    FOREIGNERS_LANDING_URL = "https://portal.dls.moi.gov.cy/stats_category/enimerosi/statistika/poliseon-se-allodapous/"

    def _find_latest_pdf(self, landing_url: str, keywords: List[str], headers: Dict[str, str]) -> str:
        """Helper to find the newest yearly page and then the PDF inside it."""
        r_landing = requests.get(landing_url, headers=headers, timeout=30)
        r_landing.raise_for_status()
        soup = BeautifulSoup(r_landing.text, "html.parser")
        
        # 1. Find all links that look like yearly statistics
        candidate_pages = []
        for a in soup.find_all("a", href=True):
            text = a.get_text().strip()
            # Extract years (4 digits)
            years = re.findall(r"\d{4}", text)
            if years:
                max_year = max(int(y) for y in years)
                candidate_pages.append((max_year, a["href"]))
        
        if not candidate_pages:
            raise RuntimeError(f"No yearly pages found on {landing_url}")
            
        # 2. Pick the page with the highest year
        latest_page_url = max(candidate_pages, key=lambda x: x[0])[1]
        print(f"  Latest page for {keywords[0]}: {latest_page_url}")
        
        # 3. Scan the latest page for the actual PDF
        r_page = requests.get(latest_page_url, headers=headers, timeout=30)
        r_page.raise_for_status()
        
        # Search for PDF links in the page content
        matches = re.findall(r'https://portal.dls.moi.gov.cy/wp-content/uploads/[\d/]+/[^\"\']+\.pdf', r_page.text)
        
        for m in matches:
            m_low = m.lower()
            # Must match at least one keyword and NOT be a surveyor list
            if any(k.lower() in m_low for k in keywords) and "surveyors" not in m_low:
                return m
                
        if matches:
            return matches[0] # Fallback
            
        raise RuntimeError(f"No PDF found on {latest_page_url}")

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "10"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Find and download Contracts of Sale
        print("Finding latest Contracts of Sale PDF...")
        contracts_url = self._find_latest_pdf(self.CONTRACTS_LANDING_URL, ["Πωλητήρια", "Contracts"], headers)
        contracts_path = out_dir / "contracts_of_sale_latest.pdf"
        meta_c = download_file(contracts_url, contracts_path, headers=headers)
        hash_c = sha256_file(contracts_path)

        # 2) Find and download Sales to Foreigners
        print("Finding latest Sales to Foreigners PDF...")
        foreigners_url = self._find_latest_pdf(self.FOREIGNERS_LANDING_URL, ["Αλλοδαπούς", "Foreigners"], headers)
        foreigners_path = out_dir / "sales_to_foreigners_latest.pdf"
        meta_f = download_file(foreigners_url, foreigners_path, headers=headers)
        hash_f = sha256_file(foreigners_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": contracts_url,
            "file_sha256_contracts": hash_c,
            "file_sha256_foreigners": hash_f,
            "last_download_path_contracts": str(contracts_path),
            "last_download_path_foreigners": str(foreigners_path),
            "contracts_url": contracts_url,
            "foreigners_url": foreigners_url,
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        c_changed = is_new_by_hash(state.get("file_sha256_contracts"), hash_c)
        f_changed = is_new_by_hash(state.get("file_sha256_foreigners"), hash_f)

        if not c_changed and not f_changed:
            return {"status": "skipped", "message": "Both DLS PDFs are unchanged.", "state": new_state}

        changed = []
        if c_changed: changed.append("Contracts of Sale")
        if f_changed: changed.append("Sales to Foreigners")

        return {"status": "delivered", "message": f"Downloaded updated: {', '.join(changed)}", "state": new_state}
