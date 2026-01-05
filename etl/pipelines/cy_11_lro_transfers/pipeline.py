from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone
from bs4 import BeautifulSoup

import requests

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "cy_11_lro_transfers"
    display_name = "Cyprus: LRO Transfers (DLS Portal)"

    # Broad landing page for all statistics
    LANDING_URL = "https://portal.dls.moi.gov.cy/stats_category/enimerosi/statistika/"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "11"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}
        
        # 1) Get landing page to find the newest "Transfers" link
        print(f"Finding latest Transfers page on DLS...")
        r_landing = requests.get(self.LANDING_URL, headers=headers, timeout=30)
        r_landing.raise_for_status()
        
        soup_landing = BeautifulSoup(r_landing.text, "html.parser")
        
        candidate_pages = []
        for a in soup_landing.find_all("a", href=True):
            text = a.get_text().strip()
            # Priority: Must mention 'Μεταβιβάσεων' (Transfers)
            if "202" in text and ("Μεταβιβάσεων" in text or "Transfers" in text):
                years = re.findall(r"\d{4}", text)
                year = int(years[0]) if years else 0
                candidate_pages.append((year, a["href"]))
        
        if not candidate_pages:
            return {"status": "error", "message": "Could not find any Transfers (Μεταβιβάσεων) pages on the DLS landing page.", "state": state}

        # Pick the one with the highest year (e.g., 2025)
        specific_page_url = max(candidate_pages, key=lambda x: x[0])[1]
        print(f"Found specific page: {specific_page_url}")
        
        # 2) Get the specific page to find the PDF download
        r_page = requests.get(specific_page_url, headers=headers, timeout=30)
        r_page.raise_for_status()
        
        soup_page = BeautifulSoup(r_page.text, "html.parser")
        
        target_pdf_url = None
        # Keywords in both Greek and Latin
        keywords = ["μεταβιβάσεων", "πωλήσεων", "παγκύπρια", "metavivaseon", "poliseon", "pagkypria"]
        
        for a in soup_page.find_all("a", href=True):
            href = a["href"]
            href_low = href.lower()
            if ".pdf" in href_low:
                if any(k in href_low for k in keywords):
                    if not any(x in href_low for x in ["surveyor", "chorometres", "mitroo", "tilefona"]):
                        if href.startswith("http"):
                            target_pdf_url = href
                        else:
                            target_pdf_url = "https://portal.dls.moi.gov.cy" + href
                        break
        
        if not target_pdf_url:
            return {"status": "error", "message": f"Could not find the data PDF link on {specific_page_url}", "state": state}

        print(f"Found PDF: {target_pdf_url}")
        
        out_path = out_dir / "lro_transfers_latest.pdf"

        # 3) Download + hash
        meta = download_file(target_pdf_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "specific_page_found": specific_page_url,
            "source_url_used": target_pdf_url,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new Transfers PDF detected.", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded latest Transfers PDF to {out_path}", "state": new_state}