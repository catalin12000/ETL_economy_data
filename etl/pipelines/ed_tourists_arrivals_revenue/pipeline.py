from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "ed_tourists_arrivals_revenue"
    display_name = "Ed Tourists Arrivals and Revenue (BoG)"

    RECEIPTS_URL = "https://www.bankofgreece.gr/RelatedDocuments/RECEIPTS_BY_COUNTRY_OF_ORIGIN.xls"
    TRAVELLERS_URL = "https://www.bankofgreece.gr/RelatedDocuments/NUMBER_OF_INBOUND_TRAVELLERS_IN_GREECE_BY_COUNTRY_OF_ORIGIN.xls"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "42"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        receipts_path = out_dir / "Receipts_By_Country_Of_Origin.xls"
        travellers_path = out_dir / "Number_Of_Inbound_Travellers.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Download Receipts
        meta_receipts = download_file(self.RECEIPTS_URL, receipts_path, headers=headers)
        hash_receipts = sha256_file(receipts_path)

        # 2) Download Travellers
        meta_travellers = download_file(self.TRAVELLERS_URL, travellers_path, headers=headers)
        hash_travellers = sha256_file(travellers_path)

        new_state = dict(state)
        new_state.update({
            "file_sha256_receipts": hash_receipts,
            "file_sha256_travellers": hash_travellers,
            "last_download_path_receipts": str(receipts_path),
            "last_download_path_travellers": str(travellers_path),
            "downloaded_at_utc": meta_receipts.get("downloaded_at_utc"),
        })

        # Check for changes
        receipts_changed = is_new_by_hash(state.get("file_sha256_receipts"), hash_receipts)
        travellers_changed = is_new_by_hash(state.get("file_sha256_travellers"), hash_travellers)

        if not receipts_changed and not travellers_changed:
            return {"status": "skipped", "message": "Both Travel spreadsheets are unchanged.", "state": new_state}

        changed = []
        if receipts_changed: changed.append("Receipts")
        if travellers_changed: changed.append("Travellers")

        return {
            "status": "delivered", 
            "message": f"Downloaded updated Travel spreadsheet(s): {', '.join(changed)}", 
            "state": new_state
        }
