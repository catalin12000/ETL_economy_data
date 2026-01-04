from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "ed_retail_price_rental_index"
    display_name = "Ed Retail Price and Rent Indices (BoG)"

    PRICE_INDEX_URL = "https://www.bankofgreece.gr/RelatedDocuments/RETAIL_PRICE_INDEX.pdf"
    RENT_INDEX_URL = "https://www.bankofgreece.gr/RelatedDocuments/RETAIL_RENT_INDEX.pdf"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "38"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        price_path = out_dir / "Retail_Price_Index.pdf"
        rent_path = out_dir / "Retail_Rent_Index.pdf"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Download Price Index
        meta_price = download_file(self.PRICE_INDEX_URL, price_path, headers=headers)
        hash_price = sha256_file(price_path)

        # 2) Download Rent Index
        meta_rent = download_file(self.RENT_INDEX_URL, rent_path, headers=headers)
        hash_rent = sha256_file(rent_path)

        new_state = dict(state)
        new_state.update({
            "file_sha256_price": hash_price,
            "file_sha256_rent": hash_rent,
            "last_download_path_price": str(price_path),
            "last_download_path_rent": str(rent_path),
            "downloaded_at_utc": meta_price.get("downloaded_at_utc"),
        })

        # Check if either file has changed
        price_changed = is_new_by_hash(state.get("file_sha256_price"), hash_price)
        rent_changed = is_new_by_hash(state.get("file_sha256_rent"), hash_rent)

        if not price_changed and not rent_changed:
            return {"status": "skipped", "message": "Both Retail PDFs are unchanged.", "state": new_state}

        changed = []
        if price_changed: changed.append("Price Index")
        if rent_changed: changed.append("Rent Index")

        return {
            "status": "delivered", 
            "message": f"Downloaded updated Retail PDF(s): {', '.join(changed)}", 
            "state": new_state
        }
