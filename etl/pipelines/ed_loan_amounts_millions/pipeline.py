from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "ed_loan_amounts_millions"
    display_name = "Housing & Consumer Loans (Amounts) - New Business"

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/financial-markets-and-interest-rates/"
        "bank-deposit-and-loan-interest-rates"
    )

    # Both Amounts and Interest Rates come from this file (different sheets)
    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/Rates_TABLE_1+1a.xls"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        out_dir = Path("data/downloads") / self.pipeline_id
        out_dir.mkdir(parents=True, exist_ok=True)

        # Using same filename as source to be clear
        out_path = out_dir / "Rates_TABLE_1+1a.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta = download_file(self.FILE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_page": self.SOURCE_PAGE,
            "source_url_used": self.FILE_URL,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {
                "status": "skipped",
                "message": "No new file detected (same file SHA256).",
                "state": new_state,
            }

        return {
            "status": "delivered",
            "message": f"Downloaded to {out_path}",
            "state": new_state,
        }