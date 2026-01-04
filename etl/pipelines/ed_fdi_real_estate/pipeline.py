from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "ed_fdi_real_estate"
    display_name = "BoG FDI Direct Investment Real Estate"

    SOURCE_PAGE = (
        "https://www.bankofgreece.gr/en/statistics/external-sector/"
        "direct-investment/direct-investment---flows"
    )

    FILE_URL = "https://www.bankofgreece.gr/RelatedDocuments/EN_Direct_Investment_in_Greece_in_Real_Estate.xlsx"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "11"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Keep original extension (.xls)
        out_path = out_dir / "EN_Direct_Investment_in_Greece_in_Real_Estate.xlsx"

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
