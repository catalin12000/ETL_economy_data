from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title


class Pipeline:
    pipeline_id = "ed_consumer_price_index"
    display_name = "Ed Consumer Price Index"

    # Greek ELSTAT item title to download from the latest month page
    TARGET_TITLE = (
        "03. Συγκρίσεις Γενικού Δείκτη Τιμών Καταναλωτή (2020=100,0) "
        "(Ιανουαρίου 2001 - Νοεμβρίου 2025)"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "05"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        xls_path = out_dir / "elstat_consumer_price_index.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # DKT87 is monthly; locale is Greek ("el") because the publication URL is /el/...
        pub_url = get_latest_publication_url(
            "DKT87",
            locale="el",
            frequency="monthly",
            headers=headers,
        )

        download_url = get_download_url_by_title(
            pub_url,
            self.TARGET_TITLE,
            headers=headers,
        )

        meta = download_file(download_url, xls_path, headers=headers)
        file_hash = sha256_file(xls_path)

        new_state = dict(state)
        new_state.update({
            "publication_url_used": pub_url,
            "download_url_used": download_url,
            "source_url_used": download_url,
            "file_sha256": file_hash,
            "downloaded_filename": xls_path.name,
            "last_download_path": str(xls_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new file detected (same file SHA256).", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded to {xls_path}", "state": new_state}
