from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title

class Pipeline:
    pipeline_id = "ed_employment"
    display_name = "Employment Status & Unemployment Rate"

    TARGET_TITLE = (
        "01A. Κατάσταση απασχόλησης και ποσοστό ανεργίας"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        out_dir = Path("data/downloads") / self.pipeline_id
        out_dir.mkdir(parents=True, exist_ok=True)

        # Keep original file format (ELSTAT usually serves .xls)
        out_path = out_dir / "ed_employment.xls"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }

        # 1) Resolve latest month page dynamically
        pub_url = get_latest_publication_url(
            publication_code="SJO02",
            locale="el",
            frequency="monthly",
            headers=headers,
        )

        # 2) Find the correct downloadable file by title
        download_url = get_download_url_by_title(
            publication_url=pub_url,
            target_title=self.TARGET_TITLE,
            headers=headers,
        )

        # 3) Download
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        # 4) Update state (even on skip)
        new_state = dict(state)
        new_state.update({
            "publication_url_used": pub_url,
            "download_url_used": download_url,
            "source_url_used": download_url,
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