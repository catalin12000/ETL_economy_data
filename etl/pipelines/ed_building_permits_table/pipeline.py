from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title


class Pipeline:
    pipeline_id = "ed_building_permits_table"
    display_name = "Ed Building Permits Table"

    TARGET_TITLE = (
        "01. New built properties, storeys, volume and surface thereon, by region and regional unit"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "02"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Save as .xls (do NOT rename/pretend it's xlsx)
        xls_path = out_dir / "elstat_building_permits.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url("SOP03", locale="en", headers=headers)
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE, headers=headers)

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
