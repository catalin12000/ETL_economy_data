from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title

class Pipeline:
    pipeline_id = "ed_wage_growth_index"
    display_name = "Ed Wage Growth Index - Quarterly"

    # We match a substring because the specific quarter "(3rd Quarter 2024)" changes over time.
    TARGET_TITLE = "Evolution of Gross Wages and Salaries in main sections of the economy"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        out_dir = Path("data/downloads") / self.pipeline_id
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "ed_wage_growth_index.xls"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        }

        # 1) Resolve latest quarterly page dynamically (DKT03)
        # Mechanism: Checks .../DKT03/- to find the latest available YYYY-QN link automatically.
        pub_url = get_latest_publication_url(
            publication_code="DKT03",
            locale="en",
            frequency="quarterly",
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

        # 4) Update state
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
                "message": f"No new file detected for {self.pipeline_id} (same file SHA256).",
                "state": new_state,
            }

        return {
            "status": "delivered",
            "message": f"Downloaded to {out_path}",
            "state": new_state,
        }
