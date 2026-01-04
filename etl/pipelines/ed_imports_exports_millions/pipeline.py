from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any, Optional, List

import requests

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_download_url_by_title, get_latest_publication_year_url


BASE = "https://www.statistics.gr"


class Pipeline:
    pipeline_id = "ed_imports_exports_millions"
    display_name = "Imports-Exports of Goods and Services (Millions) - Annual"

    PUBLICATION_CODE = "SEL30"

    # Use a stable substring: the end year changes each year
    TARGET_TITLE_SUBSTRING = "Imports-Exports of Goods and Services"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "17"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "elstat_imports_exports_goods_services.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest year page dynamically
        pub_url = get_latest_publication_year_url(
            publication_code=self.PUBLICATION_CODE,
            locale="en",
            headers=headers,
        )

        # 2) Find download link by title (substring match)
        download_url = get_download_url_by_title(
            publication_url=pub_url,
            target_title=self.TARGET_TITLE_SUBSTRING,
            headers=headers,
        )

        # 3) Download + hash
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "publication_code": self.PUBLICATION_CODE,
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
            return {"status": "skipped", "message": "No new file detected (same file SHA256).", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded to {out_path}", "state": new_state}
