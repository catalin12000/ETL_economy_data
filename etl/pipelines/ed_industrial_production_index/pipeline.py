from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title


class Pipeline:
    pipeline_id = "ed_industrial_production_index"
    display_name = "Industrial Production Index (Overall + Seasonally Adjusted)"

    PUBLICATION_CODE = "DKT21"

    # Use stable substrings (titles contain changing date ranges like "... - October 2025")
    TITLE_03 = "03. Evolution of the Overall Industrial Production Index (2015=100.0)"
    TITLE_04 = "04. Seasonally Adjusted Industrial Production Index (2015=100.0)"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "18"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest month dynamically
        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="monthly",
            headers=headers,
        )

        # 2) Resolve the 2 download URLs by title
        url_03 = get_download_url_by_title(pub_url, self.TITLE_03, headers=headers)
        url_04 = get_download_url_by_title(pub_url, self.TITLE_04, headers=headers)

        # 3) Download both files
        path_03 = out_dir / "industrial_production_index_overall_03.xls"
        path_04 = out_dir / "industrial_production_index_sa_04.xls"

        meta_03 = download_file(url_03, path_03, headers=headers)
        meta_04 = download_file(url_04, path_04, headers=headers)

        hash_03 = sha256_file(path_03)
        hash_04 = sha256_file(path_04)

        # 4) Update state
        new_state = dict(state)
        new_state.update({
            "publication_code": self.PUBLICATION_CODE,
            "publication_url_used": pub_url,

            "download_url_03": url_03,
            "download_url_04": url_04,

            "file_sha256_03": hash_03,
            "file_sha256_04": hash_04,

            "downloaded_filename_03": path_03.name,
            "downloaded_filename_04": path_04.name,

            "last_download_path_03": str(path_03),
            "last_download_path_04": str(path_04),

            # keep meta (separately) for debugging
            "meta_03": {
                "last_modified": meta_03.get("last_modified"),
                "etag": meta_03.get("etag"),
                "content_length": meta_03.get("content_length"),
                "final_url": meta_03.get("final_url"),
                "downloaded_at_utc": meta_03.get("downloaded_at_utc"),
            },
            "meta_04": {
                "last_modified": meta_04.get("last_modified"),
                "etag": meta_04.get("etag"),
                "content_length": meta_04.get("content_length"),
                "final_url": meta_04.get("final_url"),
                "downloaded_at_utc": meta_04.get("downloaded_at_utc"),
            },
        })

        unchanged_03 = not is_new_by_hash(state.get("file_sha256_03"), hash_03)
        unchanged_04 = not is_new_by_hash(state.get("file_sha256_04"), hash_04)

        if unchanged_03 and unchanged_04:
            return {
                "status": "skipped",
                "message": "No new files detected (both SHA256 unchanged).",
                "state": new_state,
            }

        changed = []
        if not unchanged_03:
            changed.append("03")
        if not unchanged_04:
            changed.append("04")

        return {
            "status": "delivered",
            "message": f"Downloaded updated file(s): {', '.join(changed)}",
            "state": new_state,
        }
