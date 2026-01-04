from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title


class Pipeline:
    pipeline_id = "ed_new_residential_building_cost_index"
    display_name = "Ed New Residential Building Cost Index (DKT63) - Quarterly"

    # DKT63: Price Indices for New Residential Buildings Construction
    PUBLICATION_CODE = "DKT63"
    
    # Substring matching to handle changing period labels
    TARGET_TITLE_SUBSTRING = "02. Price Indices for New Residential Buildings Construction"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "25"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "elstat_residential_building_cost_index.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest quarterly page dynamically
        pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE, 
            locale="en", 
            frequency="quarterly", 
            headers=headers
        )
        
        # 2) Find download link
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE_SUBSTRING, headers=headers)

        # 3) Download + hash
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "publication_url_used": pub_url,
            "download_url_used": download_url,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new file detected.", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded to {out_path}", "state": new_state}
