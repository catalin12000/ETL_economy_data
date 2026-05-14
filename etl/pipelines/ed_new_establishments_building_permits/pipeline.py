from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import shutil

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.paths import PipelinePaths


class Pipeline:
    pipeline_id = "ed_new_establishments_building_permits"
    display_name = "Ed New Establishments Building Permits (SOP03 - Table 16)"

    PUBLICATION_CODE = "SOP03"
    
    # Matching the specific Table 16 requested
    TARGET_TITLE_SUBSTRING = "16. New establishments, number and volume by category of use"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        output_dir = pp.output
        out_path = out_dir / "elstat_new_establishments.xls"
        deliverable_path = output_dir / "deliverable_ed_new_establishments_building_permits.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest page dynamically
        pub_url = get_latest_publication_url(self.PUBLICATION_CODE, locale="en", headers=headers)
        
        # 2) Find download link
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE_SUBSTRING, headers=headers)

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
            "deliverable_path": str(deliverable_path),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            if out_path.exists() and not deliverable_path.exists():
                shutil.copy2(out_path, deliverable_path)
            return {"status": "skipped", "message": "No new file detected.", "state": new_state}

        shutil.copy2(out_path, deliverable_path)
        return {
            "status": "delivered",
            "message": f"Downloaded and delivered file to {deliverable_path}",
            "state": new_state,
        }
