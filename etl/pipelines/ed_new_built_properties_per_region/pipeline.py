from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
import shutil

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title


class Pipeline:
    pipeline_id = "ed_new_built_properties_per_region"
    display_name = "Ed New Built Properties Per Region (SOP03 - Table 1)"

    PUBLICATION_CODE = "SOP03"
    TARGET_TITLE = (
        "01. New built properties, storeys, volume and surface thereon, by region and regional unit"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "23"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        output_dir = Path("data/outputs") / f"{prefix}_{self.pipeline_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "elstat_new_built_properties_region.xls"
        deliverable_path = output_dir / "deliverable_ed_new_built_properties_per_region.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest page dynamically
        pub_url = get_latest_publication_url(self.PUBLICATION_CODE, locale="en", headers=headers)
        
        # 2) Find download link
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE, headers=headers)

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
