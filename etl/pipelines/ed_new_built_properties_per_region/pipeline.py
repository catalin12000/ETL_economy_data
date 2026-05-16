from __future__ import annotations

from datetime import datetime
from shutil import copy2
from typing import Any, Dict

from etl.core.download import download_file, sha256_file
from etl.core.elstat import get_download_url_by_title, get_latest_publication_url
from etl.core.paths import PipelinePaths


class Pipeline:
    pipeline_id = "ed_new_built_properties_per_region"
    display_name = "Ed New Built Properties Per Region (SOP03 - Table 1)"

    PUBLICATION_CODE = "SOP03"
    TARGET_TITLE = "01. New built properties, storeys, volume and surface thereon, by region and regional unit"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_new_built_properties_region.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        pub_url = get_latest_publication_url(self.PUBLICATION_CODE, locale="en", headers=headers)
        download_url = get_download_url_by_title(pub_url, self.TARGET_TITLE, headers=headers)

        download_note = None
        try:
            meta = download_file(download_url, out_path, headers=headers)
        except PermissionError:
            if not out_path.exists():
                raise
            meta = {"downloaded_at_utc": state.get("downloaded_at_utc")}
            download_note = "Used existing local workbook because the source file was locked."

        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
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
            }
        )
        if download_note:
            new_state["download_note"] = download_note

        output_dir = pp.output
        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.xls"
        deliverable_path = output_dir / deliverable_name

        # This table's deliverable is the raw ELSTAT workbook itself.
        copy2(out_path, deliverable_path)

        new_state.update(
            {
                "deliverable_mode": "raw_download_copy",
                "deliverable_path": str(deliverable_path),
            }
        )

        return {
            "status": "delivered",
            "message": (
                f"Copied downloaded workbook to deliverable. File: {deliverable_name}"
                + (f" {download_note}" if download_note else "")
            ),
            "state": new_state,
        }
