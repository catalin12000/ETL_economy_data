from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "ed_eu_hicp"
    display_name = "Ed EU Harmonized Index Of Consumer Prices (Eurostat)"

    # Filter: Greece (EL), Romania (RO), Cyprus (CY), EU27 (EU27_2020), EA20
    # Unit: RCH_A (Annual rate of change)
    # coicop: CP00 (All-items HICP)
    # Period: 2025 onwards
    DATASET_CODE = "prc_hicp_manr"
    FILTER = "M.RCH_A.CP00.EL+RO+CY+EU27_2020+EA20"
    FILE_URL = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{DATASET_CODE}/{FILTER}/?format=SDMX-CSV&compressed=false&startPeriod=2025-01"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "48"
        out_dir = Path("data/downloads") / f"{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"eurostat_{self.DATASET_CODE}.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        meta = download_file(self.FILE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "dataset_code": self.DATASET_CODE,
            "filter": self.FILTER,
            "source_url_used": self.FILE_URL,
            "file_sha256": file_hash,
            "downloaded_filename": out_path.name,
            "last_download_path": str(out_path),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new data from Eurostat (same file SHA256).", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded to {out_path}", "state": new_state}
