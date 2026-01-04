from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash


class Pipeline:
    pipeline_id = "ed_eu_gdp"
    display_name = "Ed EU GDP (Eurostat)"

    # Filter: Greece (EL), Romania (RO), Cyprus (CY), EU27 (EU27_2020), EA20
    # Units: Current prices (CP_MEUR), Chain linked volumes 2020 (CLV20_MEUR), PCH previous (CLV_PCH_PRE), PCH same period (CLV_PCH_SM)
    # s_adj: SCA (Seasonally and Calendar Adjusted)
    # Period: 2024 onwards
    DATASET_CODE = "namq_10_gdp"
    FILTER = "Q.CP_MEUR+CLV20_MEUR+CLV_PCH_PRE+CLV_PCH_SM.SCA.B1GQ.EL+RO+CY+EU27_2020+EA20"
    FILE_URL = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{DATASET_CODE}/{FILTER}/?format=SDMX-CSV&compressed=false&startPeriod=2024-Q1"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "47"
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
