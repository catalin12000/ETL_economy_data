from __future__ import annotations

from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.fingerprint import dataframe_sha256
from etl.pipelines.cy_06_economic_forecast_cy.extract import extract_forecast_table


class Pipeline:
    pipeline_id = "cy_06_economic_forecast_cy"
    display_name = "Cyprus: Economic Forecast (EU Commission)"

    SOURCE_URL = (
        "https://economy-finance.ec.europa.eu/"
        "economic-surveillance-eu-member-states/"
        "country-pages/cyprus/economic-forecast-cyprus_en"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "06"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "economic_forecast_cyprus.html"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"}
        
        meta = download_file(self.SOURCE_URL, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "source_url_used": self.SOURCE_URL,
            "file_sha256": file_hash,
            "last_download_path": str(out_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No change detected in EU forecast page.", "state": new_state}

        # 2) Extract the exact table
        df = extract_forecast_table(out_path)

        # 3) Data hash
        data_hash = dataframe_sha256(df, sort_cols=["Indicators"])
        if state.get("data_sha256") == data_hash:
            new_state["data_sha256"] = data_hash
            return {"status": "skipped", "message": "Page changed, but extracted table is identical.", "state": new_state}

        # 4) Write deliverable CSV
        out_csv = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}" / "EU Economic Forecast Cyprus.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(out_csv, index=False)

        # 5) Update state
        new_state.update({
            "data_sha256": data_hash,
            "deliverable_path": str(out_csv),
        })

        return {"status": "delivered", "message": f"Extracted {len(df)} rows. Deliverable={out_csv}", "state": new_state}
