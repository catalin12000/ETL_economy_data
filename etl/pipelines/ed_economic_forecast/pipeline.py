from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.fingerprint import dataframe_sha256
from etl.pipelines.ed_economic_forecast.extract import extract_forecast_table


class Pipeline:
    pipeline_id = "ed_eu_economic_forecast_greece"
    display_name = "EU Economic Forecast – Greece"

    SOURCE_URL = (
        "https://economy-finance.ec.europa.eu/"
        "economic-surveillance-eu-member-states/"
        "country-pages/greece/economic-forecast-greece_en"
    )

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        # 1) Download HTML
        out_dir = Path("data/downloads") / self.pipeline_id
        out_dir.mkdir(parents=True, exist_ok=True)
        html_path = out_dir / "economic_forecast_greece.html"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"}
        meta = download_file(self.SOURCE_URL, html_path, headers=headers)

        file_hash = sha256_file(html_path)

        # prepare state early (traceability even on skip)
        new_state = dict(state)
        new_state.update({
            "source_url_used": self.SOURCE_URL,
            "file_sha256": file_hash,
            "downloaded_filename": html_path.name,
            "last_download_path": str(html_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No change detected in EU forecast page (same file SHA256).", "state": new_state}

        # 2) Extract the exact table
        df = extract_forecast_table(html_path)

        # 3) Data hash (so we can detect table changes even if page changes trivially)
        data_hash = dataframe_sha256(df, sort_cols=["Indicators"])
        if state.get("data_sha256") == data_hash:
            new_state["data_sha256"] = data_hash
            return {"status": "skipped", "message": "Page changed, but extracted table is identical (same data SHA256).", "state": new_state}

        # 4) Write deliverable CSV
        out_csv = Path("data/outputs") / self.pipeline_id / "EU Economic Forecast Greece.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(out_csv, index=False)

        # 5) Update state
        new_state.update({
            "data_sha256": data_hash,
            "deliverable_path": str(out_csv),
        })

        msg = f"Extracted {len(df)} rows. Deliverable={out_csv}"
        return {"status": "delivered", "message": msg, "state": new_state}
