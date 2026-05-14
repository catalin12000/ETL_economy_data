from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from etl.core.download import download_file, sha256_file, is_new_by_hash
from etl.core.elstat import get_latest_publication_url, get_download_url_by_title
from etl.core.paths import PipelinePaths


class Pipeline:
    pipeline_id = "ed_key_partners_primary_goods"
    display_name = "Key Partners - Primary Goods (SFC02) - Trade Balance Time Period"

    PUBLICATION_CODE = "SFC02"

    # Correct target table requested by user.
    # Note: numbering like "06." can shift, so we match by stable text.
    TARGET_TITLE_SUBSTRING = (
        "Imports - Arrivals, Exports - Dispatches in Value per Country by Standard International Trade Classification (SITC 1)"
    )
    LOOKBACK_MONTHS = 36

    @staticmethod
    def _prev_period(year: int, month: int) -> tuple[int, int]:
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        return year, month

    @staticmethod
    def _extract_period(pub_url: str) -> tuple[int, int]:
        # pub_url format: .../SFC02/YYYY-MMM
        period = pub_url.rstrip("/").split("/")[-1]
        y, m = period.split("-M")
        return int(y), int(m)

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "elstat_sfc02_sitc1_value_per_country.xls"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Start from latest month dynamically.
        latest_pub_url = get_latest_publication_url(
            self.PUBLICATION_CODE,
            locale="en",
            frequency="monthly",
            headers=headers,
        )

        # 2) Find the correct SITC-1 table. If it is missing in the latest month,
        # look back month-by-month (ELSTAT occasionally publishes this table with lag).
        year, month = self._extract_period(latest_pub_url)
        pub_url_used = latest_pub_url
        download_url = None
        lookback_used = 0

        for step in range(self.LOOKBACK_MONTHS + 1):
            candidate = f"https://www.statistics.gr/en/statistics/-/publication/{self.PUBLICATION_CODE}/{year}-M{month:02d}"
            try:
                found = get_download_url_by_title(
                    publication_url=candidate,
                    target_title=self.TARGET_TITLE_SUBSTRING,
                    headers=headers,
                )
                download_url = found
                pub_url_used = candidate
                lookback_used = step
                break
            except Exception:
                year, month = self._prev_period(year, month)

        if not download_url:
            return {
                "status": "error",
                "message": (
                    "Could not find SITC-1 table link within lookback window. "
                    f"Title contains: {self.TARGET_TITLE_SUBSTRING}"
                ),
                "state": dict(state),
            }

        # 3) Download + hash
        meta = download_file(download_url, out_path, headers=headers)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update({
            "publication_code": self.PUBLICATION_CODE,
            "latest_publication_url": latest_pub_url,
            "publication_url_used": pub_url_used,
            "lookback_months_used": lookback_used,
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

        return {
            "status": "delivered",
            "message": (
                f"Downloaded SITC-1 table from {pub_url_used} "
                f"(lookback={lookback_used} month(s)) to {out_path}"
            ),
            "state": new_state,
        }
