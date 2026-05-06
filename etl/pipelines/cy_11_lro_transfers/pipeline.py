from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.core.database import compare_with_postgres
from etl.core.download import download_file, sha256_file
from .extract import extract_lro_transfers


class Pipeline:
    pipeline_id = "cy_11_lro_transfers"
    display_name = "Cyprus: LRO Transfers (DLS Portal)"

    TRANSFERS_PAGE_URLS = {
        2025: "https://portal.dls.moi.gov.cy/statistics/pagkypria-statistika-poliseon-metavivaseon-2025/",
        2026: "https://portal.dls.moi.gov.cy/statistics/pagkypria-statistika-poliseon-metavivaseon-2026/",
    }
    FOREIGNERS_PAGE_URLS = {
        2025: "https://portal.dls.moi.gov.cy/statistics/statistika-poliseon-se-allodapous-2025/",
        2026: "https://portal.dls.moi.gov.cy/statistics/statistika-poliseon-se-allodapous-2026/",
    }

    @staticmethod
    def _find_page_pdf(page_url: str, headers: Dict[str, str]) -> str:
        r = requests.get(page_url, headers=headers, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        candidates: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_low = href.lower()
            if ".pdf" not in href_low:
                continue
            if "list_of_private_cadastral_surveyors" in href_low:
                continue
            if "%ce%bc%ce%b7%cf%84%cf%81%cf%8e%ce%bf" in href_low:
                continue
            candidates.append(href)

        if not candidates:
            raise RuntimeError(f"No PDF links found on {page_url}")

        for href in candidates:
            if "/wp-content/uploads/2026/" in href:
                return href

        return candidates[0]

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "11"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}

        extracted_frames: list[pd.DataFrame] = []
        source_meta: dict[str, str] = {}

        for source_year in [2025, 2026]:
            print(f"Resolving LRO transfers totals PDF for {source_year} from DLS page...")
            transfers_page_url = self.TRANSFERS_PAGE_URLS[source_year]
            totals_url = self._find_page_pdf(transfers_page_url, headers)

            print(f"Resolving LRO foreigners PDF for {source_year} from DLS page...")
            foreigners_page_url = self.FOREIGNERS_PAGE_URLS[source_year]
            foreigners_url = self._find_page_pdf(foreigners_page_url, headers)

            path_t = out_dir / f"lro_transfers_totals_{source_year}.pdf"
            path_f = out_dir / f"lro_transfers_foreigners_{source_year}.pdf"
            download_file(totals_url, path_t, headers=headers)
            download_file(foreigners_url, path_f, headers=headers)

            hash_t = sha256_file(path_t)
            hash_f = sha256_file(path_f)

            extracted_frames.append(extract_lro_transfers(path_t, path_f))

            source_meta[f"transfers_page_url_{source_year}"] = transfers_page_url
            source_meta[f"foreigners_page_url_{source_year}"] = foreigners_page_url
            source_meta[f"totals_url_{source_year}"] = totals_url
            source_meta[f"foreigners_url_{source_year}"] = foreigners_url
            source_meta[f"file_sha256_totals_{source_year}"] = hash_t
            source_meta[f"file_sha256_foreigners_{source_year}"] = hash_f
            source_meta[f"last_download_path_totals_{source_year}"] = str(path_t)
            source_meta[f"last_download_path_foreigners_{source_year}"] = str(path_f)

        new_state = dict(state)
        new_state.update(
            {
                "last_download_at": datetime.now(timezone.utc).isoformat(),
                **source_meta,
            }
        )

        print("Extracting data from historical and current PDFs using PyMuPDF...")
        df_new = (
            pd.concat(extracted_frames, ignore_index=True)
            .drop_duplicates(subset=["year", "month", "district"], keep="last")
            .sort_values(["year", "month", "district"])
            .reset_index(drop=True)
        )

        if df_new.empty:
            return {
                "status": "error",
                "message": "Extraction logic failed to find any data in the PDFs.",
                "state": new_state,
            }

        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        print("Comparing with live Cyprus Postgres DB (zeus)...")
        db_comp_res = compare_with_postgres(
            df=df_new,
            table_name="ed_lro_transfers",
            db_name="zeus",
            match_cols=["year", "month", "district"],
            sync_cols=[
                "declared_price",
                "accepted_price",
                "number_parcels_total",
                "number_parcels_locals",
                "number_parcels_eu",
                "number_parcels_non_eu",
                "number_of_buyers_total",
                "number_of_buyers_locals",
                "number_of_buyers_eu",
                "number_of_buyers_noneu",
            ],
            tolerance=0.11,
        )

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name
        differences_path = output_dir / "db_differences_only.csv"

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())

        target_cols = [
            "Year",
            "Month",
            "District",
            "Number of Buyers Total",
            "Number Parcels Total",
            "Declared Price",
            "Accepted Price",
            "Number Parcels Locals",
            "Number Parcels Eu",
            "Number Parcels Non Eu",
            "Number Buyers Locals",
            "Number Buyers Eu",
            "Number Buyers Non Eu",
        ]

        if not inserted_df.empty:
            col_map = {
                "year": "Year",
                "month": "Month",
                "district": "District",
                "number_of_buyers_total": "Number of Buyers Total",
                "number_parcels_total": "Number Parcels Total",
                "declared_price": "Declared Price",
                "accepted_price": "Accepted Price",
                "number_parcels_locals": "Number Parcels Locals",
                "number_parcels_eu": "Number Parcels Eu",
                "number_parcels_non_eu": "Number Parcels Non Eu",
                "number_of_buyers_locals": "Number Buyers Locals",
                "number_of_buyers_eu": "Number Buyers Eu",
                "number_of_buyers_noneu": "Number Buyers Non Eu",
            }
            inserted_df = inserted_df.rename(columns=col_map)
            integer_cols = [
                "Year",
                "Month",
                "Number of Buyers Total",
                "Number Parcels Total",
                "Number Parcels Locals",
                "Number Parcels Eu",
                "Number Parcels Non Eu",
                "Number Buyers Locals",
                "Number Buyers Eu",
                "Number Buyers Non Eu",
            ]
            for col in integer_cols:
                inserted_df[col] = pd.to_numeric(inserted_df[col], errors="coerce").astype("Int64")
            inserted_df.sort_values(["Year", "Month", "District"], inplace=True)
            inserted_df[target_cols].to_csv(deliverable_path, index=False)
        else:
            pd.DataFrame(columns=target_cols).to_csv(deliverable_path, index=False)

        if not updated_df.empty:
            col_map = {
                "year": "Year",
                "month": "Month",
                "district": "District",
                "number_of_buyers_total": "Number of Buyers Total",
                "number_parcels_total": "Number Parcels Total",
                "declared_price": "Declared Price",
                "accepted_price": "Accepted Price",
                "number_parcels_locals": "Number Parcels Locals",
                "number_parcels_eu": "Number Parcels Eu",
                "number_parcels_non_eu": "Number Parcels Non Eu",
                "number_of_buyers_locals": "Number Buyers Locals",
                "number_of_buyers_eu": "Number Buyers Eu",
                "number_of_buyers_noneu": "Number Buyers Non Eu",
            }
            updated_df = updated_df.rename(columns=col_map)
            integer_cols = [
                "Year",
                "Month",
                "Number of Buyers Total",
                "Number Parcels Total",
                "Number Parcels Locals",
                "Number Parcels Eu",
                "Number Parcels Non Eu",
                "Number Buyers Locals",
                "Number Buyers Eu",
                "Number Buyers Non Eu",
            ]
            for col in integer_cols:
                updated_df[col] = pd.to_numeric(updated_df[col], errors="coerce").astype("Int64")
            updated_df.sort_values(["Year", "Month", "District"], inplace=True)
            updated_df[target_cols].to_csv(differences_path, index=False)
        else:
            pd.DataFrame(columns=target_cols).to_csv(differences_path, index=False)

        new_state.update(
            {
                "db_comparison": {
                    "missing": db_comp_res.get("inserted"),
                    "different": db_comp_res.get("updated"),
                },
                "deliverable_path": str(deliverable_path),
                "differences_path": str(differences_path),
            }
        )

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows. DB Comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff."
            ),
            "state": new_state,
        }
