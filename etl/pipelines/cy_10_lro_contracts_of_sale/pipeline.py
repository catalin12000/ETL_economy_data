from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.core.database import compare_with_postgres
from etl.core.download import download_file, is_new_by_hash, sha256_file
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_lro_contracts_of_sale


class Pipeline:
    pipeline_id = "cy_10_lro_contracts_of_sale"
    display_name = "Cyprus: LRO Contracts of Sale (DLS Portal)"

    CONTRACTS_PAGE_URLS = {
        2025: "https://portal.dls.moi.gov.cy/statistics/pagkypria-statistika-stoicheia-politirion-engrafon-2024-kai-2025/",
        2026: "https://portal.dls.moi.gov.cy/statistics/pagkypria-statistika-stoicheia-politirion-engrafon-2025-kai-2026/",
    }
    FOREIGNERS_PAGE_URLS = {
        2025: "https://portal.dls.moi.gov.cy/statistics/statistika-poliseon-se-allodapous-2025/",
        2026: "https://portal.dls.moi.gov.cy/statistics/statistika-poliseon-se-allodapous-2026/",
    }

    @staticmethod
    def _find_page_pdf(page_url: str, headers: Dict[str, str]) -> str:
        response = requests.get(page_url, headers=headers, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        candidates: list[str] = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            href_lower = href.lower()
            if ".pdf" not in href_lower:
                continue
            if "list_of_private_cadastral_surveyors" in href_lower:
                continue
            if "%ce%bc%ce%b7%cf%84%cf%81%cf%8e%ce%bf" in href_lower:
                continue
            candidates.append(href)

        if not candidates:
            raise RuntimeError(f"No PDF links found on {page_url}")

        for href in candidates:
            if "/wp-content/uploads/2026/" in href:
                return href

        return candidates[0]

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        download_dir = pp.downloaded
        download_dir.mkdir(parents=True, exist_ok=True)

        headers = {"User-Agent": "Mozilla/5.0"}

        extracted_frames: list[pd.DataFrame] = []
        source_meta: dict[str, str] = {}
        source_changes: list[str] = []

        for source_year in [2025, 2026]:
            print(f"Resolving Contracts of Sale PDF for {source_year} from DLS page...")
            contracts_page_url = self.CONTRACTS_PAGE_URLS[source_year]
            contracts_url = self._find_page_pdf(contracts_page_url, headers)
            contracts_path = download_dir / f"contracts_of_sale_{source_year}.pdf"
            download_file(contracts_url, contracts_path, headers=headers)
            contracts_hash = sha256_file(contracts_path)

            print(f"Resolving Sales to Foreigners PDF for {source_year} from DLS page...")
            foreigners_page_url = self.FOREIGNERS_PAGE_URLS[source_year]
            foreigners_url = self._find_page_pdf(foreigners_page_url, headers)
            foreigners_path = download_dir / f"sales_to_foreigners_{source_year}.pdf"
            download_file(foreigners_url, foreigners_path, headers=headers)
            foreigners_hash = sha256_file(foreigners_path)

            extracted_frames.append(extract_lro_contracts_of_sale(contracts_path, foreigners_path))

            source_meta[f"contracts_page_url_{source_year}"] = contracts_page_url
            source_meta[f"foreigners_page_url_{source_year}"] = foreigners_page_url
            source_meta[f"contracts_url_{source_year}"] = contracts_url
            source_meta[f"foreigners_url_{source_year}"] = foreigners_url
            source_meta[f"file_sha256_contracts_{source_year}"] = contracts_hash
            source_meta[f"file_sha256_foreigners_{source_year}"] = foreigners_hash
            source_meta[f"last_download_path_contracts_{source_year}"] = str(contracts_path)
            source_meta[f"last_download_path_foreigners_{source_year}"] = str(foreigners_path)

            if is_new_by_hash(state.get(f"file_sha256_contracts_{source_year}"), contracts_hash):
                source_changes.append(f"Contracts {source_year}")
            if is_new_by_hash(state.get(f"file_sha256_foreigners_{source_year}"), foreigners_hash):
                source_changes.append(f"Foreigners {source_year}")

        print("Extracting contracts of sale data from historical and current PDFs...")
        df_new = (
            pd.concat(extracted_frames, ignore_index=True)
            .drop_duplicates(subset=["year", "month", "district"], keep="last")
            .sort_values(["year", "month", "district"])
            .reset_index(drop=True)
        )

        output_dir = pp.output
        print("Comparing with live Cyprus Postgres DB (zeus)...")
        db_comp_res = compare_with_postgres(
            df=df_new,
            table_name="ed_lro_contracts_of_sale",
            db_name="zeus",
            match_cols=["year", "month", "district"],
            sync_cols=[
                "number_parcels_total",
                "number_parcels_locals",
                "number_parcels_eu",
                "number_parcels_noneu",
            ],
            tolerance=0.11,
        )

        deliverable_path = output_dir / f"deliverable_{self.pipeline_id}_{datetime.now().strftime('%B_%Y')}.csv"
        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "id",
            "year",
            "month",
            "district",
            "number_parcels_total",
            "number_parcels_locals",
            "number_parcels_eu",
            "number_parcels_noneu",
        ]

        integer_cols = [
            "year", "month",
            "number_parcels_total", "number_parcels_locals",
            "number_parcels_eu", "number_parcels_noneu",
        ]

        def shape_output(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return pd.DataFrame(columns=target_cols)
            out = df.copy()
            out["id"] = pd.to_numeric(out.get("id"), errors="coerce").map(
                lambda x: "" if pd.isna(x) else str(int(x))
            )
            for col in integer_cols:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            out = out.sort_values(["year", "month", "district"]).reset_index(drop=True)
            for c in target_cols:
                if c not in out.columns:
                    out[c] = pd.NA
            return out[target_cols]

        write_deliverable_csv(shape_output(delta_df), deliverable_path)
        new_state = dict(state)
        new_state.update(
            {
                "deliverable_path": str(deliverable_path),
                "db_comparison": {
                    "missing": db_comp_res.get("inserted"),
                    "different": db_comp_res.get("updated"),
                },
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                **source_meta,
            }
        )

        if not source_changes:
            source_changes.append("none")

        return {
            "status": "delivered",
            "message": (
                f"Processed {len(df_new)} rows. Source changes: {', '.join(source_changes)}. "
                f"DB Comparison: {db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff."
            ),
            "state": new_state,
        }
