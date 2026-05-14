from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

from etl.core.compare_csv import compare_and_update_csv
from etl.core.download import is_new_by_hash, sha256_file
from etl.core.nulls import normalize_nulls
from etl.core.output import write_deliverable_csv
from .extract import extract_tourist_expenditure_distribution


class Pipeline:
    pipeline_id = "cy_14_per_day_expenditure_of_tourists"
    display_name = "Cyprus: Per Day Expenditure of Tourists (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Tourism/Revenue%20from%20Tourism/Monthly/2031024E.px"

    def _build_query(self, metadata: dict) -> dict:
        variables = metadata.get("variables", [])
        if not variables:
            raise RuntimeError("Unexpected CYSTAT metadata for per-day tourist expenditure.")

        month_var = None
        metric_var = None
        country_var = None
        for v in variables:
            code = str(v.get("code", "")).strip().upper()
            text = str(v.get("text", "")).strip().upper()
            if code == "MONTH" or text == "MONTH":
                month_var = v
            elif "EXPENDITURE, LENGTH OF STAY, ARRIVALS" in code or "EXPENDITURE, LENGTH OF STAY, ARRIVALS" in text:
                metric_var = v
            elif "COUNTRY OF USUAL RESIDENCE" in code or "COUNTRY OF USUAL RESIDENCE" in text:
                country_var = v

        if month_var is None or metric_var is None or country_var is None:
            raise RuntimeError("Could not identify MONTH/METRIC/COUNTRY metadata for tourist expenditure.")

        metric_values = metric_var.get("values", [])
        metric_texts = metric_var.get("valueTexts", [])
        country_values = country_var.get("values", [])
        if not metric_values or not country_values:
            raise RuntimeError("Missing metric/country values in tourist expenditure metadata.")

        wanted_metric_codes: list[str] = []
        for m_code, m_text in zip(metric_values, metric_texts):
            upper = str(m_text).upper()
            if "AVERAGE LENGTH OF STAY" in upper or "EXPENDITURE - PER DAY" in upper:
                wanted_metric_codes.append(m_code)

        if len(wanted_metric_codes) < 2:
            # Fallback to known slots if labels drift.
            for fallback in ("1", "3"):
                if fallback in metric_values and fallback not in wanted_metric_codes:
                    wanted_metric_codes.append(fallback)

        if not wanted_metric_codes:
            raise RuntimeError("Could not find required metric codes for tourist expenditure.")

        return {
            "query": [
                {
                    "code": month_var["code"],
                    "selection": {"filter": "all", "values": ["*"]},
                },
                {
                    "code": metric_var["code"],
                    "selection": {"filter": "item", "values": wanted_metric_codes},
                },
                {
                    "code": country_var["code"],
                    "selection": {"filter": "item", "values": country_values},
                },
            ],
            "response": {"format": "csv"},
        }

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "14"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "cystat_tourist_expenditure.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        metadata = requests.get(self.API_URL, headers=headers, timeout=60).json()
        query = self._build_query(metadata)

        print("Requesting Tourist Expenditure data from CYSTAT API...")
        response = requests.post(self.API_URL, json=query, headers=headers, timeout=60)
        response.raise_for_status()
        out_path.write_bytes(response.content)
        file_hash = sha256_file(out_path)

        new_state = dict(state)
        new_state.update(
            {
                "api_url_used": self.API_URL,
                "file_sha256": file_hash,
                "last_download_path": str(out_path),
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

        print("Extracting tourist expenditure distribution data...")
        df_new = extract_tourist_expenditure_distribution(out_path)

        db_path = Path("data/db") / f"cy_{prefix}_{self.pipeline_id}.csv"
        output_dir = Path("data/outputs") / f"cy_{prefix}_{self.pipeline_id}"
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = Path("data/reports") / f"cy_{prefix}_{self.pipeline_id}" / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path,
            df_new,
            out_csv_full,
            report_csv,
            key_cols=["Year", "Month", "Country_of_origin"],
        )

        # DB table does not exist yet: keep DB phase explicit and skipped.
        db_summary = {
            "status": "skipped",
            "reason": "target_table_not_created",
            "missing_in_db": None,
            "different_in_db": None,
        }

        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        target_cols = [
            "Year",
            "Month",
            "Country_of_origin",
            "Average_length_of_stay_(nights)",
            "Expenditure_per_day",
        ]

        # Since DB table is not available yet, deliver full extracted dataset.
        deliverable_df = df_new.copy()
        for c in target_cols:
            if c not in deliverable_df.columns:
                deliverable_df[c] = pd.NA
        deliverable_df = deliverable_df[target_cols]
        # Replace placeholder strings ('...', 'u', 'N/A', ':') with NULL in numeric columns.
        normalize_nulls(
            deliverable_df,
            columns=["Average_length_of_stay_(nights)", "Expenditure_per_day"],
        )
        for c in ["Average_length_of_stay_(nights)", "Expenditure_per_day"]:
            deliverable_df[c] = pd.to_numeric(deliverable_df[c], errors="coerce")
        write_deliverable_csv(deliverable_df, deliverable_path)
        new_state.update(
            {
                "rows_before": res.rows_before,
                "rows_after": res.rows_after,
                "new_rows": res.new_rows,
                "updated_cells": res.updated_cells,
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
                "delta_path": str(output_file),
                "mock_db_snapshot_path": str(out_csv_full),
            }
        )

        if not is_new_by_hash(state.get("file_sha256"), file_hash) and res.new_rows == 0 and res.updated_cells == 0:
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows. DB compare skipped (table not created). "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
