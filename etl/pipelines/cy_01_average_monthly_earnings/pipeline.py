from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

import pandas as pd
import requests

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import sha256_file, is_new_by_hash
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_average_monthly_earnings


class Pipeline:
    pipeline_id = "cy_01_average_monthly_earnings"
    country = "cy"
    source = "cystat"
    db_table_name = "ed_average_monthly_earnings"
    source_type = "api"
    display_name = "Cyprus: Average Monthly Earnings (Quarterly)"
    MIN_DELIVERABLE_YEAR = 2023  # Keep only data after 2024.

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/el/8.CYSTAT-DB/Labour%20Cost%20and%20Earnings/Earnings/1110010G.px"

    def _build_query(self, metadata: dict) -> dict:
        variables = metadata.get("variables", [])
        if len(variables) < 3:
            raise RuntimeError("Unexpected CYSTAT metadata structure for average monthly earnings.")

        # Dataset layout currently: Quarter, Sex, Indicator.
        sex_var = variables[1]
        indicator_var = variables[2]

        sex_values = sex_var.get("values", [])[:3]  # Total / Male / Female
        indicator_values = indicator_var.get("values", [])[:2]  # Unadjusted / Seasonally adjusted
        if len(sex_values) < 3 or len(indicator_values) < 2:
            raise RuntimeError("CYSTAT metadata missing expected sex/indicator values.")

        return {
            "query": [
                {
                    "code": sex_var["code"],
                    "selection": {"filter": "item", "values": sex_values},
                },
                {
                    "code": indicator_var["code"],
                    "selection": {"filter": "item", "values": indicator_values},
                },
            ],
            "response": {"format": "csv"},
        }

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "cystat_earnings.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 0) Build API query dynamically from metadata to avoid hardcoding Greek code strings.
        metadata = requests.get(self.API_URL, headers=headers, timeout=60).json()
        query = self._build_query(metadata)

        # 1) Download
        print("Requesting data from CYSTAT API...")
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

        # 2) Extraction
        print("Extracting average monthly earnings data...")
        df_new = extract_average_monthly_earnings(out_path)

        # 3) Local baseline comparison
        db_path = pp.baseline
        output_dir = pp.output
        out_csv_full = output_dir / "mock_db_snapshot.csv"
        report_csv = pp.output / "update_report.csv"

        print(f"Comparing with baseline DB {db_path}...")
        res = compare_and_update_csv(
            db_path,
            df_new,
            out_csv_full,
            report_csv,
            key_cols=["Year", "Quarter", "Sex"],
        )

        # 4) DB comparison (READ-ONLY, zeus)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Quarter": "quarter",
                "Sex": "sex",
                "Avg Monthly Earnings Unadjusted": "avg_monthly_earnings_unadjusted",
                "Avg Monthly Earnings Seasonally Adjusted": "avg_monthly_earnings_seasonally_adjusted",
            }
        )

        sql_path = pp.sql("ed_average_monthly_earnings.sql")
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year", "quarter", "sex"],
            sync_cols=[
                "avg_monthly_earnings_unadjusted",
                "avg_monthly_earnings_seasonally_adjusted",
            ],
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )

        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

        # 5) Deliverables
        output_file = output_dir / "new_entries.csv"
        res.updated_df.to_csv(out_csv_full, index=False)
        res.diff_df.to_csv(output_file, index=False)

        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_df = pd.concat([inserted_df, updated_df], ignore_index=True)

        target_cols = [
            "id",
            "Year",
            "Quarter",
            "Sex",
            "Avg Monthly Earnings Unadjusted",
            "Avg Monthly Earnings Seasonally Adjusted",
        ]

        if not delta_df.empty:
            delta_df = delta_df.rename(
                columns={
                    "id": "id",
                    "year": "Year",
                    "quarter": "Quarter",
                    "sex": "Sex",
                    "avg_monthly_earnings_unadjusted": "Avg Monthly Earnings Unadjusted",
                    "avg_monthly_earnings_seasonally_adjusted": "Avg Monthly Earnings Seasonally Adjusted",
                }
            )
            delta_df = delta_df[pd.to_numeric(delta_df["Year"], errors="coerce") >= self.MIN_DELIVERABLE_YEAR].copy()
            sex_order = {"Total": 0, "Male": 1, "Female": 2}
            delta_df["__sex_order"] = delta_df["Sex"].map(sex_order).fillna(99)
            delta_df = delta_df.sort_values(["Year", "Quarter", "__sex_order"]).drop(
                columns=["__sex_order"]
            )

            for c in target_cols:
                if c not in delta_df.columns:
                    delta_df[c] = pd.NA

            delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce")
            delta_df["id"] = delta_df["id"].map(lambda x: "" if pd.isna(x) else str(int(x)))

            # Keep deliverable numeric formatting clean when values are integer-like.
            for c in [
                "Avg Monthly Earnings Unadjusted",
                "Avg Monthly Earnings Seasonally Adjusted",
            ]:
                nums = pd.to_numeric(delta_df[c], errors="coerce")
                if nums.notna().any() and ((nums.dropna() % 1) == 0).all():
                    delta_df[c] = nums.astype("Int64")
                else:
                    delta_df[c] = nums

            write_deliverable_csv(delta_df[target_cols], deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
        # 6) State
        db_summary = {
            "status": db_comp_res.get("status"),
            "missing_in_db": db_comp_res.get("inserted"),
            "different_in_db": db_comp_res.get("updated"),
        }
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

        if (
            not is_new_by_hash(state.get("file_sha256"), file_hash)
            and res.new_rows == 0
            and res.updated_cells == 0
            and db_comp_res.get("inserted") == 0
            and db_comp_res.get("updated") == 0
        ):
            return {"status": "skipped", "message": "No new data detected.", "state": new_state}

        return {
            "status": "delivered",
            "message": (
                f"Extracted {len(df_new)} rows. DB (zeus) Comparison: "
                f"{db_comp_res.get('inserted')} missing, {db_comp_res.get('updated')} diff. "
                f"File: {deliverable_name}"
            ),
            "state": new_state,
        }
