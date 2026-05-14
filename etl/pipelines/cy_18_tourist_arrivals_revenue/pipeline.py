from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

from etl.core.compare_csv import compare_and_update_csv
from etl.core.database import compare_with_postgres
from etl.core.download import is_new_by_hash, sha256_file
from etl.core.output import write_deliverable_csv
from .extract import extract_tourist_arrivals_revenue


class Pipeline:
    pipeline_id = "cy_18_tourist_arrivals_revenue"
    display_name = "Cyprus: Tourist Arrivals and Revenue (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Tourism/Revenue%20from%20Tourism/Monthly/2031010E.px"

    def _build_query(self, metadata: dict) -> dict:
        variables = metadata.get("variables", [])
        if not variables:
            raise RuntimeError("Unexpected CYSTAT metadata for tourist arrivals/revenue.")

        month_var = None
        metrics_var = None
        for v in variables:
            code = str(v.get("code", "")).strip().upper()
            text = str(v.get("text", "")).strip().upper()
            if code == "MONTH" or text == "MONTH":
                month_var = v
            elif "ARRIVALS AND REVENUE" in code or "ARRIVALS AND REVENUE" in text:
                metrics_var = v

        if month_var is None or metrics_var is None:
            raise RuntimeError("Could not identify MONTH/ARRIVALS_AND_REVENUE metadata.")

        metric_values = metrics_var.get("values", [])
        if not metric_values:
            raise RuntimeError("No metric values found in tourist arrivals/revenue metadata.")

        return {
            "query": [
                {
                    "code": month_var["code"],
                    "selection": {"filter": "all", "values": ["*"]},
                },
                {
                    "code": metrics_var["code"],
                    "selection": {"filter": "item", "values": metric_values},
                },
            ],
            "response": {"format": "csv"},
        }

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "18"
        out_dir = Path("data/downloads") / f"cy_{prefix}_{self.pipeline_id}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "cystat_tourist_revenue.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        metadata = requests.get(self.API_URL, headers=headers, timeout=60).json()
        query = self._build_query(metadata)

        print("Requesting Tourist Revenue data from CYSTAT API...")
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

        print("Extracting tourist arrivals/revenue data...")
        df_new = extract_tourist_arrivals_revenue(out_path)

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
            key_cols=["Year", "Month"],
        )

        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.rename(
            columns={
                "Year": "year",
                "Month": "month",
                "Arrivals": "arrivals",
                "Revenue_Millions": "revenue_millions",
            }
        )[["year", "month", "arrivals", "revenue_millions"]]

        sql_path = Path(__file__).parent / "ed_tourist_arrivals_revenue.sql"
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_tourist_arrivals_revenue",
            db_name="zeus",
            match_cols=["year", "month"],
            sync_cols=["arrivals", "revenue_millions"],
            tolerance=0.11,
            sql_file_path=str(sql_path),
        )

        if db_comp_res.get("error"):
            return {"status": "error", "message": db_comp_res["error"], "state": new_state}

        print(
            f"Postgres (zeus) comparison result: {db_comp_res.get('inserted')} missing, "
            f"{db_comp_res.get('updated')} different."
        )

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
            "ID",
            "Year",
            "Month",
            "Arrivals",
            "Revenue_Millions",
        ]

        if not delta_df.empty:
            month_keys = delta_df[["id", "year", "month"]].drop_duplicates().rename(
                columns={"id": "ID", "year": "Year", "month": "Month"}
            )
            month_keys["ID"] = pd.to_numeric(month_keys["ID"], errors="coerce").astype("Int64")
            month_keys["Year"] = pd.to_numeric(month_keys["Year"], errors="coerce").astype("Int64")
            month_keys["Month"] = pd.to_numeric(month_keys["Month"], errors="coerce").astype("Int64")

            deliver_df = df_new.merge(month_keys, on=["Year", "Month"], how="inner").copy()
            deliver_df = deliver_df.sort_values(["Year", "Month"]).reset_index(drop=True)

            # Format to match manual deliverable style.
            deliver_df["Arrivals"] = pd.to_numeric(deliver_df["Arrivals"], errors="coerce").round(0).astype("Int64")
            for c in ["Revenue_Millions", "Arrivals_yoy_change (%)", "Revenue_yoy_change (%)"]:
                nums = pd.to_numeric(deliver_df[c], errors="coerce")
                deliver_df[c] = nums.map(lambda x: "" if pd.isna(x) else f"{x:.1f}".rstrip("0").rstrip("."))

            write_deliverable_csv(deliver_df[target_cols], deliverable_path)
        else:
            write_deliverable_csv(pd.DataFrame(columns=target_cols), deliverable_path)
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
