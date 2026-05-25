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
from etl.core.paths import PipelinePaths
from .extract import extract_construction_index


class Pipeline:
    pipeline_id = "cy_04_construction_index_cy"
    country = "cy"
    source = "cystat"
    db_table_name = "ed_construction_index_cy"
    source_type = "api"
    display_name = "Cyprus: Construction Materials Price Index (Monthly)"

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Price%20Index%20of%20Construction%20Materials/1420013E.px"
    MIN_DELIVERABLE_YEAR = 2022

    def _build_query(self, metadata: dict) -> dict:
        # Query all available periods, only direct index metric.
        variables = metadata.get("variables", [])
        if not variables:
            raise RuntimeError("Unexpected CYSTAT metadata for construction index.")

        index_var = None
        for v in variables:
            if str(v.get("code", "")).strip().upper() == "INDEX":
                index_var = v
                break
        if index_var is None:
            index_var = variables[-1]

        index_values = index_var.get("values", [])
        if not index_values:
            raise RuntimeError("No INDEX values found in construction index metadata.")

        # First value is the monthly index in this dataset.
        return {
            "query": [
                {
                    "code": index_var["code"],
                    "selection": {"filter": "item", "values": [index_values[0]]},
                }
            ],
            "response": {"format": "csv"},
        }

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "cystat_construction_materials_index.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Download
        metadata = requests.get(self.API_URL, headers=headers, timeout=60).json()
        query = self._build_query(metadata)

        print("Requesting Construction Index data from CYSTAT API...")
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
        print("Extracting construction index data...")
        df_new = extract_construction_index(out_path)

        # 3) Local baseline compare
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
            key_cols=["Year", "Month"],
        )

        # 4) DB compare (READ-ONLY, zeus)
        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = df_new.rename(columns={"Year": "year", "Month": "month", "Index": "index"})
        sql_path = pp.sql("ed_construction_index_cy.sql")

        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year", "month"],
            sync_cols=["index"],
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

        target_cols = ["id", "Year", "Month", "Index"]
        if not delta_df.empty:
            delta_df = delta_df.rename(columns={"id": "id", "year": "Year", "month": "Month", "index": "Index"})
            delta_df["id"] = pd.to_numeric(delta_df["id"], errors="coerce")
            delta_df["Year"] = pd.to_numeric(delta_df["Year"], errors="coerce")
            delta_df["Month"] = pd.to_numeric(delta_df["Month"], errors="coerce")
            delta_df["Index"] = pd.to_numeric(delta_df["Index"], errors="coerce").round(2)
            delta_df = delta_df[delta_df["Year"] >= self.MIN_DELIVERABLE_YEAR].copy()
            delta_df = delta_df.dropna(subset=["Year", "Month", "Index"])
            delta_df["Year"] = delta_df["Year"].astype(int)
            delta_df["Month"] = delta_df["Month"].astype(int)
            delta_df = delta_df.sort_values(["Year", "Month"]).reset_index(drop=True)

            delta_df["id"] = delta_df["id"].map(lambda x: "" if pd.isna(x) else str(int(x)))

            for c in target_cols:
                if c not in delta_df.columns:
                    delta_df[c] = pd.NA
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

