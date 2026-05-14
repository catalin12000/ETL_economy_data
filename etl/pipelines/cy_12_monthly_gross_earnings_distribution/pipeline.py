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
from .extract import BUCKETS_IN_ORDER, extract_monthly_gross_earnings_distribution


class Pipeline:
    pipeline_id = "cy_12_monthly_gross_earnings_distribution"
    display_name = "Cyprus: Monthly Gross Earnings Distribution (Annual)"
    MIN_DELIVERABLE_YEAR = 2021

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Labour%20Cost%20and%20Earnings/Earnings/1110060E.px"
    BUCKET_TO_DB_COL = [
        ("<500", "under_500"),
        ("500- 749", "range_500_749"),
        ("750- 999", "range_750_999"),
        ("1000- 1249", "range_1000_1249"),
        ("1250- 1499", "range_1250_1499"),
        ("1500- 1749", "range_1500_1749"),
        ("1750- 1999", "range_1750_1999"),
        ("2000- 2249", "range_2000_2249"),
        ("2250- 2499", "range_2250_2499"),
        ("2500- 2749", "range_2500_2749"),
        ("2750- 2999", "range_2750_2999"),
        ("3000- 3249", "range_3000_3249"),
        ("3250- 3499", "range_3250_3499"),
        ("3500- 3749", "range_3500_3749"),
        ("3750- 3999", "range_3750_3999"),
        ("4000- 4249", "range_4000_4249"),
        ("4250- 4499", "range_4250_4449"),
        ("4500- 4749", "range_4500_4749"),
        ("4750- 4999", "range_4750_4999"),
        ("5000- 5249", "range_5000_5249"),
        ("5250- 5499", "range_5250_5499"),
        ("5500- 5749", "range_5500_5749"),
        ("5750- 5999", "range_5750_5999"),
        (">=6000", "over_6000"),
    ]

    def _build_query(self, metadata: dict) -> dict:
        variables = metadata.get("variables", [])
        if not variables:
            raise RuntimeError("Unexpected CYSTAT metadata for monthly gross earnings distribution.")

        year_var = None
        sex_var = None
        gross_var = None
        for v in variables:
            code = str(v.get("code", "")).strip().upper()
            text = str(v.get("text", "")).strip().upper()
            if code == "YEAR" or text == "YEAR":
                year_var = v
            elif code == "SEX" or text == "SEX":
                sex_var = v
            elif "GROSS MONTHLY EARNINGS" in code or "GROSS MONTHLY EARNINGS" in text:
                gross_var = v

        if year_var is None or sex_var is None or gross_var is None:
            raise RuntimeError("Could not identify YEAR/SEX/GROSS metadata for earnings distribution.")

        sex_values = sex_var.get("values", [])
        gross_values = gross_var.get("values", [])
        if not sex_values or not gross_values:
            raise RuntimeError("Missing SEX or gross earnings values in metadata.")

        return {
            "query": [
                {
                    "code": year_var["code"],
                    "selection": {"filter": "all", "values": ["*"]},
                },
                {
                    "code": sex_var["code"],
                    "selection": {"filter": "item", "values": sex_values},
                },
                {
                    "code": gross_var["code"],
                    "selection": {"filter": "item", "values": gross_values},
                },
            ],
            "response": {"format": "csv"},
        }

    def _to_db_wide(self, df_long: pd.DataFrame) -> pd.DataFrame:
        records: list[dict[str, Any]] = []
        for (year, category), grp in df_long.groupby(["Year", "Category"]):
            row: dict[str, Any] = {"year": int(year), "sex": category}
            for bucket_label, db_col in self.BUCKET_TO_DB_COL:
                vals = grp.loc[
                    grp["Gross_monthly_earnings"] == bucket_label, "Percentage_of_employees"
                ]
                row[db_col] = pd.to_numeric(vals.iloc[0], errors="coerce") if not vals.empty else pd.NA
            records.append(row)

        out = pd.DataFrame(records)
        out = out.sort_values(["year", "sex"]).reset_index(drop=True)
        return out

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "cystat_earnings_distribution.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        metadata = requests.get(self.API_URL, headers=headers, timeout=60).json()
        query = self._build_query(metadata)

        print("Requesting Earnings Distribution data from CYSTAT API...")
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

        print("Extracting monthly gross earnings distribution data...")
        df_new = extract_monthly_gross_earnings_distribution(out_path)

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
            key_cols=["Year", "Category", "Gross_monthly_earnings"],
        )

        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = self._to_db_wide(df_new)
        sql_path = pp.sql("ed_monthly_gross_earnings_distribution.sql")
        sync_cols = [db_col for _, db_col in self.BUCKET_TO_DB_COL]
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name="ed_monthly_gross_earnings_distribution",
            db_name="zeus",
            match_cols=["year", "sex"],
            sync_cols=sync_cols,
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
        delta_wide = pd.concat([inserted_df, updated_df], ignore_index=True)

        db_cols = [db_col for _, db_col in self.BUCKET_TO_DB_COL]
        target_cols = ["ID", "Year", "sex"] + db_cols

        if not delta_wide.empty:
            delta_wide = delta_wide.rename(columns={"id": "ID", "year": "Year"})
            if "ID" in delta_wide.columns:
                delta_wide["ID"] = pd.to_numeric(delta_wide["ID"], errors="coerce").astype("Int64")
            else:
                delta_wide["ID"] = pd.NA
            delta_wide["Year"] = pd.to_numeric(delta_wide["Year"], errors="coerce").astype("Int64")

            for db_col in db_cols:
                if db_col in delta_wide.columns:
                    delta_wide[db_col] = pd.to_numeric(delta_wide[db_col], errors="coerce").round(1)

            delta_wide = delta_wide.dropna(subset=["Year"])
            delta_wide = delta_wide[delta_wide["Year"] >= self.MIN_DELIVERABLE_YEAR].copy()

            category_order = {"Total": 0, "Males": 1, "Females": 2}
            delta_wide["__cat_ord"] = delta_wide.get("sex", pd.Series([], dtype=object)).map(category_order).fillna(99)
            delta_wide = delta_wide.sort_values(["Year", "__cat_ord"]).drop(columns="__cat_ord").reset_index(drop=True)

            for c in target_cols:
                if c not in delta_wide.columns:
                    delta_wide[c] = pd.NA
            write_deliverable_csv(delta_wide[target_cols], deliverable_path)
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
