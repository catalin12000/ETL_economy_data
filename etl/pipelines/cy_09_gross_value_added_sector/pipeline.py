from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests

from etl.core.database import compare_with_postgres
from etl.core.download import is_new_by_hash, sha256_file
from etl.core.output import write_deliverable_csv
from etl.core.paths import PipelinePaths
from .extract import extract_gross_value_added_sector


class Pipeline:
    pipeline_id = "cy_09_gross_value_added_sector"
    country = "cy"
    source = "cystat"
    db_table_name = "ed_gross_value_added_sector"
    source_type = "api"
    display_name = "Cyprus: Gross Value Added By Sector (Annual)"
    MIN_DELIVERABLE_YEAR = 2021

    API_URL = "https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/National%20Accounts/Annual%20National%20Accounts/0610020E.px"
    SECTOR_VALUE_CODES = [
        "3",
        "4",
        "8",
        "9",
        "29",
        "30",
        "33",
        "34",
        "38",
        "44",
        "45",
        "50",
        "54",
        "55",
        "61",
        "66",
        "67",
        "68",
        "71",
        "74",
        "78",
    ]

    # (deliverable activity, db column)
    ACTIVITY_DB_MAP = [
        ("Total economy", "total_economy"),
        ("Agriculture, forestry and fishing", "agriculture_forestry_fishing"),
        ("Mining and quarrying", "mining_quarrying"),
        ("Manufacturing", "manufacturing"),
        ("Electricity, gas, steam and air conditioning supply", "electricity_gas_steam_ac_supply"),
        ("Water supply; sewerage, waste management and remediationies activities", "water_supply_sewerage_waste_mgmt"),
        ("Construction", "construction"),
        ("Wholesale and retail trade;repair of motor vehicles and motorcycles", "wholesale_retail_trade_motor_repair"),
        ("Transportation and storage", "transportation_storage"),
        ("Accommodation and food service activities", "accommodation_food_services"),
        ("Information and communication", "info_communication"),
        ("Financial and insurance activities", "financial_insurance_activities"),
        ("Real estate activities", "real_estate_activities"),
        ("Professional, scientific and technical activities", "prof_sci_tech_activities"),
        ("Administrative and support service activities", "admin_support_services_activities"),
        ("Public administration and defence; compulsory social security", "public_admin_defence_social_sec"),
        ("Education", "education"),
        ("Human health and social work activities", "human_health_social_work_activities"),
        ("Arts, entertainment and recreation", "arts_entertainment_recreation"),
        ("Other service activities", "other_services_activities"),
        ("Activities of households as employers", "household_employer_activities"),
    ]

    def _build_query(self, metadata: dict) -> dict:
        variables = metadata.get("variables", [])
        if not variables:
            raise RuntimeError("Unexpected CYSTAT metadata for GVA by sector.")

        indicator_var = None
        measure_var = None
        for v in variables:
            code = str(v.get("code", "")).strip().upper()
            text = str(v.get("text", "")).strip().upper()
            if "INDICATORS BY ECONOMIC ACTIVITY NACE" in code or "INDICATORS BY ECONOMIC ACTIVITY NACE" in text:
                indicator_var = v
            if code == "MEASURE":
                measure_var = v

        if indicator_var is None or measure_var is None:
            raise RuntimeError("Could not identify indicator/measure metadata for GVA by sector.")

        measure_values = measure_var.get("values", [])
        measure_texts = measure_var.get("valueTexts", [])
        if not measure_values:
            raise RuntimeError("No MEASURE values found in GVA by sector metadata.")

        in_real_code = measure_values[0]
        for value_code, value_text in zip(measure_values, measure_texts):
            if "IN REAL TERMS (MILLION EURO)" in str(value_text).upper():
                in_real_code = value_code
                break

        return {
            "query": [
                {
                    "code": measure_var["code"],
                    "selection": {"filter": "item", "values": [in_real_code]},
                },
                {
                    "code": indicator_var["code"],
                    "selection": {"filter": "item", "values": self.SECTOR_VALUE_CODES},
                },
            ],
            "response": {"format": "csv"},
        }

    def _to_db_wide(self, df_long: pd.DataFrame) -> pd.DataFrame:
        records: list[dict[str, Any]] = []
        for year, grp in df_long.groupby("year"):
            row: dict[str, Any] = {"year": int(year)}
            for activity, db_col in self.ACTIVITY_DB_MAP:
                vals = grp.loc[grp["economic_activity"] == activity, "volume_measures_million"]
                row[db_col] = pd.to_numeric(vals.iloc[0], errors="coerce") if not vals.empty else pd.NA
            records.append(row)

        out = pd.DataFrame(records).sort_values("year").reset_index(drop=True)
        return out

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        pp = PipelinePaths(self.pipeline_id)
        out_dir = pp.downloaded
        out_path = out_dir / "cystat_gva_sector_annual.csv"

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        metadata = requests.get(self.API_URL, headers=headers, timeout=60).json()
        query = self._build_query(metadata)

        print("Requesting GVA By Sector data from CYSTAT API...")
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

        print("Extracting GVA by sector data...")
        df_new = extract_gross_value_added_sector(out_path)

        output_dir = pp.output


        print("Comparing extraction with live Cyprus Postgres DB (zeus)...")
        df_for_db = self._to_db_wide(df_new)
        sql_path = pp.sql("ed_gross_value_added_sector.sql")
        sync_cols = [db_col for _, db_col in self.ACTIVITY_DB_MAP]
        db_comp_res = compare_with_postgres(
            df=df_for_db,
            table_name=self.db_table_name,
            db_name="zeus",
            match_cols=["year"],
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


        now = datetime.now()
        deliverable_name = f"deliverable_{self.pipeline_id}_{now.strftime('%B_%Y')}.csv"
        deliverable_path = output_dir / deliverable_name

        inserted_df = db_comp_res.get("inserted_df", pd.DataFrame())
        updated_df = db_comp_res.get("updated_df", pd.DataFrame())
        delta_wide = pd.concat([inserted_df, updated_df], ignore_index=True)

        db_cols = [db_col for _, db_col in self.ACTIVITY_DB_MAP]
        target_cols = ["id", "year"] + db_cols

        if not delta_wide.empty:
            if "id" in delta_wide.columns:
                delta_wide["id"] = pd.to_numeric(delta_wide["id"], errors="coerce").astype("Int64")
            else:
                delta_wide["id"] = pd.NA
            delta_wide["year"] = pd.to_numeric(delta_wide["year"], errors="coerce").astype("Int64")

            for db_col in db_cols:
                if db_col in delta_wide.columns:
                    delta_wide[db_col] = pd.to_numeric(delta_wide[db_col], errors="coerce").round(1)

            delta_wide = delta_wide.dropna(subset=["year"])
            delta_wide = delta_wide[delta_wide["year"] >= self.MIN_DELIVERABLE_YEAR].copy()
            delta_wide = delta_wide.sort_values("year").reset_index(drop=True)

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
                "db_comparison": db_summary,
                "deliverable_path": str(deliverable_path),
            }
        )

        if (
            not is_new_by_hash(state.get("file_sha256"), file_hash)
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
