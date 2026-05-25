from __future__ import annotations

from pathlib import Path

import pandas as pd


TARGET_COLUMNS = [
    "year",
    "quarter",
    "mining_and_quarrying",
    "manufacturing",
    "electricity_gas_steam_air_conditioning_supply",
    "water_supply_sewerage_waste_management_remediation_activities",
    "construction",
    "wholesale_retail_trade_repair_of_motor_vehicles_motorcycles",
    "transportation_and_storage",
    "accommodation_and_food_service_activities",
    "information_and_communication",
    "professional_scientific_and_technical_activities",
    "administrative_and_support_service_activities",
]


def extract_wage_growth_index(xls_path: Path) -> pd.DataFrame:
    df = pd.read_excel(xls_path, sheet_name="TABLE 3", header=None)

    records: list[dict[str, object]] = []
    current_year: int | None = None

    for row_idx in range(13, len(df)):
        raw_year = df.iat[row_idx, 0]
        raw_quarter = df.iat[row_idx, 1]

        if pd.notna(raw_year):
            year_text = str(raw_year).strip()
            if year_text.lower().startswith("note"):
                break
            current_year = int(float(raw_year))

        quarter_text = "" if pd.isna(raw_quarter) else str(raw_quarter).strip().upper()
        quarter = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(quarter_text)
        if current_year is None or quarter is None:
            continue

        values = {
            "mining_and_quarrying": pd.to_numeric(df.iat[row_idx, 2], errors="coerce"),
            "manufacturing": pd.to_numeric(df.iat[row_idx, 3], errors="coerce"),
            "electricity_gas_steam_air_conditioning_supply": pd.to_numeric(df.iat[row_idx, 4], errors="coerce"),
            "water_supply_sewerage_waste_management_remediation_activities": pd.to_numeric(df.iat[row_idx, 5], errors="coerce"),
            "construction": pd.to_numeric(df.iat[row_idx, 7], errors="coerce"),
            "wholesale_retail_trade_repair_of_motor_vehicles_motorcycles": pd.to_numeric(df.iat[row_idx, 8], errors="coerce"),
            "transportation_and_storage": pd.to_numeric(df.iat[row_idx, 9], errors="coerce"),
            "accommodation_and_food_service_activities": pd.to_numeric(df.iat[row_idx, 10], errors="coerce"),
            "information_and_communication": pd.to_numeric(df.iat[row_idx, 11], errors="coerce"),
            "professional_scientific_and_technical_activities": pd.to_numeric(df.iat[row_idx, 13], errors="coerce"),
            "administrative_and_support_service_activities": pd.to_numeric(df.iat[row_idx, 14], errors="coerce"),
        }

        if all(pd.isna(v) for v in values.values()):
            continue

        row = {
            "year": current_year,
            "quarter": quarter,
        }
        row.update({k: float(v) if pd.notna(v) else pd.NA for k, v in values.items()})
        records.append(row)

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No wage growth index rows extracted from TABLE 3.")

    return out[TARGET_COLUMNS]
