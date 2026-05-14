from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, "rb") as f:
        sig = f.read(2)
        if sig == b"PK":
            return "openpyxl"
    return "xlrd"


def _parse_year(value) -> Optional[int]:
    year = pd.to_numeric(value, errors="coerce")
    if pd.isna(year):
        return None
    year = int(year)
    return year if 1900 <= year <= 2100 else None


def _parse_float(value) -> float | pd.NA:
    num = pd.to_numeric(value, errors="coerce")
    return float(num) if pd.notna(num) else pd.NA


def extract_new_residential_building_cost_index(xls_path: Path) -> pd.DataFrame:
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))
    if df.empty:
        raise RuntimeError("Empty file for new residential building cost index extraction.")

    records: list[dict[str, object]] = []

    for i in range(len(df) - 3):
        year = _parse_year(df.iat[i, 0])
        if year is None:
            continue

        row_overall = str(df.iat[i + 1, 0]).strip().lower()
        row_material = str(df.iat[i + 2, 0]).strip().lower()
        row_labour = str(df.iat[i + 3, 0]).strip().lower()
        if "overall cost index" not in row_overall:
            continue
        if "material costs index" not in row_material:
            continue
        if "labour costs index" not in row_labour:
            continue

        for quarter in range(1, 5):
            overall = _parse_float(df.iat[i + 1, quarter])
            material = _parse_float(df.iat[i + 2, quarter])
            labour = _parse_float(df.iat[i + 3, quarter])
            if all(pd.isna(v) for v in (overall, material, labour)):
                continue

            records.append(
                {
                    "Year": year,
                    "Quarter": quarter,
                    "Overall Cost Index": overall,
                    "Material Costs Index": material,
                    "Labour Costs Index": labour,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No quarterly rows extracted for new residential building cost index.")

    out = out.drop_duplicates(subset=["Year", "Quarter"], keep="last")
    out = out.sort_values(["Year", "Quarter"]).reset_index(drop=True)
    return out
