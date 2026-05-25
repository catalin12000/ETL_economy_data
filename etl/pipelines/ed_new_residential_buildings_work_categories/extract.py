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


def _parse_year_from_title(value) -> Optional[int]:
    if pd.isna(value):
        return None
    text = str(value)
    for token in text.split():
        if token.isdigit() and len(token) == 4:
            year = int(token)
            if 1900 <= year <= 2100:
                return year
    return None


def _parse_float(value):
    num = pd.to_numeric(value, errors="coerce")
    return float(num) if pd.notna(num) else pd.NA


def extract_new_residential_buildings_work_categories(xls_path: Path) -> pd.DataFrame:
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))
    if df.empty:
        raise RuntimeError("Empty file for new residential buildings work categories extraction.")

    records: list[dict[str, object]] = []

    for i in range(len(df) - 19):
        title = str(df.iat[i, 0]).strip().upper()
        if "QUARTERLY PRICE INDICES OF WORK CATEGORIES IN CONSTRUCTION" not in title:
            continue

        year = _parse_year_from_title(df.iat[i + 1, 0])
        if year is None:
            continue

        records.extend(
            [
                {
                    "year": year,
                    "quarter": quarter,
                    "overall_index": _parse_float(df.iat[i + 5, quarter + 1]),
                    "earth_moving": _parse_float(df.iat[i + 6, quarter + 1]),
                    "concrete_reinforced": _parse_float(df.iat[i + 7, quarter + 1]),
                    "wall_building": _parse_float(df.iat[i + 8, quarter + 1]),
                    "plastering": _parse_float(df.iat[i + 9, quarter + 1]),
                    "electrical_installations": _parse_float(df.iat[i + 10, quarter + 1]),
                    "hydraulic_installations": _parse_float(df.iat[i + 11, quarter + 1]),
                    "central_heating_installations": _parse_float(df.iat[i + 12, quarter + 1]),
                    "coverings_coatings": _parse_float(df.iat[i + 13, quarter + 1]),
                    "carpentry": _parse_float(df.iat[i + 14, quarter + 1]),
                    "iron_steel_structures": _parse_float(df.iat[i + 15, quarter + 1]),
                    "aluminium_structures": _parse_float(df.iat[i + 16, quarter + 1]),
                    "painting": _parse_float(df.iat[i + 17, quarter + 1]),
                    "insulation": _parse_float(df.iat[i + 18, quarter + 1]),
                    "glazing": _parse_float(df.iat[i + 19, quarter + 1]),
                    "elevators": _parse_float(df.iat[i + 20, quarter + 1]),
                    "plaster_structures": _parse_float(df.iat[i + 21, quarter + 1]),
                    "special_installations_without_appliances_accessories": _parse_float(
                        df.iat[i + 22, quarter + 1]
                    ),
                }
                for quarter in range(1, 5)
            ]
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No quarterly rows extracted for new residential buildings work categories.")

    out = out.drop_duplicates(subset=["year", "quarter"], keep="last")
    out = out.sort_values(["year", "quarter"]).reset_index(drop=True)
    return out
