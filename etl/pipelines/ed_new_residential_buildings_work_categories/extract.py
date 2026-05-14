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
                    "Year": year,
                    "Quarter": quarter,
                    "Overall Index": _parse_float(df.iat[i + 5, quarter + 1]),
                    " Earth-moving": _parse_float(df.iat[i + 6, quarter + 1]),
                    " Concrete reinforced or not": _parse_float(df.iat[i + 7, quarter + 1]),
                    " Wall-building ": _parse_float(df.iat[i + 8, quarter + 1]),
                    " Plastering": _parse_float(df.iat[i + 9, quarter + 1]),
                    " Electrical installations": _parse_float(df.iat[i + 10, quarter + 1]),
                    " Hydraulic installations": _parse_float(df.iat[i + 11, quarter + 1]),
                    " Central heating installations": _parse_float(df.iat[i + 12, quarter + 1]),
                    " Coverings-Coatings ": _parse_float(df.iat[i + 13, quarter + 1]),
                    " Carpentry": _parse_float(df.iat[i + 14, quarter + 1]),
                    " Iron and steel structures": _parse_float(df.iat[i + 15, quarter + 1]),
                    " Aluminium structures ": _parse_float(df.iat[i + 16, quarter + 1]),
                    " Painting ": _parse_float(df.iat[i + 17, quarter + 1]),
                    " Insulation ": _parse_float(df.iat[i + 18, quarter + 1]),
                    " Glazing ": _parse_float(df.iat[i + 19, quarter + 1]),
                    " Elevators ": _parse_float(df.iat[i + 20, quarter + 1]),
                    " Plaster structures": _parse_float(df.iat[i + 21, quarter + 1]),
                    " Special installations without appliances and accessories ": _parse_float(
                        df.iat[i + 22, quarter + 1]
                    ),
                }
                for quarter in range(1, 5)
            ]
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No quarterly rows extracted for new residential buildings work categories.")

    out = out.drop_duplicates(subset=["Year", "Quarter"], keep="last")
    out = out.sort_values(["Year", "Quarter"]).reset_index(drop=True)
    return out
