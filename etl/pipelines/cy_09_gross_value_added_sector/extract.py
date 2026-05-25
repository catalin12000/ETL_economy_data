from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


SECTOR_CODE_TO_ACTIVITY: dict[str, str] = {
    "_T": "Total economy",
    "A": "Agriculture, forestry and fishing",
    "B": "Mining and quarrying",
    "C": "Manufacturing",
    "D": "Electricity, gas, steam and air conditioning supply",
    "E": "Water supply; sewerage, waste management and remediationies activities",
    "F": "Construction",
    "G": "Wholesale and retail trade;repair of motor vehicles and motorcycles",
    "H": "Transportation and storage",
    "I": "Accommodation and food service activities",
    "J": "Information and communication",
    "K": "Financial and insurance activities",
    "L": "Real estate activities",
    "M": "Professional, scientific and technical activities",
    "N": "Administrative and support service activities",
    "O": "Public administration and defence; compulsory social security",
    "P": "Education",
    "Q": "Human health and social work activities",
    "R": "Arts, entertainment and recreation",
    "S": "Other service activities",
    "T": "Activities of households as employers",
}


def _extract_sector_code(label: str) -> str | None:
    parts = [p.strip() for p in str(label).split(",")]
    if len(parts) < 2:
        return None
    return parts[1]


def extract_gross_value_added_sector(csv_path: Path) -> pd.DataFrame:
    """
    Extract CYSTAT GVA by sector as:
      year, economic_activity, volume_measures_million
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [str(c).replace("\ufeff", "").replace('"', "").strip() for c in df.columns]

    if df.columns.empty:
        raise RuntimeError("Empty CSV: no columns found for GVA by sector.")

    sector_col = df.columns[0]
    year_cols: list[tuple[str, int]] = []
    for col in df.columns[1:]:
        m = re.search(r"(\d{4})$", str(col))
        if m:
            year_cols.append((col, int(m.group(1))))

    if not year_cols:
        raise RuntimeError("No year columns found in GVA by sector CSV.")

    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        sector_code = _extract_sector_code(row[sector_col])
        activity = SECTOR_CODE_TO_ACTIVITY.get(sector_code or "")
        if not activity:
            continue

        for col_name, year in year_cols:
            value = pd.to_numeric(row[col_name], errors="coerce")
            if pd.isna(value):
                continue
            rows.append(
                {
                    "year": int(year),
                    "economic_activity": activity,
                    "volume_measures_million": round(float(value), 1),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No rows extracted for GVA by sector.")

    activity_order = {name: i for i, name in enumerate(SECTOR_CODE_TO_ACTIVITY.values())}
    out["__ord"] = out["economic_activity"].map(activity_order).fillna(999)
    out = out.sort_values(["year", "__ord"]).drop(columns=["__ord"]).reset_index(drop=True)
    return out
