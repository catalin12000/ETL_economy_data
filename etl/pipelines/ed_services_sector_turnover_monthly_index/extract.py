from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


_ROMAN = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6,
          "VII": 7, "VIII": 8, "IX": 9, "X": 10, "XI": 11, "XII": 12}

# XLS code (row 10, DT suffix stripped) → human-readable economic activity name
_CODE_TO_ACTIVITY: dict[str, str] = {
    "H49":      "Land transport and transport via pipelines",
    "H50":      "Water transport",
    "H51":      "Air transport",
    "H52":      "Warehousing and support activities for transportation",
    "H53":      "Postal and courier activities",
    "H__":      "Section H - Transportation and storage",
    "I55":      "Accommodation",
    "I56":      "Food and beverage service activities",
    "I__":      "Section I - Accommodation and food service activities",
    "J58":      "Publishing activities",
    "J59":      "Motion picture, video and television programme activities",
    "J60":      "Broadcasting and programming activities",
    "J61":      "Telecommunications",
    "J62":      "Computer programming and consultancy",
    "J63":      "Information service activities",
    "J__":      "Section J - Information and communication",
    "L68":      "Real estate activities",
    "L__":      "Section L - Real estate activities",
    "M69":      "Legal and accounting activities",
    "M69_702":  "Legal, accounting and management consultancy",
    "M702":     "Management consultancy activities",
    "M71":      "Architectural and engineering activities",
    "M73":      "Advertising and market research",
    "M74":      "Other professional, scientific and technical activities",
    "M__":      "Section M - Professional, scientific and technical activities",
    "N77":      "Rental and leasing activities",
    "N78":      "Employment activities",
    "N79":      "Travel agency and tour operator activities",
    "N80":      "Security and investigation activities",
    "N81":      "Services to buildings and landscape activities",
    "N82":      "Office administrative and support activities",
    "N__":      "Section N - Administrative and support service activities",
    "HTNXK":    "Total turnover index (all service sections)",
    "HTNXK_":   "Total turnover index (all service sections)",
}


def _norm_code(raw: str) -> str:
    """Strip DT suffix and trailing underscores, uppercase."""
    s = str(raw).strip().upper()
    if s.endswith("DT"):
        s = s[:-2]
    return s.rstrip("_") or s


def _parse_period(label: str, current_year: Optional[int]) -> tuple[Optional[int], Optional[int]]:
    """Parse '2021   I' or 'II' style labels into (year, month)."""
    s = str(label).strip()
    year_match = re.search(r"(20\d{2}|19\d{2})", s)
    if year_match:
        current_year = int(year_match.group(1))
        s = s[year_match.end():]

    token = re.sub(r"\s+", "", s).upper()
    month = _ROMAN.get(token)
    return current_year, month


def extract_services_sector_turnover(xls_path: Path) -> pd.DataFrame:
    """
    Extract DKT54 TABLE 1 to long form matching the DB schema.

    XLS layout:
      Row 9:  section labels (div.49, SECTION H, …)
      Row 10: codes with DT suffix (H49DT, H50DT, …)
      Row 11+: data — col 0 = period ('2021   I', 'II', …), cols 1-33 = index values

    Output columns: Year, Month, Code, Economic_Activity, Index
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name="TABLE 1", header=None)

    # Build col_index → (code, activity) from row 10
    col_meta: dict[int, tuple[str, str]] = {}
    for col_idx, cell in enumerate(df.iloc[10].tolist()):
        raw = str(cell).strip()
        if not raw or raw.lower() == "nan":
            continue
        code = _norm_code(raw)
        activity = _CODE_TO_ACTIVITY.get(code) or _CODE_TO_ACTIVITY.get(code + "_")
        if activity:
            col_meta[col_idx] = (code, activity)

    if not col_meta:
        raise RuntimeError(
            f"No recognised service codes found in row 10 of {xls_path.name}. "
            f"Row 10 values: {df.iloc[10].tolist()}"
        )

    records: list[dict] = []
    current_year: Optional[int] = None

    for row_idx in range(11, len(df)):
        raw_period = df.iat[row_idx, 0]
        if pd.isna(raw_period):
            continue

        label = str(raw_period).strip()
        if not label or label.lower() in ("nan", "source", "note"):
            break

        current_year, month = _parse_period(label, current_year)
        if current_year is None or month is None:
            continue

        for col_idx, (code, activity) in col_meta.items():
            if col_idx >= df.shape[1]:
                continue
            try:
                val = float(df.iat[row_idx, col_idx])
            except (ValueError, TypeError):
                continue
            if pd.isna(val):
                continue
            records.append(
                {
                    "Year": current_year,
                    "Month": month,
                    "Code": code,
                    "Economic_Activity": activity,
                    "Index": round(val, 6),
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(f"No data extracted from {xls_path.name}.")

    return out.sort_values(["Year", "Month", "Code"]).reset_index(drop=True)
