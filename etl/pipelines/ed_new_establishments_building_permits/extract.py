from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


AREA_COLUMNS = [
    ("Total", 1, 2),
    ("Urban Areas", 3, 4),
    ("Semi-Urban Areas", 5, 6),
    ("Rural Areas", 7, 8),
]


def _parse_float(value) -> Optional[float]:
    if pd.isna(value):
        return None
    s = str(value).strip().replace(",", "")
    if not s or s in {"-", "...", "…", ":", "u", "N/A", "nan"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_int(value) -> Optional[int]:
    v = _parse_float(value)
    return int(round(v)) if v is not None else None


def _clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _parse_period(df: pd.DataFrame) -> tuple[int, int]:
    month_row = " ".join(_clean_text(v) for v in df.iloc[3].tolist() if _clean_text(v))
    month_row = month_row.strip()
    match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
        month_row,
        flags=re.IGNORECASE,
    )
    if not match:
        raise RuntimeError(f"Could not parse period from workbook header row: {month_row!r}")

    month_map = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    month = month_map[match.group(1).lower()]
    year = int(match.group(2))
    return year, month


def _is_regional_unit_row(greek_label: str, english_label: str) -> bool:
    return (
        greek_label.startswith("ΠΕΡΙΦ. ΕΝΟΤ.")
        or english_label.startswith("REGIONAL UNIT OF")
        or greek_label == "Σ Υ Ν Ο Λ Ο Ε Λ Λ Α Δ Ο Σ"
        or english_label == "GREECE, TOTAL"
    )


def _normalize_total_label(greek_label: str, english_label: str) -> tuple[str, str]:
    if greek_label == "Σ Υ Ν Ο Λ Ο Ε Λ Λ Α Δ Ο Σ" or english_label == "GREECE, TOTAL":
        return "Greece, Total", "Total"
    return english_label or greek_label, "Total"


def extract_new_establishments(xls_path: Path) -> pd.DataFrame:
    """
    Extract SOP03 Table 16 to DB-shaped long rows.

    Output columns:
      year, month, regional_unit, area_type, category_of_use, number, volume
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None)
    if df.empty:
        raise RuntimeError(f"Workbook {xls_path.name} is empty.")

    year, month = _parse_period(df)
    records: list[dict] = []

    current_regional_unit = ""
    for row_idx in range(9, len(df)):
        greek_label = _clean_text(df.iat[row_idx, 0])
        english_label = _clean_text(df.iat[row_idx, 9]) if df.shape[1] > 9 else ""

        if not greek_label:
            continue
        if greek_label.startswith(("1)", "2)")):
            break

        if _is_regional_unit_row(greek_label, english_label):
            current_regional_unit, category_of_use = _normalize_total_label(greek_label, english_label)
        else:
            if not current_regional_unit:
                # Skip any malformed rows before the first regional bucket.
                continue
            category_of_use = english_label or greek_label

        for area_type, number_col, volume_col in AREA_COLUMNS:
            number = _parse_int(df.iat[row_idx, number_col]) if number_col < df.shape[1] else None
            volume = _parse_float(df.iat[row_idx, volume_col]) if volume_col < df.shape[1] else None
            if number is None and volume is None:
                continue
            records.append(
                {
                    "year": year,
                    "month": month,
                    "regional_unit": current_regional_unit,
                    "area_type": area_type,
                    "category_of_use": category_of_use,
                    "number": number,
                    "volume": volume,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(f"No rows extracted from {xls_path.name}.")

    out = out.sort_values(
        ["year", "month", "regional_unit", "category_of_use", "area_type"]
    ).reset_index(drop=True)
    return out
