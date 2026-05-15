from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


def _parse_float(value) -> Optional[float]:
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s in {"-", "...", "…", ":", "u", "N/A", "nan"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_int(value) -> Optional[int]:
    v = _parse_float(value)
    return int(round(v)) if v is not None else None


_KW_UNIT = {"regional unit", "περιφερειακή ενότητα", "νομός", "regional_unit"}
_KW_YEAR = {"year", "έτος"}
_KW_MONTH = {"month", "μήνας"}
_KW_AREA_TYPE = {"area type", "τύπος περιοχ", "area_type", "urban", "αστικ"}
_KW_CATEGORY = {"category of use", "κατηγορία χρήσης", "category_of_use", "use"}
_KW_NUMBER = {"number", "αριθμός", "αριθμ"}
_KW_VOLUME = {"volume", "όγκος"}


def _col_match(header: str, keywords: set[str]) -> bool:
    h = str(header).lower().strip()
    return any(kw in h for kw in keywords)


def _find_header_row(df: pd.DataFrame) -> Optional[int]:
    for i in range(min(60, len(df))):
        row_text = " | ".join(str(v) for v in df.iloc[i].tolist()).lower()
        hits = sum(
            1 for kw in ("year", "έτος", "month", "μήνας", "number", "αριθμ",
                         "category", "κατηγορία", "regional", "περιφερ")
            if kw in row_text
        )
        if hits >= 3:
            return i
    return None


def extract_new_establishments(xls_path: Path) -> pd.DataFrame:
    """
    Extract SOP03 Table 16 "New establishments, number and volume by category of use".

    Output columns match DB:
      year, month, regional_unit, area_type, category_of_use, number, volume
    """
    xls_path = Path(xls_path)
    df = None
    for sheet in ("TABLE 16", "TABLE16", "TABLE 016", "Πίνακας 16", 0):
        try:
            df = pd.read_excel(xls_path, sheet_name=sheet, header=None)
            break
        except Exception:
            continue
    if df is None or df.empty:
        raise RuntimeError(f"Could not read sheet TABLE 16 from {xls_path.name}.")

    header_row = _find_header_row(df)
    if header_row is None:
        raise RuntimeError(
            f"Could not locate header row in {xls_path.name}. "
            f"First 5 rows:\n{df.head(5).to_string()}"
        )

    headers = [str(v).strip() for v in df.iloc[header_row].tolist()]

    col_unit = col_year = col_month = col_area_type = col_category = col_number = col_volume = None

    for i, h in enumerate(headers):
        if col_unit is None and _col_match(h, _KW_UNIT):
            col_unit = i
        elif col_year is None and _col_match(h, _KW_YEAR):
            col_year = i
        elif col_month is None and _col_match(h, _KW_MONTH):
            col_month = i
        elif col_area_type is None and _col_match(h, _KW_AREA_TYPE):
            col_area_type = i
        elif col_category is None and _col_match(h, _KW_CATEGORY):
            col_category = i
        elif col_number is None and _col_match(h, _KW_NUMBER):
            col_number = i
        elif col_volume is None and _col_match(h, _KW_VOLUME):
            col_volume = i

    missing = [
        name for name, col in [("year", col_year), ("month", col_month)]
        if col is None
    ]
    if missing:
        raise RuntimeError(
            f"Could not map required columns {missing} in {xls_path.name}. "
            f"Detected headers: {headers}"
        )

    records: list[dict] = []
    current_unit: str = ""
    current_area_type: str = ""
    current_category: str = ""

    for row_idx in range(header_row + 1, len(df)):
        row = df.iloc[row_idx]

        if col_unit is not None:
            raw = str(row.iloc[col_unit]).strip()
            if raw and raw.lower() not in ("nan", "none"):
                current_unit = raw
        if col_area_type is not None:
            raw = str(row.iloc[col_area_type]).strip()
            if raw and raw.lower() not in ("nan", "none"):
                current_area_type = raw
        if col_category is not None:
            raw = str(row.iloc[col_category]).strip()
            if raw and raw.lower() not in ("nan", "none"):
                current_category = raw

        year_val = pd.to_numeric(row.iloc[col_year], errors="coerce")
        month_val = pd.to_numeric(row.iloc[col_month], errors="coerce")
        if pd.isna(year_val) or pd.isna(month_val):
            continue
        year = int(year_val)
        month = int(month_val)
        if not (1 <= month <= 12):
            continue

        records.append(
            {
                "year": year,
                "month": month,
                "regional_unit": current_unit,
                "area_type": current_area_type,
                "category_of_use": current_category,
                "number": _parse_int(row.iloc[col_number]) if col_number is not None else None,
                "volume": _parse_float(row.iloc[col_volume]) if col_volume is not None else None,
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(
            f"No rows extracted from {xls_path.name}. "
            f"Header row detected at {header_row}."
        )

    out = out.sort_values(["year", "month", "regional_unit", "area_type", "category_of_use"]).reset_index(drop=True)
    return out
