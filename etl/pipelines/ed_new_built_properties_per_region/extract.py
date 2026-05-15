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


_KEYWORDS_REGION = {"region", "περιφέρεια", "διαμέρισμα"}
_KEYWORDS_UNIT = {"regional unit", "περιφερειακή ενότητα", "νομός", "nomos"}
_KEYWORDS_YEAR = {"year", "έτος"}
_KEYWORDS_MONTH = {"month", "μήνας"}
_KEYWORDS_NUMBER = {"number", "αριθμός", "αριθμ"}
_KEYWORDS_STOREYS = {"storey", "ορόφ"}
_KEYWORDS_VOLUME = {"volume", "όγκος"}
_KEYWORDS_AREA = {"area", "surface", "επιφάνεια"}


def _col_match(header: str, keywords: set[str]) -> bool:
    h = str(header).lower().strip()
    return any(kw in h for kw in keywords)


def _find_header_row(df: pd.DataFrame) -> Optional[int]:
    """Return the first row index that looks like a column header."""
    for i in range(min(60, len(df))):
        row_text = " | ".join(str(v) for v in df.iloc[i].tolist()).lower()
        hits = sum(1 for kw in ("year", "έτος", "month", "μήνας", "number", "αριθμ", "region", "περιφέρεια")
                   if kw in row_text)
        if hits >= 3:
            return i
    return None


def extract_new_built_properties(xls_path: Path) -> pd.DataFrame:
    """
    Extract SOP03 Table 01 "New built properties, storeys, volume and surface
    thereon, by region and regional unit" to flat form.

    Expected XLS layout:
      Columns: Region, Regional Unit, Year, Month, Number, Storeys, Volume, Area
      (or time×geography pivot — both orientations attempted)

    Output columns match DB: region, regional_unit, year, month, number, storeys, volume, area
    """
    xls_path = Path(xls_path)
    df = None
    for sheet in ("TABLE 1", "TABLE1", "TABLE 01", "Πίνακας 1", 0):
        try:
            df = pd.read_excel(xls_path, sheet_name=sheet, header=None)
            break
        except Exception:
            continue
    if df is None or df.empty:
        raise RuntimeError(f"Could not read a table sheet from {xls_path.name}.")

    header_row = _find_header_row(df)
    if header_row is None:
        raise RuntimeError(
            f"Could not locate header row in {xls_path.name}. "
            f"First 5 rows:\n{df.head(5).to_string()}"
        )

    headers = [str(v).strip() for v in df.iloc[header_row].tolist()]

    # Map column positions
    col_region = col_unit = col_year = col_month = None
    col_number = col_storeys = col_volume = col_area = None

    for i, h in enumerate(headers):
        if col_region is None and _col_match(h, _KEYWORDS_REGION):
            col_region = i
        elif col_unit is None and _col_match(h, _KEYWORDS_UNIT):
            col_unit = i
        elif col_year is None and _col_match(h, _KEYWORDS_YEAR):
            col_year = i
        elif col_month is None and _col_match(h, _KEYWORDS_MONTH):
            col_month = i
        elif col_number is None and _col_match(h, _KEYWORDS_NUMBER):
            col_number = i
        elif col_storeys is None and _col_match(h, _KEYWORDS_STOREYS):
            col_storeys = i
        elif col_volume is None and _col_match(h, _KEYWORDS_VOLUME):
            col_volume = i
        elif col_area is None and _col_match(h, _KEYWORDS_AREA):
            col_area = i

    missing = [
        name for name, col in [
            ("region", col_region), ("regional_unit", col_unit),
            ("year", col_year), ("month", col_month),
            ("number", col_number),
        ]
        if col is None
    ]
    if missing:
        raise RuntimeError(
            f"Could not map columns {missing} in {xls_path.name}. "
            f"Detected headers: {headers}"
        )

    records: list[dict] = []
    current_region: str = ""
    current_unit: str = ""

    for row_idx in range(header_row + 1, len(df)):
        row = df.iloc[row_idx]

        # Propagate region / unit from non-blank cells (hierarchical fill-down)
        raw_region = str(row.iloc[col_region]).strip() if col_region is not None else ""
        raw_unit = str(row.iloc[col_unit]).strip() if col_unit is not None else ""
        if raw_region and raw_region.lower() not in ("nan", "none"):
            current_region = raw_region
        if raw_unit and raw_unit.lower() not in ("nan", "none"):
            current_unit = raw_unit

        year_val = pd.to_numeric(row.iloc[col_year], errors="coerce") if col_year is not None else None
        month_val = pd.to_numeric(row.iloc[col_month], errors="coerce") if col_month is not None else None

        if pd.isna(year_val) or pd.isna(month_val):
            continue
        year = int(year_val)
        month = int(month_val)
        if not (1 <= month <= 12):
            continue

        number = _parse_int(row.iloc[col_number]) if col_number is not None else None
        storeys = _parse_float(row.iloc[col_storeys]) if col_storeys is not None else None
        volume = _parse_float(row.iloc[col_volume]) if col_volume is not None else None
        area = _parse_float(row.iloc[col_area]) if col_area is not None else None

        records.append(
            {
                "region": current_region,
                "regional_unit": current_unit,
                "year": year,
                "month": month,
                "number": number,
                "storeys": storeys,
                "volume": volume,
                "area": area,
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(
            f"No rows extracted from {xls_path.name}. "
            f"Header row detected at {header_row}."
        )

    out = out.sort_values(["region", "regional_unit", "year", "month"]).reset_index(drop=True)
    return out
