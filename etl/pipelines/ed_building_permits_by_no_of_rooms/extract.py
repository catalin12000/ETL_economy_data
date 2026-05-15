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


_KW_REGION = {"region", "περιφέρεια", "διαμέρισμα"}
_KW_UNIT = {"regional unit", "περιφερειακή ενότητα", "νομός"}
_KW_YEAR = {"year", "έτος"}
_KW_MONTH = {"month", "μήνας"}
_KW_NUMBER_DW = {"number of new", "αριθμός νέων", "νέων κατοικ"}
_KW_VOLUME_DW = {"volume of new", "όγκος νέων"}
_KW_SURFACE_DW = {"surface of new", "επιφάνεια νέων"}
_KW_ROOMS = {"habitable", "δωμάτια", "κατοικήσιμα"}
_KW_VOLUME_IMP = {"volume of improv", "όγκος βελτ"}


def _col_match(header: str, keywords: set[str]) -> bool:
    h = str(header).lower().strip()
    return any(kw in h for kw in keywords)


def _find_header_row(df: pd.DataFrame) -> Optional[int]:
    for i in range(min(60, len(df))):
        row_text = " | ".join(str(v) for v in df.iloc[i].tolist()).lower()
        hits = sum(
            1 for kw in ("year", "έτος", "month", "μήνας", "dwelling", "κατοικ", "region", "περιφέρ")
            if kw in row_text
        )
        if hits >= 3:
            return i
    return None


def extract_building_permits_by_rooms(xls_path: Path) -> pd.DataFrame:
    """
    Extract SOP03 Table 04 "New dwellings and improvements of dwellings,
    number of habitable rooms volume and surface thereon,
    by region and regional unit".

    Output columns match DB:
      year, month, number_of_new_dwellings, volume_of_new_dwellings,
      surface_of_new_dwellings, habitable_rooms_of_new_dwellings,
      volume_of_improvements, region, regional_unit
    """
    xls_path = Path(xls_path)
    df = None
    for sheet in ("TABLE 4", "TABLE 04", "TABLE4", "Πίνακας 4", 0):
        try:
            df = pd.read_excel(xls_path, sheet_name=sheet, header=None)
            break
        except Exception:
            continue
    if df is None or df.empty:
        raise RuntimeError(f"Could not read sheet TABLE 4 / TABLE 04 from {xls_path.name}.")

    header_row = _find_header_row(df)
    if header_row is None:
        raise RuntimeError(
            f"Could not locate header row in {xls_path.name}. "
            f"First 5 rows:\n{df.head(5).to_string()}"
        )

    headers = [str(v).strip() for v in df.iloc[header_row].tolist()]

    col_region = col_unit = col_year = col_month = None
    col_num_dw = col_vol_dw = col_surf_dw = col_rooms = col_vol_imp = None

    for i, h in enumerate(headers):
        if col_region is None and _col_match(h, _KW_REGION):
            col_region = i
        elif col_unit is None and _col_match(h, _KW_UNIT):
            col_unit = i
        elif col_year is None and _col_match(h, _KW_YEAR):
            col_year = i
        elif col_month is None and _col_match(h, _KW_MONTH):
            col_month = i
        elif col_num_dw is None and _col_match(h, _KW_NUMBER_DW):
            col_num_dw = i
        elif col_vol_dw is None and _col_match(h, _KW_VOLUME_DW):
            col_vol_dw = i
        elif col_surf_dw is None and _col_match(h, _KW_SURFACE_DW):
            col_surf_dw = i
        elif col_rooms is None and _col_match(h, _KW_ROOMS):
            col_rooms = i
        elif col_vol_imp is None and _col_match(h, _KW_VOLUME_IMP):
            col_vol_imp = i

    missing = [
        name for name, col in [
            ("year", col_year), ("month", col_month), ("number_of_new_dwellings", col_num_dw)
        ]
        if col is None
    ]
    if missing:
        raise RuntimeError(
            f"Could not map required columns {missing} in {xls_path.name}. "
            f"Detected headers: {headers}"
        )

    records: list[dict] = []
    current_region: str = ""
    current_unit: str = ""

    for row_idx in range(header_row + 1, len(df)):
        row = df.iloc[row_idx]

        if col_region is not None:
            raw = str(row.iloc[col_region]).strip()
            if raw and raw.lower() not in ("nan", "none"):
                current_region = raw
        if col_unit is not None:
            raw = str(row.iloc[col_unit]).strip()
            if raw and raw.lower() not in ("nan", "none"):
                current_unit = raw

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
                "number_of_new_dwellings": _parse_int(row.iloc[col_num_dw]) if col_num_dw is not None else None,
                "volume_of_new_dwellings": _parse_float(row.iloc[col_vol_dw]) if col_vol_dw is not None else None,
                "surface_of_new_dwellings": _parse_float(row.iloc[col_surf_dw]) if col_surf_dw is not None else None,
                "habitable_rooms_of_new_dwellings": _parse_int(row.iloc[col_rooms]) if col_rooms is not None else None,
                "volume_of_improvements": _parse_float(row.iloc[col_vol_imp]) if col_vol_imp is not None else None,
                "region": current_region,
                "regional_unit": current_unit,
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(
            f"No rows extracted from {xls_path.name}. "
            f"Header row detected at {header_row}."
        )

    out = out.sort_values(["year", "month", "region", "regional_unit"]).reset_index(drop=True)
    return out
