from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


def _parse_year(v) -> Optional[int]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None
    if "annual average" in s.lower():
        return None
    # Handles "2025*:", "2026:", "2024*"
    m = re.search(r"(19|20)\d{2}", s)
    if not m:
        return None
    y = int(m.group(0))
    if 1900 <= y <= 2100:
        return y
    return None


def _parse_month(v) -> Optional[int]:
    if pd.isna(v):
        return None
    try:
        m = int(float(v))
    except Exception:
        return None
    if 1 <= m <= 12:
        return m
    return None


def _parse_float(v) -> Optional[float]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if not s or s in {"-", "...", "â€¦"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _extract_overall_index(path_03: Path) -> pd.DataFrame:
    """
    File 03, sheet TABLE 4:
      col0 year marker, col1 month, col2 overall index.
    """
    df = pd.read_excel(path_03, sheet_name="TABLE 4", header=None, engine="openpyxl")
    records = []
    current_year: Optional[int] = None

    for i in range(4, len(df)):
        y = _parse_year(df.iat[i, 0])
        if y is not None:
            current_year = y
        if current_year is None:
            continue

        month = _parse_month(df.iat[i, 1])
        if month is None:
            continue

        overall = _parse_float(df.iat[i, 2])
        if overall is None:
            continue

        records.append(
            {
                "Year": current_year,
                "Month": month,
                "overall_index": round(overall, 2),
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No rows extracted from file 03 (overall index).")
    return out


def _extract_sa_and_sections(path_04: Path) -> pd.DataFrame:
    """
    File 04:
      - sheet INDUSTRIAL PRODUCTION INDEX, col2 = seasonally adjusted overall index
      - sheet SECTIONS, col2/4/6/8 = mining/manufacturing/electricity/water
    """
    sa = pd.read_excel(path_04, sheet_name="INDUSTRIAL PRODUCTION INDEX", header=None, engine="openpyxl")
    sec = pd.read_excel(path_04, sheet_name="SECTIONS", header=None, engine="openpyxl")

    sa_rows = []
    current_year: Optional[int] = None
    for i in range(5, len(sa)):
        y = _parse_year(sa.iat[i, 0])
        if y is not None:
            current_year = y
        if current_year is None:
            continue

        month = _parse_month(sa.iat[i, 1])
        if month is None:
            continue

        v = _parse_float(sa.iat[i, 2])
        if v is None:
            continue

        sa_rows.append(
            {
                "Year": current_year,
                "Month": month,
                "seasonally_adjusted_overall_index": round(v, 2),
            }
        )

    sec_rows = []
    current_year = None
    for i in range(5, len(sec)):
        y = _parse_year(sec.iat[i, 0])
        if y is not None:
            current_year = y
        if current_year is None:
            continue

        month = _parse_month(sec.iat[i, 1])
        if month is None:
            continue

        mq = _parse_float(sec.iat[i, 2])
        mf = _parse_float(sec.iat[i, 4])
        el = _parse_float(sec.iat[i, 6])
        ws = _parse_float(sec.iat[i, 8])
        if any(v is None for v in (mq, mf, el, ws)):
            continue

        sec_rows.append(
            {
                "Year": current_year,
                "Month": month,
                "mining_quarrying": round(mq, 2),
                "manufacturing": round(mf, 2),
                "electricity": round(el, 2),
                "water_supply": round(ws, 2),
            }
        )

    df_sa = pd.DataFrame(sa_rows)
    df_sec = pd.DataFrame(sec_rows)
    if df_sa.empty:
        raise RuntimeError("No rows extracted from file 04 sheet INDUSTRIAL PRODUCTION INDEX.")
    if df_sec.empty:
        raise RuntimeError("No rows extracted from file 04 sheet SECTIONS.")

    out = df_sa.merge(df_sec, on=["Year", "Month"], how="inner")
    return out


def extract_industrial_production(path_03: Path, path_04: Path) -> pd.DataFrame:
    df_overall = _extract_overall_index(path_03)
    df_sa_sec = _extract_sa_and_sections(path_04)

    out = df_overall.merge(df_sa_sec, on=["Year", "Month"], how="inner")
    out = out.sort_values(["Year", "Month"]).reset_index(drop=True)

    if out.empty:
        raise RuntimeError("No merged rows extracted for industrial production index.")
    return out
