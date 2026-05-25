from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


_MONTH_MAP = {
    "I": 1,
    "II": 2,
    "III": 3,
    "IV": 4,
    "V": 5,
    "VI": 6,
    "VII": 7,
    "VIII": 8,
    "IX": 9,
    "X": 10,
    "XI": 11,
    "XII": 12,
}


def _parse_float(value) -> Optional[float]:
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s in {"-", "...", "\u2026", "\u00e2\u20ac\u00a6"}:
        return None
    try:
        return round(float(s), 2)
    except Exception:
        return None


def _normalize_month_token(token: str) -> str:
    token = token.strip().lstrip("*").strip()
    token = token.replace("\u0399", "I").replace("\u03a7", "X")
    token = re.sub(r"\s+", "", token)
    return token.upper()


def _parse_year(value) -> Optional[int]:
    if pd.isna(value):
        return None
    match = re.search(r"(19|20)\d{2}", str(value))
    if not match:
        return None
    year = int(match.group(0))
    return year if 1900 <= year <= 2100 else None


def _parse_month(value) -> Optional[int]:
    if pd.isna(value):
        return None

    text = str(value)
    year_match = re.search(r"(19|20)\d{2}", text)
    if year_match:
        text = text[year_match.end():]

    token = _normalize_month_token(text)
    return _MONTH_MAP.get(token)


def _extract_two_col_table(path: Path, sheet_name: str, col1: str, col2: str) -> pd.DataFrame:
    """Generic extractor for ELSTAT motor trade 3-column layout (Year-Month, col1, col2)."""
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)

    records = []
    current_year: Optional[int] = None

    for i in range(16, len(df)):
        raw_period = df.iat[i, 0]
        year = _parse_year(raw_period)
        if year is not None:
            current_year = year

        if current_year is None:
            continue

        month = _parse_month(raw_period)
        if month is None:
            continue

        v1 = _parse_float(df.iat[i, 1])
        v2 = _parse_float(df.iat[i, 2])
        if v1 is None and v2 is None:
            continue

        records.append({"year": current_year, "month": month, col1: v1, col2: v2})

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(f"No rows extracted from {sheet_name} in {path.name}.")

    out = out.drop_duplicates(subset=["year", "month"], keep="last")
    out = out.sort_values(["year", "month"]).reset_index(drop=True)
    return out


def extract_motor_trade_turnover(path: Path) -> pd.DataFrame:
    return _extract_two_col_table(
        path, "TABLE 1",
        "motor_trade_turnover_index", "vehicle_sale_turnover_index",
    )


def extract_motor_trade_volume(path: Path) -> pd.DataFrame:
    return _extract_two_col_table(
        path, "TABLE 2",
        "motor_trade_volume_index", "vehicle_sale_volume_index",
    )
