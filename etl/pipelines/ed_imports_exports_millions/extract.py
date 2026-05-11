from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


# ELSTAT SEL30 workbook layout (annual table in wide format)
ROW_YEAR_HEADER = 10
ROW_MAP = {
    # I. Current prices
    "current_prices_goods": 11,
    "current_prices_services": 12,
    "current_prices_imports": 13,
    "current_prices_expenditures_of_residents_in_rest_of_the_world": 15,
    "current_prices_goods_exports": 17,
    "current_prices_services_exports": 18,
    "current_prices_exports": 19,
    "current_prices_expenditures_of_residents_on_economic_territory": 21,
    "current_prices_exports_imports_balance": 22,
    # II. Constant prices of previous year
    "constant_prices_goods": 25,
    "constant_prices_services": 26,
    "constant_prices_imports": 27,
    "constant_prices_expenditures_of_residents_in_rest_of_the_world": 29,
    "constant_prices_goods_exports": 31,
    "constant_prices_services_exports": 32,
    "constant_prices_exports": 33,
    "constant_prices_expenditures_of_residents_on_economic_territory": 35,
    "constant_prices_exports_imports_balance": 36,
}


def _clean_text(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _parse_year(v) -> Optional[int]:
    s = _clean_text(v).replace("*", "")
    if not s:
        return None
    try:
        y = int(float(s))
        if 1900 <= y <= 2100:
            return y
    except Exception:
        return None
    return None


def _to_num(v) -> Optional[float]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if not s or s in {"...", "â€¦", "-", "â€”"}:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _year_columns(df: pd.DataFrame) -> List[tuple[int, int]]:
    cols: List[tuple[int, int]] = []
    for col in range(2, df.shape[1]):
        y = _parse_year(df.iat[ROW_YEAR_HEADER, col])
        if y is not None:
            cols.append((col, y))
    return cols


def extract_imports_exports_millions(xls_path: Path) -> pd.DataFrame:
    xls_path = Path(xls_path)
    # Keep engine unspecified: source is BIFF .xls, not .xlsx zip.
    df = pd.read_excel(xls_path, sheet_name=0, header=None)

    year_cols = _year_columns(df)
    if not year_cols:
        raise RuntimeError("No year columns found in imports/exports workbook.")

    records: List[Dict[str, object]] = []
    for col_idx, year in year_cols:
        row: Dict[str, object] = {"Year": int(year)}
        for out_col, row_idx in ROW_MAP.items():
            row[out_col] = _to_num(df.iat[row_idx, col_idx])
        records.append(row)

    out = pd.DataFrame(records).sort_values("Year").reset_index(drop=True)
    if out.empty:
        raise RuntimeError("No rows extracted from imports/exports workbook.")
    return out

