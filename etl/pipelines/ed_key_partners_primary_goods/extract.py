from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


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


def _clean_text(value) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _parse_year(df: pd.DataFrame, xls_path: Path) -> int:
    for row_idx in range(min(5, len(df))):
        row_text = " ".join(_clean_text(v) for v in df.iloc[row_idx].tolist() if _clean_text(v))
        match = re.search(r"\b(19|20)\d{2}\b", row_text)
        if match:
            return int(match.group(0))
    raise RuntimeError(f"Could not parse year from header rows in {xls_path.name}.")


def extract_key_partners_primary_goods(xls_path: Path) -> pd.DataFrame:
    """
    Extract SFC02 SITC-1 trade table from the actual one-sheet workbook layout.

    Output columns match DB:
      year, imports_value, exports_value, categories, country, codes
    """
    xls_path = Path(xls_path)
    try:
        df = pd.read_excel(xls_path, sheet_name=0, header=None)
    except Exception as exc:
        raise RuntimeError(f"Cannot open {xls_path.name}: {exc}")

    if df.empty:
        raise RuntimeError(f"Workbook {xls_path.name} is empty.")

    year = _parse_year(df, xls_path)

    expected_headers = {
        0: "ΚΩΔΙΚΟΙ",
        1: "ΚΑΤΗΓΟΡΙΕΣ",
        2: "ΧΩΡΑ",
        5: "CATEGORIES",
        6: "COUNTRY",
        7: "CODES",
    }
    header_row = 3
    for col_idx, expected in expected_headers.items():
        actual = _clean_text(df.iat[header_row, col_idx]) if col_idx < df.shape[1] else ""
        if expected not in actual:
            raise RuntimeError(
                f"Unexpected header layout in {xls_path.name}: column {col_idx} on row {header_row} "
                f"contains {actual!r}, expected to include {expected!r}."
            )

    records: list[dict] = []
    for row_idx in range(header_row + 1, len(df)):
        category = _clean_text(df.iat[row_idx, 5]) if 5 < df.shape[1] else ""
        country = _clean_text(df.iat[row_idx, 6]) if 6 < df.shape[1] else ""
        code_raw = df.iat[row_idx, 7] if 7 < df.shape[1] else None
        imports_value = _parse_float(df.iat[row_idx, 3]) if 3 < df.shape[1] else None
        exports_value = _parse_float(df.iat[row_idx, 4]) if 4 < df.shape[1] else None

        # Data rows are the only rows that contain both English category and country.
        if not category or not country:
            continue
        if imports_value is None and exports_value is None:
            continue

        code_text = _clean_text(code_raw)
        if not code_text:
            code_text = _clean_text(df.iat[row_idx, 0]) if 0 < df.shape[1] else ""

        records.append(
            {
                "year": year,
                "imports_value": imports_value,
                "exports_value": exports_value,
                "categories": category,
                "country": country,
                "codes": code_text,
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(f"No data rows extracted from {xls_path.name}.")

    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype(int)
    out["country"] = out["country"].str.title()
    out["categories"] = out["categories"].str.title()
    out = out.sort_values(["year", "country", "categories"]).reset_index(drop=True)
    return out
