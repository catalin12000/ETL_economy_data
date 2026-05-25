from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


AREA_HEADERS = {"Euro area countries", "EU countries excl. euro area", "Other countries"}
NON_COUNTRY_ROWS = {"TOTAL", "International Organizations", "Not allocated"}


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _find_year_columns(df: pd.DataFrame) -> tuple[int, list[tuple[int, int]]]:
    max_rows = min(len(df), 20)
    for row_idx in range(max_rows):
        year_cols: list[tuple[int, int]] = []
        for col_idx, raw in enumerate(df.iloc[row_idx].tolist()):
            txt = _clean_text(raw)
            if not txt:
                continue
            m = re.search(r"(19|20)\d{2}", txt)
            if not m:
                continue
            year = int(m.group(0))
            if 1900 <= year <= 2100:
                year_cols.append((col_idx, year))
        if len(year_cols) >= 5:
            return row_idx, year_cols
    raise RuntimeError("Could not find year header row in FDI real estate source.")


def extract_fdi_real_estate(xlsx_path: Path) -> pd.DataFrame:
    """
    Extract BoG FDI in real estate as:
      Year, Country, Area, Amount
    """
    xlsx_path = Path(xlsx_path)
    df = pd.read_excel(xlsx_path, sheet_name="Net", header=None, engine="openpyxl")
    if df.empty:
        raise RuntimeError("Empty FDI real estate source file.")

    header_row_idx, year_cols = _find_year_columns(df)

    records: list[dict[str, object]] = []
    current_area = ""
    row_order = 0

    for row_idx in range(header_row_idx + 1, len(df)):
        code = _clean_text(df.iat[row_idx, 0]) if df.shape[1] > 0 else ""
        country = _clean_text(df.iat[row_idx, 1]) if df.shape[1] > 1 else ""

        if not code and not country:
            continue
        if code.lower().startswith("source") or code.startswith("("):
            break

        if not code:
            if country in AREA_HEADERS:
                current_area = country
                continue
            if country in NON_COUNTRY_ROWS or not current_area:
                continue
            # Keep valid missing-code country rows (e.g., Namibia).
        elif not current_area:
            continue

        row_order += 1
        for col_idx, year in year_cols:
            if col_idx >= df.shape[1]:
                continue
            amount = pd.to_numeric(df.iat[row_idx, col_idx], errors="coerce")
            if pd.isna(amount):
                continue

            records.append(
                {
                    "year": year,
                    "country": country,
                    "area": current_area,
                    "amount": float(amount),
                    "_row_order": row_order,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No rows extracted from FDI real estate source.")

    out = out.sort_values(["year", "_row_order"], ascending=[False, True]).reset_index(drop=True)
    return out[["year", "country", "area", "amount"]]
