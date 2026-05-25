from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


CONTINENT_CODES = {"W0", "E1", "F1", "A1", "S1", "O1"}
AREA_CODES_LEVEL_1 = {"W190", "E19", "F19", "F4", "F2", "A19", "A2", "A5", "A7", "S19", "S3", "S6", "O19"}
AREA_CODES_LEVEL_2 = {"S35", "S37"}  # Nested under "Near and Middle East countries" (S3)


def _engine_for(path: Path) -> str:
    with open(path, "rb") as f:
        sig = f.read(2)
        if sig == b"PK":
            return "openpyxl"
    return "xlrd"


def _clean_cell(value: object) -> str:
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
            txt = _clean_cell(raw)
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
    raise RuntimeError("Could not find year header row in FDI country source.")


def extract_fdi_country_sheet(xls_path: Path, sheet_name: int | str) -> pd.DataFrame:
    """
    Extract a BoG FDI country sheet as:
      Year, Country, Area, Amount, Continent
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=sheet_name, header=None, engine=_engine_for(xls_path))
    if df.empty:
        raise RuntimeError("Empty FDI country source file.")

    header_row_idx, year_cols = _find_year_columns(df)

    records: list[dict[str, object]] = []
    current_continent = ""
    current_area = ""
    row_order = 0

    for row_idx in range(header_row_idx + 1, len(df)):
        code = _clean_cell(df.iat[row_idx, 0]) if df.shape[1] > 0 else ""
        name = _clean_cell(df.iat[row_idx, 1]) if df.shape[1] > 1 else ""

        code_key = code.strip()
        if not code_key and not name.strip():
            continue

        # Stop on notes footer.
        lower_code = code_key.lower()
        if lower_code.startswith("source") or lower_code == "notes" or code_key.startswith("("):
            break

        row_order += 1
        country = name

        if code_key in CONTINENT_CODES:
            current_continent = name
            current_area = name
            area = name
            continent = name
        elif code_key in AREA_CODES_LEVEL_1:
            if "not allocated" not in name.lower():
                current_area = name
            area = name
            continent = current_continent
        elif code_key in AREA_CODES_LEVEL_2:
            # Keep current_area as the parent level area (S3).
            area = current_area or name
            continent = current_continent
        else:
            area = current_area or current_continent
            continent = current_continent

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
                    "area": area,
                    "amount": float(amount),
                    "continent": continent,
                    "_row_order": row_order,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No rows extracted from FDI country source.")

    out = out.sort_values(["year", "_row_order"], ascending=[False, True]).reset_index(drop=True)
    return out[["year", "country", "area", "amount", "continent"]]


def extract_fdi_country(xls_path: Path) -> pd.DataFrame:
    return extract_fdi_country_sheet(xls_path, 0)
