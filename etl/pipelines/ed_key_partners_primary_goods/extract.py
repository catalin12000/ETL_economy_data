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
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _detect_year_cols(df: pd.DataFrame, header_row: int) -> dict[int, int]:
    """Return mapping col_index → year for columns whose header is a 4-digit year."""
    result = {}
    for col_idx, cell in enumerate(df.iloc[header_row].tolist()):
        s = str(cell).strip()
        if re.fullmatch(r"(19|20)\d{2}", s):
            result[col_idx] = int(s)
    return result


def _find_header_row(df: pd.DataFrame) -> Optional[int]:
    """Find row containing year columns (4-digit integers)."""
    for i in range(min(30, len(df))):
        years = [
            v for v in df.iloc[i].tolist()
            if re.fullmatch(r"(19|20)\d{2}", str(v).strip())
        ]
        if len(years) >= 2:
            return i
    return None


def _find_section_col(df: pd.DataFrame, data_start: int, keywords: tuple[str, ...]) -> Optional[int]:
    """Scan the first column of data rows for the column that holds section/category names."""
    for col_idx in range(min(5, df.shape[1])):
        vals = [
            str(df.iat[r, col_idx]).strip().lower()
            for r in range(data_start, min(data_start + 30, len(df)))
            if str(df.iat[r, col_idx]).strip().lower() not in ("nan", "none", "")
        ]
        if any(any(kw in v for kw in keywords) for v in vals):
            return col_idx
    return None


# SITC-1 standard section names and codes
_SITC1_CODE_MAP = {
    "food and live animals": "0",
    "beverages and tobacco": "1",
    "crude materials, inedible, except fuels": "2",
    "mineral fuels, lubricants and related materials": "3",
    "animal and vegetable oils, fats and waxes": "4",
    "chemicals and related products": "5",
    "manufactured goods classified chiefly by material": "6",
    "machinery and transport equipment": "7",
    "miscellaneous manufactured articles": "8",
    "commodities and transactions not classified elsewhere": "9",
}


def _normalise_sitc_code(category_name: str) -> str:
    """Return the SITC-1 digit code for a known category, or derive from name."""
    lower = category_name.lower().strip()
    for name, code in _SITC1_CODE_MAP.items():
        if name in lower or lower in name:
            return code
    # Try extracting a leading digit
    m = re.match(r"^(\d)", lower)
    return m.group(1) if m else ""


def extract_key_partners_primary_goods(xls_path: Path) -> pd.DataFrame:
    """
    Extract SFC02 SITC-1 trade table (imports/exports by country, category, year).

    Expected XLS layout (two-sheet or two-section structure):
      - One section / sheet for Imports, one for Exports
      - Rows: Country × SITC category
      - Columns: Years

    Output columns match DB:
      year, imports_value, exports_value, categories, country, codes
    """
    xls_path = Path(xls_path)

    sheets = []
    try:
        xl = pd.ExcelFile(xls_path)
        sheets = xl.sheet_names
    except Exception as e:
        raise RuntimeError(f"Cannot open {xls_path.name}: {e}")

    imports_df: Optional[pd.DataFrame] = None
    exports_df: Optional[pd.DataFrame] = None

    for sheet in sheets:
        raw = pd.read_excel(xls_path, sheet_name=sheet, header=None)
        flat = " ".join(str(v) for v in raw.values.flatten()).lower()
        if "import" in flat or "εισαγωγ" in flat:
            imports_df = raw
        elif "export" in flat or "εξαγωγ" in flat:
            exports_df = raw

    if imports_df is None and exports_df is None:
        # Fall back: try first two sheets
        if len(sheets) >= 2:
            imports_df = pd.read_excel(xls_path, sheet_name=sheets[0], header=None)
            exports_df = pd.read_excel(xls_path, sheet_name=sheets[1], header=None)
        elif len(sheets) == 1:
            imports_df = exports_df = pd.read_excel(xls_path, sheet_name=sheets[0], header=None)
        else:
            raise RuntimeError(f"No sheets found in {xls_path.name}.")

    def _parse_section(section_df: pd.DataFrame, value_name: str) -> pd.DataFrame:
        header_row = _find_header_row(section_df)
        if header_row is None:
            raise RuntimeError(
                f"Could not find year-column header row in {xls_path.name} ({value_name} section). "
                f"First 5 rows:\n{section_df.head(5).to_string()}"
            )

        year_cols = _detect_year_cols(section_df, header_row)
        if not year_cols:
            raise RuntimeError(
                f"No year columns found in row {header_row} of {value_name} section."
            )

        # Identify country column (col 0 or 1)
        country_col = 0
        category_col = None

        # Detect if there's a SITC category column before country
        head_vals = [str(section_df.iat[header_row, c]).lower() for c in range(min(5, section_df.shape[1]))]
        for ci, hv in enumerate(head_vals):
            if "categ" in hv or "sitc" in hv or "product" in hv or "κατηγ" in hv:
                category_col = ci
                country_col = ci + 1
                break

        records: list[dict] = []
        current_category: str = ""
        current_country: str = ""

        for row_idx in range(header_row + 1, len(section_df)):
            row = section_df.iloc[row_idx]

            # Category / country may fill-down
            if category_col is not None:
                raw = str(row.iloc[category_col]).strip()
                if raw and raw.lower() not in ("nan", "none"):
                    current_category = raw

            raw_country = str(row.iloc[country_col]).strip()
            if raw_country and raw_country.lower() not in ("nan", "none"):
                current_country = raw_country

            if not current_country:
                continue

            for col_idx, year in year_cols.items():
                if col_idx >= section_df.shape[1]:
                    continue
                val = _parse_float(row.iloc[col_idx])
                if val is None:
                    continue
                records.append(
                    {
                        "year": year,
                        "country": current_country,
                        "categories": current_category,
                        value_name: val,
                    }
                )

        return pd.DataFrame(records)

    imp = _parse_section(imports_df, "imports_value")
    exp = _parse_section(exports_df, "exports_value")

    if imp.empty and exp.empty:
        raise RuntimeError(f"No data extracted from {xls_path.name}.")

    # Merge imports and exports on (year, country, categories)
    key = ["year", "country", "categories"]
    if imp.empty:
        merged = exp.copy()
        merged["imports_value"] = None
    elif exp.empty:
        merged = imp.copy()
        merged["exports_value"] = None
    else:
        merged = imp.merge(exp, on=key, how="outer")

    # Add SITC codes derived from category name
    merged["codes"] = merged["categories"].apply(_normalise_sitc_code)

    # Ensure required columns exist
    for col in ["imports_value", "exports_value"]:
        if col not in merged.columns:
            merged[col] = None

    out = merged[["year", "imports_value", "exports_value", "categories", "country", "codes"]].copy()
    out = out.sort_values(["year", "country", "categories"]).reset_index(drop=True)
    return out
