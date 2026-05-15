from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


_MONTH_MAP = {
    "I": 1, "II": 2, "III": 3, "IV": 4,
    "V": 5, "VI": 6, "VII": 7, "VIII": 8,
    "IX": 9, "X": 10, "XI": 11, "XII": 12,
}

# Known NACE Rev.2 service section codes and canonical names used in DB.
# The XLS header may have slightly different wording; we normalise via the code.
_CODE_TO_ACTIVITY = {
    "G": "Wholesale and retail trade; repair of motor vehicles and motorcycles",
    "H": "Transportation and storage",
    "I": "Accommodation and food service activities",
    "J": "Information and communication",
    "L": "Real estate activities",
    "M": "Professional, scientific and technical activities",
    "N": "Administrative and support service activities",
    "R": "Arts, entertainment and recreation",
    "S": "Other service activities",
}


def _parse_float(value) -> Optional[float]:
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s in {"-", "...", "…", ":", "u", "N/A"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _normalize_token(token: str) -> str:
    token = token.strip().lstrip("*").strip()
    token = token.replace("Ι", "I").replace("Χ", "X")
    return re.sub(r"\s+", "", token).upper()


def _parse_year(value) -> Optional[int]:
    if pd.isna(value):
        return None
    m = re.search(r"(19|20)\d{2}", str(value))
    return int(m.group(0)) if m else None


def _parse_month(value) -> Optional[int]:
    if pd.isna(value):
        return None
    text = str(value)
    ym = re.search(r"(19|20)\d{2}", text)
    if ym:
        text = text[ym.end():]
    return _MONTH_MAP.get(_normalize_token(text))


def _detect_code_row(df: pd.DataFrame) -> Optional[int]:
    """Return index of row that contains single-letter NACE service codes."""
    for i in range(min(30, len(df))):
        vals = [str(v).strip() for v in df.iloc[i].tolist()]
        single_letters = [v for v in vals if re.fullmatch(r"[A-Z]", v)]
        if len(single_letters) >= 3:
            return i
    return None


def extract_services_sector_turnover(xls_path: Path) -> pd.DataFrame:
    """
    Extract DKT54 Table 01 "Turnover Indices in Services Sections" to long form.

    Expected XLS layout (wide):
      - Sheet "TABLE 1"
      - Rows ~8-15:  header rows containing NACE codes (G, H, I, J, L, M, N, R, S)
      - Row ~16+:    data rows: col 0 = Year/Month period, cols 1+ = index values

    Output: (Year, Month, Code, Economic_Activity, Index) per row.
    """
    xls_path = Path(xls_path)
    df = None
    for sheet in ("TABLE 1", "TABLE1", "TABLE 01", 0):
        try:
            df = pd.read_excel(xls_path, sheet_name=sheet, header=None)
            break
        except Exception:
            continue
    if df is None or df.empty:
        raise RuntimeError(f"Could not open TABLE 1 sheet in {xls_path.name}.")

    code_row = _detect_code_row(df)
    if code_row is None:
        raise RuntimeError(
            f"Could not find NACE code row (single uppercase letters G, H, I...) "
            f"in the first 30 rows of {xls_path.name}. "
            f"First 5 rows:\n{df.head(5).to_string()}"
        )

    # Build mapping: column_index → (code, economic_activity)
    col_meta: dict[int, tuple[str, str]] = {}
    for col_idx, cell in enumerate(df.iloc[code_row].tolist()):
        code = str(cell).strip()
        if re.fullmatch(r"[A-Z]", code) and code in _CODE_TO_ACTIVITY:
            col_meta[col_idx] = (code, _CODE_TO_ACTIVITY[code])

    if not col_meta:
        raise RuntimeError(
            f"No recognised NACE service codes found in row {code_row} of {xls_path.name}. "
            f"Row content: {df.iloc[code_row].tolist()}"
        )

    data_start = code_row + 1
    records: list[dict] = []
    current_year: Optional[int] = None

    for row_idx in range(data_start, len(df)):
        raw_period = df.iat[row_idx, 0]
        year = _parse_year(raw_period)
        if year is not None:
            current_year = year

        if current_year is None:
            continue

        month = _parse_month(raw_period)
        if month is None:
            continue

        for col_idx, (code, activity) in col_meta.items():
            if col_idx >= df.shape[1]:
                continue
            val = _parse_float(df.iat[row_idx, col_idx])
            if val is None:
                continue
            records.append(
                {
                    "Year": current_year,
                    "Month": month,
                    "Code": code,
                    "Economic_Activity": activity,
                    "Index": val,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(
            f"No data extracted from {xls_path.name}. "
            f"Detected code row at {code_row}, data starts at {data_start}."
        )

    out = out.sort_values(["Year", "Month", "Code"]).reset_index(drop=True)
    return out
