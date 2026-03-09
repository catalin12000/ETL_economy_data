from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


_SECTION_SUFFIX_RE = re.compile(r"\s*\((?:Section|Sections)[^)]+\)\s*$", re.IGNORECASE)
_KEEP_FULL_SECTION_NAME_CODES = {"GTU", "O_T_U"}


def _engine_for(path: Path) -> str:
    with open(path, "rb") as f:
        sig = f.read(2)
        if sig == b"PK":
            return "openpyxl"
    return "xlrd"


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_section_suffix(label: str) -> str:
    return _SECTION_SUFFIX_RE.sub("", _clean_text(label)).strip()


def _find_year_columns(df: pd.DataFrame) -> tuple[int, list[tuple[int, int]]]:
    max_rows = min(len(df), 20)
    for row_idx in range(max_rows):
        year_cols: list[tuple[int, int]] = []
        for col_idx, raw in enumerate(df.iloc[row_idx].tolist()):
            text = _clean_text(raw)
            if not text:
                continue
            m = re.search(r"(19|20)\d{2}", text)
            if not m:
                continue
            year = int(m.group(0))
            if 1900 <= year <= 2100:
                year_cols.append((col_idx, year))

        if len(year_cols) >= 5:
            return row_idx, year_cols

    raise RuntimeError("Could not find year header row in FDI activity source.")


def extract_fdi_activity(xls_path: Path) -> pd.DataFrame:
    """
    Extract BoG FDI Home by Activity as:
      Year, Section Code, Section Name, Subsection Code, Subsection Name, Amount
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name="INDUSTRY-IN", header=None, engine=_engine_for(xls_path))
    if df.empty:
        raise RuntimeError("Empty FDI activity source file.")

    header_row_idx, year_cols = _find_year_columns(df)

    records: list[dict[str, object]] = []
    current_section_code: str | None = None
    current_section_name: str | None = None
    row_order = 0

    for row_idx in range(header_row_idx + 1, len(df)):
        code = _clean_text(df.iat[row_idx, 0]) if df.shape[1] > 0 else ""
        name = _clean_text(df.iat[row_idx, 1]) if df.shape[1] > 1 else ""

        if not code and not name:
            continue
        if code.lower().startswith("source") or code.lower() == "notes" or code.startswith("("):
            break
        if not code or not re.match(r"^[A-Za-z0-9_]+$", code):
            continue

        row_order += 1
        is_section_row = re.search(r"\(Section[s]?[^)]*\)", name, flags=re.IGNORECASE) is not None

        if is_section_row:
            section_code = code
            section_name = _strip_section_suffix(name)
            current_section_code = section_code
            current_section_name = section_name
        elif current_section_code and code != current_section_code and code.startswith(current_section_code):
            section_code = current_section_code
            section_name = current_section_name or _strip_section_suffix(name)
        else:
            section_code = code
            section_name = _strip_section_suffix(name)
            current_section_code = section_code
            current_section_name = section_name

        if code in _KEEP_FULL_SECTION_NAME_CODES:
            section_name = _clean_text(name)
            if is_section_row:
                current_section_name = section_name

        for col_idx, year in year_cols:
            if col_idx >= df.shape[1]:
                continue
            amount = pd.to_numeric(df.iat[row_idx, col_idx], errors="coerce")
            if pd.isna(amount):
                continue

            records.append(
                {
                    "Year": year,
                    "Section Code": section_code,
                    "Section Name": section_name,
                    "Subsection Code": code,
                    "Subsection Name": name,
                    "Amount": float(amount),
                    "_row_order": row_order,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No rows extracted from FDI activity source.")

    out = out.sort_values(["Year", "_row_order"], ascending=[False, True]).reset_index(drop=True)
    return out[
        ["Year", "Section Code", "Section Name", "Subsection Code", "Subsection Name", "Amount"]
    ]
