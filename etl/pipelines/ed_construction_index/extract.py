from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, "rb") as f:
        sig = f.read(2)
        if sig == b"PK":
            return "openpyxl"
    return "xlrd"


def _parse_year(v: object) -> int | None:
    s = str(v).strip()
    m = re.search(r"(\d{4})", s)
    if m:
        return int(m.group(1))
    return None


def _parse_quarter(v: object) -> int | None:
    s = str(v).strip().upper()
    m = re.search(r"Q([1-4])", s)
    if m:
        return int(m.group(1))
    return None


def extract_construction_index_quarterly(xls_path: Path) -> pd.DataFrame:
    """
    Extract quarterly construction production indices as:
      Year, Quarter, Production Index in Construction,
      Production Index Building Construction, Production Index Civil Engineering
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))
    if df.empty:
        raise RuntimeError("Empty file for construction index extraction.")

    # find row with "Year-quarter"
    start_idx = None
    for i in range(min(len(df), 80)):
        row = " | ".join(str(v) for v in df.iloc[i].tolist()).lower()
        if "year-quarter" in row:
            start_idx = i + 2  # data starts after second header row
            break
    if start_idx is None:
        raise RuntimeError("Could not locate quarterly data header in construction index file.")

    records: list[dict[str, object]] = []
    current_year: int | None = None

    for i in range(start_idx, len(df)):
        row = df.iloc[i]

        y = _parse_year(row[0])
        if y is not None:
            current_year = y

        q = _parse_quarter(row[1])
        if current_year is None or q is None:
            continue

        total_idx = pd.to_numeric(row[2], errors="coerce")
        building_idx = pd.to_numeric(row[4], errors="coerce")
        civil_idx = pd.to_numeric(row[6], errors="coerce")

        if pd.isna(total_idx) and pd.isna(building_idx) and pd.isna(civil_idx):
            continue

        records.append(
            {
                "Year": int(current_year),
                "Quarter": int(q),
                "Production Index in Construction": round(float(total_idx), 3) if pd.notna(total_idx) else pd.NA,
                "Production Index Building Construction": round(float(building_idx), 3)
                if pd.notna(building_idx)
                else pd.NA,
                "Production Index Civil Engineering": round(float(civil_idx), 3)
                if pd.notna(civil_idx)
                else pd.NA,
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No quarterly rows extracted for construction index.")

    out = out.sort_values(["Year", "Quarter"]).reset_index(drop=True)
    return out
