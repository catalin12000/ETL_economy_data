# etl/pipelines/ed_building_permits_table/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def _engine_for(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".xls":
        return "xlrd"
    return "openpyxl"


def extract_building_permits(xls_path: Path) -> pd.DataFrame:
    """
    ELSTAT file layout (observed):
    - First sheet contains a title, then a header row, then:
      * annual total row: Year in col0, month label in col1 (string)
      * monthly rows: Year empty, Month number in col1, values in col2..4
    We keep ONLY monthly rows (Month 1..12).
    """
    xls_path = Path(xls_path)

    df = pd.read_excel(
        xls_path,
        sheet_name=0,
        header=None,
        engine=_engine_for(xls_path),
    )

    # Find header row (the row that contains "Year" and "Month"/"Μήνας")
    header_idx = None
    for i in range(min(len(df), 80)):
        row = df.iloc[i].astype(str)
        if row.str.contains("Year", case=False, na=False).any() and (
            row.str.contains("Month", case=False, na=False).any()
            or row.str.contains("Μήνας", case=False, na=False).any()
        ):
            header_idx = i
            break
    if header_idx is None:
        # fallback: the first data-like row in your sample is row 7, so header is row 6
        header_idx = 6

    data = df.iloc[header_idx + 1 :].copy()

    # The file effectively has 5 columns of interest
    data = data.iloc[:, :5]
    data.columns = ["Year", "Month", "Permits Number", "Area", "Volume"]

    records = []
    current_year = None

    for _, r in data.iterrows():
        y = r["Year"]
        m = r["Month"]

        # Update current_year on annual total rows
        if pd.notna(y):
            try:
                current_year = int(float(y))
            except Exception:
                pass

        # Keep only month rows where Month is numeric 1..12
        if pd.isna(m) or current_year is None:
            continue
        if isinstance(m, str):
            # strings like "Annual Total" -> skip
            continue

        try:
            month = int(float(m))
        except Exception:
            continue

        if month < 1 or month > 12:
            continue

        def to_int(v):
            if pd.isna(v):
                return pd.NA
            try:
                return int(float(v))
            except Exception:
                # sometimes imported as text with commas
                s = str(v).strip().replace(",", "")
                return int(float(s)) if s else pd.NA

        permits = to_int(r["Permits Number"])
        area = to_int(r["Area"])
        volume = to_int(r["Volume"])

        # If a row is malformed, skip it
        if pd.isna(permits) and pd.isna(area) and pd.isna(volume):
            continue

        records.append(
            {
                "Year": current_year,
                "Month": month,
                "Permits Number": permits,
                "Area": area,
                "Volume": volume,
            }
        )

    out = pd.DataFrame(records).sort_values(["Year", "Month"]).reset_index(drop=True)
    return out
