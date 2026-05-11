from __future__ import annotations

from pathlib import Path

import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, "rb") as f:
        sig = f.read(2)
        if sig == b"PK":
            return "openpyxl"
    return "xlrd"


def extract_building_permits_monthly(xls_path: Path) -> pd.DataFrame:
    """
    Extract ELSTAT monthly private building activity table as:
      Year, Month, Permits Number, Area, Volume
    Keeps only monthly rows (Month 1..12), skips annual totals.
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))
    if df.empty:
        raise RuntimeError("Empty file for building permits monthly extraction.")

    # Locate header row with Year / Month and permit metrics.
    header_idx = None
    for i in range(min(len(df), 80)):
        row_text = " | ".join(str(v) for v in df.iloc[i].tolist()).lower()
        if (
            "year" in row_text
            and "month" in row_text
            and "number of permits" in row_text
            and "surface" in row_text
            and "volume" in row_text
        ):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Could not find building permits monthly header row.")

    data = df.iloc[header_idx + 1 :, :5].copy()
    data.columns = ["Year", "Month", "Permits Number", "Area", "Volume"]

    records: list[dict[str, int]] = []
    current_year: int | None = None

    for _, r in data.iterrows():
        y = pd.to_numeric(r["Year"], errors="coerce")
        if pd.notna(y):
            current_year = int(y)

        m = pd.to_numeric(r["Month"], errors="coerce")
        if current_year is None or pd.isna(m):
            continue

        month = int(m)
        if month < 1 or month > 12:
            continue

        permits = pd.to_numeric(r["Permits Number"], errors="coerce")
        area = pd.to_numeric(r["Area"], errors="coerce")
        volume = pd.to_numeric(r["Volume"], errors="coerce")
        if pd.isna(permits) and pd.isna(area) and pd.isna(volume):
            continue

        records.append(
            {
                "Year": current_year,
                "Month": month,
                "Permits Number": int(permits) if pd.notna(permits) else pd.NA,
                "Area": int(area) if pd.notna(area) else pd.NA,
                "Volume": int(volume) if pd.notna(volume) else pd.NA,
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No monthly rows extracted for building permits.")

    out = out.sort_values(["Year", "Month"]).reset_index(drop=True)
    return out
