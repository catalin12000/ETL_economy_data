# etl/pipelines/cy_02_building_permits_by_district/extract.py
from __future__ import annotations

import pandas as pd
from pathlib import Path


def _read_pxweb_csv(csv_path: Path) -> pd.DataFrame:
    # CYSTAT CSV files are UTF-8 with BOM; fallback kept for safety.
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="latin-1")

    # Normalize header artifacts (BOM, quotes, extra spaces).
    df.columns = [
        str(c).replace("\ufeff", "").replace('"', "").strip()
        for c in df.columns
    ]
    return df


def _require_column(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    upper_map = {col.upper(): col for col in df.columns}
    for c in candidates:
        if c.upper() in upper_map:
            return upper_map[c.upper()]
    raise KeyError(f"Missing expected columns {candidates}. Found: {list(df.columns)}")


def extract_building_permits_district(csv_path: Path) -> pd.DataFrame:
    """
    Extracts Cyprus Building Permits by District from PxWeb CSV.
    """
    df = _read_pxweb_csv(csv_path)

    month_col = _require_column(df, ["MONTH"])
    district_col = _require_column(df, ["DISTRICT"])
    urban_col = _require_column(df, ["URBAN/RURAL"])

    # 1. Parse Year and Month
    # Format: "2003M01"
    period = df[month_col].astype(str).str.strip().str.replace('"', "", regex=False)
    df["Year"] = pd.to_numeric(period.str.extract(r"^(\d{4})")[0], errors="coerce")
    df["Month"] = pd.to_numeric(period.str.extract(r"M(\d{2})")[0], errors="coerce")
    df = df.dropna(subset=["Year", "Month"]).copy()
    df["Year"] = df["Year"].astype(int)
    df["Month"] = df["Month"].astype(int)

    # 2. Rename Columns and Map Labels
    # DISTRICT Mapping to match zeus DB
    DISTRICT_MAP = {
        "Lefkosia": "Nicosia",
        "Lemesos": "Limassol",
        "Larnaka": "Larnaca",
        "Ammochostos": "Famagusta",
        "Pafos": "Paphos"
    }

    df["District"] = df[district_col].map(lambda x: DISTRICT_MAP.get(x, x))
    df["Urban_Rural"] = df[urban_col]

    # CSV columns are named like: "Number Monthly data Number of permits", etc.
    col_map = {}
    for col in df.columns:
        if "Number of permits" in col: col_map[col] = "Number"
        if "Area (m2)" in col: col_map[col] = "Area_m2"
        if "Value (" in col: col_map[col] = "Value_000s"
        if "Dwelling units" in col: col_map[col] = "Dwelling_Units"
    
    df = df.rename(columns=col_map)
    
    # 3. Filter and Clean
    # DB only has 'Urban' and 'Rural' (no 'Total')
    df = df[df["Urban_Rural"].isin(["Urban", "Rural"])].copy()
    # DB only has the 5 specific districts
    df = df[df["District"].isin(DISTRICT_MAP.values())].copy()
    
    numeric_cols = ["Number", "Area_m2", "Value_000s", "Dwelling_Units"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    # 4. Final selection
    target_cols = ["Year", "Month", "District", "Urban_Rural"] + numeric_cols
    out = df[target_cols].copy()
    out = out.sort_values(["Year", "Month", "District", "Urban_Rural"]).reset_index(drop=True)
    
    return out
