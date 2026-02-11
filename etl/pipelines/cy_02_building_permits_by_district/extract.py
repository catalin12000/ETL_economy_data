# etl/pipelines/cy_02_building_permits_by_district/extract.py
from __future__ import annotations

import pandas as pd
from pathlib import Path

def extract_building_permits_district(csv_path: Path) -> pd.DataFrame:
    """
    Extracts Cyprus Building Permits by District from PxWeb CSV.
    """
    df = pd.read_csv(csv_path, encoding='latin-1')
    
    # 1. Parse Year and Month
    # Format: "2003M01"
    df["Year"] = df["MONTH"].str[:4].astype(int)
    df["Month"] = df["MONTH"].str[5:7].astype(int)
    
    # 2. Rename Columns
    # CSV columns are named like: "Number Monthly data Number of permits", etc.
    # We find them by index or substring
    col_map = {}
    for col in df.columns:
        if "Number of permits" in col: col_map[col] = "Number"
        if "Area (m2)" in col: col_map[col] = "Area_m2"
        if "Value (" in col or "Value (" in col: col_map[col] = "Value_000s"
        if "Dwelling units" in col: col_map[col] = "Dwelling_Units"
    
    df = df.rename(columns=col_map)
    df = df.rename(columns={"DISTRICT": "District", "URBAN/RURAL": "Urban_Rural"})
    
    # 3. Clean numeric columns
    numeric_cols = ["Number", "Area_m2", "Value_000s", "Dwelling_Units"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    # 4. Final selection
    target_cols = ["Year", "Month", "District", "Urban_Rural"] + numeric_cols
    out = df[target_cols].copy()
    out = out.sort_values(["Year", "Month", "District", "Urban_Rural"]).reset_index(drop=True)
    
    return out
