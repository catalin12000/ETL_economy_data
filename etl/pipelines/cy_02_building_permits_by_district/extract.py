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
    
    # 2. Rename Columns and Map Labels
    # DISTRICT Mapping to match zeus DB
    DISTRICT_MAP = {
        "Lefkosia": "Nicosia",
        "Lemesos": "Limassol",
        "Larnaka": "Larnaca",
        "Ammochostos": "Famagusta",
        "Pafos": "Paphos"
    }
    
    df["District"] = df["DISTRICT"].map(lambda x: DISTRICT_MAP.get(x, x))
    df["Urban_Rural"] = df["URBAN/RURAL"]
    
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
