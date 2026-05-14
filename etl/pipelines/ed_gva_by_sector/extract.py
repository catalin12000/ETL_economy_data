# etl/pipelines/ed_gva_by_sector/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"


def extract_gva(xls_path: Path) -> pd.DataFrame:
    """
    Extracts Annual Gross Value Added by Industry.
    Layout observed in SEL12:
    - Row 8: Years (header)
    - Row 9 onwards: Data
    - Col 0: NACE Code
    - Col 1: Industry Name (Greek)
    - Cols 2+: Values for each year
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))

    # Identify years in Row 8
    years_row = df.iloc[8].tolist()
    year_cols = []
    for j, val in enumerate(years_row):
        try:
            s = str(val).replace('*', '').strip()
            y = int(float(s))
            if 1900 < y < 2100:
                year_cols.append((j, y))
        except:
            continue

    records = []
    for i in range(9, len(df)):
        row = df.iloc[i]
        nace_code = str(row[0]).strip()
        industry_name = str(row[1]).strip()
        
        if not nace_code or nace_code == 'nan' or not industry_name or industry_name == 'nan':
            continue
            
        for col_idx, year in year_cols:
            val = row[col_idx]
            try:
                val = round(float(val), 2)
            except:
                val = pd.NA
                
            if pd.notna(val):
                records.append({
                    "Year": year,
                    "Industry_Code": nace_code,
                    "Industry_Name_EL": industry_name,
                    "Value": val
                })

    out = pd.DataFrame(records).sort_values(["Year", "Industry_Code"]).reset_index(drop=True)
    return out
