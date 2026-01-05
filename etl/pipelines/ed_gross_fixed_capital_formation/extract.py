# etl/pipelines/ed_gross_fixed_capital_formation/extract.py
from __future__ import annotations

import re
from pathlib import Path
import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"


def extract_gfcf(xls_path: Path) -> pd.DataFrame:
    """
    Extracts Quarterly Gross Fixed Capital Formation (GFCF).
    Layout observed in SEL81:
    - Row 7 onwards: Data
    - Col 0: Period like '1995-Q1'
    - Col 1: Total gross fixed capital formation
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))

    records = []

    for i in range(7, len(df)):
        row = df.iloc[i]
        period = str(row[0]).strip()
        
        # Match YYYY-QN
        m = re.match(r"(\d{4})-Q([1-4])", period)
        if m:
            year = int(m.group(1))
            quarter = int(m.group(2))
            
            total_val = row[1]
            try:
                total_val = round(float(total_val), 2)
            except:
                total_val = pd.NA
                
            if pd.notna(total_val):
                records.append({
                    "Year": year,
                    "Quarter": quarter,
                    "Index": total_val
                })

    out = pd.DataFrame(records).sort_values(["Year", "Quarter"]).reset_index(drop=True)
    return out
