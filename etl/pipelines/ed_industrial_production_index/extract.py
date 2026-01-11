# etl/pipelines/ed_industrial_production_index/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"


def extract_industrial_production(xls_path: Path) -> pd.DataFrame:
    """
    Extracts Industrial Production Index.
    Layout observed in DKT21:
    - Row 5 onwards: Data
    - Col 0: Year (only on first month of year)
    - Col 1: Month (numeric)
    - Col 2: Index value
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))

    records = []
    current_year = None

    for i in range(5, len(df)):
        row = df.iloc[i]
        
        # Parse Year
        y_val = str(row[0]).strip()
        if y_val and y_val != 'nan':
            try:
                if y_val.isdigit():
                    current_year = int(float(y_val))
                elif "average" in y_val.lower():
                    continue 
            except:
                pass
        
        if current_year is None:
            continue
            
        # Parse Month
        m_val = row[1]
        try:
            month = int(float(m_val))
        except:
            continue
            
        if month < 1 or month > 12:
            continue
            
        # Parse Index Value
        index_val = row[2]
        try:
            index_val = round(float(index_val), 2)
        except:
            index_val = pd.NA

        if pd.notna(index_val):
            records.append({
                "Year": current_year,
                "Month": month,
                "Index": index_val
            })

    out = pd.DataFrame(records).sort_values(["Year", "Month"]).reset_index(drop=True)
    return out