# etl/pipelines/cy_15_residential_price_indices/extract.py
from __future__ import annotations

import pandas as pd
from pathlib import Path

def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"

def extract_rppi(xls_path: Path) -> pd.DataFrame:
    """
    Extracts Cyprus Residential Property Price Indices (RPPI) from CBC spreadsheet.
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name='English', header=None, engine=_engine_for(xls_path))
    
    # Data starts at row 5 (0-indexed)
    data = df.iloc[5:].copy()
    
    # 1. Handle Year ffill
    data[0] = data[0].ffill()
    
    # 2. Map Quarters
    def map_quarter(val):
        s = str(val).strip().upper()
        if '1' in s: return 1
        if '2' in s: return 2
        if '3' in s: return 3
        if '4' in s: return 4
        return None
    
    data["Q_Int"] = data[1].apply(map_quarter)
    data = data.dropna(subset=["Q_Int"])
    
    def to_f(val):
        try:
            # Handle 'na' or other strings
            if str(val).strip().lower() == 'na': return pd.NA
            return float(val)
        except:
            return pd.NA

    # 3. Comprehensive Mapping
    records = []
    for _, row in data.iterrows():
        records.append({
            "year": int(row[0]),
            "quarter": int(row["Q_Int"]),
            "residential_price_property_price_index": to_f(row[2]),
            "apartments_cy": to_f(row[3]),
            "houses_cy": to_f(row[4]),
            "nicosia_residential": to_f(row[5]),
            "limassol_residential": to_f(row[6]),
            "larnaca_residential": to_f(row[7]),
            "paphos_residential": to_f(row[8]),
            "famagusta_residential": to_f(row[9]),
            "nicosia_apartments": to_f(row[10]),
            "limassol_apartments": to_f(row[11]),
            "larnaca_apartments": to_f(row[12]),
            "paphos_apartments": to_f(row[13]),
            "famagusta_apartments": to_f(row[14]),
            "nicosia_houses": to_f(row[15]),
            "limassol_houses": to_f(row[16]),
            "larnaca_houses": to_f(row[17]),
            "paphos_houses": to_f(row[18]),
            "famagusta_houses": to_f(row[19])
        })
        
    out = pd.DataFrame(records).sort_values(["year", "quarter"]).reset_index(drop=True)
    return out
