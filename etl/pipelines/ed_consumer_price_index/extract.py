# etl/pipelines/ed_consumer_price_index/extract.py
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


def extract_cpi(xls_path: Path) -> pd.DataFrame:
    """
    Extracts General Consumer Price Index and Year-over-Year change.
    Layout observed in DKT01/DKT87:
    - Row 8 onwards: Data
    - Col 0 contains Year and Month combined like '  2001 :  1' or '               2 '
    - Col 1: General Index value
    - Col 3: Year-over-Year change (%)
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))

    records = []
    current_year = None

    for i in range(8, len(df)):
        row = df.iloc[i]
        label = str(row[0]).strip()
        
        if not label or label == 'nan':
            continue
            
        # 1. Check for Year : Month format e.g. "2001 : 1"
        m_year_month = re.search(r"(\d{4})\s*:\s*(\d+)", label)
        if m_year_month:
            current_year = int(m_year_month.group(1))
            month = int(m_year_month.group(2))
        else:
            # 2. Check for just Month format e.g. " 2 "
            m_month = re.search(r"^\s*(\d+)\s*$", label)
            if m_month:
                month = int(m_month.group(1))
            else:
                # Skip labels like "Μέσος ετήσιος" (Annual average)
                continue
        
        if current_year is None:
            continue
            
        if month < 1 or month > 12:
            continue
            
        # Index value (Col 1)
        try:
            index_val = round(float(row[1]), 2)
        except:
            index_val = pd.NA
            
        # Year-over-Year change (Col 3)
        try:
            yoy_val = round(float(row[3]), 2)
        except:
            yoy_val = pd.NA
            
        if pd.notna(index_val):
            records.append({
                "Year": current_year,
                "Month": month,
                "Index": index_val,
                "Year Over Year": yoy_val
            })

    out = pd.DataFrame(records).sort_values(["Year", "Month"]).reset_index(drop=True)
    return out
