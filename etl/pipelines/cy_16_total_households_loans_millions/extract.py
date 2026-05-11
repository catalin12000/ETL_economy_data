# etl/pipelines/cy_16_total_households_loans_millions/extract.py
from __future__ import annotations

import pandas as pd
from pathlib import Path
import datetime

def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"

def extract_households_loans(xls_path: Path) -> pd.DataFrame:
    """
    Extracts Cyprus Household Loans and NPLs from the aggregate banking sector data spreadsheet.
    """
    xls_path = Path(xls_path)
    # Using 'NPLs as from 31.12.2017' sheet as it contains the most recent structured data
    df = pd.read_excel(xls_path, sheet_name='NPLs as from 31.12.2017', header=None, engine=_engine_for(xls_path))
    
    # Mapping based on DB schema:
    # total_loans -> Row 11 (Total excluding institutions)
    # total_households_loans -> Row 35 (Households row, Total loans Gross)
    # non_performing_loans -> Row 35, Col+1
    # loan_amounts_past_90_days -> Row 35, Col+2
    # restructured_loans_forbearance -> Row 35, Col+3
    # non_performing_restructured_loans -> Row 35, Col+5 (Skipping impairment in Col+4)
    
    row_total = 11
    row_households = 35
    
    records = []
    
    # Dates are in row 4, every 7 columns starting from index 1
    for col_idx in range(1, df.shape[1], 7):
        date_val = df.iloc[4, col_idx]
        if pd.isna(date_val): continue
        
        if not isinstance(date_val, (pd.Timestamp, datetime.datetime)):
            try:
                date_val = pd.to_datetime(date_val)
            except:
                continue
        
        year = int(date_val.year)
        month = int(date_val.month)
        
        def to_m(val):
            try:
                # File has values in €'000, DB has values in Millions (€)
                return round(float(val) / 1000.0, 2)
            except:
                return 0.0

        records.append({
            "Year": year,
            "Month": month,
            "Total_Loans": to_m(df.iloc[row_total, col_idx]),
            "Total_Households_Loans": to_m(df.iloc[row_households, col_idx]),
            "Non_Performing_Loans": to_m(df.iloc[row_households, col_idx + 1]),
            "Loan_Amounts_Past_90_Days": to_m(df.iloc[row_households, col_idx + 2]),
            "Restructured_Loans_Forbearance": to_m(df.iloc[row_households, col_idx + 3]),
            "Non_Performing_Restructured_Loans": to_m(df.iloc[row_households, col_idx + 5]),
        })
        
    out = pd.DataFrame(records).sort_values(["Year", "Month"]).reset_index(drop=True)
    return out
