# etl/pipelines/cy_13_new_loans_millions/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import re

def extract_new_loans(xls_path: Path) -> pd.DataFrame:
    """
    Extracts Cyprus New Loans data from 5 tables.
    Refined mapping based on exact user column requirements.
    """
    xls_path = Path(xls_path)
    xl = pd.ExcelFile(xls_path)

    def get_time_series_map(df):
        ts_map = {}
        month_lookup = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        
        # 1. Collect rows that look like data
        data_rows = []
        for i in range(len(df)):
            y_val = str(df.iloc[i, 0]).strip()
            m_val = str(df.iloc[i, 1]).strip()
            m_clean = m_val.lower().replace('.', '').strip()[:3]
            m_num = month_lookup.get(m_clean)
            if m_num:
                year = None
                if re.match(r"^\d{4}$", y_val):
                    try:
                        year = int(float(y_val))
                    except ValueError:
                        pass
                data_rows.append({'idx': i, 'month': m_num, 'year': year})
        
        if not data_rows: return {}
        
        # 2. Find first grounded year
        first_year_idx = -1
        for i, r in enumerate(data_rows):
            if r['year']:
                first_year_idx = i
                break
        
        if first_year_idx == -1: return {}
        
        # 3. Backward fill from first found year
        curr_y = data_rows[first_year_idx]['year']
        for i in range(first_year_idx - 1, -1, -1):
            if data_rows[i+1]['month'] < data_rows[i]['month']: # e.g. Jan < Dec
                curr_y -= 1
            data_rows[i]['year'] = curr_y
            
        # 4. Forward fill
        curr_y = data_rows[first_year_idx]['year']
        for i in range(first_year_idx + 1, len(data_rows)):
            if data_rows[i]['year']:
                curr_y = data_rows[i]['year']
            else:
                if data_rows[i]['month'] < data_rows[i-1]['month']: # e.g. Jan < Dec
                    curr_y += 1
                data_rows[i]['year'] = curr_y
        
        # 5. Populate map
        for r in data_rows:
            if (r['year'], r['month']) not in ts_map:
                ts_map[(r['year'], r['month'])] = r['idx']
        
        return ts_map

    # Load sheets
    df12 = xl.parse("T12", header=None)
    ts12_map = get_time_series_map(df12)
    
    df9 = xl.parse("T9", header=None)
    ts9_map = get_time_series_map(df9)
    
    df61 = xl.parse("T6.1", header=None)
    ts61_map = get_time_series_map(df61)
    
    df62 = xl.parse("T6.2", header=None)
    ts62_map = get_time_series_map(df62)
    
    df63 = xl.parse("T6.3", header=None)
    ts63_map = get_time_series_map(df63)

    all_periods = sorted(list(set(ts12_map.keys()) | set(ts9_map.keys()) | set(ts61_map.keys()) | set(ts62_map.keys()) | set(ts63_map.keys())))

    records = []
    for y, m in all_periods:
        def get_val(df, ts_map, col):
            idx = ts_map.get((y, m))
            if idx is not None:
                val = df.iloc[idx, col]
                try: 
                    f = float(val)
                    if pd.isna(f): return pd.NA
                    return round(f, 2)
                except: return pd.NA
            return pd.NA

        records.append({
            "Year": y, "Month": m,
            # T12: D(3), E(4), G(6), H(7)
            "Consumer_Pure_New_Loans": get_val(df12, ts12_map, 3),
            "Consumer_Renegotiated_Loans": get_val(df12, ts12_map, 4),
            "Housing_Pure_New_Loans": get_val(df12, ts12_map, 6),
            "Housing_Renegotiated_Loans": get_val(df12, ts12_map, 7),
            # T9: D(3), E(4), F(5), G(6)
            "Consumer_Floating_Rate_Up_to_1_Year_Initial_Fixation_Rate": get_val(df9, ts9_map, 3),
            "Housing_Floating_Rate_Up_to_1_Year_Initial_Fixation_Rate": get_val(df9, ts9_map, 4),
            "Consumer_Annual_Percentage_Rate_Of_Charge": get_val(df9, ts9_map, 5),
            "Housing_Annual_Percentage_Rate_Of_Charge": get_val(df9, ts9_map, 6),
            # T6.1: H(7), I(8)
            "Outstanding_Housing_Loans_Locals": get_val(df61, ts61_map, 8),
            "Outstanding_Consumer_Loans_Locals": get_val(df61, ts61_map, 7),
            # T6.2: G(6), H(7)
            "Outstanding_Housing_Loans_Eu": get_val(df62, ts62_map, 7),
            "Outstanding_Consumer_Loans_Eu": get_val(df62, ts62_map, 6),
            # T6.3: G(6), H(7)
            "Outstanding_Housing_Loans_Non_Eu_Rates": get_val(df63, ts63_map, 7),
            "Outstanding_Consumer_Loans_Non_Eu_Rates": get_val(df63, ts63_map, 6)
        })

    out = pd.DataFrame(records).sort_values(["Year", "Month"]).reset_index(drop=True)
    return out
