# etl/pipelines/ed_employment/extract.py
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


def extract_employment(xls_path: Path) -> pd.DataFrame:
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))

    GREEK_MONTHS = {
        "ιανουάριος": 1, "ιανουαριος": 1,
        "φεβρουάριος": 2, "φεβρουαριος": 2,
        "μάρτιος": 3, "μαρτιος": 3,
        "απρίλιος": 4, "απριλιος": 4,
        "μάιος": 5, "μαιος": 5,
        "ιούνιος": 6, "ιουνιος": 6,
        "ιούλιος": 7, "ιουλιος": 7,
        "αύγουστος": 8, "αυγουστος": 8,
        "σεπτέμβριος": 9, "σεπτεμβριος": 9,
        "οκτώβριος": 10, "οκτωβριος": 10,
        "νοέμβριος": 11, "νοεμβριος": 11,
        "δεκέμβριος": 12, "δεκεμβριος": 12
    }

    records = []
    current_year = None

    for i in range(len(df)):
        row = df.iloc[i]
        val0 = str(row[0]).strip()
        
        if not val0 or val0 == 'nan':
            continue
            
        # Detect year headers
        if re.fullmatch(r"(\d{4})(\.0)?", val0):
            year_candidate = int(float(val0))
            rest_of_row = " ".join(row.astype(str).tolist()).lower()
            if "απασχολ" in rest_of_row or "employed" in rest_of_row:
                current_year = year_candidate
                continue
            elif i < 10:
                current_year = year_candidate
        
        month = GREEK_MONTHS.get(val0.lower())
        if month and current_year:
            def to_f(v):
                try: 
                    if isinstance(v, str): v = v.replace(',', '.')
                    return round(float(v), 2)
                except: return pd.NA
            
            records.append({
                "Year": current_year,
                "Month": month,
                "Seasonally": "Unadjusted",
                "Employed 000s": to_f(row[1]),
                "Unemployed 000s": to_f(row[2]),
                "Inactives 000s": to_f(row[3]),
                "Adjusted_Unemployment_Rate": pd.NA,
                "Unadjusted_Unemployment_Rate": to_f(row[4])
            })
            records.append({
                "Year": current_year,
                "Month": month,
                "Seasonally": "Adjusted",
                "Employed 000s": to_f(row[5]),
                "Unemployed 000s": to_f(row[6]),
                "Inactives 000s": to_f(row[7]),
                "Adjusted_Unemployment_Rate": to_f(row[8]),
                "Unadjusted_Unemployment_Rate": pd.NA
            })

    out = pd.DataFrame(records)
    out = out.drop_duplicates(subset=["Year", "Month", "Seasonally"], keep="last")
    out = out.sort_values(["Year", "Month", "Seasonally"]).reset_index(drop=True)
    
    # Print the last few months for verification
    print("\n--- Extracted Data Tail ---")
    print(out.tail(6).to_string(index=False))
    
    return out
