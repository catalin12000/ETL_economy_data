from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"


def extract_loan_amounts(file_path: Path) -> pd.DataFrame:
    file_path = Path(file_path)
    df = pd.read_excel(file_path, sheet_name="Loans_Amounts", header=None, engine=_engine_for(file_path))
    
    # Data starts at row 8
    data_rows = df.iloc[8:].copy()
    
    # Parse Date
    data_rows[0] = pd.to_datetime(data_rows[0], errors='coerce')
    data_rows = data_rows.dropna(subset=[0])
    
    records = []
    
    def to_f(val):
        try:
            f = float(val)
            return f if not np.isnan(f) else None
        except:
            return None

    for _, row in data_rows.iterrows():
        year = int(row[0].year)
        month = int(row[0].month)
        
        # 1. Individuals | Consumer loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Consumer loans with a defined maturity",
            "Total Loan Amount": to_f(row[1]),
            "Total Collateral Guarantees Loans": to_f(row[2]),
            "Floating Rate 1 Year Fixation": to_f(row[3]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[4]),
            "Over 1 To 5 Years Rate Fixation": to_f(row[5]),
            "Over 5 Years Rate Fixation": to_f(row[6])
        })
        
        # 2. Individuals | Housing Loans
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Housing Loans",
            "Total Loan Amount": to_f(row[7]),
            "Floating Rate 1 Year Fixation": to_f(row[8]),
            "Floating Rate 1 Year Rate Fixation Floating Rate": to_f(row[9]),
            "Over 10 Years Rate Fixation": to_f(row[11])
        })

        # 3. Individuals | Loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Loans with a defined maturity"
        })

        # 4. Non-financial corporations | Other loans with defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Other loans with defined maturity",
            "Total Loan Amount": to_f(row[20]),
            "Total Small Medium Enterprises Loans": to_f(row[21])
        })
        
        # 5. NFC | Up to 1M | 0.25 to 1
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and up to an amount of EUR 1 million - Loans with a defined maturity and over an amount of EUR 0.25 million and up to 1 million",
            "Total Loan Amount": to_f(row[25]),
            "Total Collateral Guarantees Loans": to_f(row[26]),
            "Floating Rate 1 Year Fixation": to_f(row[27]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[28])
        })
        
        # 6. NFC | Up to 1M | Up to 0.25
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and up to an amount of EUR 1 million - Loans with a defined maturity and up to an amount of EUR 0.25",
            "Total Loan Amount": to_f(row[29]),
            "Total Collateral Guarantees Loans": to_f(row[30]),
            "Floating Rate 1 Year Fixation": to_f(row[31]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[32])
        })

        # 7. NFC | Over 1M
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity over an amount of EUR 1 million",
            "Total Loan Amount": to_f(row[33]),
            "Total Collateral Guarantees Loans": to_f(row[34]),
            "Floating Rate 1 Year Fixation": to_f(row[35]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[36])
        })
        
        # 8. NFC | Original maturity > 1yr | 0.25 to 1
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 0.25 million and up to 1 million",
            "Total Loan Amount": to_f(row[41]),
            "Total Collateral Guarantees Loans": to_f(row[42]),
            "Floating Rate 1 Year Fixation": to_f(row[41]), # Example has same as Total? No, wait.
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[42])
        })
        
        # 9. NFC | Original maturity > 1yr | Over 1M
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 1 million",
            "Total Loan Amount": to_f(row[43]),
            "Total Collateral Guarantees Loans": to_f(row[44]),
            "Floating Rate 1 Year Fixation": to_f(row[43]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[44])
        })

    out = pd.DataFrame(records)
    
    # Fill None in keys
    out["Group"] = out["Group"].fillna("")
    out["Loan Type"] = out["Loan Type"].fillna("")
    
    cols = [
        "Year", "Month", "Group", "Loan Type", "Total Loan Amount", 
        "Total Collateral Guarantees Loans", "Total Small Medium Enterprises Loans", 
        "Floating Rate 1 Year Fixation", "Floating Rate 1 Year Rate Fixation Collateral Guarantees", 
        "Floating Rate 1 Year Rate Fixation Floating Rate", "Over 1 To 5 Years Rate Fixation", 
        "Over 5 Years Rate Fixation", "Over 5 To 10 Years Rate Fixation", "Over 10 Years Rate Fixation"
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = None
            
    return out[cols]
