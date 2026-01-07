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


def extract_loan_interest_rates(file_path: Path) -> pd.DataFrame:
    file_path = Path(file_path)
    df = pd.read_excel(file_path, sheet_name="Loans_Interest rates", header=None, engine=_engine_for(file_path))
    
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
        
        # Row 1: Totals
        records.append({
            "Year": year, "Month": month,
            "Total Consumer Loans Aprc": to_f(row[58]),
            "Total Housing Loans Aprc": to_f(row[59]),
            "Delta Interest Rate Deposits": to_f(row[60]),
            "Weighted Average Interest Rate New Loans In Euro": to_f(row[1]),
            "Group": "", "Loan Type": ""
        })
        
        # Segment 1: Individuals | Weighted average interest rate
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Weighted average interest rate",
            "Weighted Average Interest Rate": to_f(row[2])
        })
        
        # Segment 2: Individuals | Loans without a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Loans without a defined maturity",
            "Credit Cards": to_f(row[3]),
            "Open Account Loans": to_f(row[4]),
            "Debit Balances On Current Accounts": to_f(row[5])
        })
        
        # Segment 3: Individuals | Consumer loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Consumer loans with a defined maturity",
            "Total Interest Rate": to_f(row[7]),
            "Total Collateral Guarantees Interest Rates": to_f(row[6]),
            "Floating Rate 1 Year Fixation": to_f(row[9]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[6]), # Example matches Col 6
            "Over 1 To 5 Years Rate Fixation": to_f(row[10]),
            "Over 5 Years Rate Fixation": to_f(row[11])
        })
        
        # Segment 4: Individuals | Housing loans
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Housing loans",
            "Total Interest Rate": to_f(row[13]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[14]),
            "Floating Rate 1 Year Rate Fixation Floating Rate": to_f(row[15]),
            "Over 1 To 5 Years Rate Fixation": to_f(row[17])
        })
        
        # Segment 5: Sole proprietors | Loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Sole proprietors and unicorporated businesses",
            "Loan Type": "Loans with a defined maturity",
            "Total Interest Rate": to_f(row[21]),
            "Total Collateral Guarantees Interest Rates": to_f(row[22]),
            "Floating Rate 1 Year Fixation": to_f(row[23]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[24])
        })
        
        # Segment 6: Sole proprietors | Loans without a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Sole proprietors and unicorporated businesses",
            "Loan Type": "Loans without a defined maturity (Credit lines - open account loans)",
            "Credit Lines": to_f(row[20])
        })
        
        # Segment 7: Sole proprietors | Weighted average interest rate
        records.append({
            "Year": year, "Month": month,
            "Group": "Sole proprietors and unicorporated businesses",
            "Loan Type": "Weighted average interest rate",
            "Weighted Average Interest Rate": to_f(row[25])
        })

        # Segment 8: Non-financial corporations | Weighted average interest rate
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Weighted average interest rate",
            "Weighted Average Interest Rate": to_f(row[28])
        })
        
        # Segment 9: Non-financial corporations | Loans without a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans without a defined maturity",
            "Total Interest Rate": to_f(row[29]),
            "Total Small Medium Enterprises Interest Rates": to_f(row[30]),
            "Credit Lines": to_f(row[31]),
            "Debit Balances Sight Deposits": to_f(row[32])
        })
        
        # Segment 10: Non-financial corporations | Up to 0.25
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and up to an amount of EUR 0.25",
            "Total Interest Rate": to_f(row[38]),
            "Total Collateral Guarantees Interest Rates": to_f(row[39]),
            "Floating Rate 1 Year Fixation": to_f(row[40]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[41])
        })
        
        # Segment 11: Non-financial corporations | 0.25 to 1
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and over an amount of EUR 0.25 million and up to 1 million",
            "Total Interest Rate": to_f(row[42]),
            "Total Collateral Guarantees Interest Rates": to_f(row[43]),
            "Floating Rate 1 Year Fixation": to_f(row[44]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[45])
        })
        
        # Segment 12: Non-financial corporations | Over 1
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity over an amount of EUR 1 million",
            "Total Interest Rate": to_f(row[46]),
            "Total Collateral Guarantees Interest Rates": to_f(row[47]),
            "Floating Rate 1 Year Fixation": to_f(row[48]),
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[49]),
            "Over 1 To 5 Years Rate Fixation": to_f(row[50]),
            "Over 5 Years Rate Fixation": to_f(row[51])
        })

    out = pd.DataFrame(records)
    
    # Fill None in keys with empty strings
    out["Group"] = out["Group"].fillna("")
    out["Loan Type"] = out["Loan Type"].fillna("")
    
    # Final column order
    cols = [
        "Year", "Month", "Total Consumer Loans Aprc", "Total Housing Loans Aprc", 
        "Delta Interest Rate Deposits", "Weighted Average Interest Rate New Loans In Euro", 
        "Group", "Weighted Average Interest Rate", "Loan Type", "Credit Cards", 
        "Open Account Loans", "Debit Balances On Current Accounts", "Total Interest Rate", 
        "Total Collateral Guarantees Interest Rates", "Total Small Medium Enterprises Interest Rates", 
        "Floating Rate 1 Year Fixation", "Floating Rate 1 Year Rate Fixation Collateral Guarantees", 
        "Floating Rate 1 Year Rate Fixation Floating Rate", "Over 1 To 5 Years Rate Fixation", 
        "Over 5 Years Rate Fixation", "Over 5 To 10 Years Rate Fixation", "Over 10 Years Rate Fixation", 
        "Credit Lines", "Debit Balances Sight Deposits"
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = None
            
    return out[cols]
