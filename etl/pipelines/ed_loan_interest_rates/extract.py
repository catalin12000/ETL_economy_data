from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np
import shutil
import tempfile


def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"


def extract_loan_interest_rates(file_path: Path) -> pd.DataFrame:
    file_path = Path(file_path)
    
    # Bypass file lock on Windows
    with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as tmp:
        shutil.copy2(file_path, tmp.name)
        tmp_path = Path(tmp.name)
    
    try:
        df = pd.read_excel(tmp_path, sheet_name="Loans_Interest rates", header=None, engine=_engine_for(tmp_path))
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    
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
        
        # 0. Total row
        records.append({
            "Year": year, "Month": month,
            "Total Consumer Loans Aprc": to_f(row[58]), # BG
            "Total Housing Loans Aprc": to_f(row[59]), # BH
            "Delta Interest Rate Deposits": to_f(row[60]), # BI
            "Weighted Average Interest Rate New Loans In Euro": to_f(row[1]), # B
            "Group": "", "Loan Type": "",
            "_sort_order": 0
        })
        
        # --- Individuals ---
        
        # Weighted average interest rate
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Weighted average interest rate",
            "Weighted Average Interest Rate": to_f(row[2]), # C
            "_sort_order": 1
        })
        
        # Loans without a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Loans without a defined maturity",
            "Credit Cards": to_f(row[4]), # E
            "Open Account Loans": to_f(row[5]), # F
            "Debit Balances On Current Accounts": to_f(row[6]), # G
            "_sort_order": 2
        })
        
        # Consumer loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Consumer loans with a defined maturity",
            "Total Interest Rate": to_f(row[7]), # H
            "Total Collateral Guarantees Interest Rates": to_f(row[8]), # I
            "Floating Rate 1 Year Fixation": to_f(row[9]), # J
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[10]), # K
            "Over 1 To 5 Years Rate Fixation": to_f(row[11]), # L
            "Over 5 Years Rate Fixation": to_f(row[12]), # M
            "_sort_order": 3
        })
        
        # Housing loans
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Housing loans",
            "Total Interest Rate": to_f(row[13]), # N
            "Floating Rate 1 Year Fixation": to_f(row[14]), # O
            "Floating Rate 1 Year Rate Fixation Floating Rate": to_f(row[15]), # P
            "Over 1 To 5 Years Rate Fixation": to_f(row[16]), # Q
            "Over 5 To 10 Years Rate Fixation": to_f(row[17]), # R
            "Over 10 Years Rate Fixation": to_f(row[18]), # S
            "_sort_order": 4
        })
        
        # Other loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Other loans with a defined maturity",
            "Floating Rate 1 Year Fixation": to_f(row[19]), # T
            "Over 1 To 5 Years Rate Fixation": to_f(row[20]), # U
            "Over 5 Years Rate Fixation": to_f(row[21]), # V
            "_sort_order": 5
        })
        
        # --- Sole Proprietors ---
        
        # Loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Sole proprietors and unicorporated businesses",
            "Loan Type": "Loans with a defined maturity",
            "Total Interest Rate": to_f(row[24]), # Y
            "Total Collateral Guarantees Interest Rates": to_f(row[25]), # Z
            "Floating Rate 1 Year Fixation": to_f(row[26]), # AA
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[27]), # AB
            "_sort_order": 6
        })
        
        # Loans without a defined maturity (Credit lines - open account loans)
        records.append({
            "Year": year, "Month": month,
            "Group": "Sole proprietors and unicorporated businesses",
            "Loan Type": "Loans without a defined maturity (Credit lines - open account loans)",
            "Credit Lines": to_f(row[23]), # X
            "_sort_order": 7
        })
        
        # Weighted average interest rate
        records.append({
            "Year": year, "Month": month,
            "Group": "Sole proprietors and unicorporated businesses",
            "Loan Type": "Weighted average interest rate",
            "Weighted Average Interest Rate": to_f(row[22]), # W
            "_sort_order": 8
        })

        # --- NFC ---

        # Weighted average interest rate
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Weighted average interest rate",
            "Weighted Average Interest Rate": to_f(row[28]), # AC
            "_sort_order": 9
        })
        
        # Loans without a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans without a defined maturity",
            "Total Interest Rate": to_f(row[29]), # AD
            "Total Small Medium Enterprises Interest Rates": to_f(row[30]), # AE
            "Credit Lines": to_f(row[31]), # AF
            "Debit Balances Sight Deposits": to_f(row[32]), # AG
            "_sort_order": 10
        })
        
        # Loans with a defined maturity and up to an amount of EUR 1 million
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and up to an amount of EUR 1 million",
            "_sort_order": 11
        })
        
        # Loans with a defined maturity and up to an amount of EUR 0.25
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and up to an amount of EUR 0.25",
            "Total Interest Rate": to_f(row[38]), # AM
            "Total Collateral Guarantees Interest Rates": to_f(row[39]), # AN
            "Floating Rate 1 Year Fixation": to_f(row[40]), # AO
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[41]), # AP
            "_sort_order": 12
        })
        
        # Loans with a defined maturity and over an amount of EUR 0.25 million and up to 1 million
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and over an amount of EUR 0.25 million and up to 1 million",
            "Total Interest Rate": to_f(row[42]), # AQ
            "Total Collateral Guarantees Interest Rates": to_f(row[43]), # AR
            "Floating Rate 1 Year Fixation": to_f(row[44]), # AS
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[45]), # AT
            "_sort_order": 13
        })
        
        # Loans with a defined maturity over an amount of EUR 1 million
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity over an amount of EUR 1 million",
            "Total Interest Rate": to_f(row[46]), # AU
            "Total Collateral Guarantees Interest Rates": to_f(row[47]), # AV
            "Floating Rate 1 Year Fixation": to_f(row[48]), # AW
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[49]), # AX
            "Over 1 To 5 Years Rate Fixation": to_f(row[50]), # AY
            "Over 5 Years Rate Fixation": to_f(row[51]), # AZ
            "_sort_order": 14
        })
        
        # NFC Orig Maturity Segments
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans up to an amount of EUR 0.25 million",
            "Floating Rate 1 Year Fixation": to_f(row[52]), # BA
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[53]), # BB
            "_sort_order": 15
        })
        
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 0.25 million and up to 1 million",
            "Floating Rate 1 Year Fixation": to_f(row[54]), # BC
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[55]), # BD
            "_sort_order": 16
        })
        
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 1 million",
            "Floating Rate 1 Year Fixation": to_f(row[56]), # BE
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[57]), # BF
            "_sort_order": 17
        })

    out = pd.DataFrame(records)
    
    # Fill None in keys
    out["Group"] = out["Group"].fillna("")
    out["Loan Type"] = out["Loan Type"].fillna("")
    
    cols = [
        "Year", "Month", "Total Consumer Loans Aprc", "Total Housing Loans Aprc", 
        "Delta Interest Rate Deposits", "Weighted Average Interest Rate New Loans In Euro", 
        "Group", "Weighted Average Interest Rate", "Loan Type", "Credit Cards", 
        "Open Account Loans", "Debit Balances On Current Accounts", "Total Interest Rate", 
        "Total Collateral Guarantees Interest Rates", "Total Small Medium Enterprises Interest Rates", 
        "Floating Rate 1 Year Fixation", "Floating Rate 1 Year Rate Fixation Collateral Guarantees", 
        "Floating Rate 1 Year Rate Fixation Floating Rate", "Over 1 To 5 Years Rate Fixation", 
        "Over 5 Years Rate Fixation", "Over 5 To 10 Years Rate Fixation", "Over 10 Years Rate Fixation", 
        "Credit Lines", "Debit Balances Sight Deposits",
        "_sort_order"
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = None
            
    return out[cols]
