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


def extract_loan_amounts(file_path: Path) -> pd.DataFrame:
    file_path = Path(file_path)
    
    # Bypass file lock on Windows
    with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as tmp:
        shutil.copy2(file_path, tmp.name)
        tmp_path = Path(tmp.name)
    
    try:
        df = pd.read_excel(tmp_path, sheet_name="Loans_Amounts", header=None, engine=_engine_for(tmp_path))
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
        
        # --- Individuals ---
        
        # Segment 1: Consumer loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Consumer loans with a defined maturity",
            "Total Loan Amount": to_f(row[1]), # B
            "Total Collateral Guarantees Loans": to_f(row[2]), # C
            "Floating Rate 1 Year Fixation": to_f(row[3]), # D
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[4]), # E
            "Over 1 To 5 Years Rate Fixation": to_f(row[5]), # F
            "Over 5 Years Rate Fixation": to_f(row[6]), # G
            "_sort_order": 1
        })
        
        # Segment 2: Housing loans
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Housing loans",
            "Total Loan Amount": to_f(row[7]), # H
            "Floating Rate 1 Year Fixation": to_f(row[8]), # I
            "Floating Rate 1 Year Rate Fixation Floating Rate": to_f(row[9]), # J
            "Over 1 To 5 Years Rate Fixation": to_f(row[10]), # K
            "Over 5 To 10 Years Rate Fixation": to_f(row[11]), # L
            "Over 10 Years Rate Fixation": to_f(row[12]), # M
            "_sort_order": 2
        })

        # Segment 3: Other loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Individuals and private non-profit institutions",
            "Loan Type": "Other loans with a defined maturity",
            "Floating Rate 1 Year Fixation": to_f(row[13]), # N
            "Over 1 To 5 Years Rate Fixation": to_f(row[14]), # O
            "Over 5 Years Rate Fixation": to_f(row[15]), # P
            "_sort_order": 3
        })

        # --- Sole Proprietors ---

        # Segment 4: Other loans with a defined maturity
        records.append({
            "Year": year, "Month": month,
            "Group": "Sole proprietors and unicorporated businesses",
            "Loan Type": "Other loans with a defined maturity",
            "Total Loan Amount": to_f(row[16]), # Q
            "Total Collateral Guarantees Loans": to_f(row[17]), # R
            "Floating Rate 1 Year Fixation": to_f(row[18]), # S
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[19]), # T
            "_sort_order": 4
        })

        # --- NFC ---

        # Segment 5: Loans with a defined maturity (Total)
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity",
            "Total Loan Amount": to_f(row[20]), # U
            "Total Small Medium Enterprises Loans": to_f(row[21]), # V
            "Floating Rate 1 Year Fixation": to_f(row[22]), # W
            "Over 1 To 5 Years Rate Fixation": to_f(row[23]), # X
            "Over 5 Years Rate Fixation": to_f(row[24]), # Y
            "_sort_order": 5
        })
        
        # Segment 6: 0.25 to 1M
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and up to an amount of EUR 1 million - Loans with a defined maturity and over an amount of EUR 0.25 million and up to 1 million",
            "Total Loan Amount": to_f(row[29]), # AD
            "Total Collateral Guarantees Loans": to_f(row[30]), # AE
            "Floating Rate 1 Year Fixation": to_f(row[31]), # AF
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[32]), # AG
            "_sort_order": 6
        })
        
        # Segment 7: up to 0.25M
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity and up to an amount of EUR 1 million - Loans with a defined maturity and up to an amount of EUR 0.25",
            "Total Loan Amount": to_f(row[25]), # Z
            "Total Collateral Guarantees Loans": to_f(row[26]), # AA
            "Floating Rate 1 Year Fixation": to_f(row[27]), # AB
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[28]), # AC
            "_sort_order": 7
        })

        # Segment 8: Over 1M
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with a defined maturity over an amount of EUR 1 million",
            "Total Loan Amount": to_f(row[33]), # AH
            "Total Collateral Guarantees Loans": to_f(row[34]), # AI
            "Floating Rate 1 Year Fixation": to_f(row[35]), # AJ
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[36]), # AK
            "Over 1 To 5 Years Rate Fixation": to_f(row[37]), # AL
            "Over 5 Years Rate Fixation": to_f(row[38]), # AM
            "_sort_order": 8
        })
        
        # Segment 9: Orig Mat > 1yr | 0.25 to 1M
        # Shift values to Floating Slots per snippet
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 0.25 million and up to 1 million",
            "Floating Rate 1 Year Fixation": to_f(row[41]), # AP
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[42]), # AQ
            "_sort_order": 9
        })
        
        # Segment 10: Orig Mat > 1yr | Over 1M
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 1 million",
            "Floating Rate 1 Year Fixation": to_f(row[43]), # AR
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[44]), # AS
            "_sort_order": 10
        })
        
        # Segment 11: Orig Mat > 1yr | Up to 0.25M
        records.append({
            "Year": year, "Month": month,
            "Group": "Non-financial corporations",
            "Loan Type": "Loans with an original maturity over 1 year - Loans up to an amount of EUR 0.25 million",
            "Floating Rate 1 Year Fixation": to_f(row[39]), # AN
            "Floating Rate 1 Year Rate Fixation Collateral Guarantees": to_f(row[40]), # AO
            "_sort_order": 11
        })

    out = pd.DataFrame(records)
    
    # Fill None in keys with empty strings
    out["Group"] = out["Group"].fillna("")
    out["Loan Type"] = out["Loan Type"].fillna("")
    
    cols = [
        "Year", "Month", "Group", "Loan Type", "Total Loan Amount", 
        "Total Collateral Guarantees Loans", "Total Small Medium Enterprises Loans", 
        "Floating Rate 1 Year Fixation", "Floating Rate 1 Year Rate Fixation Collateral Guarantees", 
        "Floating Rate 1 Year Rate Fixation Floating Rate", "Over 1 To 5 Years Rate Fixation", 
        "Over 5 Years Rate Fixation", "Over 5 To 10 Years Rate Fixation", "Over 10 Years Rate Fixation",
        "_sort_order"
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = None
            
    return out[cols]