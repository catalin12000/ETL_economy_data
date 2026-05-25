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
            "year": year, "month": month,
            "group": "Individuals and private non-profit institutions",
            "loan_type": "Consumer loans with a defined maturity",
            "total_loan_amount": to_f(row[1]), # B
            "total_collateral_guarantees_loans": to_f(row[2]), # C
            "floating_rate_1_year_fixation": to_f(row[3]), # D
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[4]), # E
            "over_1_to_5_years_rate_fixation": to_f(row[5]), # F
            "over_5_years_rate_fixation": to_f(row[6]), # G
            "_sort_order": 1
        })
        
        # Segment 2: Housing loans
        records.append({
            "year": year, "month": month,
            "group": "Individuals and private non-profit institutions",
            "loan_type": "Housing loans",
            "total_loan_amount": to_f(row[7]), # H
            "floating_rate_1_year_fixation": to_f(row[8]), # I
            "floating_rate_1_year_rate_fixation_floating_rate": to_f(row[9]), # J
            "over_1_to_5_years_rate_fixation": to_f(row[10]), # K
            "over_5_to_10_years_rate_fixation": to_f(row[11]), # L
            "over_10_years_rate_fixation": to_f(row[12]), # M
            "_sort_order": 2
        })

        # Segment 3: Other loans with a defined maturity
        records.append({
            "year": year, "month": month,
            "group": "Individuals and private non-profit institutions",
            "loan_type": "Other loans with a defined maturity",
            "floating_rate_1_year_fixation": to_f(row[13]), # N
            "over_1_to_5_years_rate_fixation": to_f(row[14]), # O
            "over_5_years_rate_fixation": to_f(row[15]), # P
            "_sort_order": 3
        })

        # --- Sole Proprietors ---

        # Segment 4: Other loans with a defined maturity
        records.append({
            "year": year, "month": month,
            "group": "Sole proprietors and unicorporated businesses",
            "loan_type": "Other loans with a defined maturity",
            "total_loan_amount": to_f(row[16]), # Q
            "total_collateral_guarantees_loans": to_f(row[17]), # R
            "floating_rate_1_year_fixation": to_f(row[18]), # S
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[19]), # T
            "_sort_order": 4
        })

        # --- NFC ---

        # Segment 5: Loans with a defined maturity (Total)
        records.append({
            "year": year, "month": month,
            "group": "Non-financial corporations",
            "loan_type": "Loans with a defined maturity",
            "total_loan_amount": to_f(row[20]), # U
            "total_small_medium_enterprises_loans": to_f(row[21]), # V
            "floating_rate_1_year_fixation": to_f(row[22]), # W
            "over_1_to_5_years_rate_fixation": to_f(row[23]), # X
            "over_5_years_rate_fixation": to_f(row[24]), # Y
            "_sort_order": 5
        })
        
        # Segment 6: 0.25 to 1M
        records.append({
            "year": year, "month": month,
            "group": "Non-financial corporations",
            "loan_type": "Loans with a defined maturity and up to an amount of EUR 1 million - Loans with a defined maturity and over an amount of EUR 0.25 million and up to 1 million",
            "total_loan_amount": to_f(row[29]), # AD
            "total_collateral_guarantees_loans": to_f(row[30]), # AE
            "floating_rate_1_year_fixation": to_f(row[31]), # AF
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[32]), # AG
            "_sort_order": 6
        })
        
        # Segment 7: up to 0.25M
        records.append({
            "year": year, "month": month,
            "group": "Non-financial corporations",
            "loan_type": "Loans with a defined maturity and up to an amount of EUR 1 million - Loans with a defined maturity and up to an amount of EUR 0.25",
            "total_loan_amount": to_f(row[25]), # Z
            "total_collateral_guarantees_loans": to_f(row[26]), # AA
            "floating_rate_1_year_fixation": to_f(row[27]), # AB
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[28]), # AC
            "_sort_order": 7
        })

        # Segment 8: Over 1M
        records.append({
            "year": year, "month": month,
            "group": "Non-financial corporations",
            "loan_type": "Loans with a defined maturity over an amount of EUR 1 million",
            "total_loan_amount": to_f(row[33]), # AH
            "total_collateral_guarantees_loans": to_f(row[34]), # AI
            "floating_rate_1_year_fixation": to_f(row[35]), # AJ
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[36]), # AK
            "over_1_to_5_years_rate_fixation": to_f(row[37]), # AL
            "over_5_years_rate_fixation": to_f(row[38]), # AM
            "_sort_order": 8
        })
        
        # Segment 9: Orig Mat > 1yr | 0.25 to 1M
        # Shift values to Floating Slots per snippet
        records.append({
            "year": year, "month": month,
            "group": "Non-financial corporations",
            "loan_type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 0.25 million and up to 1 million",
            "floating_rate_1_year_fixation": to_f(row[41]), # AP
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[42]), # AQ
            "_sort_order": 9
        })
        
        # Segment 10: Orig Mat > 1yr | Over 1M
        records.append({
            "year": year, "month": month,
            "group": "Non-financial corporations",
            "loan_type": "Loans with an original maturity over 1 year - Loans over an amount of EUR 1 million",
            "floating_rate_1_year_fixation": to_f(row[43]), # AR
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[44]), # AS
            "_sort_order": 10
        })
        
        # Segment 11: Orig Mat > 1yr | Up to 0.25M
        records.append({
            "year": year, "month": month,
            "group": "Non-financial corporations",
            "loan_type": "Loans with an original maturity over 1 year - Loans up to an amount of EUR 0.25 million",
            "floating_rate_1_year_fixation": to_f(row[39]), # AN
            "floating_rate_1_year_rate_fixation_collateral_guarantees": to_f(row[40]), # AO
            "_sort_order": 11
        })

    out = pd.DataFrame(records)
    
    # Fill None in keys with empty strings
    out["group"] = out["group"].fillna("")
    out["loan_type"] = out["loan_type"].fillna("")

    cols = [
        "year", "month", "group", "loan_type", "total_loan_amount",
        "total_collateral_guarantees_loans", "total_small_medium_enterprises_loans",
        "floating_rate_1_year_fixation", "floating_rate_1_year_rate_fixation_collateral_guarantees",
        "floating_rate_1_year_rate_fixation_floating_rate", "over_1_to_5_years_rate_fixation",
        "over_5_years_rate_fixation", "over_5_to_10_years_rate_fixation", "over_10_years_rate_fixation",
        "_sort_order"
    ]
    for c in cols:
        if c not in out.columns:
            out[c] = None
            
    return out[cols]