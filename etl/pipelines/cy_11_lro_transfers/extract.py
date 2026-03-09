import fitz
import pandas as pd
import re

DISTRICT_MAP = {
    "ΛΕΥΚΩΣΙΑ": "Nicosia",
    "ΛΕΜΕΣΟΣ": "Limassol",
    "ΛΑΡΝΑΚΑ": "Larnaca",
    "ΑΜΜΟΧΩΣΤΟΣ": "Famagusta",
    "ΠΑΦΟΣ": "Paphos"
}

MONTH_MAP = {
    "ΙΑΝΟΥΑΡΙΟΣ": 1, "ΦΕΒΡΟΥΑΡΙΟΣ": 2, "ΜΑΡΤΙΟΣ": 3, "ΑΠΡΙΛΙΟΣ": 4,
    "ΜΑΪΟΣ": 5, "ΙΟΥΝΙΟΣ": 6, "ΙΟΥΛΙΟΣ": 7, "ΑΥΓΟΥΣΤΟΣ": 8,
    "ΣΕΠΤΕΜΒΡΙΟΣ": 9, "ΟΚΤΩΒΡΙΟΣ": 10, "ΝΟΕΜΒΡΙΟΣ": 11, "ΔΕΚΕΜΒΡΙΟΣ": 12
}

def clean_val(val):
    if val is None: return 0.0
    s = str(val).replace('€', '').replace(',', '').strip()
    try: return float(s)
    except: return 0.0

def parse_totals_fitz(pdf_path):
    records = []
    doc = fitz.open(pdf_path)
    for page in doc:
        text = page.get_text()
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        district = None
        for gr, en in DISTRICT_MAP.items():
            if gr in text.upper():
                district = en
                break
        if not district: continue
        
        year_match = re.search(r'202\d', text)
        year = int(year_match.group()) if year_match else 0
        
        # In January PDFs, every number is repeated because Month == Total column
        # Logic: Collect all numbers, and if we see duplicates in sequence, it's January layout
        month = 0
        all_nums = []
        for line in lines:
            for gr, m_val in MONTH_MAP.items():
                if gr in line.upper(): month = m_val
            
            clean = line.replace('€', '').replace(',', '').strip()
            if re.match(r'^\d+(\.\d+)?$', clean):
                all_nums.append(clean_val(clean))

        if month == 1 and len(all_nums) >= 8:
            # Layout: [Cases, CasesTotal, Props, PropsTotal, Declared, DeclaredTotal, Accepted, AcceptedTotal]
            records.append({
                "year": year, "month": month, "district": district,
                "number_of_buyers_total": all_nums[0],
                "number_parcels_total": all_nums[2],
                "declared_price": all_nums[4],
                "accepted_price": all_nums[6]
            })
        elif month > 1 and len(all_nums) >= 4:
            # Standard layout
            records.append({
                "year": year, "month": month, "district": district,
                "number_of_buyers_total": all_nums[0],
                "number_parcels_total": all_nums[1],
                "declared_price": all_nums[2],
                "accepted_price": all_nums[3]
            })
    doc.close()
    return pd.DataFrame(records)

def parse_foreigners_fitz(pdf_path):
    records = []
    doc = fitz.open(pdf_path)
    page = doc[0]
    text = page.get_text()
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    date_match = re.search(r'(\d{2})/(\d{4})', text)
    if not date_match: return pd.DataFrame()
    month = int(date_match.group(1))
    year = int(date_match.group(2))
    
    # Extract all numbers
    nums = []
    for line in lines:
        if '/' in line and len(line) <= 7: continue
        clean = line.replace(',', '').strip()
        if clean.isdigit():
            nums.append(float(clean))
            
    # Foreigners 2026 Table (Jan only):
    # Row 1 (Props): [32, 17, 7, 16, 27, 33, 26, 55, 49, 42, 141, 163] (12 nums)
    # Row 2 (Buyers): [37, 16, 11, 24, 35, 47, 34, 66, 78, 65, 195, 218] (12 nums)
    
    # In January PDFs, the PDF might duplicate rows too if Month == Total
    # But usually, it's just 12 numbers per line.
    
    order = ["Nicosia", "Famagusta", "Larnaca", "Limassol", "Paphos"]
    
    if len(nums) >= 24:
        prop_row = nums[0:12]
        buyer_row = nums[12:24]
        
        for i, dist in enumerate(order):
            records.append({
                "year": year, "month": month, "district": dist,
                "number_parcels_eu": prop_row[i*2],
                "number_parcels_non_eu": prop_row[i*2 + 1],
                "number_of_buyers_eu": buyer_row[i*2],
                "number_of_buyers_noneu": buyer_row[i*2 + 1]
            })
            
    doc.close()
    return pd.DataFrame(records)

def extract_lro_transfers(totals_path, foreigners_path):
    df_tot = parse_totals_fitz(totals_path)
    df_for = parse_foreigners_fitz(foreigners_path)
    
    if df_tot.empty or df_for.empty:
        return pd.DataFrame()
        
    df = pd.merge(df_tot, df_for, on=["year", "month", "district"], how="inner")
    
    df["number_of_buyers_locals"] = (df["number_of_buyers_total"] - (df["number_of_buyers_eu"] + df["number_of_buyers_noneu"])).clip(lower=0)
    df["number_parcels_locals"] = (df["number_parcels_total"] - (df["number_parcels_eu"] + df["number_parcels_non_eu"])).clip(lower=0)
    
    return df
