from __future__ import annotations

from pathlib import Path
import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"


def extract_loan_amounts(file_path: Path) -> pd.DataFrame:
    file_path = Path(file_path)
    # Load sheet 'Loans_Amounts'
    # Header rows: 3, 4, 5, 6, 7 (0-indexed) -> Excel rows 4-8
    # Data starts at row 8 (Excel row 9)
    
    df = pd.read_excel(file_path, sheet_name="Loans_Amounts", header=None, engine=_engine_for(file_path))
    
    # 1. Parse Headers (Rows 3-7)
    header_rows = df.iloc[3:8].copy()
    
    # Forward fill headers horizontally to handle merged cells
    header_rows = header_rows.ffill(axis=1)
    
    # Construct column names
    raw_columns = []
    for col_idx in range(df.shape[1]):
        if col_idx == 0:
            raw_columns.append("Date_Raw")
            continue
            
        parts = []
        for row_idx in range(3, 8):
            val = df.iloc[row_idx, col_idx]
            if pd.notna(val) and str(val).strip():
                parts.append(str(val).strip())
        
        # Join parts to form a unique name
        col_name = " | ".join(parts)
        # Clean up
        col_name = col_name.replace("\n", " ").replace("  ", " ")
        if not col_name:
            col_name = f"Unknown_{col_idx}"
        raw_columns.append(col_name)

    # Deduplicate columns
    seen = {}
    unique_columns = []
    for c in raw_columns:
        if c not in seen:
            seen[c] = 0
            unique_columns.append(c)
        else:
            seen[c] += 1
            unique_columns.append(f"{c}___{seen[c]}")

    # 2. Extract Data (Row 8 onwards)
    data = df.iloc[8:].copy()
    data.columns = unique_columns
    
    # Filter out rows where Date is missing
    data = data.dropna(subset=["Date_Raw"])
    
    # 3. Process Date
    data["Date_Raw"] = pd.to_datetime(data["Date_Raw"], errors='coerce')
    data = data.dropna(subset=["Date_Raw"])
    
    data["Year"] = data["Date_Raw"].dt.year
    data["Month"] = data["Date_Raw"].dt.month
    
    # 4. Clean Numeric Columns
    # All columns except Date_Raw, Year, Month are amounts
    amount_cols = [c for c in unique_columns if c not in ("Date_Raw", "Year", "Month")]
    
    # Use a dictionary to build the DataFrame
    cleaned_dict = {
        "Year": data["Year"].astype(int),
        "Month": data["Month"].astype(int)
    }
    
    for col in amount_cols:
        # Force numeric, coerce errors to NaN
        series = pd.to_numeric(data[col], errors='coerce')
        
        # Drop columns that are completely empty
        if not series.isna().all():
            cleaned_dict[col] = series
            
    result = pd.DataFrame(cleaned_dict)
    
    # Sort
    result = result.sort_values(["Year", "Month"]).reset_index(drop=True)
    
    return result
