from __future__ import annotations

from pathlib import Path
import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, 'rb') as f:
        sig = f.read(2)
        if sig == b'PK':
            return "openpyxl"
    return "xlrd"


def extract_gdp(file_path: Path) -> pd.DataFrame:
    file_path = Path(file_path)
    # Read the file. It's an XLSX file despite the extension.
    df = pd.read_excel(file_path, sheet_name="Table EL", header=None, engine=_engine_for(file_path))
    
    # Data starts at row 5 (0-indexed) based on visual inspection
    data = df.iloc[5:].copy()
    
    # Col 0: Year, Col 1: Quarter
    # Fill Year forward
    data[0] = data[0].ffill()
    
    # Map Quarters
    def map_quarter(val):
        s = str(val).strip().upper()
        if s in ("I", "Ι", "1", "Q1"): return 1
        if s in ("II", "ΙΙ", "2", "Q2"): return 2
        if s in ("III", "ΙΙΙ", "3", "Q3"): return 3
        if s in ("IV", "ΙV", "4", "Q4"): return 4
        return None

    data["Q_Int"] = data[1].apply(map_quarter)
    
    # Filter out rows with no valid quarter
    data = data.dropna(subset=["Q_Int"])
    
    def to_float(x):
        try:
            return float(x)
        except:
            return pd.NA

    # Mapping per user requirement:
    # Year	Quarter	Chain_Linked_Volumes	Quarter_Over_Quarter	Year_Over_Year	Current_prices
    
    result = pd.DataFrame({
        "Year": data[0].astype(int),
        "Quarter": data["Q_Int"].astype(int),
        "Chain_Linked_Volumes": data[2].apply(to_float),       # Col 2
        "Quarter_Over_Quarter": data[3].apply(to_float),       # Col 3
        "Year_Over_Year": data[4].apply(to_float),             # Col 4
        "Current_prices": data[5].apply(to_float)              # Col 5
    })
    
    # Sort
    result = result.sort_values(["Year", "Quarter"]).reset_index(drop=True)
    
    return result
