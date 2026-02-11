import pandas as pd
import glob
import os
import numpy as np

def check_dates():
    # Define the output directory
    base_dir = r"data/outputs"
    
    # Pattern to match deliverable files
    pattern = os.path.join(base_dir, "**", "deliverable_*.csv")
    
    # Get all matching files
    files = glob.glob(pattern, recursive=True)
    
    # Filter out DEBUG_RULES if any
    files = [f for f in files if "DEBUG_RULES" not in f]
    
    results = []

    print(f"Found {len(files)} deliverable files.")

    for file_path in files:
        try:
            # Read the file
            df = pd.read_csv(file_path)
            
            # Normalize column names to title case for easier checking (e.g. year -> Year)
            # Actually, let's just use the existing names but check case-insensitively
            cols_map = {c.lower(): c for c in df.columns}
            
            date_col_series = None
            
            if 'year' in cols_map and 'month' in cols_map:
                # Handle Year/Month
                # Clean Month column - sometimes it might be text or have extra spaces
                # Ensure numeric
                try:
                    # Try to convert month names to numbers if they are strings? 
                    # Assuming they are integers for now based on typical patterns, 
                    # but if they are 'January', 'Feb'... pandas to_datetime might handle it if we construct string.
                    # Safest: construct "Year-Month-01" string
                    
                    # Convert to string and pad month
                    df['temp_date_str'] = df[cols_map['year']].astype(str) + '-' + df[cols_map['month']].astype(str) + '-01'
                    date_col_series = pd.to_datetime(df['temp_date_str'], errors='coerce')
                except Exception as e:
                    print(f"Error converting Year/Month in {os.path.basename(file_path)}: {e}")

            elif 'year' in cols_map and 'quarter' in cols_map:
                # Handle Year/Quarter
                try:
                    # Quarter 1 -> 01, 2 -> 04, 3 -> 07, 4 -> 10
                    # (Q-1)*3 + 1
                    df['temp_month'] = (df[cols_map['quarter']].astype(int) - 1) * 3 + 1
                    df['temp_date_str'] = df[cols_map['year']].astype(str) + '-' + df['temp_month'].astype(str) + '-01'
                    date_col_series = pd.to_datetime(df['temp_date_str'], errors='coerce')
                except Exception as e:
                    print(f"Error converting Year/Quarter in {os.path.basename(file_path)}: {e}")
            
            else:
                # Look for a single date column
                date_col_name = None
                for col in df.columns:
                    if "date" in col.lower():
                        date_col_name = col
                        break
                
                if date_col_name:
                    date_col_series = pd.to_datetime(df[date_col_name], errors='coerce')
            
            if date_col_series is None or date_col_series.dropna().empty:
                 print(f"WARNING: No valid date info found in {os.path.basename(file_path)}")
                 # Print columns to help debug
                 print(f"   Columns: {df.columns.tolist()}")
                 continue
            
            dates = date_col_series.dropna()
            min_date = dates.min()
            max_date = dates.max()
            
            results.append({
                "File": os.path.basename(file_path),
                "Min Date": min_date.strftime('%Y-%m-%d'),
                "Max Date": max_date.strftime('%Y-%m-%d'),
                "Count": len(df)
            })
            
        except Exception as e:
            print(f"ERROR processing {os.path.basename(file_path)}: {e}")

    # Sort results by filename
    results.sort(key=lambda x: x["File"])

    # Print results table
    print(f"{'File':<60} | {'Min Date':<12} | {'Max Date':<12} | {'Count':<6}")
    print("-" * 100)
    for res in results:
        print(f"{res['File']:<60} | {res['Min Date']:<12} | {res['Max Date']:<12} | {res['Count']:<6}")

if __name__ == "__main__":
    check_dates()
