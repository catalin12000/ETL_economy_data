import pandas as pd
import sys

file_path = "data/downloads/08_ed_employment/ed_employment.xls"
try:
    df = pd.read_excel(file_path, sheet_name=0, header=None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print("\nRows 0-10:")
    for i in range(10):
        print(f"Row {i}: {df.iloc[i].tolist()}")
except Exception as e:
    print(f"Error: {e}")

