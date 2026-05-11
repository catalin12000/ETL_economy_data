import pandas as pd
import io

def extract_cpi(file_path):
    """
    Extracts Cyprus CPI data from CYSTAT CSV.
    Input format: MONTH, 1986, 1992, 2005, 2015, 2025
    """
    # Read CSV, handling possible BOM
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    
    # Melt the base years into a long format
    # The columns are MONTH, "1986", "1992", etc.
    id_vars = ['MONTH']
    value_vars = [c for c in df.columns if c != 'MONTH']
    
    df_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='base_year', value_name='index')
    
    # Parse MONTH (e.g., "1980M01")
    # Month is index 5-7, Year is index 0-4
    df_long['year'] = df_long['MONTH'].str[:4].astype(int)
    df_long['month'] = df_long['MONTH'].str[5:].astype(int)
    
    # Clean index column (CYSTAT uses ".." for missing values)
    df_long['index'] = pd.to_numeric(df_long['index'], errors='coerce')
    
    # Drop rows with missing values (common for future base years or very old data)
    df_long = df_long.dropna(subset=['index']).copy()
    
    # Final cleanup
    df_long['base_year'] = df_long['base_year'].astype(int)
    
    return df_long[['year', 'month', 'base_year', 'index']]
