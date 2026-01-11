import os
import pandas as pd
from sqlalchemy import create_engine, text
from typing import List

def get_engine(db_name: str = "athena"):
    """
    Returns a SQLAlchemy engine for the specified database.
    athena -> Greece
    zeus -> Cyprus
    Fetches base URL from DATABASE_URL environment variable.
    """
    base_url = os.getenv("DATABASE_URL")
    if not base_url:
        raise EnvironmentError("DATABASE_URL environment variable not set. Please provide connection string without the database name suffix.")
    
    # Ensure URL ends with / for suffixing
    if not base_url.endswith("/"):
        base_url += "/"
        
    url = f"{base_url}{db_name}?sslmode=require"
    return create_engine(url)

def compare_with_postgres(df: pd.DataFrame, table_name: str, db_name: str, match_cols: List[str], sync_cols: List[str], tolerance: float = 0.05, sql_file_path: str = None):
    """
    READ-ONLY comparison logic for Postgres.
    - match_cols: Columns used to identify the same record (e.g., ['year', 'month'])
    - sync_cols: Columns to compare.
    - sql_file_path: Optional path to a .sql file containing the SELECT query to fetch current DB state.
    
    Returns a dictionary containing DataFrames of missing or different rows. 
    Does NOT write to the database.
    """
    engine = get_engine(db_name)
    
    # 1. Fetch current DB state
    if sql_file_path:
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                query = f.read()
        except Exception as e:
            return {"error": f"Failed to read SQL file {sql_file_path}: {e}"}
    else:
        # Fallback to dynamic construction
        cols_to_fetch = match_cols + sync_cols + ["id"]
        cols_str = ", ".join(cols_to_fetch)
        query = f'SELECT {cols_str} FROM "public"."{table_name}"'
    
    try:
        df_db = pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error fetching from DB table {table_name}: {e}")
        return {"error": str(e)}

    # Standardize column names for comparison
    df_db.columns = [c.lower() for c in df_db.columns]
    df.columns = [c.lower() for c in df.columns]
    
    # Ensure types and cleaning for keys to prevent merge misses
    for col in match_cols:
        if col in ['year', 'month']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df_db[col] = pd.to_numeric(df_db[col], errors='coerce')
        elif df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
            df_db[col] = df_db[col].astype(str).str.strip()

    # 2. Merge
    merged = pd.merge(
        df, 
        df_db, 
        on=match_cols, 
        how='outer', 
        suffixes=('_local', '_db'),
        indicator=True
    )

    # 3. Identify Missing Rows (Inserts)
    to_insert = merged[merged['_merge'] == 'left_only'].copy()
    inserted_rows_list = []

    if not to_insert.empty:
        for _, row in to_insert.iterrows():
            row_data = {}
            for col in match_cols + sync_cols:
                val = row[f"{col}_local"] if f"{col}_local" in row else row.get(col)
                row_data[col] = val
            inserted_rows_list.append(row_data)

    # 4. Identify Different Rows (Updates)
    common = merged[merged['_merge'] == 'both']
    updated_rows_list = []

    if not common.empty:
        for _, row in common.iterrows():
            row_updates = {}
            has_diff = False
            
            for k in match_cols:
                row_updates[k] = row[k]

            for col in sync_cols:
                local_val = row[f"{col}_local"]
                db_val = row[f"{col}_db"]
                row_updates[col] = local_val

                is_diff = False
                if pd.notna(local_val) and pd.notna(db_val):
                    try:
                        if abs(float(local_val) - float(db_val)) > tolerance:
                            is_diff = True
                    except:
                        if str(local_val) != str(db_val):
                            is_diff = True
                elif pd.notna(local_val) and pd.isna(db_val):
                     is_diff = True

                if is_diff:
                    has_diff = True

            if has_diff:
                updated_rows_list.append(row_updates)

    # Construct return DFs
    inserted_df = pd.DataFrame(inserted_rows_list) if inserted_rows_list else pd.DataFrame()
    updated_df = pd.DataFrame(updated_rows_list) if updated_rows_list else pd.DataFrame()

    return {
        "status": "success",
        "inserted": len(inserted_df),
        "updated": len(updated_df),
        "inserted_df": inserted_df,
        "updated_df": updated_df
    }