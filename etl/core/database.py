import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from typing import List, Any

def get_engine(db_name: str = "athena"):
    """
    Returns a SQLAlchemy engine for the specified database.
    Works with FULL DATABASE_URL like DigitalOcean:
      postgresql://user:pass@host:port/defaultdb?sslmode=require
    Switches defaultdb -> athena/zeus by replacing the database in the URL.
    """
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("DATABASE_URL environment variable not set.")

    url = make_url(db_url).set(database=db_name)
    return create_engine(url)

def _normalize_str(s: Any) -> str:
    if pd.isna(s):
        return ""
    # Standardize dashes, lowercase, strip, and collapse multiple spaces
    import re
    res = str(s).lower().strip()
    res = res.replace("–", "-").replace("—", "-")
    res = re.sub(r'\s+', ' ', res)
    return res

def compare_with_postgres(df: pd.DataFrame, table_name: str, db_name: str, match_cols: List[str], sync_cols: List[str], tolerance: float = 0.11, sql_file_path: str = None):
    """
    READ-ONLY comparison logic for Postgres.
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
        cols_to_fetch = match_cols + sync_cols + (["id"] if "id" not in match_cols else [])
        cols_str = ", ".join(cols_to_fetch)
        query = f'SELECT {cols_str} FROM "public"."{table_name}"'
    
    try:
        df_db = pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error fetching from DB table {table_name}: {e}")
        return {"error": str(e)}

    # Standardize column names
    df_db.columns = [c.lower() for c in df_db.columns]
    df.columns = [c.lower() for c in df.columns]
    
    # Store original DB values for restoration later
    orig_db_values = {}
    cols_to_restore = ['geopolitical_entity', 'group', 'loan_type', 'seasonally']
    for col in cols_to_restore:
        if col in df_db.columns:
            # Create a map: normalized_key -> original_db_value
            # We use the match columns as the key for this map
            temp_db = df_db.copy()
            # We need a unique key for restoration. Month/Year/Quarter + Normalized String
            pass # We will handle this during the loop instead for better accuracy

    # 2. Key Normalization for JOIN
    for col in match_cols:
        if col in ['year', 'month', 'quarter']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            df_db[col] = pd.to_numeric(df_db[col], errors='coerce').fillna(0).astype(int)
        else:
            df[col + "_norm"] = df[col].apply(_normalize_str)
            df_db[col + "_norm"] = df_db[col].apply(_normalize_str)

    # 3. Merge on normalized keys
    norm_match_cols = [(c + "_norm" if c not in ['year', 'month', 'quarter'] else c) for c in match_cols]
    
    merged = pd.merge(
        df, 
        df_db, 
        left_on=norm_match_cols,
        right_on=norm_match_cols,
        how='outer', 
        suffixes=('_local', '_db'),
        indicator=True
    )

    # 4. Identify Missing Rows (Inserts)
    to_insert = merged[merged['_merge'] == 'left_only'].copy()
    inserted_rows_list = []

    if not to_insert.empty:
        for _, row in to_insert.iterrows():
            row_data = {}
            for col in match_cols + sync_cols:
                # Use local values for missing rows
                val = row[f"{col}_local"] if f"{col}_local" in row else row.get(col)
                row_data[col] = val
            inserted_rows_list.append(row_data)

    # 5. Identify Different Rows (Updates)
    common = merged[merged['_merge'] == 'both']
    updated_rows_list = []

    if not common.empty:
        for _, row in common.iterrows():
            row_updates = {}
            has_diff = False
            
            # CRITICAL: Use the ORIGINAL DB string values for the deliverable
            for k in match_cols:
                if k in cols_to_restore and f"{k}_db" in row:
                    row_updates[k] = row[f"{k}_db"] # Restore DB naming!
                else:
                    row_updates[k] = row[k]

            for col in sync_cols:
                local_val = row[f"{col}_local"]
                db_val = row[f"{col}_db"]
                
                # Default to local value for the update object
                row_updates[col] = local_val

                is_diff = False
                if pd.notna(local_val) and pd.notna(db_val):
                    try:
                        # Round to 2 decimals for comparison
                        if abs(round(float(local_val), 2) - round(float(db_val), 2)) > tolerance:
                            is_diff = True
                    except:
                        if _normalize_str(local_val) != _normalize_str(db_val):
                            is_diff = True
                elif pd.notna(local_val) != pd.notna(db_val):
                     is_diff = True

                if is_diff:
                    has_diff = True

            if has_diff:
                updated_rows_list.append(row_updates)

    inserted_df = pd.DataFrame(inserted_rows_list) if inserted_rows_list else pd.DataFrame()
    updated_df = pd.DataFrame(updated_rows_list) if updated_rows_list else pd.DataFrame()

    return {
        "status": "success",
        "inserted": len(inserted_df),
        "updated": len(updated_df),
        "inserted_df": inserted_df,
        "updated_df": updated_df
    }