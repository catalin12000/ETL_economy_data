import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from typing import List, Any
from pathlib import Path


def _load_dotenv_if_present() -> None:
    """
    Minimal .env loader (no external dependency).
    Loads variables from project-root .env only if they are not already set.
    """
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        os.environ[key] = value

def get_engine(db_name: str = "athena"):
    """
    Returns a SQLAlchemy engine for the specified database.
    Works with FULL DATABASE_URL like DigitalOcean:
      postgresql://user:pass@host:port/defaultdb?sslmode=require
    Switches defaultdb -> athena/zeus by replacing the database in the URL.
    """
    _load_dotenv_if_present()
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
    res = res.replace("–", "-").replace("—", "-")
    res = re.sub(r'\s+', ' ', res)
    return res

def _normalize_match_value(col: str, s: Any) -> str:
    """
    Column-aware key normalization used only for matching.
    Handles known wording drift between source files and DB labels.
    """
    v = _normalize_str(s)

    if col == "loan_type":
        if v in {
            "loans with a defined maturity",
            "loans with defined maturity",
            "other loans with a defined maturity",
            "other loans with defined maturity",
        }:
            return "loans with defined maturity"

    if col == "area":
        if v in {"euro_area", "euro_area_countries"}:
            return "euro_area"
        if v in {"eu_countries", "eu_countries_excl_euro_area"}:
            return "eu_countries"

    if col == "country_of_origin":
        if v in {"czech rep", "czech republic", "czech_ republic"}:
            return "czech republic"
        if v in {"united kingdom", "united_kingdom"}:
            return "united kingdom"

    return v

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

    # Schema validation: surface typos/mismatches loudly instead of producing
    # a misleading Delta where every row looks "different" for a missing column.
    extracted_cols = set(df.columns)
    db_cols = set(df_db.columns)
    missing_in_extract = [c for c in match_cols + sync_cols if c.lower() not in extracted_cols]
    missing_in_db      = [c for c in match_cols + sync_cols if c.lower() not in db_cols]
    if missing_in_extract or missing_in_db:
        msg = (
            f"Schema mismatch for table {table_name!r}: "
            f"missing in extracted df={missing_in_extract}, "
            f"missing in DB={missing_in_db}. "
            f"Extracted cols: {sorted(extracted_cols)}. "
            f"DB cols: {sorted(db_cols)}."
        )
        print(f"ERROR: {msg}")
        return {"error": msg}

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
            df[col + "_norm"] = df[col].apply(lambda x: _normalize_match_value(col, x))
            df_db[col + "_norm"] = df_db[col].apply(lambda x: _normalize_match_value(col, x))

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
    has_id_col = ("id" in merged.columns) or ("id_db" in merged.columns)

    if not to_insert.empty:
        for _, row in to_insert.iterrows():
            row_data = {}
            for col in match_cols + sync_cols:
                # Use local values for missing rows
                val = row[f"{col}_local"] if f"{col}_local" in row else row.get(col)
                row_data[col] = val
            if has_id_col:
                row_data["id"] = row.get("id_db", row.get("id", pd.NA))
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
                # After merge, original columns are suffixed _local or _db
                local_key = f"{k}_local" if f"{k}_local" in row else k
                db_key = f"{k}_db" if f"{k}_db" in row else k
                
                if k in cols_to_restore and db_key in row:
                    row_updates[k] = row[db_key] # Restore DB naming!
                else:
                    row_updates[k] = row[local_key]

            if has_id_col:
                row_updates["id"] = row.get("id_db", row.get("id", pd.NA))

            for col in sync_cols:
                local_val = row[f"{col}_local"] if f"{col}_local" in row else row.get(col)
                db_val = row[f"{col}_db"] if f"{col}_db" in row else row.get(col)
                
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
