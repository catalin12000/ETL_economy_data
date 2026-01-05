# etl/core/compare_csv.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class CsvUpdateResult:
    rows_before: int
    rows_after: int
    updated_cells: int
    new_rows: int
    report_df: pd.DataFrame
    updated_df: pd.DataFrame


def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cols = [str(c).strip() for c in df.columns]

    # dedupe: Year, Year -> Year, Year__1
    seen = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}__{seen[c]}")
    df.columns = out
    return df


def _to_num(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    if s == "" or s in ("…", "...", "—"):
        return pd.NA
    # remove thousands separators
    s = s.replace(",", "")
    try:
        f = float(s)
        # If it's an integer value, return as int, else float
        if f == int(f):
            return int(f)
        return f
    except Exception:
        return pd.NA


def _period_num(df: pd.DataFrame) -> pd.Series:
    return df["Year"].astype(int) * 100 + df["Month"].astype(int)


def compare_and_update_csv(
    db_csv_path: Path,
    extracted_df: pd.DataFrame,
    out_csv_path: Path,
    report_csv_path: Path,
    *,
    key_cols: List[str] = ["Year", "Month"],
    val_cols: Optional[List[str]] = None,
    prevent_older_than_db: bool = True,
) -> CsvUpdateResult:
    if not isinstance(db_csv_path, Path):
        db_csv_path = Path(db_csv_path)

    df_new = extracted_df.copy()
    df_new = _clean_cols(df_new)
    
    # If val_cols not provided, assume everything except key_cols
    if val_cols is None:
        val_cols = [c for c in df_new.columns if c not in key_cols]

    # Handle missing DB by creating an empty one with the same schema
    if not db_csv_path.exists():
        print(f"Creating new baseline DB: {db_csv_path}")
        db_csv_path.parent.mkdir(parents=True, exist_ok=True)
        # Create empty df with same columns as new data
        df_db = pd.DataFrame(columns=df_new.columns)
        df_db.to_csv(db_csv_path, index=False)
    else:
        df_db = pd.read_csv(db_csv_path)
        df_db = _clean_cols(df_db)

    # normalize types for keys
    for c in key_cols:
        df_db[c] = pd.to_numeric(df_db[c], errors="coerce").astype("Int64")
        df_new[c] = pd.to_numeric(df_new[c], errors="coerce").astype("Int64")

    # normalize types for values
    for c in val_cols:
        if c in df_db.columns:
            df_db[c] = df_db[c].map(_to_num)
        if c in df_new.columns:
            df_new[c] = df_new[c].map(_to_num)

    df_db = df_db.dropna(subset=key_cols).copy()
    df_new = df_new.dropna(subset=key_cols).copy()

    if prevent_older_than_db and not df_db.empty:
        min_period = int(_period_num(df_db).min())
        df_new = df_new[_period_num(df_new) >= min_period].copy()

    db_idx = df_db.set_index(key_cols)
    new_idx = df_new.set_index(key_cols)

    common = db_idx.index.intersection(new_idx.index)
    only_new = new_idx.index.difference(db_idx.index)

    changes: List[Dict[str, Any]] = []
    updated_cells = 0

    for k in common:
        for c in val_cols:
            if c not in db_idx.columns or c not in new_idx.columns:
                continue
            old = db_idx.loc[k, c]
            new = new_idx.loc[k, c]
            
            # Handle both being NA
            if pd.isna(old) and pd.isna(new):
                continue
                
            # Compare values (with float precision handling)
            is_different = False
            if pd.isna(old) != pd.isna(new):
                is_different = True
            else:
                try:
                    if abs(float(old) - float(new)) > 1e-9:
                        is_different = True
                except:
                    if old != new:
                        is_different = True

            if is_different:
                updated_cells += 1
                row_key = {key_cols[i]: int(k[i]) if isinstance(k, tuple) else int(k) for i in range(len(key_cols))}
                change_entry = {
                    "ChangeType": "UPDATE",
                    **row_key,
                    "Field": c,
                    "OldValue": old if pd.notna(old) else "",
                    "NewValue": new if pd.notna(new) else "",
                }
                changes.append(change_entry)
                db_idx.at[k, c] = new

    df_added = new_idx.loc[only_new].reset_index()
    for _, r in df_added.iterrows():
        row_key = {c: int(r[c]) for c in key_cols}
        val_summary = ", ".join([f"{c}={r[c]}" for c in val_cols if c in r])
        changes.append({
            "ChangeType": "ADD_ROW",
            **row_key,
            "Field": "",
            "OldValue": "",
            "NewValue": val_summary,
        })

    df_updated = pd.concat([db_idx.reset_index(), df_added], ignore_index=True)
    df_updated = df_updated.sort_values(key_cols).reset_index(drop=True)

    out_csv_path.parent.mkdir(parents=True, exist_ok=True)
    report_csv_path.parent.mkdir(parents=True, exist_ok=True)

    df_updated.to_csv(out_csv_path, index=False)
    # Also update the master DB
    df_updated.to_csv(db_csv_path, index=False)
    pd.DataFrame(changes).to_csv(report_csv_path, index=False)

    return CsvUpdateResult(
        rows_before=len(df_db),
        rows_after=len(df_updated),
        updated_cells=updated_cells,
        new_rows=len(df_added),
        report_df=pd.DataFrame(changes),
        updated_df=df_updated,
    )