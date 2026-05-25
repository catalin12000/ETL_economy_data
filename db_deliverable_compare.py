"""
Compare each deliverable in march_updated_deliverable/ against the live DB.

For each pipeline:
  1. Read the deliverable CSV
  2. Query the matching DB table (using the pipeline's SQL file)
  3. Join on key columns (year/month/quarter + any string keys)
  4. Report:
       - rows in deliverable but MISSING from DB
       - rows in both but with VALUE DIFFERENCES (beyond tolerance)
       - rows fully matched (OK)

Output: db_deliverable_compare_report.csv + per-pipeline console summary.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from etl.core.database import get_engine  # noqa: E402

DELIVERABLE_DIR = ROOT / "march_updated_deliverable"
PIPELINES_DIR   = ROOT / "etl" / "pipelines"
TOLERANCE       = 0.11


def _snake(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("&", "and").replace("+", "and")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _find_pipeline_id(code: str) -> Optional[str]:
    m = re.search(r'pipeline_id\s*=\s*["\']([^"\']+)["\']', code)
    return m.group(1) if m else None


def _parse_pipeline_meta(code: str) -> Optional[dict]:
    # Pattern 1: table_name="literal"
    m_table = re.search(r'table_name\s*=\s*["\']([^"\']+)["\']', code)
    # Pattern 2: DB_TABLE_NAME = "literal"  (cy_05 style)
    if not m_table:
        m_table = re.search(r'DB_TABLE_NAME\s*=\s*["\']([^"\']+)["\']', code)
    # Pattern 3: table_name=self.pipeline_id  → use pipeline_id value
    if not m_table:
        if 'table_name=self.pipeline_id' in code or 'table_name = self.pipeline_id' in code:
            pid = _find_pipeline_id(code)
            if pid:
                # Fake a match object result by using a dict instead
                m_table = type("M", (), {"group": lambda self, n: pid})()
    # Pattern 4: residence permits use shared runner with pipeline_id= kwarg
    if not m_table:
        if 'run_shared_appendix_b_pipeline' in code or 'pipeline_id=self.pipeline_id' in code:
            pid = _find_pipeline_id(code)
            if pid:
                m_table = type("M", (), {"group": lambda self, n: pid})()

    # db_name: literal or self.pipeline_id pattern
    m_db = re.search(r'db_name\s*=\s*["\']([^"\']+)["\']', code)

    if not m_table or not m_db:
        return None

    table = m_table.group(1)
    db    = m_db.group(1)

    m_match = re.search(r'match_cols\s*=\s*\[([^\]]+)\]', code)
    m_sync  = re.search(r'sync_cols\s*=\s*\[([^\]]+)\]', code, re.DOTALL)
    match_cols = re.findall(r'["\']([^"\']+)["\']', m_match.group(1)) if m_match else []
    sync_cols  = re.findall(r'["\']([^"\']+)["\']', m_sync.group(1))  if m_sync  else []

    pid = _find_pipeline_id(code)
    sql_files = list((PIPELINES_DIR / pid).glob("*.sql")) if pid and (PIPELINES_DIR / pid).exists() else []

    return {
        "table":      table,
        "db":         db,
        "match_cols": match_cols,
        "sync_cols":  sync_cols,
        "sql_file":   sql_files[0] if sql_files else None,
    }


def _find_pipeline_dir(code: str) -> Optional[str]:
    return _find_pipeline_id(code)


def _slug(fname: str) -> str:
    s = re.sub(r'_(March|April|May)_\d{4}.*$', '', fname, flags=re.I)
    s = re.sub(r'\.(csv|xlsx|xls)$', '', s, flags=re.I)
    s = re.sub(r'^deliverable_', '', s)
    s = re.sub(r'^(cy|ed)_\d+_', lambda m: m.group(1) + '_', s)
    return s.lower()


def _match_pipeline(deliverable_slug: str) -> Optional[Path]:
    for pdir in PIPELINES_DIR.iterdir():
        if not pdir.is_dir() or pdir.name.startswith("__"):
            continue
        p_slug = re.sub(r'^(cy|ed)_\d+_', lambda m: m.group(1) + '_', pdir.name.lower())
        if p_slug == deliverable_slug or pdir.name.lower() == deliverable_slug:
            return pdir / "pipeline.py"
    return None


def _read_deliverable(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def _compare(df_deliv: pd.DataFrame, df_db: pd.DataFrame,
             match_cols: list[str], sync_cols: list[str]) -> dict:
    """
    Returns counts of: matched, missing_in_db, value_diff rows,
    plus DataFrames for each category.
    """
    if df_deliv.empty:
        return {"matched": 0, "missing": 0, "diff": 0,
                "missing_df": pd.DataFrame(), "diff_df": pd.DataFrame()}

    # Lowercase all columns
    df_deliv = df_deliv.copy()
    df_db    = df_db.copy()
    df_deliv.columns = [_snake(c) for c in df_deliv.columns]
    df_db.columns    = [c.lower()  for c in df_db.columns]

    # Only keep match + sync cols that exist in deliverable
    avail_match = [c for c in match_cols if c in df_deliv.columns and c in df_db.columns]
    avail_sync  = [c for c in sync_cols  if c in df_deliv.columns and c in df_db.columns]

    if not avail_match:
        return {"error": "No usable match columns", "matched": 0, "missing": 0, "diff": 0,
                "missing_df": pd.DataFrame(), "diff_df": pd.DataFrame()}

    # Normalise match key types
    for c in avail_match:
        if c in ("year", "month", "quarter"):
            df_deliv[c] = pd.to_numeric(df_deliv[c], errors="coerce")
            df_db[c]    = pd.to_numeric(df_db[c],    errors="coerce")
        else:
            df_deliv[c] = df_deliv[c].astype(str).str.strip().str.lower()
            df_db[c]    = df_db[c].astype(str).str.strip().str.lower()

    # Drop ID column from deliverable (not a comparison key)
    df_deliv = df_deliv[[c for c in df_deliv.columns if c != "id"]].copy()

    merged = pd.merge(df_deliv, df_db, on=avail_match,
                      how="left", suffixes=("_deliv", "_db"), indicator=True)

    missing_df = merged[merged["_merge"] == "left_only"].copy()
    common     = merged[merged["_merge"] == "both"].copy()

    diff_rows = []
    for _, row in common.iterrows():
        for col in avail_sync:
            dc = f"{col}_deliv" if f"{col}_deliv" in row.index else col
            bc = f"{col}_db"    if f"{col}_db"    in row.index else col
            if dc == bc:
                continue
            dv, bv = row.get(dc), row.get(bc)
            if pd.isna(dv) and pd.isna(bv):
                continue
            if pd.isna(dv) != pd.isna(bv):
                diff_rows.append({**{c: row[c] for c in avail_match},
                                   "column": col, "deliv": dv, "db": bv})
                continue
            try:
                if abs(float(dv) - float(bv)) > TOLERANCE:
                    diff_rows.append({**{c: row[c] for c in avail_match},
                                       "column": col, "deliv": dv, "db": bv})
            except (TypeError, ValueError):
                if str(dv).strip().lower() != str(bv).strip().lower():
                    diff_rows.append({**{c: row[c] for c in avail_match},
                                       "column": col, "deliv": dv, "db": bv})

    diff_df = pd.DataFrame(diff_rows)
    matched = len(common) - len(diff_df["column"].unique() if not diff_df.empty else [])

    return {
        "matched":    len(common) - (len(diff_df) if not diff_df.empty else 0),
        "missing":    len(missing_df),
        "diff":       len(diff_rows),
        "missing_df": missing_df[avail_match].drop_duplicates() if not missing_df.empty else pd.DataFrame(),
        "diff_df":    diff_df,
    }


def run() -> None:
    rows = []

    for deliv_file in sorted(DELIVERABLE_DIR.iterdir()):
        if deliv_file.suffix.lower() not in {".csv", ".xlsx", ".xls"}:
            continue

        slug  = _slug(deliv_file.name)
        pipe_file = _match_pipeline(slug)
        if pipe_file is None:
            print(f"[NO PIPELINE] {deliv_file.name}")
            rows.append({"deliverable": deliv_file.name, "table": "-", "status": "no_pipeline_found",
                         "matched": "-", "missing_in_db": "-", "value_diffs": "-", "notes": ""})
            continue

        code = pipe_file.read_text(encoding="utf-8")
        meta = _parse_pipeline_meta(code)
        if meta is None:
            print(f"[NO DB META]  {deliv_file.name}")
            rows.append({"deliverable": deliv_file.name, "table": "-", "status": "no_db_meta",
                         "matched": "-", "missing_in_db": "-", "value_diffs": "-", "notes": ""})
            continue

        table, db_name = meta["table"], meta["db"]

        try:
            df_deliv = _read_deliverable(deliv_file)
        except Exception as e:
            print(f"[READ ERR]    {deliv_file.name}: {e}")
            rows.append({"deliverable": deliv_file.name, "table": table, "status": "read_error",
                         "matched": "-", "missing_in_db": "-", "value_diffs": "-", "notes": str(e)[:120]})
            continue

        try:
            engine = get_engine(db_name)
            if meta["sql_file"]:
                query = meta["sql_file"].read_text(encoding="utf-8")
            else:
                query = f'SELECT * FROM "public"."{table}"'
            df_db = pd.read_sql(query, engine)
        except Exception as e:
            print(f"[DB ERR]      {deliv_file.name} ({table}): {e}")
            rows.append({"deliverable": deliv_file.name, "table": table, "status": "db_error",
                         "matched": "-", "missing_in_db": "-", "value_diffs": "-", "notes": str(e)[:120]})
            continue

        result = _compare(df_deliv, df_db, meta["match_cols"], meta["sync_cols"])

        if "error" in result:
            status = "compare_error"
            notes  = result["error"]
        elif result["missing"] == 0 and result["diff"] == 0:
            status = "OK"
            notes  = ""
        elif result["missing"] > 0 and result["diff"] == 0:
            status = "ROWS_MISSING"
            notes  = f"{result['missing']} rows in deliverable not found in DB"
        elif result["missing"] == 0 and result["diff"] > 0:
            status = "VALUE_DIFF"
            notes  = f"{result['diff']} value differences"
        else:
            status = "ROWS_MISSING+VALUE_DIFF"
            notes  = f"{result['missing']} missing + {result['diff']} value diffs"

        symbol = "✓" if status == "OK" else "✗"
        print(f"[{symbol}] {deliv_file.name[:65]:<65}  {status:<25}  matched={result.get('matched','-'):>5}  "
              f"missing={result['missing']:>4}  diffs={result['diff']:>4}")

        rows.append({
            "deliverable":   deliv_file.name,
            "table":         table,
            "status":        status,
            "matched":       result.get("matched", "-"),
            "missing_in_db": result["missing"],
            "value_diffs":   result["diff"],
            "notes":         notes,
        })

    out = ROOT / "db_deliverable_compare_report.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"\nReport written to {out}")
    ok    = sum(1 for r in rows if r["status"] == "OK")
    bad   = len(rows) - ok
    print(f"Summary: {ok} OK  |  {bad} need attention  |  {len(rows)} total")


if __name__ == "__main__":
    run()
