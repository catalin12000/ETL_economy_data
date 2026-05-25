"""
Audit: for every DB-wired pipeline, compare the deliverable that was actually
uploaded in March against the live table, and flag columns that ended up NULL
in the DB even though the uploaded CSV had data.

Source of truth for uploaded CSVs:
    uploaded_march_actual/deliverable_march/

Output:
    db_null_audit_report.csv   (Table, Column, Issue, Proposed Fix, Detail)
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

PIPELINES_DIR = ROOT / "etl" / "pipelines"
UPLOADED_DIR = ROOT / "uploaded_march_actual" / "deliverable_march"
RECENT_CUTOFF = "2026-01-01"


def _snake(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("&", "and").replace("+", "and")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _table_info(pipeline_code: str) -> Optional[tuple[str, str, list[str], list[str]]]:
    m_table = re.search(r'table_name\s*=\s*["\']([^"\']+)["\']', pipeline_code)
    m_db = re.search(r'db_name\s*=\s*["\']([^"\']+)["\']', pipeline_code)
    if not m_table or not m_db:
        return None
    table = m_table.group(1)
    db = m_db.group(1)
    m_match = re.search(r'match_cols\s*=\s*\[([^\]]+)\]', pipeline_code)
    m_sync = re.search(r'sync_cols\s*=\s*\[([^\]]+)\]', pipeline_code, re.DOTALL)
    match_cols = re.findall(r'["\']([^"\']+)["\']', m_match.group(1)) if m_match else []
    sync_cols = re.findall(r'["\']([^"\']+)["\']', m_sync.group(1)) if m_sync else []
    return table, db, match_cols, sync_cols


def _slug_from_uploaded(name: str) -> str:
    s = name
    s = re.sub(r"_(March|April|May|June|July|August|September|October|November|December|January|February)_\d{4}.*$", "", s, flags=re.I)
    s = re.sub(r"\.(csv|xlsx|xls)$", "", s, flags=re.I)
    return s.lower()


def _uploaded_for(pipeline_id: str, uploaded_files: list[Path]) -> Optional[Path]:
    """Match an uploaded file to a pipeline id (filenames sometimes drop the index prefix)."""
    pid = pipeline_id.lower()
    pid_no_prefix = re.sub(r"^(cy|ed)_\d+_", lambda m: m.group(1) + "_", pid)
    pid_no_prefix_at_all = re.sub(r"^(cy|ed)_\d+_", "", pid)

    candidates = []
    for f in uploaded_files:
        slug = _slug_from_uploaded(f.name)
        # Exact match
        if slug == pid:
            return f
        if slug == pid_no_prefix:
            candidates.append((0, f))
            continue
        # Pipeline id appears as substring
        if pid_no_prefix_at_all and pid_no_prefix_at_all in slug:
            candidates.append((1, f))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return None


def _date_filter_clause(engine, table: str) -> tuple[str, str]:
    with engine.connect() as conn:
        cols = pd.read_sql(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=:t"
            ),
            conn,
            params={"t": table},
        )["column_name"].tolist()
    for ts in ("modified_at", "created_at", "effective_dt"):
        if ts in cols:
            return f'WHERE "{ts}" >= \'{RECENT_CUTOFF}\'', ts
    return "", ""


def audit() -> list[dict]:
    issues: list[dict] = []
    uploaded_files = [
        f for f in UPLOADED_DIR.iterdir()
        if f.suffix.lower() in {".csv", ".xlsx", ".xls"}
    ]
    print(f"Uploaded files found: {len(uploaded_files)}")

    seen_tables: set[tuple[str, str]] = set()

    for pdir in sorted(PIPELINES_DIR.iterdir()):
        if not pdir.is_dir() or pdir.name.startswith("__"):
            continue
        pipe_file = pdir / "pipeline.py"
        if not pipe_file.exists():
            continue
        info = _table_info(pipe_file.read_text(encoding="utf-8"))
        if info is None:
            continue
        table, db, _match, _sync = info

        key = (db, table)
        first_pipeline_for_table = key not in seen_tables
        seen_tables.add(key)

        uploaded_path = _uploaded_for(pdir.name, uploaded_files)
        if uploaded_path is None:
            if first_pipeline_for_table:
                issues.append({
                    "Table": table,
                    "Column": "*",
                    "Issue": "Pipeline has DB compare wired but no matching file in uploaded zip",
                    "Proposed Fix": "Include this pipeline's deliverable in next upload",
                    "Detail": pdir.name,
                })
            continue

        # Read uploaded CSV / xlsx
        try:
            if uploaded_path.suffix.lower() == ".csv":
                df_up = pd.read_csv(uploaded_path)
            else:
                df_up = pd.read_excel(uploaded_path)
        except Exception as e:
            issues.append({
                "Table": table,
                "Column": "*",
                "Issue": f"Cannot read uploaded file {uploaded_path.name}: {e}",
                "Proposed Fix": "Inspect file manually",
                "Detail": pdir.name,
            })
            continue

        up_snake = {_snake(c): c for c in df_up.columns}

        try:
            engine = get_engine(db)
            where, ts_col = _date_filter_clause(engine, table)
            df_db = pd.read_sql(
                f'SELECT * FROM "public"."{table}" {where} LIMIT 50000', engine
            )
        except Exception as e:
            issues.append({
                "Table": table,
                "Column": "*",
                "Issue": f"DB query failed: {e}",
                "Proposed Fix": "Check connection / table",
                "Detail": db,
            })
            continue

        if df_db.empty:
            if first_pipeline_for_table:
                issues.append({
                    "Table": table,
                    "Column": "*",
                    "Issue": f"No DB rows since {RECENT_CUTOFF} ({ts_col or 'no timestamp col'})",
                    "Proposed Fix": "Confirm upload happened",
                    "Detail": f"uploaded='{uploaded_path.name}'  rows_in_csv={len(df_up)}",
                })
            continue

        skip_cols = {"id", "effective_dt", "modified_at", "created_at"}
        for col in df_db.columns:
            if col in skip_cols:
                continue
            null_count = int(df_db[col].isna().sum())
            total = len(df_db)
            if total == 0:
                continue
            null_pct = null_count / total
            if null_pct < 0.3:
                continue

            up_col = up_snake.get(col)
            if up_col is not None:
                non_null_in_csv = df_up[up_col].notna().sum()
                if non_null_in_csv > 0:
                    issues.append({
                        "Table": table,
                        "Column": col,
                        "Issue": f"DB NULL on {null_pct:.0%} of recent rows ({null_count}/{total}); uploaded CSV had {non_null_in_csv} non-null values for this column",
                        "Proposed Fix": "Upload script likely dropped this column — investigate mapping",
                        "Detail": f"uploaded_header='{up_col}'  pipeline='{pdir.name}'",
                    })
                else:
                    issues.append({
                        "Table": table,
                        "Column": col,
                        "Issue": f"DB NULL on {null_pct:.0%} of rows; uploaded CSV column also empty",
                        "Proposed Fix": "Check extractor — source data missing for this column",
                        "Detail": f"uploaded_header='{up_col}'  pipeline='{pdir.name}'",
                    })
            else:
                issues.append({
                    "Table": table,
                    "Column": col,
                    "Issue": f"DB NULL on {null_pct:.0%} of rows; column not in uploaded CSV at all",
                    "Proposed Fix": "Add column to deliverable (header should snake_case to DB column name)",
                    "Detail": f"uploaded_headers={list(df_up.columns)[:8]}  pipeline='{pdir.name}'",
                })

    return issues


if __name__ == "__main__":
    rows = audit()
    out = ROOT / "db_null_audit_report.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"Wrote {len(rows)} issues to {out}")
