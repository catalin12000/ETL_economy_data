"""
Fix deliverable target_cols to use exact DB column names.

Strategy: only touch the deliverable section of each pipeline —
specifically target_cols lists and the rename dicts that map DB-lowercase
back to display-case. Leave key_cols, sort_values, dropna and anything
that works with the extracted DataFrame (which stays Title Case internally).

Run:  python scripts/fix_deliverable_headers.py [--dry-run]
"""
from __future__ import annotations
import argparse, re, sys
from pathlib import Path
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from etl.core.database import get_engine  # noqa: E402

SKIP = {"id", "effective_dt", "modified_at", "created_at", "modified_dt"}


def _get_db_cols(table: str, db: str) -> list[str]:
    try:
        e = get_engine(db)
        with e.connect() as c:
            return [
                r for r in pd.read_sql(
                    text(f"SELECT column_name FROM information_schema.columns "
                         f"WHERE table_schema='public' AND table_name='{table}' "
                         f"ORDER BY ordinal_position"),
                    c,
                )["column_name"]
                if r not in SKIP
            ]
    except Exception:
        return []


def _get_meta(code: str):
    m = (re.search(r'table_name\s*=\s*["\'](\w+)["\'],', code)
         or re.search(r'DB_TABLE_NAME\s*=\s*["\'](\w+)["\']', code))
    if not m and "table_name=self.pipeline_id" in code:
        pid = re.search(r'pipeline_id\s*=\s*["\'](\w+)["\']', code)
        if pid:
            class F:
                def group(self, n): return pid.group(1)
            m = F()
    db = re.search(r'db_name\s*=\s*["\'](\w+)["\']', code)
    return (m.group(1) if m else None), (db.group(1) if db else None)


def _snake(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace("&", "and").replace("+", "and")
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def fix_file(path: Path, db_cols: list[str], dry_run: bool = False) -> tuple[bool, list[str]]:
    """
    Only modify:
    1. target_cols lists — replace Title/Camel-case header strings with
       the exact DB column name (snake_case lowercase).
    2. Rename-back dicts that map `"db_col": "DisplayCol"` → keep as
       `"db_col": "db_col"` (i.e., stop renaming to display case).

    We do NOT touch:
    - compare_and_update_csv(key_cols=...) — those use extracted df columns
    - sort_values, dropna, df["Col"] — same
    - Any code before the deliverable section
    """
    code = path.read_text(encoding="utf-8")
    original = code
    changes = []

    # Build a mapping: Title/Camel variants → exact DB name
    col_map: dict[str, str] = {}
    for col in db_cols:
        # title variant: "Year", "Month", "Gdp Growth"
        title = " ".join(w.capitalize() for w in col.split("_"))
        # camel_underscore: "Year_Over_Year"
        camel = "_".join(w.capitalize() for w in col.split("_"))
        for variant in {title, camel, col.capitalize()}:
            if variant != col:
                col_map[variant] = col

    # 1. Fix target_cols list items
    # Match the list literal that defines target_cols
    # Pattern: [  "SomeHeader",  "AnotherHeader"  ]
    # We only replace INSIDE target_cols = [...] blocks
    def fix_target_cols(m: re.Match) -> str:
        block = m.group(0)
        for variant, db_col in col_map.items():
            old = f'"{variant}"'
            new = f'"{db_col}"'
            if old in block and old != new:
                block = block.replace(old, new)
                changes.append(f'  target_cols: {old!r} -> {new!r}')
            old2 = f"'{variant}'"
            new2 = f"'{db_col}'"
            if old2 in block and old2 != new2:
                block = block.replace(old2, new2)
                changes.append(f'  target_cols: {old2!r} -> {new2!r}')
        return block

    code = re.sub(
        r'target_cols\s*=\s*\[[^\]]+\]',
        fix_target_cols,
        code,
        flags=re.DOTALL,
    )

    # 2. Fix rename-back dicts: {"db_col": "DisplayCol"} → {"db_col": "db_col"}
    # These appear in deliverable shape_output / rename(columns={...}) sections
    # Only match where the KEY is already a DB col name (lowercase) and the
    # VALUE is a different display variant.
    def fix_rename_val(m: re.Match) -> str:
        key = m.group(1)   # e.g. "year"
        val = m.group(2)   # e.g. "Year"
        # Only fix if key is a known DB col and val is a variant of it
        if key in db_cols and _snake(val) == key and val != key:
            new = f'"{key}": "{key}"'
            changes.append(f'  rename: "{key}": "{val}" -> {new}')
            return new
        return m.group(0)

    code = re.sub(
        r'"(\w+)"\s*:\s*"([^"]+)"',
        fix_rename_val,
        code,
    )

    changed = code != original
    if changed and not dry_run:
        path.write_text(code, encoding="utf-8")

    return changed, changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    pipelines = ROOT / "etl" / "pipelines"
    total_changed = 0

    for pdir in sorted(pipelines.iterdir()):
        if not pdir.is_dir() or pdir.name.startswith("__"):
            continue
        pf = pdir / "pipeline.py"
        if not pf.exists():
            continue
        code = pf.read_text(encoding="utf-8")
        table, db = _get_meta(code)
        db_cols = _get_db_cols(table, db) if table and db else []
        if not db_cols:
            continue

        changed, changes = fix_file(pf, db_cols, dry_run=args.dry_run)
        if changed:
            total_changed += 1
            tag = "[DRY]" if args.dry_run else "[OK] "
            print(f"{tag} {pdir.name}")
            for c in set(changes[:8]):
                print(c)

    print(f"\n{'[DRY-RUN] ' if args.dry_run else ''}Changed {total_changed} files")


if __name__ == "__main__":
    main()
