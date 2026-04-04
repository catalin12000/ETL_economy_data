from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


GROUP_ORDER = {
    "Resources": 0,
    "Uses": 1,
    "Balancing items": 2,
}


def _clean_text(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).replace("\t", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _parse_year(v) -> Optional[int]:
    s = _clean_text(v).replace("*", "")
    if not s:
        return None
    try:
        y = int(float(s))
        if 1900 <= y <= 2100:
            return y
    except Exception:
        return None
    return None


def _parse_quarter(v) -> Optional[int]:
    s = _clean_text(v).upper()
    m = re.search(r"Q([1-4])", s)
    if not m:
        return None
    return int(m.group(1))


def _to_num(v) -> Optional[float]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if not s or s in {"...", "…", "-", "—"}:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def _is_parent_code(prev_code: str, current_code: str) -> bool:
    prev_code = _clean_text(prev_code)
    current_code = _clean_text(current_code)
    if not prev_code or not current_code or prev_code == current_code:
        return False

    if "/" in prev_code:
        parts = [_clean_text(p) for p in prev_code.split("/") if _clean_text(p)]
        for p in parts:
            if current_code == p or current_code.startswith(p):
                return True
        return False

    return current_code.startswith(prev_code)


def _extract_time_columns(df: pd.DataFrame, year_row: int = 5, quarter_row: int = 6) -> List[Tuple[int, int, int]]:
    """
    ELSTAT layout stores Year only on the first quarter column of each year block.
    We carry-forward the last seen year across subsequent Q2/Q3/Q4 columns.
    """
    out: List[Tuple[int, int, int]] = []
    current_year: Optional[int] = None
    for col in range(2, df.shape[1]):
        y = _parse_year(df.iat[year_row, col])
        if y is not None:
            current_year = y
        q = _parse_quarter(df.iat[quarter_row, col])
        if current_year is None or q is None:
            continue
        out.append((col, current_year, q))
    return out


def extract_housing_finances(xls_path: Path) -> pd.DataFrame:
    """
    Extracts ELSTAT SEL95 wide table to long format.
    Output includes both deliverable-facing columns and DB-matching helper columns.
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine="openpyxl")

    time_cols = _extract_time_columns(df)
    if not time_cols:
        raise RuntimeError("No year/quarter columns found in SEL95 workbook.")

    # 1) Collect row metadata with group context
    rows_meta: List[dict] = []
    current_group: Optional[str] = None
    category_counter = 0
    for row_idx in range(5, df.shape[0]):
        code = _clean_text(df.iat[row_idx, 0])
        label = _clean_text(df.iat[row_idx, 1])

        if code in GROUP_ORDER and not label:
            current_group = code
            category_counter = 0
            continue

        if not current_group:
            continue
        if not code or not label:
            continue
        if code.startswith("*"):
            continue

        rows_meta.append(
            {
                "group": current_group,
                "code": code,
                "label": label,
                "order": category_counter,
                "row_idx": row_idx,
            }
        )
        category_counter += 1

    # 2) Build records aligned to DB schema behavior
    records = []
    standalone_child_codes = {"P.51C"}

    for i, row in enumerate(rows_meta):
        code = row["code"]
        label = row["label"]
        grp = row["group"]
        row_idx = row["row_idx"]

        # Find nearest parent within same group
        parent_label = None
        parent_code_len = -1
        for j in range(i):
            prev = rows_meta[j]
            if prev["group"] != grp:
                continue
            if _is_parent_code(prev["code"], code):
                code_len = len(prev["code"])
                if code_len > parent_code_len:
                    parent_code_len = code_len
                    parent_label = prev["label"]

        # Detect whether this row is a pure parent aggregate (has children)
        has_children = False
        for k in range(i + 1, len(rows_meta)):
            nxt = rows_meta[k]
            if nxt["group"] != grp:
                continue
            if _is_parent_code(code, nxt["code"]):
                has_children = True
                break

        # DB does not store most parent aggregate rows as standalone entries when children exist.
        # Keep slash-coded aggregates (e.g., B.2g/B.3g) because DB stores them as real categories.
        if has_children and not label.lower().startswith("of which:") and "/" not in code:
            continue

        # DB stores "of which: ..." as top-level category (no sub_category).
        if label.lower().startswith("of which:"):
            parent_label = None

        # Some child-like codes are standalone categories in DB.
        if code in standalone_child_codes:
            parent_label = None

        if parent_label:
            db_category = parent_label
            db_sub_category = label
        else:
            db_category = label
            db_sub_category = pd.NA

        for col_idx, year, quarter in time_cols:
            val = _to_num(df.iat[row_idx, col_idx])
            if val is None:
                continue
            records.append(
                {
                    "Group": grp,
                    "Category": label,
                    "Year": int(year),
                    "Quarter": int(quarter),
                    "Value (mln)": val,
                    "DB_Category": db_category,
                    "DB_Sub_Category": db_sub_category,
                    "Group_Order": GROUP_ORDER.get(grp, 99),
                    "Category_Order": row["order"],
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No rows extracted from SEL95 workbook.")

    # ELSTAT sometimes publishes the next quarter as a full zero placeholder
    # (e.g., all series for 2025-Q4 equal 0 while data is available only to Q3).
    # Drop any quarter block where all values are exactly zero.
    quarter_stats = (
        out.groupby(["Year", "Quarter"])["Value (mln)"]
        .agg(total_rows="size", non_zero=lambda s: (s.fillna(0) != 0).sum())
        .reset_index()
    )
    zero_quarters = quarter_stats[quarter_stats["non_zero"] == 0][["Year", "Quarter"]]
    if not zero_quarters.empty:
        out = out.merge(zero_quarters.assign(_drop=1), on=["Year", "Quarter"], how="left")
        out = out[out["_drop"].isna()].drop(columns=["_drop"])

    out = out.sort_values(
        ["Year", "Quarter", "Group_Order", "Category_Order", "Category"],
        na_position="last",
    ).reset_index(drop=True)
    return out
