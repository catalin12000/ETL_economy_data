from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


def _pick_index_column(columns: list[str]) -> str:
    # Prefer the direct monthly index column and avoid percentage-change columns.
    lowered = [c.lower() for c in columns]
    for raw, low in zip(columns, lowered):
        if "index" in low and "%" not in low and "change" not in low:
            return raw
    # Fallback: first non-period column.
    if len(columns) > 1:
        return columns[1]
    raise RuntimeError("Could not identify index column in construction index CSV.")


def extract_construction_index(csv_path: Path) -> pd.DataFrame:
    """
    Extract CYSTAT monthly construction index as:
      Year, Month, Index
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [str(c).replace("\ufeff", "").replace('"', "").strip() for c in df.columns]

    if df.columns.empty:
        raise RuntimeError("Empty CSV: no columns found.")

    period_col = df.columns[0]
    index_col = _pick_index_column(list(df.columns))

    period = df[period_col].astype(str).str.strip().str.replace('"', "", regex=False)
    out = pd.DataFrame(
        {
            "year": pd.to_numeric(period.str.extract(r"^(\d{4})M\d{2}$")[0], errors="coerce"),
            "month": pd.to_numeric(period.str.extract(r"^\d{4}M(\d{2})$")[0], errors="coerce"),
            "index": pd.to_numeric(df[index_col], errors="coerce"),
        }
    )

    out = out.dropna(subset=["year", "month", "index"]).copy()
    out["year"] = out["year"].astype(int)
    out["month"] = out["month"].astype(int)
    out["index"] = out["index"].round(2)
    out = out.sort_values(["year", "month"]).reset_index(drop=True)

    if out.empty:
        raise RuntimeError("No rows extracted from construction index source.")

    return out
