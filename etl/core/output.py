from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from etl.core.pipeline_logging import emit_event


def _to_db_col(name: str) -> str:
    """Convert any header to snake_case matching DB column convention."""
    s = str(name).strip().lower()
    s = s.replace("&", "and").replace("+", "and")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def round_for_output(df: pd.DataFrame, decimals: int = 3) -> pd.DataFrame:
    """
    Return a copy of df with numeric columns rounded to at most `decimals`
    decimal places. Non-numeric columns are left untouched.
    """
    if df is None or df.empty:
        return df
    numeric_cols = df.select_dtypes(include="number").columns
    if len(numeric_cols) == 0:
        return df
    return df.round({c: decimals for c in numeric_cols})


def write_deliverable_csv(
    df: pd.DataFrame,
    path: str | Path,
    *,
    decimals: int = 3,
    **kwargs: Any,
) -> None:
    """
    Write a deliverable CSV with:
    - numeric columns rounded to at most `decimals` decimal places
    - all column headers normalised to lowercase snake_case
      to match DB column names exactly
    """
    kwargs.setdefault("index", False)
    out = round_for_output(df, decimals=decimals).copy()
    out.columns = [_to_db_col(c) for c in out.columns]
    out.to_csv(path, **kwargs)
    emit_event(
        stage="deliverable",
        event="deliverable_written",
        status="success",
        path=str(path),
        rows_written=len(out),
        columns=list(out.columns),
        decimals=decimals,
    )
