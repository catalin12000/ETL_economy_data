from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


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
    Write a deliverable CSV with numeric columns rounded to at most
    `decimals` decimal places. `index=False` is set by default.
    """
    kwargs.setdefault("index", False)
    round_for_output(df, decimals=decimals).to_csv(path, **kwargs)
