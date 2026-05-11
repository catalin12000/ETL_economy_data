from __future__ import annotations

import pandas as pd

NULL_TOKENS = {
    "",
    "...",
    "..",
    "…",
    "—",
    "-",
    "–",
    "u",
    "U",
    "n/a",
    "N/A",
    "na",
    "NA",
    ":",
}


def _normalize_scalar(x):
    if x is None:
        return pd.NA
    if isinstance(x, float) and pd.isna(x):
        return pd.NA
    if isinstance(x, str) and x.strip() in NULL_TOKENS:
        return pd.NA
    return x


def normalize_nulls(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    """
    Replace placeholder strings ('...', 'u', 'N/A', ':' etc.) with pd.NA.
    Operates in place on the given columns (default: all columns).
    """
    cols = columns if columns is not None else list(df.columns)
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(_normalize_scalar)
    return df
