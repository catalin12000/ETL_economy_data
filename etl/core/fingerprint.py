# etl/core/fingerprint.py
from __future__ import annotations

import hashlib
import pandas as pd


def dataframe_sha256(
    df: pd.DataFrame,
    *,
    sort_cols: list[str] | None = None,
    float_round: int = 6,
) -> str:
    """
    Stable hash of the DATA (not formatting):
    - sort rows
    - sort columns
    - normalize NaN
    - round floats
    - hash CSV bytes
    """
    x = df.copy()

    # Ensure stable column order
    x = x.reindex(sorted(x.columns), axis=1)

    # Stable row order
    if sort_cols:
        x = x.sort_values(sort_cols).reset_index(drop=True)
    else:
        x = x.sort_values(list(x.columns)).reset_index(drop=True)

    # Normalize missing
    x = x.where(pd.notna(x), None)

    # Round floats to avoid tiny binary differences
    for c in x.columns:
        if pd.api.types.is_float_dtype(x[c]):
            x[c] = x[c].round(float_round)

    csv_bytes = x.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()
