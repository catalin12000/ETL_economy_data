# etl/core/fingerprint.py
"""
Fingerprinting helpers for the freshness check (the "should I skip this run?" question).

Different Sources need different skip signals:

  SourceType        Skip signal          Helper
  -----------       ------------------   ----------------------------
  static_file       file_sha256          should_skip_static_file()
  dynamic_file      latest_period_seen   should_skip_dynamic_file()
  api               data_sha256          should_skip_api()
  scraped           latest_period_seen   should_skip_scraped()

The right helper is picked by the Pipeline's class attribute `source_type`.
Each helper returns True when the Pipeline can safely skip the run.
"""
from __future__ import annotations

import hashlib
from typing import Literal

import pandas as pd


SourceType = Literal["static_file", "dynamic_file", "api", "scraped"]


# --------------------------------------------------------------------------- #
# Hash primitives                                                              #
# --------------------------------------------------------------------------- #
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
    x = x.reindex(sorted(x.columns), axis=1)

    if sort_cols:
        x = x.sort_values(sort_cols).reset_index(drop=True)
    else:
        x = x.sort_values(list(x.columns)).reset_index(drop=True)

    x = x.where(pd.notna(x), None)

    for c in x.columns:
        if pd.api.types.is_float_dtype(x[c]):
            x[c] = x[c].round(float_round)

    csv_bytes = x.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


# --------------------------------------------------------------------------- #
# Per source-type skip helpers                                                 #
# --------------------------------------------------------------------------- #
def should_skip_static_file(state: dict, file_hash: str) -> bool:
    """
    Static-file sources (e.g. Bank of Greece — same URL, same filename,
    content updates in place). Skip when the bytes are identical to the
    previous run.
    """
    prev = state.get("file_sha256")
    return bool(prev) and bool(file_hash) and prev == file_hash


def should_skip_dynamic_file(state: dict, period: str | None) -> bool:
    """
    Dynamic-file sources (e.g. ELSTAT — URL changes every publication).
    Skip when the publication period matches the previous run. `period`
    is whatever string the resolver produced (e.g. "2026-M03", "2026-Q1").
    """
    prev = state.get("latest_period_seen")
    return bool(prev) and bool(period) and prev == period


def should_skip_api(state: dict, data_hash: str) -> bool:
    """
    API sources (e.g. CYSTAT, Eurostat). API response bytes vary even when
    the data is identical — only the hash of the extracted DataFrame is
    a reliable skip signal.
    """
    prev = state.get("data_sha256")
    return bool(prev) and bool(data_hash) and prev == data_hash


def should_skip_scraped(state: dict, period: str | None) -> bool:
    """
    Scraped sources (e.g. DLS, migration.gov.gr — file links discovered
    from an index page, one file per period). Same semantics as
    dynamic_file: skip when the period hasn't advanced.
    """
    return should_skip_dynamic_file(state, period)


# --------------------------------------------------------------------------- #
# Generic dispatch                                                             #
# --------------------------------------------------------------------------- #
def should_skip(
    source_type: SourceType,
    state: dict,
    *,
    file_hash: str | None = None,
    data_hash: str | None = None,
    period: str | None = None,
) -> bool:
    """
    Dispatch to the right skip check for a Pipeline's source_type.
    Pass only the signal(s) relevant to that source_type; unused kwargs
    are ignored.
    """
    if source_type == "static_file":
        return should_skip_static_file(state, file_hash or "")
    if source_type == "dynamic_file":
        return should_skip_dynamic_file(state, period)
    if source_type == "api":
        return should_skip_api(state, data_hash or "")
    if source_type == "scraped":
        return should_skip_scraped(state, period)
    raise ValueError(f"Unknown source_type {source_type!r}")
