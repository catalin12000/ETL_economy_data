from __future__ import annotations

from pathlib import Path

import pandas as pd


def _extract_single_index(xls_path: Path, value_name: str) -> pd.DataFrame:
    df = pd.read_excel(xls_path, sheet_name="TABLE 1", header=None)

    records: list[dict[str, object]] = []
    current_year: int | None = None
    current_month = 0

    for row_idx in range(16, len(df)):
        raw_label = df.iat[row_idx, 0] if df.shape[1] > 0 else None
        raw_value = pd.to_numeric(df.iat[row_idx, 1], errors="coerce") if df.shape[1] > 1 else pd.NA

        label = "" if pd.isna(raw_label) else str(raw_label).strip()
        if label.lower().startswith("*provisional data") or label.lower().startswith("source"):
            break
        if pd.isna(raw_value):
            continue

        if label and label.split()[0].isdigit():
            current_year = int(label.split()[0])
            current_month = 1
        else:
            current_month += 1

        if current_year is None or not (1 <= current_month <= 12):
            continue

        records.append(
            {
                "Year": current_year,
                "Month": current_month,
                value_name: float(raw_value),
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError(f"No rows extracted from {Path(xls_path).name}.")
    return out


def extract_wholesale_trade_indices(turnover_path: Path, volume_path: Path) -> pd.DataFrame:
    turnover_df = _extract_single_index(Path(turnover_path), "Turnover Index")
    volume_df = _extract_single_index(Path(volume_path), "Volume Index")

    out = turnover_df.merge(volume_df, on=["Year", "Month"], how="inner", validate="one_to_one")
    out = out.sort_values(["Year", "Month"]).reset_index(drop=True)
    if out.empty:
        raise RuntimeError("No merged wholesale trade rows extracted.")
    return out
