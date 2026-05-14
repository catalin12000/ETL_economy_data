from __future__ import annotations

from pathlib import Path

import pandas as pd


TARGET_COLUMNS = [
    "Year",
    "Quarter",
    "Mining and Quarrying",
    "Manufacturing",
    "Electricity, Gas, Steam and Air Conditioning Supply",
    "Water Supply, Sewerage, Waste Management and Remediation Activities",
    "Construction",
    "Wholesale and Retal Trade, Repair of Motor Vehicles and Motorcycles",
    "Transportation and Storage",
    "Accommodation and Food Service Activities",
    "Information and Communication",
    "Professional, Scientific and Technical Activities",
    "Administrative and Support Service Activities",
]


def extract_wage_growth_index(xls_path: Path) -> pd.DataFrame:
    df = pd.read_excel(xls_path, sheet_name="TABLE 3", header=None)

    records: list[dict[str, object]] = []
    current_year: int | None = None

    for row_idx in range(13, len(df)):
        raw_year = df.iat[row_idx, 0]
        raw_quarter = df.iat[row_idx, 1]

        if pd.notna(raw_year):
            year_text = str(raw_year).strip()
            if year_text.lower().startswith("note"):
                break
            current_year = int(float(raw_year))

        quarter_text = "" if pd.isna(raw_quarter) else str(raw_quarter).strip().upper()
        quarter = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(quarter_text)
        if current_year is None or quarter is None:
            continue

        values = {
            "Mining and Quarrying": pd.to_numeric(df.iat[row_idx, 2], errors="coerce"),
            "Manufacturing": pd.to_numeric(df.iat[row_idx, 3], errors="coerce"),
            "Electricity, Gas, Steam and Air Conditioning Supply": pd.to_numeric(df.iat[row_idx, 4], errors="coerce"),
            "Water Supply, Sewerage, Waste Management and Remediation Activities": pd.to_numeric(df.iat[row_idx, 5], errors="coerce"),
            "Construction": pd.to_numeric(df.iat[row_idx, 7], errors="coerce"),
            "Wholesale and Retal Trade, Repair of Motor Vehicles and Motorcycles": pd.to_numeric(df.iat[row_idx, 8], errors="coerce"),
            "Transportation and Storage": pd.to_numeric(df.iat[row_idx, 9], errors="coerce"),
            "Accommodation and Food Service Activities": pd.to_numeric(df.iat[row_idx, 10], errors="coerce"),
            "Information and Communication": pd.to_numeric(df.iat[row_idx, 11], errors="coerce"),
            "Professional, Scientific and Technical Activities": pd.to_numeric(df.iat[row_idx, 13], errors="coerce"),
            "Administrative and Support Service Activities": pd.to_numeric(df.iat[row_idx, 14], errors="coerce"),
        }

        if all(pd.isna(v) for v in values.values()):
            continue

        row = {
            "Year": current_year,
            "Quarter": quarter,
        }
        row.update({k: float(v) if pd.notna(v) else pd.NA for k, v in values.items()})
        records.append(row)

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No wage growth index rows extracted from TABLE 3.")

    return out[TARGET_COLUMNS]
