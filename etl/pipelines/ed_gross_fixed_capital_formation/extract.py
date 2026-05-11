from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


def _engine_for(path: Path) -> str:
    with open(path, "rb") as f:
        sig = f.read(2)
        if sig == b"PK":
            return "openpyxl"
    return "xlrd"


def _parse_sheet(xls_path: Path, sheet_name: str, seasonality: str) -> pd.DataFrame:
    df = pd.read_excel(xls_path, sheet_name=sheet_name, header=None, engine=_engine_for(xls_path))
    records: list[dict[str, object]] = []

    for i in range(7, len(df)):
        period = str(df.iat[i, 0]).strip()
        m = re.match(r"(\d{4})-Q([1-4])$", period)
        if not m:
            continue

        year = int(m.group(1))
        quarter = int(m.group(2))

        vals = [pd.to_numeric(df.iat[i, c], errors="coerce") for c in range(1, 9)]
        if all(pd.isna(v) for v in vals):
            continue

        records.append(
            {
                "Year": year,
                "Quarter": quarter,
                "Seasonally": seasonality,
                "Total gross fixed capital formation": pd.NA if pd.isna(vals[0]) else int(round(float(vals[0]))),
                "Dwellings": pd.NA if pd.isna(vals[1]) else int(round(float(vals[1]))),
                "Other buildings and structures": pd.NA if pd.isna(vals[2]) else int(round(float(vals[2]))),
                "Cultivated biological resources": pd.NA if pd.isna(vals[3]) else int(round(float(vals[3]))),
                "Transport equipment": pd.NA if pd.isna(vals[4]) else int(round(float(vals[4]))),
                "Information Communication Technology (ICT) equipment": pd.NA
                if pd.isna(vals[5])
                else int(round(float(vals[5]))),
                "Other machinery and equipment +weapon systems": pd.NA
                if pd.isna(vals[6])
                else int(round(float(vals[6]))),
                "Intellectual property products": pd.NA if pd.isna(vals[7]) else int(round(float(vals[7]))),
            }
        )

    return pd.DataFrame(records)


def extract_gfcf(xls_path: Path) -> pd.DataFrame:
    """
    Extract SEL81 table 02 from both sheets:
      - NSA -> Unadjusted
      - SA  -> Adjusted
    Output columns:
      Year, Quarter, Seasonally, Total gross fixed capital formation, Dwellings,
      Other buildings and structures, Cultivated biological resources,
      Transport equipment, Information Communication Technology (ICT) equipment,
      Other machinery and equipment +weapon systems, Intellectual property products
    """
    xls_path = Path(xls_path)

    df_unadj = _parse_sheet(xls_path, "NSA", "Unadjusted")
    df_adj = _parse_sheet(xls_path, "SA", "Adjusted")
    out = pd.concat([df_unadj, df_adj], ignore_index=True)

    if out.empty:
        raise RuntimeError("No rows extracted from gross fixed capital formation source.")

    season_order = {"Unadjusted": 0, "Adjusted": 1}
    out["_season_order"] = out["Seasonally"].map(season_order).fillna(9).astype(int)
    out = out.sort_values(["Year", "Quarter", "_season_order"]).drop(columns=["_season_order"]).reset_index(drop=True)
    return out
