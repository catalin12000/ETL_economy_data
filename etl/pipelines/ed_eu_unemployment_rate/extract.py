# etl/pipelines/ed_eu_unemployment_rate/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def extract_eu_unemployment(csv_path: Path) -> pd.DataFrame:
    """
    Extracts EU Unemployment data from Eurostat CSV.
    Matches internal DB columns for sync.
    """
    df = pd.read_csv(csv_path)

    # Mapping Eurostat GEO codes to requested names
    GEO_MAP = {
        "EU27_2020": "European Union - 27 countries (from 2020)",
        "EA20": "Euro area – 20 countries (from 2023)",
        "EA21": "Euro area – 21 countries (from 2025)",
        "EL": "Greece",
        "CY": "Cyprus",
        "RO": "Romania"
    }

    # 1. Parse Year and Month from TIME_PERIOD (e.g., "2025-01")
    df[["year", "month"]] = df["TIME_PERIOD"].str.split("-", expand=True)
    df["year"] = pd.to_numeric(df["year"])
    df["month"] = pd.to_numeric(df["month"])

    # 2. Map Geopolitical Entity
    df["geopolitical_entity"] = df["geo"].map(lambda x: GEO_MAP.get(x, x))

    # 3. Pivot Units to Columns
    df_pivot = df.pivot_table(
        index=["geopolitical_entity", "year", "month"],
        columns="unit",
        values="OBS_VALUE",
        aggfunc="first"
    ).reset_index()

    # 4. Rename Columns to match DB
    df_pivot = df_pivot.rename(columns={
        "PC_ACT": "adjusted_unemployment_rate",
        "THS_PER": "adjusted_unemployed_000s"
    })

    # 5. Ensure all requested columns exist
    for col in ["adjusted_unemployment_rate", "adjusted_unemployed_000s"]:
        if col not in df_pivot.columns:
            df_pivot[col] = pd.NA

    # 6. Reorder and Clean
    cols = ["geopolitical_entity", "year", "month", "adjusted_unemployed_000s", "adjusted_unemployment_rate"]
    df_out = df_pivot[cols].copy()

    # Rounding
    df_out["adjusted_unemployment_rate"] = pd.to_numeric(df_out["adjusted_unemployment_rate"], errors="coerce").round(1)
    df_out["adjusted_unemployed_000s"] = pd.to_numeric(df_out["adjusted_unemployed_000s"], errors="coerce").round(0)

    df_out = df_out.sort_values(["year", "month", "geopolitical_entity"]).reset_index(drop=True)
    
    return df_out