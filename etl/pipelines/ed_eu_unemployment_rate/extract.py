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
        "EU27_2020": "European Union (EU6-1958, EU9-1973, EU10-1981, EU12-1986, EU15-1995, EU25-2004, EU27-2007, EU28-2013, EU27-2020)",
        "EA20": "Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)",
        "EL": "Greece",
        "CY": "Cyprus",
        "RO": "Romania"
    }

    # 1. Parse Year and Month from TIME_PERIOD (e.g., "2025-01")
    df[["Year", "Month"]] = df["TIME_PERIOD"].str.split("-", expand=True)
    df["Year"] = pd.to_numeric(df["Year"])
    df["Month"] = pd.to_numeric(df["Month"])

    # 2. Map Geopolitical Entity
    df["Geopolitical Entity"] = df["geo"].map(lambda x: GEO_MAP.get(x, x))

    # 3. Pivot Units to Columns
    df_pivot = df.pivot_table(
        index=["Geopolitical Entity", "Year", "Month"],
        columns="unit",
        values="OBS_VALUE",
        aggfunc="first"
    ).reset_index()

    # 4. Rename Columns to match DB
    df_pivot = df_pivot.rename(columns={
        "PC_ACT": "Adjusted Unemployment Rate",
        "THS_PER": "Adjusted Unemployed 000s"
    })

    # 5. Ensure all requested columns exist
    for col in ["Adjusted Unemployment Rate", "Adjusted Unemployed 000s"]:
        if col not in df_pivot.columns:
            df_pivot[col] = pd.NA

    # 6. Reorder and Clean
    cols = ["Geopolitical Entity", "Year", "Month", "Adjusted Unemployed 000s", "Adjusted Unemployment Rate"]
    df_out = df_pivot[cols].copy()
    
    # Rounding
    df_out["Adjusted Unemployment Rate"] = pd.to_numeric(df_out["Adjusted Unemployment Rate"], errors="coerce").round(1)
    df_out["Adjusted Unemployed 000s"] = pd.to_numeric(df_out["Adjusted Unemployed 000s"], errors="coerce").round(0)

    df_out = df_out.sort_values(["Year", "Month", "Geopolitical Entity"]).reset_index(drop=True)
    
    return df_out