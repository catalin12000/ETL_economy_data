# etl/pipelines/ed_eu_gdp/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def extract_eu_gdp(csv_path: Path) -> pd.DataFrame:
    """
    Extracts EU GDP data from Eurostat CSV.
    Matches DB columns with spaces.
    """
    df = pd.read_csv(csv_path)

    # Mapping Eurostat GEO codes to requested full names
    GEO_MAP = {
        "EU27_2020": "European Union (EU6-1958, EU9-1973, EU10-1981, EU12-1986, EU15-1995, EU25-2004, EU27-2007, EU28-2013, EU27-2020)",
        "EA": "Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)",
        "EA20": "Euro area (EA11-1999, EA12-2001, EA13-2007, EA15-2008, EA16-2009, EA17-2011, EA18-2014, EA19-2015, EA20-2023)",
        "EL": "Greece",
        "CY": "Cyprus",
        "RO": "Romania"
    }

    # Mapping Eurostat unit codes to requested column names
    UNIT_MAP = {
        "CLV20_MEUR": "Chain Linked Volumes",
        "CLV_PCH_PRE": "Quarter Over Quarter",
        "CLV_PCH_SM": "Year Over Year",
        "CP_MEUR": "Current Prices"
    }

    # 1. Parse Year and Quarter from TIME_PERIOD (e.g., "2024-Q1")
    df[["Year", "Quarter"]] = df["TIME_PERIOD"].str.split("-Q", expand=True)
    df["Year"] = pd.to_numeric(df["Year"])
    df["Quarter"] = pd.to_numeric(df["Quarter"])

    # 2. Map Geopolitical Entity
    df["Geopolitical Entity"] = df["geo"].map(lambda x: GEO_MAP.get(x, x))

    # 3. Pivot Units to Columns
    df_pivot = df.pivot_table(
        index=["Geopolitical Entity", "Year", "Quarter"],
        columns="unit",
        values="OBS_VALUE",
        aggfunc="first"
    ).reset_index()

    # 4. Rename Columns based on UNIT_MAP
    df_pivot = df_pivot.rename(columns=UNIT_MAP)

    # 5. Ensure all requested columns exist
    for col in UNIT_MAP.values():
        if col not in df_pivot.columns:
            df_pivot[col] = pd.NA

    # 6. Reorder and Round
    cols = ["Geopolitical Entity", "Year", "Quarter", "Chain Linked Volumes", "Quarter Over Quarter", "Year Over Year", "Current Prices"]
    df_out = df_pivot[cols].copy()
    
    val_cols = ["Chain Linked Volumes", "Quarter Over Quarter", "Year Over Year", "Current Prices"]
    for c in val_cols:
        df_out[c] = pd.to_numeric(df_out[c], errors="coerce").round(1)

    df_out = df_out.sort_values(["Year", "Quarter", "Geopolitical Entity"]).reset_index(drop=True)
    
    return df_out