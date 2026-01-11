# etl/pipelines/ed_eu_hicp/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def extract_eu_hicp(csv_path: Path) -> pd.DataFrame:
    """
    Extracts EU Harmonized Index of Consumer Prices (Annual Rate of Change).
    Matches DB internal columns.
    """
    df = pd.read_csv(csv_path)

    # Mapping Eurostat GEO codes to requested names
    GEO_MAP = {
        "EU27_2020": "European Union - 27 countries (from 2020)",
        "EA20": "Euro area – 20 countries (from 2023)",
        "EL": "Greece",
        "CY": "Cyprus",
        "RO": "Romania"
    }

    records = []
    for _, row in df.iterrows():
        # Parse TIME_PERIOD (e.g., "2025-01")
        time_str = str(row["TIME_PERIOD"])
        try:
            year, month = map(int, time_str.split("-"))
        except:
            continue

        geo_code = row["geo"]
        if geo_code not in GEO_MAP:
            continue
            
        geo_name = GEO_MAP[geo_code]

        val = row["OBS_VALUE"]
        try:
            val = round(float(val), 1)
        except:
            val = pd.NA

        records.append({
            "Geopolitical Entity": geo_name,
            "Year": year,
            "Month": month,
            "Annual Rate Of Change": val
        })

    out = pd.DataFrame(records)
    out = out.sort_values(["Year", "Month", "Geopolitical Entity"]).reset_index(drop=True)
    return out