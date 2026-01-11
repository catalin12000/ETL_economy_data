# etl/pipelines/ed_eu_consumer_confidence_index/extract.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


def extract_eu_consumer_confidence(csv_path: Path) -> pd.DataFrame:
    """
    Extracts EU Consumer Confidence Index from Eurostat CSV.
    Input columns: TIME_PERIOD, OBS_VALUE, geo
    Output columns: Year, Month, Geopolitical_Entity, Consumer_confidence_indicator
    """
    df = pd.read_csv(csv_path)

    # Mapping Eurostat GEO codes to requested full names
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
        geo_name = GEO_MAP.get(geo_code, geo_code)

        val = row["OBS_VALUE"]
        try:
            val = round(float(val), 1)
        except:
            val = pd.NA

        records.append({
            "Year": year,
            "Month": month,
            "Geopolitical_Entity": geo_name,
            "Consumer_confidence_indicator": val
        })

    out = pd.DataFrame(records)
    out = out.sort_values(["Year", "Month", "Geopolitical_Entity"]).reset_index(drop=True)
    return out
