from __future__ import annotations

from pathlib import Path

import pandas as pd


GEO_MAP = {
    "EU27_2020": "European Union - 27 countries (from 2020)",
    "EA20": "Euro area - 20 countries (from 2023)",
    "EL": "Greece",
    "CY": "Cyprus",
    "RO": "Romania",
}


def extract_economic_sentiment_indicator(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    records = []
    for _, row in df.iterrows():
        time_str = str(row.get("TIME_PERIOD", ""))
        try:
            year, month = map(int, time_str.split("-"))
        except Exception:
            continue

        geo_code = row.get("geo")
        if geo_code not in GEO_MAP:
            continue

        try:
            value = round(float(row.get("OBS_VALUE")), 1)
        except Exception:
            value = pd.NA

        records.append(
            {
                "Year": year,
                "Month": month,
                "Geopolitical Entity": GEO_MAP[geo_code],
                "Economic Sentiment Indicator": value,
            }
        )

    out = pd.DataFrame(records)
    out = out.sort_values(["Year", "Month", "Geopolitical Entity"]).reset_index(drop=True)
    return out
