from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


COUNTRY_RENAMES = {
    "Ireland (July 2005 - 2014)": "Ireland",
    "Lebanon (from 2015)": "Lebanon",
    "Poland (from 2015)": "Poland",
    "Ukraine (2015 - March 2020)": "Ukraine",
    "Russia (July 2005 - March 2020)": "Russia",
}

METRIC_LENGTH = "Average Length of Stay (Nights)"
METRIC_PER_DAY = "Expenditure - Per Day (€)"


def _clean_value(v: object) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() == "nan":
        return ""
    return s


def _parse_year_month(period: str) -> tuple[int, int] | None:
    m = re.match(r"^(\d{4})M(\d{2})$", str(period).strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def extract_tourist_expenditure_distribution(csv_path: Path) -> pd.DataFrame:
    """
    Extract tourism monthly data as:
      Year, Month, Country_of_origin, Average_length_of_stay_(nights), Expenditure_per_day
    Keeps non-numeric placeholders like "..." and "u".
    """
    last_err: Exception | None = None
    df = None
    for enc in ("utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(csv_path, encoding=enc, dtype=str)
            break
        except Exception as e:  # pragma: no cover - fallback branch
            last_err = e
    if df is None:
        raise RuntimeError(f"Unable to decode CSV for tourist expenditure: {last_err}")
    df.columns = [str(c).replace("\ufeff", "").replace('"', "").strip() for c in df.columns]

    if df.columns.empty:
        raise RuntimeError("Empty CSV: no columns found for per-day tourist expenditure.")

    month_col = "MONTH" if "MONTH" in df.columns else df.columns[0]
    metric_col = None
    for c in df.columns:
        if "EXPENDITURE" in c.upper() and "LENGTH OF STAY" in c.upper():
            metric_col = c
            break
    if metric_col is None:
        raise RuntimeError("Could not identify metric column in tourist expenditure CSV.")

    country_cols = [c for c in df.columns if c not in [month_col, metric_col]]
    if not country_cols:
        raise RuntimeError("No country columns found in tourist expenditure CSV.")

    country_order = []
    for raw_country in country_cols:
        norm_country = COUNTRY_RENAMES.get(raw_country, raw_country)
        if norm_country not in country_order:
            country_order.append(norm_country)
    country_rank = {c: i for i, c in enumerate(country_order)}

    records_map: dict[tuple[int, int, str], dict[str, object]] = {}

    for _, row in df.iterrows():
        ym = _parse_year_month(row[month_col])
        if not ym:
            continue
        year, month = ym

        metric_name = _clean_value(row[metric_col])
        is_length = metric_name == METRIC_LENGTH
        is_per_day = metric_name == METRIC_PER_DAY
        if not is_length and not is_per_day:
            continue

        for raw_country in country_cols:
            country = COUNTRY_RENAMES.get(raw_country, raw_country)
            key = (year, month, country)
            if key not in records_map:
                records_map[key] = {
                    "year": year,
                    "month": month,
                    "country_of_origin": country,
                    "average_length_of_stay_nights": "",
                    "expenditure_per_day": "",
                }

            value = _clean_value(row.get(raw_country, ""))
            if is_length:
                records_map[key]["average_length_of_stay_nights"] = value
            elif is_per_day:
                records_map[key]["expenditure_per_day"] = value

    out = pd.DataFrame(records_map.values())
    if out.empty:
        raise RuntimeError("No rows extracted for tourist expenditure distribution.")

    out["__country_ord"] = out["country_of_origin"].map(country_rank).fillna(999)
    out = out.sort_values(["year", "month", "__country_ord"]).drop(columns=["__country_ord"])
    out = out.reset_index(drop=True)
    return out
