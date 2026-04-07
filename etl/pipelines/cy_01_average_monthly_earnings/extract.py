from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


def _norm_text(value: str) -> str:
    s = str(value).strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def _detect_sex(col_name: str) -> str | None:
    n = _norm_text(col_name)
    if "συνολο" in n or "total" in n:
        return "Total"
    if "αντρες" in n or "male" in n or "men" in n:
        return "Males"
    if "γυναικες" in n or "female" in n or "women" in n:
        return "Females"
    return None


def _detect_metric(col_name: str) -> str | None:
    n = _norm_text(col_name)
    if "χωρις διορθωση" in n or "unadjusted" in n:
        return "Avg Monthly Earnings Unadjusted"
    if "διορθωμενες" in n or "seasonally" in n or "adjusted" in n:
        return "Avg Monthly Earnings Seasonally Adjusted"
    return None


def _extract_year_quarter(period_label: str) -> tuple[int, int] | None:
    s = str(period_label).strip()
    m = re.search(r"(\d{4}).*?([1-4])$", s)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def extract_average_monthly_earnings(csv_path: Path) -> pd.DataFrame:
    """
    Extract quarterly average monthly earnings by sex with both:
    - unadjusted
    - seasonally adjusted
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [str(c).replace("\ufeff", "").replace('"', "").strip() for c in df.columns]

    period_col = df.columns[0]
    metric_cols: list[tuple[str, str, str]] = []
    for c in df.columns[1:]:
        sex = _detect_sex(c)
        metric = _detect_metric(c)
        if sex and metric:
            metric_cols.append((c, sex, metric))

    records_map: dict[tuple[int, int, str], dict[str, object]] = {}
    for _, row in df.iterrows():
        yq = _extract_year_quarter(row[period_col])
        if not yq:
            continue
        year, quarter = yq

        for col_name, sex, metric in metric_cols:
            key = (year, quarter, sex)
            if key not in records_map:
                records_map[key] = {
                    "Year": year,
                    "Quarter": quarter,
                    "Sex": sex,
                    "Avg Monthly Earnings Unadjusted": pd.NA,
                    "Avg Monthly Earnings Seasonally Adjusted": pd.NA,
                }

            value = pd.to_numeric(pd.Series([row[col_name]]), errors="coerce").iloc[0]
            if pd.notna(value):
                records_map[key][metric] = round(float(value), 3)

    out = pd.DataFrame(records_map.values())
    if out.empty:
        raise RuntimeError("No rows extracted for average monthly earnings.")

    out = out.sort_values(["Year", "Quarter", "Sex"]).reset_index(drop=True)
    return out
