from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


def _clean_country_display(raw: str) -> str:
    s = str(raw).strip()
    if s == "Switzerland (including Liechtenstein)":
        return s
    if s.startswith("E.U. COUNTRIES"):
        return "E.U. COUNTRIES"
    s = re.sub(r"\s*\([^)]*\)", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _to_key(display: str, raw: str) -> str:
    r = str(raw)
    if "Until 2010 it was included in Europe" in r:
        return "georgia_europe"
    if "included in Asia since 2011" in r:
        return "georgia_asia"

    d = str(display).strip()
    if d == "E.U. COUNTRIES":
        return "eu_countries"
    if d in {"TOTAL", "EUROPE", "AFRICA", "AMERICA", "ASIA", "OCEANIA", "NOT STATED"}:
        return d.lower().replace(" ", "_")

    k = d.lower()
    k = k.replace("&", " and ")
    k = re.sub(r"[^a-z0-9]+", "_", k)
    k = re.sub(r"_+", "_", k).strip("_")
    return k


def _parse_period(col: str) -> tuple[int, int] | None:
    m = re.match(r"^(\d{4})M(\d{2})$", str(col).strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _format_arrivals(value: object) -> str:
    s = str(value).strip()
    if not s or s.lower() == "nan" or s in {"...", "u", "U", "…", "—", "-"}:
        return ""
    n = pd.to_numeric(s.replace(",", ""), errors="coerce")
    if pd.isna(n):
        return ""
    return str(int(round(float(n))))


def extract_tourist_arrivals_country(csv_path: Path) -> pd.DataFrame:
    """
    Extract CYSTAT tourist arrivals by country as:
      Year, Month, Country_key, Country_order, Country_of_origin, Arrivals
    """
    last_err: Exception | None = None
    df = None
    for enc in ("utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(csv_path, encoding=enc, dtype=str)
            break
        except Exception as e:  # pragma: no cover
            last_err = e
    if df is None:
        raise RuntimeError(f"Unable to decode CSV for tourist arrivals by country: {last_err}")

    df.columns = [str(c).replace("\ufeff", "").replace('"', "").strip() for c in df.columns]
    if df.columns.empty:
        raise RuntimeError("Empty CSV: no columns found for tourist arrivals by country.")

    country_col = df.columns[0]
    period_cols: list[tuple[str, int, int]] = []
    for c in df.columns[1:]:
        parsed = _parse_period(c)
        if parsed:
            period_cols.append((c, parsed[0], parsed[1]))
    if not period_cols:
        raise RuntimeError("No period columns found in tourist arrivals CSV.")

    records: list[dict[str, object]] = []
    for row_idx, row in df.iterrows():
        raw_country = str(row[country_col]).strip()
        if not raw_country or raw_country.lower() == "nan":
            continue

        display = _clean_country_display(raw_country)
        key = _to_key(display, raw_country)

        for col_name, year, month in period_cols:
            arrivals = _format_arrivals(row.get(col_name, ""))
            records.append(
                {
                    "year": year,
                    "month": month,
                    "country_key": key,
                    "country_order": int(row_idx),
                    "country_of_origin": display,
                    "arrivals": arrivals,
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No rows extracted for tourist arrivals by country.")

    out = out.sort_values(["year", "month", "country_order"]).reset_index(drop=True)
    return out
