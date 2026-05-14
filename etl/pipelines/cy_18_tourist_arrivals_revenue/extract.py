from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


def _parse_period(s: str) -> tuple[int, int] | None:
    m = re.match(r"^(\d{4})M(\d{2})$", str(s).strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _to_number_or_na(value: object) -> float | pd._libs.missing.NAType:
    s = str(value).strip()
    if not s or s.lower() in {"nan", "n.a.", "na", "...", "u", "—", "-"}:
        return pd.NA
    n = pd.to_numeric(s.replace(",", ""), errors="coerce")
    if pd.isna(n):
        return pd.NA
    return float(n)


def extract_tourist_arrivals_revenue(csv_path: Path) -> pd.DataFrame:
    """
    Extract monthly tourist arrivals/revenue with YoY changes as:
      Year, Month, Arrivals, Revenue_Millions, Arrivals_yoy_change (%), Revenue_yoy_change (%)
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
        raise RuntimeError(f"Unable to decode CSV for tourist arrivals/revenue: {last_err}")

    df.columns = [str(c).replace("\ufeff", "").replace('"', "").strip() for c in df.columns]
    if df.columns.empty:
        raise RuntimeError("Empty CSV: no columns found for tourist arrivals/revenue.")

    month_col = "MONTH" if "MONTH" in df.columns else df.columns[0]
    arrivals_col = None
    revenue_col = None
    arrivals_yoy_col = None
    revenue_yoy_col = None

    for c in df.columns:
        low = c.lower()
        if "arrivals of tourists" in low and "change" not in low:
            arrivals_col = c
        elif "revenue" in low and "mn" in low:
            revenue_col = c
        elif "arrivals of tourists" in low and "change" in low:
            arrivals_yoy_col = c
        elif "revenue" in low and "change" in low:
            revenue_yoy_col = c

    if not all([arrivals_col, revenue_col, arrivals_yoy_col, revenue_yoy_col]):
        raise RuntimeError("Could not identify expected columns in tourist arrivals/revenue CSV.")

    records: list[dict[str, object]] = []
    for _, row in df.iterrows():
        ym = _parse_period(row[month_col])
        if not ym:
            continue
        year, month = ym

        arrivals = _to_number_or_na(row[arrivals_col])
        revenue = _to_number_or_na(row[revenue_col])
        arr_yoy = _to_number_or_na(row[arrivals_yoy_col])
        rev_yoy = _to_number_or_na(row[revenue_yoy_col])

        records.append(
            {
                "Year": year,
                "Month": month,
                "Arrivals": arrivals,
                "Revenue_Millions": revenue,
                "Arrivals_yoy_change (%)": arr_yoy,
                "Revenue_yoy_change (%)": rev_yoy,
            }
        )

    out = pd.DataFrame(records)
    if out.empty:
        raise RuntimeError("No rows extracted for tourist arrivals/revenue.")

    out = out.sort_values(["Year", "Month"]).reset_index(drop=True)
    return out
