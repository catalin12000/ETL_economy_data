from __future__ import annotations

from pathlib import Path

import pandas as pd


BUCKETS_IN_ORDER = [
    "<500",
    "500- 749",
    "750- 999",
    "1000- 1249",
    "1250- 1499",
    "1500- 1749",
    "1750- 1999",
    "2000- 2249",
    "2250- 2499",
    "2500- 2749",
    "2750- 2999",
    "3000- 3249",
    "3250- 3499",
    "3500- 3749",
    "3750- 3999",
    "4000- 4249",
    "4250- 4499",
    "4500- 4749",
    "4750- 4999",
    "5000- 5249",
    "5250- 5499",
    "5500- 5749",
    "5750- 5999",
    ">=6000",
]


def _norm_bucket(s: str) -> str:
    return str(s).strip().replace(" ", "")


def extract_monthly_gross_earnings_distribution(csv_path: Path) -> pd.DataFrame:
    """
    Extract CYSTAT monthly gross earnings distribution as:
      year, category, gross_monthly_earnings, percentage_of_employees
    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [str(c).replace("\ufeff", "").replace('"', "").strip() for c in df.columns]

    if df.columns.empty:
        raise RuntimeError("Empty CSV: no columns found for monthly gross earnings distribution.")

    year_col = "YEAR" if "YEAR" in df.columns else df.columns[0]
    sex_col = "SEX" if "SEX" in df.columns else df.columns[1]

    norm_to_real_col = {_norm_bucket(c): c for c in df.columns}
    bucket_cols: list[tuple[str, str]] = []  # (deliverable label, source col)
    for bucket in BUCKETS_IN_ORDER:
        source_col = norm_to_real_col.get(_norm_bucket(bucket))
        if source_col:
            bucket_cols.append((bucket, source_col))

    if len(bucket_cols) != len(BUCKETS_IN_ORDER):
        missing = [b for b in BUCKETS_IN_ORDER if _norm_bucket(b) not in norm_to_real_col]
        raise RuntimeError(f"Missing expected gross earnings buckets in CSV: {missing}")

    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        year = pd.to_numeric(row[year_col], errors="coerce")
        if pd.isna(year):
            continue
        category = str(row[sex_col]).strip()
        if not category or category.lower() == "nan":
            continue

        for bucket_label, source_col in bucket_cols:
            value = pd.to_numeric(row[source_col], errors="coerce")
            if pd.isna(value):
                continue
            rows.append(
                {
                    "year": int(year),
                    "category": category,
                    "gross_monthly_earnings": bucket_label,
                    "percentage_of_employees": round(float(value), 1),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No rows extracted for monthly gross earnings distribution.")

    category_order = {"Total": 0, "Males": 1, "Females": 2}
    bucket_order = {bucket: i for i, bucket in enumerate(BUCKETS_IN_ORDER)}
    out["__cat_ord"] = out["category"].map(category_order).fillna(99)
    out["__bucket_ord"] = out["gross_monthly_earnings"].map(bucket_order).fillna(999)
    out = out.sort_values(["year", "__cat_ord", "__bucket_ord"]).drop(
        columns=["__cat_ord", "__bucket_ord"]
    )
    out = out.reset_index(drop=True)
    return out
