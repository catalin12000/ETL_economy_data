from __future__ import annotations

import re
from pathlib import Path

import fitz
import pandas as pd

from etl.pipelines.lro_pdf_common import (
    detect_district,
    extract_tokens_between,
    parse_currency_token,
    parse_foreigners_blocks,
    parse_int_token,
)


BUYERS_LABEL = "Ολικός Αριθμός Υποθέσεων:"
PARCELS_LABEL = "Ολικός Αριθμός Ακινήτων:"
DECLARED_LABEL = "Ολικό Συνολικό Δηλωθέν Ποσό:"
ACCEPTED_LABEL = "Ολικό Συνολικό Αποδεχθέν Ποσό:"
CURRENCY_PATTERN = r"€\s*[\d,]+(?:\.\d+)?"


def parse_transfer_totals(pdf_path: Path) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    doc = fitz.open(pdf_path)
    try:
        for page in doc:
            text = page.get_text()
            district = detect_district(text)
            if not district or district == "Pancypria":
                continue

            year_match = re.search(r"-\s*(20\d{2})", text)
            if not year_match:
                raise ValueError(f"Could not detect transfers page year for {district}")
            year = int(year_match.group(1))

            buyers_values = extract_tokens_between(text, BUYERS_LABEL, PARCELS_LABEL)
            parcels_values = extract_tokens_between(text, PARCELS_LABEL, DECLARED_LABEL)
            declared_values = extract_tokens_between(text, DECLARED_LABEL, ACCEPTED_LABEL, CURRENCY_PATTERN)
            accepted_values = extract_tokens_between(text, ACCEPTED_LABEL, token_pattern=CURRENCY_PATTERN)

            if min(len(buyers_values), len(parcels_values), len(declared_values), len(accepted_values)) < 2:
                raise ValueError(f"Incomplete transfer totals parsed for {district}")

            buyers_monthly = [parse_int_token(value) for value in buyers_values[:-1]]
            parcels_monthly = [parse_int_token(value) for value in parcels_values[:-1]]
            declared_monthly = [parse_currency_token(value) for value in declared_values[:-1]]
            accepted_monthly = [parse_currency_token(value) for value in accepted_values[:-1]]

            month_count = min(
                len(buyers_monthly),
                len(parcels_monthly),
                len(declared_monthly),
                len(accepted_monthly),
            )

            for month in range(1, month_count + 1):
                records.append(
                    {
                        "year": year,
                        "month": month,
                        "district": district,
                        "number_of_buyers_total": buyers_monthly[month - 1],
                        "number_parcels_total": parcels_monthly[month - 1],
                        "declared_price": declared_monthly[month - 1],
                        "accepted_price": accepted_monthly[month - 1],
                    }
                )
    finally:
        doc.close()

    return pd.DataFrame(records)


def extract_lro_transfers(totals_path: Path, foreigners_path: Path) -> pd.DataFrame:
    totals_df = parse_transfer_totals(totals_path)
    foreigners_df = parse_foreigners_blocks(foreigners_path)

    parcels_df = (
        foreigners_df[foreigners_df["block_number"] == 1]
        .loc[
            foreigners_df["district"] != "Pancypria",
            ["year", "month", "district", "eu_count", "non_eu_count"],
        ]
        .rename(
            columns={
                "eu_count": "number_parcels_eu",
                "non_eu_count": "number_parcels_non_eu",
            }
        )
    )

    buyers_df = (
        foreigners_df[foreigners_df["block_number"] == 2]
        .loc[
            foreigners_df["district"] != "Pancypria",
            ["year", "month", "district", "eu_count", "non_eu_count"],
        ]
        .rename(
            columns={
                "eu_count": "number_of_buyers_eu",
                "non_eu_count": "number_of_buyers_noneu",
            }
        )
    )

    merged = pd.merge(totals_df, parcels_df, on=["year", "month", "district"], how="inner")
    merged = pd.merge(merged, buyers_df, on=["year", "month", "district"], how="inner")

    merged["number_of_buyers_locals"] = (
        merged["number_of_buyers_total"] - merged["number_of_buyers_eu"] - merged["number_of_buyers_noneu"]
    )
    merged["number_parcels_locals"] = (
        merged["number_parcels_total"] - merged["number_parcels_eu"] - merged["number_parcels_non_eu"]
    )

    if (merged["number_of_buyers_locals"] < 0).any() or (merged["number_parcels_locals"] < 0).any():
        raise ValueError("Transfers locals calculation produced negative values.")

    integer_columns = [
        "year",
        "month",
        "number_of_buyers_total",
        "number_parcels_total",
        "number_parcels_eu",
        "number_parcels_non_eu",
        "number_of_buyers_eu",
        "number_of_buyers_noneu",
        "number_of_buyers_locals",
        "number_parcels_locals",
    ]
    for column in integer_columns:
        merged[column] = merged[column].astype(int)

    for column in ["declared_price", "accepted_price"]:
        merged[column] = merged[column].astype(float)

    return merged.sort_values(["year", "month", "district"]).reset_index(drop=True)
