from __future__ import annotations

import re
from pathlib import Path

import fitz
import pandas as pd

from etl.pipelines.lro_pdf_common import detect_district, extract_tokens_between, parse_foreigners_blocks, parse_int_token


def parse_contracts_totals(pdf_path: Path) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    doc = fitz.open(pdf_path)
    try:
        for page in doc:
            text = page.get_text()
            district = detect_district(text)
            if not district or district == "Pancypria":
                continue

            label_year_matches = [int(match) for match in re.findall(r"ΑΡΙΘΜΟΣ ΠΩΛΗΤΗΡΙΩΝ ΕΓΓΡΑΦΩΝ (20\d{2})", text)]
            if not label_year_matches:
                raise ValueError(f"Could not detect contracts label year in page for {district}")
            year = max(label_year_matches)
            label = f"ΑΡΙΘΜΟΣ ΠΩΛΗΤΗΡΙΩΝ ΕΓΓΡΑΦΩΝ {year}"

            values = extract_tokens_between(text, label, "ΜΗΝΑΣ")
            if len(values) < 2:
                raise ValueError(f"Could not parse contracts totals for {district} / {year}")

            monthly_totals = [parse_int_token(value) for value in values[:-1]]
            for month, number_parcels_total in enumerate(monthly_totals, start=1):
                records.append(
                    {
                        "year": year,
                        "month": month,
                        "district": district,
                        "number_parcels_total": number_parcels_total,
                    }
                )
    finally:
        doc.close()

    return pd.DataFrame(records)


def extract_lro_contracts_of_sale(contracts_pdf_path: Path, foreigners_pdf_path: Path) -> pd.DataFrame:
    totals_df = parse_contracts_totals(contracts_pdf_path)
    foreigners_df = parse_foreigners_blocks(foreigners_pdf_path)

    contracts_foreigners_df = (
        foreigners_df[foreigners_df["block_number"] == 3]
        .loc[
            foreigners_df["district"] != "Pancypria",
            ["year", "month", "district", "eu_count", "non_eu_count"],
        ]
        .rename(
            columns={
                "eu_count": "number_parcels_eu",
                "non_eu_count": "number_parcels_noneu",
            }
        )
    )

    merged = pd.merge(
        totals_df,
        contracts_foreigners_df,
        on=["year", "month", "district"],
        how="inner",
    )

    merged["number_parcels_locals"] = (
        merged["number_parcels_total"] - merged["number_parcels_eu"] - merged["number_parcels_noneu"]
    )

    if (merged["number_parcels_locals"] < 0).any():
        raise ValueError("Contracts locals calculation produced negative values.")

    for column in [
        "year",
        "month",
        "number_parcels_total",
        "number_parcels_locals",
        "number_parcels_eu",
        "number_parcels_noneu",
    ]:
        merged[column] = merged[column].astype(int)

    return merged.sort_values(["year", "month", "district"]).reset_index(drop=True)
