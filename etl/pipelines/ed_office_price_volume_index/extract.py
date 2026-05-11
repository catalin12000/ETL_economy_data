from __future__ import annotations

import re
from pathlib import Path

import fitz
import pandas as pd


YEAR_RE = re.compile(r"^\d{4}$")
HALF_RE = re.compile(r"^[H\u0397](1|2)\*?$")
NUMBER_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")
ELLIPSIS = {"...", "\u2026", "…"}


def _normalize_line(value: str) -> str:
    text = str(value).strip()
    text = text.replace("\u0397", "H")
    text = text.replace("−", "-")
    return text


def _extract_lines(pdf_path: Path) -> list[str]:
    doc = fitz.open(pdf_path)
    try:
        return [
            _normalize_line(line)
            for page in doc
            for line in page.get_text("text").splitlines()
            if _normalize_line(line)
        ]
    finally:
        doc.close()


def _to_float(value: str):
    if value in ELLIPSIS:
        return pd.NA
    return float(value.replace(",", "."))


def _parse_index_pdf(pdf_path: Path, expected_title: str) -> pd.DataFrame:
    lines = _extract_lines(pdf_path)
    full_text = "\n".join(lines).upper()
    if expected_title.upper() not in full_text:
        raise RuntimeError(f"Expected title '{expected_title}' not found in {pdf_path.name}.")

    rows: list[dict[str, object]] = []
    current_year: int | None = None
    i = 0

    while i < len(lines):
        line = lines[i]

        if line.startswith("Source") or line.startswith("\u03A0\u03B7\u03B3\u03AE"):
            break

        if YEAR_RE.match(line):
            current_year = int(line)
            i += 1
            continue

        half_match = HALF_RE.match(line)
        if half_match and current_year is not None:
            year_half = int(half_match.group(1))
            tokens: list[str] = []
            j = i + 1

            while j < len(lines) and len(tokens) < 12:
                next_line = lines[j]
                if (
                    YEAR_RE.match(next_line)
                    or HALF_RE.match(next_line)
                    or next_line.startswith("Source")
                    or next_line.startswith("\u03A0\u03B7\u03B3\u03AE")
                ):
                    break
                if next_line in ELLIPSIS or NUMBER_RE.match(next_line.replace(",", ".")):
                    tokens.append(next_line)
                j += 1

            if len(tokens) < 10:
                raise RuntimeError(
                    f"Could not extract 4 index groups for {current_year} H{year_half} from {pdf_path.name}."
                )

            rows.append(
                {
                    "Year": current_year,
                    "Year Half": year_half,
                    "Total Index": _to_float(tokens[0]),
                    "Athens Index": _to_float(tokens[3]),
                    "Thessaloniki Index": _to_float(tokens[6]),
                    "Rest Of Greece Index": _to_float(tokens[9]),
                }
            )
            i = j
            continue

        i += 1

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"No half-year rows extracted from {pdf_path.name}.")
    return df


def extract_office_price_volume_index(price_pdf_path: Path, rent_pdf_path: Path) -> pd.DataFrame:
    price_df = _parse_index_pdf(price_pdf_path, "OFFICE PRICE INDEX").rename(
        columns={
            "Total Index": "Total Price Index",
            "Athens Index": "Athens Price Index",
            "Thessaloniki Index": "Thessaloniki Price Index",
            "Rest Of Greece Index": "Rest Of Greece Price Index",
        }
    )
    rent_df = _parse_index_pdf(rent_pdf_path, "OFFICE RENT INDEX").rename(
        columns={
            "Total Index": "Total Rent Index",
            "Athens Index": "Athens Rent Index",
            "Thessaloniki Index": "Thessaloniki Rent Index",
            "Rest Of Greece Index": "Rest Of Greece Rent Index",
        }
    )

    df = price_df.merge(rent_df, on=["Year", "Year Half"], how="inner", validate="one_to_one")
    value_cols = [c for c in df.columns if c not in {"Year", "Year Half"}]
    df = df.dropna(subset=value_cols).copy()
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["Year Half"] = pd.to_numeric(df["Year Half"], errors="coerce").astype("Int64")
    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["Year", "Year Half"]).reset_index(drop=True)
    if df.empty:
        raise RuntimeError("No complete office price/rent rows remained after merging both PDFs.")
    return df
