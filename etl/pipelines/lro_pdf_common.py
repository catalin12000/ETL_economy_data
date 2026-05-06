from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import fitz
import pandas as pd


DISTRICT_PATTERNS = [
    ("ΠΑΓΚΥΠΡΙΑ", "Pancypria"),
    ("ΛΕΥΚΩΣΙΑ", "Nicosia"),
    ("ΛΕΥΚΩΣΙΑΣ", "Nicosia"),
    ("ΛΕΜΕΣΟΣ", "Limassol"),
    ("ΛΕΜΕΣΟΥ", "Limassol"),
    ("ΛΕΜΕΣΟ", "Limassol"),
    ("ΛΑΡΝΑΚΑ", "Larnaca"),
    ("ΛΑΡΝΑΚΑΣ", "Larnaca"),
    ("ΑΜΜΟΧΩΣΤΟΣ", "Famagusta"),
    ("ΑΜΜΟΧΩΣΤΟΥ", "Famagusta"),
    ("ΑΜΜΟΧΩΣΤΟ", "Famagusta"),
    ("ΠΑΦΟΣ", "Paphos"),
    ("ΠΑΦΟΥ", "Paphos"),
    ("ΠΑΦΟ", "Paphos"),
]

FOREIGNER_DISTRICT_ORDER = [
    "Nicosia",
    "Famagusta",
    "Larnaca",
    "Limassol",
    "Paphos",
    "Pancypria",
]


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def detect_district(text: str) -> Optional[str]:
    upper_text = text.upper()
    for greek_name, english_name in DISTRICT_PATTERNS:
        if greek_name in upper_text:
            return english_name
    return None


def extract_tokens_between(
    text: str,
    start_label: str,
    end_label: Optional[str] = None,
    token_pattern: str = r"\b\d+\b",
) -> list[str]:
    normalized = normalize_whitespace(text)
    start_index = normalized.find(start_label)
    if start_index < 0:
        raise ValueError(f"Could not find start label: {start_label}")

    start_index += len(start_label)
    if end_label:
        end_index = normalized.find(end_label, start_index)
        if end_index < 0:
            raise ValueError(f"Could not find end label: {end_label}")
        chunk = normalized[start_index:end_index]
    else:
        chunk = normalized[start_index:]

    return re.findall(token_pattern, chunk)


def parse_int_token(token: str) -> int:
    return int(token.replace(",", "").strip())


def parse_currency_token(token: str) -> float:
    return float(token.replace("€", "").replace(",", "").strip())


def parse_foreigners_blocks(pdf_path: Path) -> pd.DataFrame:
    doc = fitz.open(pdf_path)
    try:
        text = "\n".join(page.get_text() for page in doc)
    finally:
        doc.close()

    lines = [normalize_whitespace(line) for line in text.splitlines() if normalize_whitespace(line)]

    raw_rows: list[tuple[int, int, list[int]]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if re.fullmatch(r"\d{2}/\d{4}", line):
            value_lines = lines[index + 1 : index + 13]
            if len(value_lines) == 12 and all(re.fullmatch(r"\d+", value) for value in value_lines):
                month = int(line[:2])
                year = int(line[3:])
                raw_rows.append((year, month, [int(value) for value in value_lines]))
                index += 13
                continue
        index += 1

    if not raw_rows:
        raise ValueError(f"No foreigners rows found in {pdf_path}")

    first_period = raw_rows[0][:2]
    periods_per_block = 0
    for year, month, _values in raw_rows:
        if periods_per_block and (year, month) == first_period:
            break
        periods_per_block += 1

    if periods_per_block == 0 or len(raw_rows) % periods_per_block != 0:
        raise ValueError(f"Unexpected foreigners PDF row layout in {pdf_path}")

    records: list[dict[str, object]] = []
    for row_index, (year, month, values) in enumerate(raw_rows):
        block_number = (row_index // periods_per_block) + 1
        for district_index, district in enumerate(FOREIGNER_DISTRICT_ORDER):
            records.append(
                {
                    "block_number": block_number,
                    "year": year,
                    "month": month,
                    "district": district,
                    "eu_count": values[district_index * 2],
                    "non_eu_count": values[(district_index * 2) + 1],
                }
            )

    return pd.DataFrame(records)
