import re
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import pdfplumber

QNUM = {"I": 1, "II": 2, "III": 3, "IV": 4}

def norm_q(s: str) -> str:
    s = str(s).strip().replace("*", "")
    s = s.replace("ΙII", "III").replace("ΙV", "IV").replace("Ι", "I")
    return s

def to_float(x):
    if x is None:
        return None
    s = str(x).strip()
    if s in ("", "", "...", ""):
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def parse_year_quarter(cell, current_year):
    if cell is None:
        return None
    s = str(cell).strip()
    if not s:
        return None
    if s.startswith("Πηγή") or s.startswith("Source"):
        return ("STOP", None)

    s = s.replace("ΙII", "III").replace("ΙV", "IV").replace("Ι", "I")

    if re.fullmatch(r"\d{4}", s):
        return ("YEAR_ONLY", int(s))

    m = re.match(r"^(\d{4})\s+(I|II|III|IV)\*?$", s)
    if m:
        y = int(m.group(1))
        q = QNUM.get(norm_q(m.group(2)))
        return (y, q) if q else None

    m = re.match(r"^(I|II|III|IV)\*?$", s)
    if m and current_year is not None:
        q = QNUM.get(norm_q(m.group(1)))
        return (current_year, q) if q else None

    return None

def normalize_table(table):
    maxlen = max(len(r) for r in table)
    norm = [r + [None] * (maxlen - len(r)) for r in table]
    return pd.DataFrame(norm)

def load_page_table1(pdf_path: Path, page_idx: int) -> pd.DataFrame:
    with pdfplumber.open(str(pdf_path)) as pdf:
        page = pdf.pages[page_idx]
        tables = page.extract_tables() or []
        if not tables:
            raise RuntimeError(f"No tables found on page {page_idx+1}")
        return normalize_table(tables[0])

def parse_ii6_greece(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    current_year = None

    for _, row in df.iterrows():
        pq = parse_year_quarter(row.iloc[0], current_year)
        if pq is None:
            continue
        if pq[0] == "STOP":
            break
        if pq[0] == "YEAR_ONLY":
            current_year = pq[1]
            continue

        y, qn = pq
        current_year = y

        total = to_float(row.iloc[1]) if len(row) > 1 else None
        new5  = to_float(row.iloc[4]) if len(row) > 4 else None
        old5  = to_float(row.iloc[7]) if len(row) > 7 else None

        if None in (total, new5, old5):
            tokens = [to_float(x) for x in row.tolist()[1:] if to_float(x) is not None]
            if len(tokens) >= 7:
                total, new5, old5 = tokens[0], tokens[3], tokens[6]

        if None not in (total, new5, old5):
            rows.append({
                "Year": y,
                "Quarter": qn,
                "Region": "Greece",
                "Index": total,
                "Up To 5 Years Old Index": new5,
                "Over 5 Years Old Index": old5,
            })

    return pd.DataFrame(rows)

def parse_geo_table(df: pd.DataFrame) -> Dict[Tuple[int, int, str], float]:
    out: Dict[Tuple[int, int, str], float] = {}
    current_year = None
    regions = ["Athens", "Thessaloniki", "Other Cities", "Other Areas"]

    for _, row in df.iterrows():
        pq = parse_year_quarter(row.iloc[0], current_year)
        if pq is None:
            continue
        if pq[0] == "STOP":
            break
        if pq[0] == "YEAR_ONLY":
            current_year = pq[1]
            continue

        y, qn = pq
        current_year = y

        vals = None
        if len(row) >= 11:
            vals = [to_float(row.iloc[i]) for i in (1, 4, 7, 10)]

        if vals is None or any(v is None for v in vals):
            tokens = [to_float(x) for x in row.tolist()[1:] if to_float(x) is not None]
            vals = []
            for idx in (0, 3, 6, 9):
                vals.append(tokens[idx] if len(tokens) > idx else None)

        for rname, v in zip(regions, vals):
            if v is not None:
                out[(y, qn, rname)] = v

    return out

def extract_apartment_indices(pdf_path: Path) -> pd.DataFrame:
    df_ii6  = load_page_table1(pdf_path, 0)
    df_ii7  = load_page_table1(pdf_path, 1)
    df_ii71 = load_page_table1(pdf_path, 2)
    df_ii72 = load_page_table1(pdf_path, 3)

    greece_df = parse_ii6_greece(df_ii6)

    total_map = parse_geo_table(df_ii7)
    new_map   = parse_geo_table(df_ii71)
    old_map   = parse_geo_table(df_ii72)

    rows = greece_df.to_dict("records")

    for key, total in total_map.items():
        if key in new_map and key in old_map:
            y, qn, region = key
            rows.append({
                "Year": y,
                "Quarter": qn,
                "Region": str(region).strip(),
                "Index": total,
                "Up To 5 Years Old Index": new_map[key],
                "Over 5 Years Old Index": old_map[key],
            })

    df = pd.DataFrame(rows)
    df["Region"] = df["Region"].astype(str).str.strip()
    df = df.sort_values(["Year", "Quarter", "Region"]).reset_index(drop=True)
    return df
