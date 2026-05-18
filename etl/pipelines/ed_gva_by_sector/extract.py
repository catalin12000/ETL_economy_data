from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


# NACE A64 code → DB column name.
# Handles both hyphen (C10-C12) and underscore (C10_C12) separators from ELSTAT.
NACE_TO_DB_COL: dict[str, str] = {
    "A01": "crop_animal_prod_hunting_svc",
    "A02": "forestry_logging",
    "A03": "fishing_aquaculture",
    "B":   "mining_quarrying",
    "C10-C12": "manufacture_food_beverages",
    "C13-C15": "manufacture_textiles_apparel",
    "C16":     "manufacture_wood_products",
    "C17":     "manufacture_paper_products",
    "C18":     "printing_reproduction_media",
    "C19":     "manufacture_coke_petroleum",
    "C20":     "manufacture_chemicals",
    "C21":     "manufacture_pharmaceuticals",
    "C22":     "manufacture_rubber_plastic",
    "C23":     "manufacture_non_metallic_products",
    "C24":     "manufacture_basic_metals",
    "C25":     "manufacture_metal_products",
    "C26":     "manufacture_computer_optical_products",
    "C27":     "manufacture_electrical_equipment",
    "C28":     "manufacture_machinery",
    "C29":     "manufacture_vehicles",
    "C30":     "manufacture_transport_equip",
    "C31-C32": "manufacture_furniture",
    "C33":     "repair_install_machinery_equip",
    "D":   "electric_gas_steam_supply",
    "E36":     "water_collection_treatment",
    "E37-E39": "sewerage_waste_management",
    "F":   "construction",
    "G45": "wholesale_retail_motor_vehicles",
    "G46": "wholesale_trade_non_motor_vehicles",
    "G47": "retail_trade_non_motor_vehicles",
    "H49": "land_transport_pipeline_transport",
    "H50": "water_transport",
    "H51": "air_transport",
    "H52": "warehousing_transport_support",
    "H53": "postal_courier_activities",
    "I":   "accommodation_food_beverage_svc",
    "J58":     "publishing_activities",
    "J59-J60": "motion_picture_broadcasting",
    "J61":     "telecommunications",
    "J62-J63": "computer_programming_consultancy",
    "K64": "financial_svc_ex_insurance",
    "K65": "insurance_pension_funding",
    "K66": "aux_financial_insurance_svc",
    "L":   "real_estate_activities",
    "M69-M70": "legal_accounting_consultancy_svc",
    "M71":     "arch_eng_technical_testing",
    "M72":     "scientific_research_development",
    "M73":     "advertising_market_research",
    "M74-M75": "professional_scientific_tech_svc",
    "N77": "rental_leasing_activities",
    "N78": "employment_activities",
    "N79": "travel_agency_tour_operator",
    "N80-N82": "security_investigation_services",
    "O":   "public_admin_defence",
    "P":   "education",
    "Q86":     "human_health_activities",
    "Q87-Q88": "social_work_activities",
    "R90-R92": "creative_arts_cultural_activities",
    "R93":     "sports_recreation_activities",
    "S94": "org_membership_activities",
    "S95": "repair_computers_personal_goods",
    "S96": "personal_service_activities",
    "T":   "household_employers_private_household_svc",
    "U":   "extraterritorial_org_activities",
    # GDP aggregate and taxes/subsidies row
    "TLS_P": "taxes_less_subsidies",
    "P1":    "gdp",
    "B1GQ":  "gdp",
    "GDP":   "gdp",
}

# Build a normalised lookup: strip whitespace, uppercase, replace _ → -
_NORM: dict[str, str] = {
    re.sub(r"[\s_]", "-", k).upper(): v
    for k, v in NACE_TO_DB_COL.items()
}

DB_COLS = list(dict.fromkeys(NACE_TO_DB_COL.values()))  # ordered, deduplicated


def _norm_code(raw: str) -> str:
    return re.sub(r"[\s_]", "-", str(raw).strip()).upper()


def _engine_for(path: Path) -> str:
    with open(path, "rb") as f:
        return "openpyxl" if f.read(2) == b"PK" else "xlrd"


def extract_gva(xls_path: Path) -> pd.DataFrame:
    """
    Extract SEL12 GVA by sector (A64) to wide form matching the DB schema.

    XLS layout:
      Row 8:  year values in columns 2+
      Row 9+: col 0 = NACE code, col 1 = Greek name, cols 2+ = values per year

    Output: one row per year, one column per NACE sector (DB snake_case names).

    # QA: validate that all NACE codes in the file map correctly — run once and
    # check printed unmapped codes. Adjust NACE_TO_DB_COL if ELSTAT uses
    # different code variants (e.g. spaces, alternate separators).
    """
    xls_path = Path(xls_path)
    df = pd.read_excel(xls_path, sheet_name=0, header=None, engine=_engine_for(xls_path))

    # Identify year columns from row 8
    year_cols: list[tuple[int, int]] = []
    for j, val in enumerate(df.iloc[8].tolist()):
        try:
            y = int(float(str(val).replace("*", "").strip()))
            if 1900 < y < 2100:
                year_cols.append((j, y))
        except (ValueError, TypeError):
            continue

    if not year_cols:
        raise RuntimeError(f"No year columns found in row 8 of {xls_path.name}.")

    # Build {year: {db_col: value}} accumulator
    wide: dict[int, dict[str, float]] = {y: {} for _, y in year_cols}
    unmapped: set[str] = set()

    for i in range(9, len(df)):
        row = df.iloc[i]
        raw_code = str(row.iloc[0]).strip()
        if not raw_code or raw_code.lower() == "nan":
            continue

        norm = _norm_code(raw_code)
        db_col = _NORM.get(norm)
        if db_col is None:
            unmapped.add(raw_code)
            continue

        for col_idx, year in year_cols:
            try:
                val = round(float(row.iloc[col_idx]), 2)
            except (ValueError, TypeError):
                val = None
            if val is not None:
                wide[year][db_col] = val

    if unmapped:
        print(f"  [ed_gva_by_sector] unmapped NACE codes (add to NACE_TO_DB_COL): {sorted(unmapped)}")

    rows = []
    for _, year in sorted(year_cols):
        row = {"year": year}
        for col in DB_COLS:
            row[col] = wide[year].get(col)
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError(f"No data extracted from {xls_path.name}.")

    return out.sort_values("year").reset_index(drop=True)
