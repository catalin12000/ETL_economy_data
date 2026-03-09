# etl/pipelines/cy_03_building_permits_by_property_type/extract.py
from __future__ import annotations

import pandas as pd
from pathlib import Path


def _read_pxweb_csv(csv_path: Path) -> pd.DataFrame:
    # CYSTAT CSV files are UTF-8 with BOM; fallback kept for safety.
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="latin-1")

    # Normalize header artifacts (BOM, quotes, extra spaces).
    df.columns = [
        str(c).replace("\ufeff", "").replace('"', "").strip()
        for c in df.columns
    ]
    return df


def _require_column(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    upper_map = {col.upper(): col for col in df.columns}
    for c in candidates:
        if c.upper() in upper_map:
            return upper_map[c.upper()]
    raise KeyError(f"Missing expected columns {candidates}. Found: {list(df.columns)}")


def extract_building_permits_type(csv_path: Path) -> pd.DataFrame:
    """
    Extracts Cyprus Building Permits by Property Type from PxWeb CSV.
    Outputs a DataFrame in the DB format (Pivoted by Project Type, Long by Metric).
    """
    df = _read_pxweb_csv(csv_path)

    month_col = _require_column(df, ["MONTH"])
    project_type_col = _require_column(df, ["TYPE OF PROJECT"])

    # 1. Parse Year and Month
    period = df[month_col].astype(str).str.strip().str.replace('"', "", regex=False)
    df["Year"] = pd.to_numeric(period.str.extract(r"^(\d{4})")[0], errors="coerce")
    df["Month"] = pd.to_numeric(period.str.extract(r"M(\d{2})")[0], errors="coerce")
    df = df.dropna(subset=["Year", "Month"]).copy()
    df["Year"] = df["Year"].astype(int)
    df["Month"] = df["Month"].astype(int)

    # 2. Map Metrics to DB Strings
    # CSV columns are like: "Number Monthly data Number of permits", etc.
    # We map them to the exact strings found in the DB 'permits' column.
    METRIC_MAP = {
        "Number of permits": "Number of permits",
        "Area (m2)": "Area (m2)",
        "Value (": "Value (€000's)", # Handles encoding issues with Euro symbol
        "Dwelling units": "Dwelling Units"
    }
    
    # Identify which CSV column corresponds to which DB metric
    csv_metric_cols = []
    for csv_col in df.columns:
        for key, db_val in METRIC_MAP.items():
            if key in csv_col:
                csv_metric_cols.append((csv_col, db_val))
                break
    
    # 3. Map Project Types to DB Columns
    TYPE_MAP = {
        "Single houses": "single_houses",
        "Residential buildings with two housing units": "buildings_with_two_housing_units",
        "Residential apartment blocks": "residential_apartment_blocks",
        "Residential/commercial apartment blocks": "residential_commercial_apartment_blocks",
        "Cottage apartment complexes": "cottage_apartment_complexes",
        "Residencies for communities": "residencies_for_communities",
        "Hotels": "hotels",
        "Tourist apartments and villages": "tourist_apartments_and_villages",
        "Restaurants, coffee-bars etc": "restaurants_coffee_bars",
        "Other tourist accommodation": "other_tourist_accommodation",
        "Office buildings": "office_buildings",
        "Wholesale and retail trade buildings": "wholesale_retail_buildings",
        "Transport and communication buildings": "transport_communication_buildings",
        "Industrial buildings and warehouses": "industrial_buildings_and_warehouses",
        "Public entertainment, educational, medical and other institutional buildings": "public_entertainment_educational_medical",
        "Other non-residential buildings": "other_non_residential_buildings",
        "Civil engineering": "civil_engineering",
        "Division of plots": "division_of_plots",
        "Road construction": "road_construction"
    }
    
    # We only care about specific types or 'Total'? 
    # The DB schema has individual columns for these types.
    
    records = []

    for _, row in df.iterrows():
        project_type_raw = row[project_type_col]
        db_col = TYPE_MAP.get(project_type_raw)

        if db_col:
            for csv_col, db_metric in csv_metric_cols:
                val = row[csv_col]
                try:
                    f_val = float(val) if val != "..." else pd.NA
                except:
                    f_val = pd.NA
                
                records.append({
                    "Year": row["Year"],
                    "Month": row["Month"],
                    "permits": db_metric, # This is the metric column in DB
                    "db_col": db_col,
                    "value": f_val
                })

    # Pivot so that db_col (Project types) become columns
    out = pd.DataFrame(records).pivot_table(
        index=["Year", "Month", "permits"],
        columns="db_col",
        values="value",
        aggfunc="first"
    ).reset_index()
    
    # Ensure all DB columns exist
    for col in TYPE_MAP.values():
        if col not in out.columns:
            out[col] = pd.NA
            
    # Sort
    out = out.sort_values(["Year", "Month", "permits"]).reset_index(drop=True)
    return out
