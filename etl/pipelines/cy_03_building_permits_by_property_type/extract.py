# etl/pipelines/cy_03_building_permits_by_property_type/extract.py
from __future__ import annotations

import pandas as pd
from pathlib import Path

def extract_building_permits_type(csv_path: Path) -> pd.DataFrame:
    """
    Extracts Cyprus Building Permits by Property Type from PxWeb CSV.
    """
    df = pd.read_csv(csv_path, encoding='latin-1')
    
    # 1. Parse Year and Month
    # Format: "2003M01"
    df["Year"] = df["MONTH"].str[:4].astype(int)
    df["Month"] = df["MONTH"].str[5:7].astype(int)
    
    # 2. Map Metrics (We only care about "Number of permits" for this table based on schema)
    # The DB 'permits' column seems to correspond to the 'Number of permits' metric.
    # The project types are the other columns.
    
    # Filter for "Number of permits" metric if multiple exist
    # But wait, looking at the schema, the project types ARE the columns.
    # This means we need to pivot the project types.
    
    # Mapping Type of Project labels to DB columns
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
    
    df["Type"] = df["TYPE OF PROJECT"].map(lambda x: TYPE_MAP.get(x))
    
    # The 'permits' column in DB likely corresponds to the 'Number of permits' metric
    # Find the 'Number of permits' column name
    permits_col = [c for c in df.columns if "Number of permits" in c][0]
    
    # Pivot
    df_pivot = df.pivot_table(
        index=["Year", "Month"],
        columns="Type",
        values=permits_col,
        aggfunc="sum"
    ).reset_index()
    
    # The DB also has a 'permits' column which might be the Total. 
    # Let's check the labels for 'Total' project type.
    total_df = df[df["TYPE OF PROJECT"] == "Total"].copy()
    total_df = total_df.rename(columns={permits_col: "permits"})
    
    out = pd.merge(df_pivot, total_df[["Year", "Month", "permits"]], on=["Year", "Month"], how="left")
    
    # Clean numeric
    for c in out.columns:
        if c not in ["Year", "Month"]:
            out[c] = pd.to_numeric(out[c], errors='coerce').round(0)
            
    return out
