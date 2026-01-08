# Pipeline Architecture Analysis

This document details the implementation patterns for all data pipelines in the `ETL_economy_data` project. It serves as a guide for understanding existing logic and creating new pipelines.

## Common Architecture
*   **Location:** `etl/pipelines/<pipeline_id>/pipeline.py`
*   **Entry Point:** `run(state: Dict[str, Any]) -> Dict[str, Any]`
*   **Download Convention:** To align with the Master Tracking Sheet, all raw downloads are stored in `data/downloads/` with a prefix corresponding to the official numbering (e.g., `39_ed_retail_trade_turnover_index`).

## Mapping Table

| No. | Pipeline ID | Source | Notes |
| :-- | :--- | :--- | :--- |
| 1 | `ed_apartments_price_index_table` | BoG (PDF) | Type 3: Extraction & Sync |
| 2 | `ed_building_permits_table` | ELSTAT (SOP03) | Type 1: Dynamic Download |
| 3 | `ed_building_permits_by_no_of_rooms` | ELSTAT (SOP03) | Type 1: Dynamic Download |
| 4 | `ed_construction_index` | ELSTAT (DKT66) | Type 1: Dynamic Download |
| 5 | `ed_consumer_price_index` | ELSTAT (DKT01) | Type 3: Extraction & Sync |
| 7 | `ed_economic_forecast` | EU Commission | Type 3: Extraction & Sync |
| 8 | `ed_employment` | ELSTAT (SJO02) | Type 3: Extraction & Sync |
| 9 | `ed_fdi_activity` | BoG (BPM6) | Type 2: Static URL |
| 10 | `ed_fdi_country` | BoG (BPM6) | Type 2: Static URL |
| 11 | `ed_fdi_real_estate` | BoG (Excel) | Type 2: Static URL |
| 12 | `ed_geo_distribution_...` | Migration (PDF) | Type 4: Master Cluster |
| 13 | `ed_gross_fixed_capital_formation` | ELSTAT (SEL81) | Type 1: Dynamic Download |
| 14 | `ed_gva_by_sector` | ELSTAT (SEL12) | Type 1: Dynamic Download |
| 16 | `ed_housing_finances` | ELSTAT (SEL95) | Type 1: Dynamic Download |
| 17 | `ed_imports_exports_millions` | ELSTAT (SEL30) | Type 1: Dynamic Download |
| 18 | `ed_industrial_production_index` | ELSTAT (DKT21) | Type 1: Dynamic Download |
| 19 | `ed_key_partners_primary_goods` | ELSTAT (SFC02) | Type 1: Dynamic Download |
| 20 | `ed_loan_amounts_millions` | BoG (Excel) | Type 3: Extraction & Sync |
| 21 | `ed_loan_interest_rates` | BoG (Excel) | Type 3: Extraction & Sync |
| 22 | `ed_motor_trade_turnover_index` | ELSTAT (DKT45) | Type 1: Dynamic Download |
| 23 | `ed_new_built_properties_per_region` | ELSTAT (SOP03) | Type 1: Dynamic Download |
| 24 | `ed_new_establishments_building_permits`| ELSTAT (SOP03) | Type 1: Dynamic Download |
| 25 | `ed_new_residential_building_cost_index`| ELSTAT (DKT63) | Type 1: Dynamic Download |
| 26 | `ed_new_residential_buildings_work_categories`| ELSTAT (DKT63) | Type 1: Dynamic Download |
| 27 | `ed_office_price_volume_index` | BoG (PDF) | Type 2: Static URL (2 files) |
| 29 | `ed_residence_permits_application`| Migration (PDF) | Type 4: Satellite Cluster |
| 36 | `ed_residents_di_activity` | BoG (BPM6) | Type 2: Static URL |
| 37 | `ed_residents_di_country` | BoG (BPM6) | Type 2: Static URL |
| 38 | `ed_retail_price_rental_index` | BoG (PDF) | Type 2: Static URL (2 files) |
| 39 | `ed_retail_trade_turnover_index` | ELSTAT (DKT39) | Type 1: Dynamic Download |
| 40 | `ed_retail_trade_volume_index` | ELSTAT (DKT39) | Type 1: Dynamic Download |
| 42 | `ed_tourists_arrivals_revenue` | BoG (Excel) | Type 2: Static URL (2 files) |
| 43 | `ed_wage_growth_index` | ELSTAT (DKT03) | Type 1: Dynamic Download |
| 44 | `ed_wholesale_trade_turnover_index`| ELSTAT (DKT42) | Type 1: Dynamic Download |
| 45 | `ed_services_sector_turnover_monthly_index`| ELSTAT (DKT54) | Type 1: Dynamic Download |
| 46 | `ed_eu_consumer_confidence_index` | Eurostat (API) | Type 3: Extraction & Sync |
| 47 | `ed_eu_gdp` | Eurostat (API) | Type 3: Extraction & Sync |
| 48 | `ed_eu_hicp` | Eurostat (API) | Type 3: Extraction & Sync |
| 49 | `ed_eu_unemployment_rate` | Eurostat (API) | Type 3: Extraction & Sync |
| 54 | `gdp_greece` | ELSTAT (SEL84) | Type 3: Extraction & Sync |

---

## Detailed Implementation Types

### 1. Type 1: ELSTAT Dynamic Download
These pipelines handle datasets from ELSTAT. They dynamically resolve the latest publication URL (Monthly, Quarterly, or Annual) by scanning the category landing page.

### 2. Type 2: Static URL / API Download
Used for datasets with stable permalinks (e.g., Bank of Greece spreadsheets) or direct API endpoints (e.g., Eurostat SDMX-JSON) where we can filter for specific countries and units directly.

### 3. Type 3: Extraction & Sync (Complex)
Handle data locked in complex formats (like PDF tables) and require historical tracking. They extract, compare with a "master" database file, and update it.

### 4. Type 4: Dependent / Cluster Pipelines
One "Master" pipeline downloads the file, and "Satellite" pipelines extract different parts of it. Useful for large PDF reports containing multiple tables.
