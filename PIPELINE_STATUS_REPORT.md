# Pipeline Status & Methodology Report
**Date:** January 27, 2026
**Total Pipelines:** 64
**Completion Status:** 10/64 Fully Automated, 64/64 Downloaders Active

## Legend
*   **Methodology:**
    *   **ELSTAT Dynamic:** Scrapes HTML to find the latest Excel/PDF link.
    *   **BoG Static:** Direct download of a fixed URL (usually Excel).
    *   **Eurostat API:** Direct JSON data fetch (High reliability).
    *   **Migration PDF:** Scrapes data from complex PDF tables (High complexity).
    *   **Cyprus Excel:** Downloads and parses Excel files from CBC/CYSTAT.
*   **Effort:**
    *   **Low:** API or simple CSV.
    *   **Medium:** Standard Excel tables.
    *   **High:** PDF scraping or complex/messy Excel structures.
*   **Status:**
    *   **✅ COMPLETED:** Fully extracting, syncing, and delivering data.
    *   **📥 DOWNLOADER ONLY:** Raw file is verified and collected; extraction logic pending.

---

## 1. Urgent Tier (Completed)
These 10 pipelines are fully operational in Production.

| ID | Source | Methodology | Effort | Status |
| :--- | :--- | :--- | :--- | :--- |
| `ed_consumer_price_index` | ELSTAT | Excel (Dynamic Link) | Medium | ✅ COMPLETED |
| `ed_employment` | ELSTAT | Excel (Dynamic Link) | Medium | ✅ COMPLETED |
| `gdp_greece` | ELSTAT | Excel (Dynamic Link) | Medium | ✅ COMPLETED |
| `ed_loan_interest_rates` | BoG | Excel (Complex Formatting) | High | ✅ COMPLETED |
| `ed_loan_amounts_millions` | BoG | Excel (Complex Formatting) | High | ✅ COMPLETED |
| `cy_13_new_loans_millions` | CBC | Excel (Cyprus) | Medium | ✅ COMPLETED |
| `ed_eu_consumer_confidence_index` | Eurostat | API (JSON) | Low | ✅ COMPLETED |
| `ed_eu_gdp` | Eurostat | API (JSON) | Low | ✅ COMPLETED |
| `ed_eu_hicp` | Eurostat | API (JSON) | Low | ✅ COMPLETED |
| `ed_eu_unemployment_rate` | Eurostat | API (JSON) | Low | ✅ COMPLETED |

---

## 2. High Priority Tier (Next Up)
Targeting these for the next phase (Phase 2). Focus on Trade & Production.

| ID | Source | Methodology | Effort | Status |
| :--- | :--- | :--- | :--- | :--- |
| `ed_industrial_production_index` | ELSTAT | Excel (Dynamic Link) | Medium | 📥 DOWNLOADER ONLY |
| `ed_retail_trade_turnover_index` | ELSTAT | Excel (Dynamic Link) | Medium | 📥 DOWNLOADER ONLY |
| `ed_wholesale_trade_turnover_index` | ELSTAT | Excel (Dynamic Link) | Medium | 📥 DOWNLOADER ONLY |
| `ed_services_sector_turnover_monthly_index` | ELSTAT | Excel (Dynamic Link) | Medium | 📥 DOWNLOADER ONLY |
| `ed_motor_trade_turnover_index` | ELSTAT | Excel (Dynamic Link) | Medium | 📥 DOWNLOADER ONLY |
| `ed_motor_trade_volume_index` | ELSTAT | Excel (Dynamic Link) | Medium | 📥 DOWNLOADER ONLY |

---

## 3. Standard Priority (Pending Extraction)
Raw data is being collected, but extraction logic is not yet implemented.

### Greece (ELSTAT & BoG)
| ID | Source | Methodology | Effort | Status |
| :--- | :--- | :--- | :--- | :--- |
| `ed_apartments_price_index_table` | BoG | PDF (Complex) | High | 📥 DOWNLOADER ONLY |
| `ed_building_permits_table` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_building_permits_by_no_of_rooms` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_construction_index` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_economic_forecast` | EU Comm | PDF/Excel | High | 📥 DOWNLOADER ONLY |
| `ed_fdi_activity` | BoG | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_fdi_country` | BoG | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_fdi_real_estate` | BoG | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_geo_distribution_of_issued_and_pending_permits` | Migration | PDF (Scraping) | High | 📥 DOWNLOADER ONLY |
| `ed_gross_fixed_capital_formation` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_gva_by_sector` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_household_income_allocation` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_housing_finances` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_imports_exports_millions` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_key_partners_primary_goods` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_new_built_properties_per_region` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_new_establishments_building_permits` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_new_residential_building_cost_index` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_new_residential_buildings_work_categories` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_office_price_volume_index` | BoG | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residence_permits_aggregate` | Migration | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residence_permits_application` | Migration | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residence_permits_current` | Migration | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residence_permits_golden_visa` | Migration | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residence_permits_issued` | Migration | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residence_permits_top10_countries` | Migration | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residence_permits_top10_countries_golden_visa` | Migration | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_residents_di_activity` | BoG | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_residents_di_country` | BoG | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_retail_price_rental_index` | BoG | PDF | High | 📥 DOWNLOADER ONLY |
| `ed_retail_trade_volume_index` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_tourists_arrivals_revenue` | BoG | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_wage_growth_index` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `ed_wholesale_trade_volume_index` | ELSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |

### Cyprus (CBC & CYSTAT)
| ID | Source | Methodology | Effort | Status |
| :--- | :--- | :--- | :--- | :--- |
| `cy_01_average_monthly_earnings` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_02_building_permits_by_district` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_03_building_permits_by_property_type` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_04_construction_index_cy` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_05_consumer_price_index` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_06_economic_forecast_cy` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_09_gross_value_added_sector` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_10_lro_contracts_of_sale` | CBC | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_11_lro_transfers` | CBC | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_12_monthly_gross_earnings_distribution` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_14_per_day_expenditure_of_tourists` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_15_residential_price_indices` | CBC | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_16_total_households_loans_millions` | CBC | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_17_tourist_arrivals_country` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
| `cy_18_tourist_arrivals_revenue` | CYSTAT | Excel | Medium | 📥 DOWNLOADER ONLY |
