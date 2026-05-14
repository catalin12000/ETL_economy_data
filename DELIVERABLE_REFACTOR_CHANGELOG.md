# Deliverable Refactor — Full Table Reference

Branch: `metabase-deliverable-refactor`

Every pipeline that writes a deliverable to the DB is listed below.
Deliverable headers now match the exact DB column names so the upload
script maps them without any remapping logic.

---

## cy_01_average_monthly_earnings
**DB table:** `ed_average_monthly_earnings` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (5):** `year`, `quarter`, `sex`, `avg_monthly_earnings_unadjusted`, `avg_monthly_earnings_seasonally_adjusted`

---

## cy_02_building_permits_by_district
**DB table:** `ed_building_permits_by_district` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (8):** `year`, `month`, `district`, `urban_rural`, `area_m2`, `dwelling_units`, `number`, `value_000s`

---

## cy_03_building_permits_by_property_type
**DB table:** `ed_building_permits_by_property_type` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (22):** `year`, `month`, `permits`, `single_houses`, `buildings_with_two_housing_units`, `residential_apartment_blocks`, `residential_commercial_apartment_blocks`, `cottage_apartment_complexes`, `residencies_for_communities`, `hotels`, `tourist_apartments_and_villages`, `restaurants_coffee_bars`, `other_tourist_accommodation`, `office_buildings`, `wholesale_retail_buildings`, `transport_communication_buildings`, `industrial_buildings_and_warehouses`, `public_entertainment_educational_medical`, `other_non_residential_buildings`, `civil_engineering`, `division_of_plots`, `road_construction`

---

## cy_04_construction_index_cy
**DB table:** `ed_construction_index_cy` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (3):** `year`, `month`, `index`

---

## cy_05_consumer_price_index
**DB table:** `ed_consumer_price_index` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (4):** `year`, `month`, `index`, `year_over_year`

---

## cy_06_economic_forecast_cy
**DB table:** `ed_economic_forecast_cy` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (7):** `year`, `gdp_growth`, `inflation`, `unemployment`, `general_government_balance`, `gross_public_debt`, `current_account_balance`

---

## cy_09_gross_value_added_sector
**DB table:** `ed_gross_value_added_sector` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (22):** `year`, `total_economy`, `agriculture_forestry_fishing`, `mining_quarrying`, `manufacturing`, `electricity_gas_steam_ac_supply`, `water_supply_sewerage_waste_mgmt`, `construction`, `wholesale_retail_trade_motor_repair`, `transportation_storage`, `accommodation_food_services`, `info_communication`, `financial_insurance_activities`, `real_estate_activities`, `prof_sci_tech_activities`, `admin_support_services_activities`, `public_admin_defence_social_sec`, `education`, `human_health_social_work_activities`, `arts_entertainment_recreation`, `other_services_activities`, `household_employer_activities`

---

## cy_10_lro_contracts_of_sale
**DB table:** `ed_lro_contracts_of_sale` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (7):** `year`, `month`, `district`, `number_parcels_locals`, `number_parcels_eu`, `number_parcels_noneu`, `number_parcels_total`

---

## cy_11_lro_transfers
**DB table:** `ed_lro_transfers` on **zeus**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `Year` | `year` | 🔧 Lowercased to match DB |
| `Month` | `month` | 🔧 |
| `District` | `district` | 🔧 |
| `Number of Buyers Total` | `number_of_buyers_total` | 🔧 |
| `Number Parcels Total` | `number_parcels_total` | 🔧 |
| `Declared Price` | `declared_price` | 🔧 |
| `Accepted Price` | `accepted_price` | 🔧 |
| `Number Parcels Locals` | `number_parcels_locals` | 🔧 |
| `Number Parcels Eu` | `number_parcels_eu` | 🔧 |
| `Number Parcels Non Eu` | `number_parcels_non_eu` | 🔧 |
| `Number Buyers Locals` | `number_of_buyers_locals` | 🔧 **Fixed** — missing "of" in column name |
| `Number Buyers Eu` | `number_of_buyers_eu` | 🔧 **Fixed** — missing "of" |
| `Number Buyers Non Eu` | `number_of_buyers_noneu` | 🔧 **Fixed** — missing "of" |

---

## cy_12_monthly_gross_earnings_distribution
**DB table:** `ed_monthly_gross_earnings_distribution` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (26):** `year`, `sex`, `under_500`, `range_500_749`, `range_750_999`, `range_1000_1249`, `range_1250_1499`, `range_1500_1749`, `range_1750_1999`, `range_2000_2249`, `range_2250_2499`, `range_2500_2749`, `range_2750_2999`, `range_3000_3249`, `range_3250_3499`, `range_3500_3749`, `range_3750_3999`, `range_4000_4249`, `range_4250_4449`, `range_4500_4749`, `range_4750_4999`, `range_5000_5249`, `range_5250_5499`, `range_5500_5749`, `range_5750_5999`, `over_6000`

---

## cy_13_new_loans_millions
**DB table:** `ed_new_loans_millions` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (16):** `year`, `month`, `housing_pure_new_loans`, `housing_renegotiated_loans`, `housing_floating_rate_up_to_1_year_initial_fixation_rate`, `housing_annual_percentage_rate_of_charge`, `outstanding_housing_loans_locals`, `outstanding_housing_loans_eu`, `outstanding_housing_loans_non_eu`, `consumer_annual_percentage_rate_of_charge`, `consumer_floating_rate_up_to_1_year_initial_fixation_rate`, `consumer_pure_new_loans`, `consumer_renegotiated_loans`, `outstanding_consumer_loans_eu`, `outstanding_consumer_loans_locals`, `outstanding_consumer_loans_non_eu`

---

## cy_15_residential_price_indices
**DB table:** `ed_residential_price_indices` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (20):** `year`, `quarter`, `residential_price_property_price_index`, `apartments_cy`, `houses_cy`, `nicosia_residential`, `limassol_residential`, `larnaca_residential`, `paphos_residential`, `famagusta_residential`, `nicosia_apartments`, `limassol_apartments`, `larnaca_apartments`, `paphos_apartments`, `famagusta_apartments`, `nicosia_houses`, `limassol_houses`, `larnaca_houses`, `paphos_houses`, `famagusta_houses`

---

## cy_16_total_households_loans_millions
**DB table:** `ed_total_households_loans_millions` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (8):** `year`, `month`, `total_loans`, `total_households_loans`, `non_performing_loans`, `loan_amounts_past_90_days`, `restructured_loans_forbearance`, `non_performing_restructured_loans`

---

## cy_17_tourist_arrivals_country
**DB table:** `ed_tourist_arrivals_country` on **zeus**

No changes. All deliverable headers already matched DB schema.

**DB columns (61):** `year`, `month`, `belgium`, `bulgaria`, `czech_republic`, `denmark`, `germany`, `estonia`, `greece`, `spain`, `france`, `ireland`, `italy`, `latvia`, `lithuania`, `hungary`, `malta`, `netherlands`, `austria`, `poland`, `romania`, `slovakia`, `finland`, `sweden`, `united_kingdom`, `other_eu`, `norway`, `switzerland`, `russia`, `belarus`, `ukraine`, `serbia`, `other_non_eu`, `south_africa`, `egypt`, `other_africa`, `united_states`, `canada`, `other_america`, `kuwait`, `bahrain`, `united_arab_emirates`, `saudi_arabia`, `qatar`, `georgia`, `jordan`, `armenia`, `israel`, `lebanon`, `other_asia`, `australia`, `other_oceania`, `not_stated`, `total`, `total_europe`, `total_eu`, `total_non_eu`, `total_africa`, `total_america`, `total_asia`, `total_oceania`

---

## cy_18_tourist_arrivals_revenue
**DB table:** `ed_tourist_arrivals_revenue` on **zeus**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `ID` | `ID` |  Unchanged |
| `Year` | `Year` |  Unchanged |
| `Month` | `Month` |  Unchanged |
| `Arrivals` | `Arrivals` |  Unchanged |
| `Revenue_Millions` | `Revenue_Millions` |  Unchanged |
| `Arrivals_yoy_change (%)` | `*(removed)*` | 🗑 **Removed** — not a DB column |
| `Revenue_yoy_change (%)` | `*(removed)*` | 🗑 **Removed** — not a DB column |

---

## ed_building_permits_table
**DB table:** `ed_building_permits` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (5):** `year`, `month`, `permits_number`, `area`, `volume`

---

## ed_construction_index
**DB table:** `ed_construction_index` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (5):** `year`, `quarter`, `production_index_in_construction`, `production_index_building_construction`, `production_index_civil_engineering`

---

## ed_consumer_price_index
**DB table:** `ed_consumer_price_index` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (4):** `year`, `month`, `index`, `year_over_year`

---

## ed_economic_forecast
**DB table:** `ed_economic_forecast` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (7):** `year`, `gdp_growth`, `inflation`, `unemployment`, `general_government_balance`, `gross_public_debt`, `current_account_balance`

---

## ed_economic_sentiment_indicator
**DB table:** `ed_economic_sentiment_indicator` on **athena**

**EA21 update:** `EA21` added to `GEO_MAP` and `+EA21` added to FILTER (`teibs010`). Bulgaria joined euro area 2025-01-01.

No changes. All deliverable headers already matched DB schema.

**DB columns (4):** `year`, `month`, `geopolitical_entity`, `economic_sentiment_indicator`

---

## ed_employment
**DB table:** `ed_employment` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (8):** `year`, `month`, `seasonally`, `employed_000s`, `unemployed_000s`, `inactives_000s`, `adjusted_unemployment_rate`, `unadjusted_unemployment_rate`

---

## ed_eu_consumer_confidence_index
**DB table:** `ed_eu_consumer_confidence_index` on **athena**

**EA21 update:** `EA21` added to `GEO_MAP` and `+EA21` added to FILTER (`ei_bsco_m`).

No changes. All deliverable headers already matched DB schema.

**DB columns (4):** `year`, `month`, `geopolitical_entity`, `consumer_confidence_indicator`

---

## ed_eu_gdp
**DB table:** `ed_eu_gdp` on **athena**

**EA21 update:** `EA21` added to `GEO_MAP` and `+EA21` added to FILTER (`namq_10_gdp`).

No changes. All deliverable headers already matched DB schema.

**DB columns (7):** `geopolitical_entity`, `year`, `quarter`, `chain_linked_volumes`, `quarter_over_quarter`, `year_over_year`, `current_prices`

---

## ed_eu_hicp
**DB table:** `ed_eu_harmonized_index_of_consumer_prices` on **athena**

**EA21 update:** `EA21` added to `GEO_MAP`. **FILTER kept at EA20 only** — Eurostat returns 400 for EA21 on `prc_hicp_manr` as of May 2026. Add `+EA21` once Eurostat publishes it.

No changes. All deliverable headers already matched DB schema.

**DB columns (4):** `year`, `month`, `geopolitical_entity`, `annual_rate_of_change`

---

## ed_eu_unemployment_rate
**DB table:** `ed_eu_unemployment_rate` on **athena**

**EA21 update:** FILTER already changed to EA21-only (Eurostat retired EA20 for `une_rt_m`). `EA21` → `Euro area – 21 countries (from 2025)` added to `GEO_MAP`.

No changes. All deliverable headers already matched DB schema.

**DB columns (5):** `geopolitical_entity`, `year`, `month`, `adjusted_unemployed_000s`, `adjusted_unemployment_rate`

---

## ed_fdi_activity
**DB table:** `ed_fdi_activity` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (6):** `year`, `section_code`, `section_name`, `subsection_code`, `subsection_name`, `amount`

---

## ed_fdi_country
**DB table:** `ed_fdi_country` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (5):** `year`, `country`, `area`, `amount`, `continent`

---

## ed_fdi_real_estate
**DB table:** `ed_fdi_real_estate` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (4):** `year`, `country`, `area`, `amount`

---

## ed_geo_distribution_of_issued_and_pending_permits
**DB table:** `ed_geo_distribution_of_issued_and_pending_permits` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (9):** `year`, `month`, `permit_type`, `period`, `area`, `issued`, `rejected`, `revoked`, `pending`

---

## ed_gross_fixed_capital_formation
**DB table:** `ed_gross_fixed_capital_formation` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (11):** `year`, `quarter`, `seasonally`, `total_gross_fixed_capital_formation`, `dwellings`, `other_buildings_and_structures`, `cultivated_biological_resources`, `transport_equipment`, `information_communication_technology_equipment`, `other_machinery_and_equipment_and_weapon_systems`, `intellectual_property_products`

---

## ed_housing_finances
**DB table:** `ed_housing_finances` on **athena**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `ID` | `ID` |  Unchanged |
| `Year` | `year` | 🔧 Lowercased |
| `Quarter` | `quarter` | 🔧 Lowercased |
| `Group` | `group` | 🔧 Lowercased |
| `Category` | `category` | 🔧 Lowercased |
| `Subcategory` | `sub_category` | 🔧 **Fixed** — renamed to match DB |
| `Value (mln)` | `value_millions` | 🔧 **Fixed** — renamed to match DB |

---

## ed_imports_exports_millions
**DB table:** `ed_imports_exports_millions` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (19):** `year`, `current_prices_goods`, `current_prices_services`, `current_prices_imports`, `current_prices_expenditures_of_residents_in_rest_of_the_world`, `current_prices_goods_exports`, `current_prices_services_exports`, `current_prices_exports`, `current_prices_expenditures_of_residents_on_economic_territory`, `current_prices_exports_imports_balance`, `constant_prices_goods`, `constant_prices_services`, `constant_prices_imports`, `constant_prices_expenditures_of_residents_in_rest_of_the_world`, `constant_prices_goods_exports`, `constant_prices_services_exports`, `constant_prices_exports`, `constant_prices_expenditures_of_residents_on_economic_territory`, `constant_prices_exports_imports_balance`

---

## ed_industrial_production_index
**DB table:** `ed_industrial_production_index` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (8):** `year`, `month`, `overall_index`, `seasonally_adjusted_overall_index`, `mining_quarrying`, `manufacturing`, `electricity`, `water_supply`

---

## ed_loan_amounts_millions
**DB table:** `ed_loan_amounts_millions` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (14):** `year`, `month`, `group`, `loan_type`, `total_loan_amount`, `total_collateral_guarantees_loans`, `total_small_medium_enterprises_loans`, `floating_rate_1_year_fixation`, `floating_rate_1_year_rate_fixation_collateral_guarantees`, `floating_rate_1_year_rate_fixation_floating_rate`, `over_1_to_5_years_rate_fixation`, `over_5_years_rate_fixation`, `over_5_to_10_years_rate_fixation`, `over_10_years_rate_fixation`

---

## ed_loan_interest_rates
**DB table:** `ed_loan_interest_rates` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (24):** `year`, `month`, `total_consumer_loans_aprc`, `total_housing_loans_aprc`, `delta_interest_rate_deposits`, `weighted_average_interest_rate_new_loans_in_euro`, `group`, `weighted_average_interest_rate`, `loan_type`, `credit_cards`, `open_account_loans`, `debit_balances_on_current_accounts`, `total_interest_rate`, `total_collateral_guarantees_interest_rates`, `total_small_medium_enterprises_interest_rates`, `floating_rate_1_year_fixation`, `floating_rate_1_year_rate_fixation_collateral_guarantees`, `floating_rate_1_year_rate_fixation_floating_rate`, `over_1_to_5_years_rate_fixation`, `over_5_years_rate_fixation`, `over_5_to_10_years_rate_fixation`, `over_10_years_rate_fixation`, `credit_lines`, `debit_balances_sight_deposits`

---

## ed_motor_trade_turnover_index
**DB table:** `ed_motor_trade_turnover_index` on **athena**


**Source change — now downloads two files from DKT45:**

| File | Title | Sheet | Columns extracted |
|---|---|---|---|
| 03 | Turnover Index for Motor Trade | TABLE 1 | `motor_trade_turnover_index`, `vehicle_sale_turnover_index` |
| 04 | Volume Index for Motor Trade | TABLE 2 | `motor_trade_volume_index`, `vehicle_sale_volume_index` |

Both files are merged on Year/Month. Previously only file 03 was downloaded, leaving the volume columns permanently NULL.

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `Motor Trade Turnover Index` | `motor_trade_turnover_index` | 🔧 Renamed to DB column name |
| `Vehicle Sale Turnover Index` | `vehicle_sale_turnover_index` | 🔧 Renamed to DB column name |
| `*(absent — always NA)*` | `motor_trade_volume_index` | ✅ **Added** — now extracted from file 04 |
| `*(absent — always NA)*` | `vehicle_sale_volume_index` | ✅ **Added** — now extracted from file 04 |

---

## ed_new_residential_building_cost_index
**DB table:** `ed_new_residential_building_cost_index` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (5):** `year`, `quarter`, `overall_cost_index`, `material_costs_index`, `labour_costs_index`

---

## ed_new_residential_buildings_work_categories
**DB table:** `ed_new_residential_buildings_work_categories` on **athena**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `ID` | `ID` |  Unchanged |
| `Year` | `year` | 🔧 Lowercased |
| `Quarter` | `quarter` | 🔧 Lowercased |
| `Overall Index` | `overall_index` | 🔧 Lowercased |
| ` Earth-moving` | `earth_moving` | 🔧 Lowercased + stripped leading space |
| ` Concrete reinforced or not` | `concrete_reinforced` | 🔧 **Fixed** — "or not" suffix broke match |
| ` Wall-building ` | `wall_building` | 🔧 Lowercased |
| ` Plastering` | `plastering` | 🔧 Lowercased |
| ` Electrical installations` | `electrical_installations` | 🔧 Lowercased |
| ` Hydraulic installations` | `hydraulic_installations` | 🔧 Lowercased |
| ` Central heating installations` | `central_heating_installations` | 🔧 Lowercased |
| ` Coverings-Coatings ` | `coverings_coatings` | 🔧 Lowercased |
| ` Carpentry` | `carpentry` | 🔧 Lowercased |
| ` Iron and steel structures` | `iron_steel_structures` | 🔧 **Fixed** — "and" added extra underscore |
| ` Aluminium structures ` | `aluminium_structures` | 🔧 Lowercased |
| ` Painting ` | `painting` | 🔧 Lowercased |
| ` Insulation ` | `insulation` | 🔧 Lowercased |
| ` Glazing ` | `glazing` | 🔧 Lowercased |
| ` Elevators ` | `elevators` | 🔧 Lowercased |
| ` Plaster structures` | `plaster_structures` | 🔧 Lowercased |
| ` Special installations without appliances and accessories ` | `special_installations_without_appliances_accessories` | 🔧 **Fixed** — "and" + trailing space broke match |

---

## ed_office_price_volume_index
**DB table:** `ed_office_price_volume_index` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (10):** `year`, `year_half`, `total_price_index`, `total_rent_index`, `athens_price_index`, `athens_rent_index`, `thessaloniki_price_index`, `thessaloniki_rent_index`, `rest_of_greece_price_index`, `rest_of_greece_rent_index`

---

## ed_residents_di_activity
**DB table:** `ed_residents_di_by_activity` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (6):** `year`, `section_code`, `section_name`, `subsection_code`, `subsection_name`, `amount_millions`

---

## ed_residents_di_country
**DB table:** `ed_residents_di_by_country` on **athena**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `ID` | `ID` |  Unchanged |
| `Year` | `year` | 🔧 Lowercased |
| `Country` | `country` | 🔧 Lowercased |
| `Area` | `area` | 🔧 Lowercased |
| `Amount` | `amount_millions` | 🔧 **Fixed** — missing _millions suffix caused NULL in DB |
| `Continent` | `continent` | 🔧 Lowercased |

---

## ed_retail_price_rental_index
**DB table:** `ed_retail_price_rental_index` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (10):** `year`, `year_half`, `total_price_index`, `total_rent_index`, `athens_price_index`, `athens_rent_index`, `thessaloniki_price_index`, `thessaloniki_rent_index`, `rest_of_greece_price_index`, `rest_of_greece_rent_index`

---

## ed_retail_trade_turnover_index
**DB table:** `ed_retail_trade_turnover_index` on **athena**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `Overall Index` | `overall_index` | 🔧 Renamed to DB column name |
| `Overall index except automotive fuel` | `overall_index_excl_automotive` | 🔧 **Fixed** — full name didn't match abbreviated DB name |
| `Food sector (supermarkets, food,beverages, tobacco)` | `food_sector_index` | 🔧 **Fixed** — verbose label replaced with DB key |
| `Overall index except Food sector and Automotive fuel` | `overall_index_excl_food_sector` | 🔧 **Fixed** |
| `Super Markets` | `supermarkets_index` | 🔧 **Fixed** — missing _index suffix |
| `Department stores` | `department_stores_index` | 🔧 **Fixed** — missing _index suffix |
| `Automotive fuel` | `automotive_fuel_index` | 🔧 **Fixed** — missing _index suffix |
| `Food beverages, tobacco` | `food_beverages_tobacco_index` | 🔧 **Fixed** |
| `Pharmaceutical products, cosmetics` | `pharmaceutical_cosmetics_index` | 🔧 **Fixed** |
| `Clothing and footwear` | `clothing_footwear_index` | 🔧 **Fixed** |
| `Furniture, electrical and household equipment` | `furniture_electrical_household_equipment_index` | 🔧 **Fixed** |
| `Books stationery, other goods` | `books_stationary_other_goods_index` | 🔧 **Fixed** |
| `Retail sale not in stores` | `retail_sale_outside_stores_index` | 🔧 **Fixed** — different wording |

---

## ed_retail_trade_volume_index
**DB table:** `ed_retail_trade_volume_index` on **athena**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `Overall Index` | `overall_index` | 🔧 Renamed to DB column name |
| `Overall index except automotive fuel` | `overall_index_excl_automotive` | 🔧 **Fixed** |
| `Food sector (supermarkets, food,beverages, tobacco)` | `food_sector_index` | 🔧 **Fixed** |
| `Overall index except Food sector and Automotive fuel` | `overall_index_excl_food_sector` | 🔧 **Fixed** |
| `Super Markets` | `supermarkets_index` | 🔧 **Fixed** |
| `Department stores` | `department_stores_index` | 🔧 **Fixed** |
| `Automotive fuel` | `automotive_fuel_index` | 🔧 **Fixed** |
| `Food beverages, tobacco` | `food_beverages_tobacco_index` | 🔧 **Fixed** |
| `Pharmaceutical products, cosmetics` | `pharmaceutical_cosmetics_index` | 🔧 **Fixed** |
| `Clothing and footwear` | `clothing_footwear_index` | 🔧 **Fixed** |
| `Furniture, electrical and household equipment` | `furniture_electrical_household_equipment_index` | 🔧 **Fixed** |
| `Books stationery, other goods` | `books_stationary_other_goods_index` | 🔧 **Fixed** |
| `*(absent)*` | `retail_sale_outside_stores_index` | ✅ Added as NA by design — ELSTAT TABLE 2 does not publish this for volume. DB column exists but stays NULL. |

---

## ed_tourists_arrivals_revenue
**DB table:** `ed_tourists_arrivals_revenue` on **athena**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `ID` | `ID` |  Unchanged |
| `Year` | `year` | 🔧 Lowercased |
| `Quarter` | `quarter` | 🔧 Lowercased |
| `Area` | `area` | 🔧 Lowercased |
| `Country of Origin` | `country_of_origin` | 🔧 Lowercased |
| `Number of Travellers` | `number_of_travellers_000s` | 🔧 **Fixed** — missing _000s suffix caused NULL in DB |
| `Revenues (millions)` | `revenues_by_country_of_origin_millions` | 🔧 **Fixed** — abbreviated name caused NULL in DB |

---

## ed_wage_growth_index
**DB table:** `ed_wage_growth_index` on **athena**

| Before (old header) | After (DB column name) | Note |
|---|---|---|
| `ID` | `ID` |  Unchanged |
| `Year` | `year` | 🔧 Lowercased |
| `Quarter` | `quarter` | 🔧 Lowercased |
| `Mining and Quarrying` | `mining_and_quarrying` | 🔧 Unchanged |
| `Manufacturing` | `manufacturing` | 🔧 Unchanged |
| `Electricity, Gas, Steam and Air Conditioning Supply` | `electricity_gas_steam_air_conditioning_supply` | 🔧 **Fixed** — commas and "and" created extra underscores |
| `Water Supply, Sewerage, Waste Management and Remediation Activities` | `water_supply_sewerage_waste_management_remediation_activities` | 🔧 **Fixed** — "and" created extra underscore |
| `Construction` | `construction` | 🔧 Unchanged |
| `Wholesale and Retal Trade, Repair of Motor Vehicles and Motorcycles` | `wholesale_retail_trade_repair_of_motor_vehicles_motorcycles` | 🔧 **Fixed** — typo "Retal" + "and" broke snake_case match |
| `Transportation and Storage` | `transportation_and_storage` | 🔧 Unchanged |
| `Accommodation and Food Service Activities` | `accommodation_and_food_service_activities` | 🔧 Unchanged |
| `Information and Communication` | `information_and_communication` | 🔧 Unchanged |
| `Professional, Scientific and Technical Activities` | `professional_scientific_and_technical_activities` | 🔧 Unchanged |
| `Administrative and Support Service Activities` | `administrative_and_support_service_activities` | 🔧 Unchanged |

---

## ed_wholesale_trade_turnover_index
**DB table:** `ed_wholesales_turnover_index` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (4):** `year`, `month`, `turnover_index`, `volume_index`

---

## gdp_greece
**DB table:** `gdp_greece` on **athena**

No changes. All deliverable headers already matched DB schema.

**DB columns (6):** `year`, `quarter`, `chain_linked_volumes`, `quarter_over_quarter`, `year_over_year`, `current_prices`

---
