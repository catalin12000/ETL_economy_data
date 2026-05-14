# Codex Pipeline Dashboard

Generated at: 2026-02-24 16:43:38 +0200

## Summary

- Database policy: **READ-ONLY ONLY** (no writes to Postgres).
- Pipeline directories under `etl/pipelines`: **67**
- Runnable pipeline modules (`pipeline.py` exists): **65**
- Missing `pipeline.py`: **2**
  - `__pycache__`
  - `cy_08_employment_cy`

### Phase-Status Snapshot

| Phase Status | Count |
|---|---:|
| Good | 65 |
| Blocked (config) | 0 |
| Broken | 0 |
| Not run yet | 0 |

### Technical-Status Snapshot

| Technical Status | Count |
|---|---:|
| delivered | 30 |
| skipped | 35 |

### Implementation Breakdown

| Implementation Status | Count |
|---|---:|
| Fully implemented (extract + local compare + live Postgres diff) | 28 |
| Implemented (extract + local baseline compare only) | 3 |
| Implemented (extract + semantic data hash) | 2 |
| Placeholder dependent (shared source tracked, extraction pending) | 7 |
| Downloader/source-only (no extraction or compare yet) | 25 |

### Error Context

- Source/link/file-not-found errors: **0**
- DB-config-blocked pipelines: **0**

## Pipeline Status Table

| Pipeline ID | Folder | Implementation Status | Technical Status | Phase Status | Phase Detail | Last Run (UTC) | Last Message | Notes |
|---|---|---|---|---|---|---|---|---|
| cy_01_average_monthly_earnings | cy_01_average_monthly_earnings | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:42:41.361336+00:00 | Extracted 273 rows. DB (zeus) Comparison: 251 missing, 21 diff. File: deliverable_cy_01_average_monthly_ear... | - |
| cy_02_building_permits_by_district | cy_02_building_permits_by_district | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:09:31.689510+00:00 | Extracted 2740 rows. DB (zeus) Comparison: 2060 missing, 0 diff. File: deliverable_cy_02_building_permits_b... | - |
| cy_03_building_permits_by_property_type | cy_03_building_permits_by_property_type | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:09:33.751387+00:00 | Extracted 1096 rows. DB Comparison result ready. File: deliverable_cy_03_building_permits_by_property_type_... | - |
| cy_04_construction_index_cy | cy_04_construction_index_cy | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:47:54.198762+00:00 | Extracted 481 rows. DB (zeus) Comparison: 377 missing, 104 diff. File: deliverable_cy_04_construction_index... | - |
| cy_05_consumer_price_index | cy_05_consumer_price_index | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:09:35.203688+00:00 | Extracted 1817 rows. DB Comparison: None missing, None diff. | - |
| cy_09_gross_value_added_sector | cy_09_gross_value_added_sector | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:54:50.604577+00:00 | Extracted 630 rows. DB (zeus) Comparison: 2 missing, 28 diff. File: deliverable_cy_09_gross_value_added_sec... | - |
| cy_11_lro_transfers | cy_11_lro_transfers | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:09:41.497630+00:00 | Extracted 5 rows. DB Comparison: 5 missing, 0 diff. | - |
| cy_12_monthly_gross_earnings_distribution | cy_12_monthly_gross_earnings_distribution | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T10:02:08.199023+00:00 | Extracted 1512 rows. DB (zeus) Comparison: 48 missing, 0 diff. File: deliverable_cy_12_monthly_gross_earnin... | - |
| cy_13_new_loans_millions | cy_13_new_loans_millions | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:09:45.450479+00:00 | Extracted 242 rows. DB (zeus) Comparison: 147 missing, 72 diff. File: deliverable_cy_13_new_loans_millions_... | - |
| cy_15_residential_price_indices | cy_15_residential_price_indices | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:09:47.424664+00:00 | Extracted 79 rows. DB (zeus) Comparison ready. File: deliverable_cy_15_residential_price_indices_February_2... | - |
| cy_16_total_households_loans_millions | cy_16_total_households_loans_millions | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:09:49.661022+00:00 | Extracted 97 rows. DB (zeus) Comparison: 31 missing, 66 diff. File: deliverable_cy_16_total_households_loan... | - |
| cy_17_tourist_arrivals_country | cy_17_tourist_arrivals_country | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T10:23:00.980242+00:00 | Extracted 19652 rows. DB (zeus) Comparison: 220 missing, 69 diff. File: deliverable_cy_17_tourist_arrivals_... | - |
| cy_18_tourist_arrivals_revenue | cy_18_tourist_arrivals_revenue | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T10:28:59.969135+00:00 | Extracted 299 rows. DB (zeus) Comparison: 232 missing, 0 diff. File: deliverable_cy_18_tourist_arrivals_rev... | - |
| ed_building_permits_table | ed_building_permits_table | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T10:43:29.171199+00:00 | Extracted 223 rows. DB (athena) Comparison: 3 missing, 0 diff. File: deliverable_ed_building_permits_table_... | - |
| ed_construction_index | ed_construction_index | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T11:23:12.736658+00:00 | Extracted 19 rows. DB (athena) Comparison: 1 missing, 2 diff. File: deliverable_ed_construction_index_Febru... | - |
| ed_consumer_price_index | ed_consumer_price_index | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:03.789185+00:00 | Extracted 301 rows. DB Comparison: 1 missing, 8 diff. File: deliverable_ed_consumer_price_index_February_20... | - |
| ed_employment | ed_employment | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:11.251106+00:00 | Extracted 528 rows. DB Comparison: 2 missing, 331 diff. File: deliverable_ed_employment_February_2026.csv | - |
| ed_eu_consumer_confidence_index | ed_eu_consumer_confidence_index | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:13.006412+00:00 | Extracted 64 rows. DB Comparison: 4 missing, 19 diff. File: deliverable_ed_eu_consumer_confidence_index_Feb... | - |
| ed_eu_gdp | ed_eu_gdp | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:14.713183+00:00 | Extracted 55 rows. DB Comparison: 6 missing, 42 diff. File: deliverable_ed_eu_gdp_February_2026.csv | - |
| ed_eu_hicp | ed_eu_hicp | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:16.357566+00:00 | Extracted 60 rows. DB Comparison: 24 missing, 0 diff. File: deliverable_ed_eu_hicp_February_2026.csv | - |
| ed_eu_unemployment_rate | ed_eu_unemployment_rate | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:18.009039+00:00 | Extracted 60 rows. DB Comparison: 5 missing, 35 diff. File: deliverable_ed_eu_unemployment_rate_February_20... | - |
| ed_fdi_activity | ed_fdi_activity | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T11:34:53.042743+00:00 | Extracted 430 rows. DB (athena) Comparison: 0 missing, 6 diff. File: deliverable_ed_fdi_activity_February_2... | - |
| ed_fdi_country | ed_fdi_country | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T11:45:51.323386+00:00 | Extracted 1305 rows. DB (athena) Comparison: 0 missing, 2 diff. File: deliverable_ed_fdi_country_February_2... | - |
| ed_fdi_real_estate | ed_fdi_real_estate | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T11:52:49.040753+00:00 | Extracted 240 rows. DB (athena) Comparison: 0 missing, 0 diff. File: deliverable_ed_fdi_real_estate_Februar... | - |
| ed_gross_fixed_capital_formation | ed_gross_fixed_capital_formation | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T12:01:53.476413+00:00 | Extracted 62 rows. DB (athena) Comparison: 4 missing, 53 diff. File: deliverable_ed_gross_fixed_capital_for... | - |
| ed_loan_amounts_millions | ed_loan_amounts_millions | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:40.624202+00:00 | Extracted 3080 rows. DB Comparison: 2075 missing, 38 diff. File: deliverable_ed_loan_amounts_millions_Febru... | - |
| ed_loan_interest_rates | ed_loan_interest_rates | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:10:43.715989+00:00 | Extracted 5040 rows. DB Comparison: 3330 missing, 137 diff. File: deliverable_ed_loan_interest_rates_Februa... | - |
| gdp_greece | gdp_greece | Fully implemented (extract + local compare + live Postgres diff) | delivered | Good | Pipeline completed expected DB read-only comparison flow. | 2026-02-24T09:11:19.426130+00:00 | Extracted 124 rows. DB Comparison: 81 missing, 36 diff. File: deliverable_gdp_greece_February_2026.csv | - |
| cy_14_per_day_expenditure_of_tourists | cy_14_per_day_expenditure_of_tourists | Implemented (extract + local baseline compare only) | delivered | Good | Pipeline completed expected phase flow. | 2026-02-24T10:13:11.872956+00:00 | Extracted 5145 rows. DB compare skipped (table not created). File: deliverable_cy_14_per_day_expenditure_of... | - |
| ed_apartments_price_index_table | ed_apartments_price_index_table | Implemented (extract + local baseline compare only) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:09:51.152467+00:00 | No new file detected (same file SHA256). | - |
| ed_gva_by_sector | ed_gva_by_sector | Implemented (extract + local baseline compare only) | delivered | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:26.287892+00:00 | Extracted 1980 rows. 1980 new rows. | - |
| cy_06_economic_forecast_cy | cy_06_economic_forecast_cy | Implemented (extract + semantic data hash) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:09:35.627590+00:00 | No change detected in EU forecast page. | - |
| ed_eu_economic_forecast_greece | ed_economic_forecast | Implemented (extract + semantic data hash) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:04.162488+00:00 | No change detected in EU forecast page (same file SHA256). | Folder=ed_economic_forecast, pipeline_id=ed_eu_economic_forecast_greece |
| ed_residence_permits_aggregate | ed_residence_permits_aggregate | Placeholder dependent (shared source tracked, extraction pending) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.046264+00:00 | Source Appendix B PDF unchanged. | - |
| ed_residence_permits_application | ed_residence_permits_application | Placeholder dependent (shared source tracked, extraction pending) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.046264+00:00 | Source Appendix B PDF unchanged. | - |
| ed_residence_permits_current | ed_residence_permits_current | Placeholder dependent (shared source tracked, extraction pending) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.046264+00:00 | Source Appendix B PDF unchanged. | - |
| ed_residence_permits_golden_visa | ed_residence_permits_golden_visa | Placeholder dependent (shared source tracked, extraction pending) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.046264+00:00 | Source Appendix B PDF unchanged. | - |
| ed_residence_permits_issued | ed_residence_permits_issued | Placeholder dependent (shared source tracked, extraction pending) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.046264+00:00 | Source Appendix B PDF unchanged. | - |
| ed_residence_permits_top10_countries | ed_residence_permits_top10_countries | Placeholder dependent (shared source tracked, extraction pending) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.046264+00:00 | Source Appendix B PDF unchanged. | - |
| ed_residence_permits_top10_countries_golden_visa | ed_residence_permits_top10_countries_golden_visa | Placeholder dependent (shared source tracked, extraction pending) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.046264+00:00 | Source Appendix B PDF unchanged. | - |
| cy_10_lro_contracts_of_sale | cy_10_lro_contracts_of_sale | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:09:38.814157+00:00 | Both DLS PDFs are unchanged. | - |
| ed_building_permits_by_no_of_rooms | ed_building_permits_by_no_of_rooms | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T10:35:33.247089+00:00 | No new file detected (same file SHA256). | - |
| ed_geo_distribution_of_issued_and_pending_permits | ed_geo_distribution_of_issued_and_pending_permits | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:21.377206+00:00 | No new file detected (same file SHA256). | - |
| ed_household_income_allocation | ed_household_income_allocation | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:28.306792+00:00 | No new file detected (same file SHA256). | - |
| ed_housing_finances | ed_housing_finances | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:30.530296+00:00 | No new file detected (same file SHA256). | - |
| ed_imports_exports_millions | ed_imports_exports_millions | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:32.503383+00:00 | No new file detected (same file SHA256). | - |
| ed_industrial_production_index | ed_industrial_production_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:36.123667+00:00 | No new files detected (both SHA256 unchanged). | - |
| ed_key_partners_primary_goods | ed_key_partners_primary_goods | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:38.527241+00:00 | No new file detected (same file SHA256). | - |
| ed_motor_trade_turnover_index | ed_motor_trade_turnover_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:45.590068+00:00 | No new file detected (same file SHA256). | - |
| ed_motor_trade_volume_index | ed_motor_trade_volume_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:47.486679+00:00 | No new file detected (same file SHA256). | - |
| ed_new_built_properties_per_region | ed_new_built_properties_per_region | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:49.230797+00:00 | No new file detected. | - |
| ed_new_establishments_building_permits | ed_new_establishments_building_permits | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:52.724136+00:00 | No new file detected. | - |
| ed_new_residential_building_cost_index | ed_new_residential_building_cost_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:54.668469+00:00 | No new file detected. | - |
| ed_new_residential_buildings_work_categories | ed_new_residential_buildings_work_categories | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:56.878465+00:00 | No new file detected. | - |
| ed_office_price_volume_index | ed_office_price_volume_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.014627+00:00 | Both Office PDFs are unchanged. | - |
| ed_residents_di_activity | ed_residents_di_activity | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.523699+00:00 | No new file detected (same file SHA256). | - |
| ed_residents_di_country | ed_residents_di_country | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:58.935374+00:00 | No new file detected (same file SHA256). | - |
| ed_retail_price_rental_index | ed_retail_price_rental_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:10:59.946346+00:00 | Both Retail PDFs are unchanged. | - |
| ed_retail_trade_turnover_index | ed_retail_trade_turnover_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:11:01.952486+00:00 | No new file detected (same file SHA256). | - |
| ed_retail_trade_volume_index | ed_retail_trade_volume_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:11:04.470400+00:00 | No new file detected (same file SHA256). | - |
| ed_services_sector_turnover_monthly_index | ed_services_sector_turnover_monthly_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:11:07.110397+00:00 | No new file detected. | - |
| ed_tourists_arrivals_revenue | ed_tourists_arrivals_revenue | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:11:08.222056+00:00 | Both Travel spreadsheets are unchanged. | - |
| ed_wage_growth_index | ed_wage_growth_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:11:10.267739+00:00 | No new file detected for ed_wage_growth_index (same file SHA256). | - |
| ed_wholesale_trade_turnover_index | ed_wholesale_trade_turnover_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:11:12.435175+00:00 | No new file detected (same file SHA256). | - |
| ed_wholesale_trade_volume_index | ed_wholesale_trade_volume_index | Downloader/source-only (no extraction or compare yet) | skipped | Good | Pipeline completed expected phase flow. | 2026-02-24T09:11:15.388019+00:00 | No new file detected (same file SHA256). | - |
