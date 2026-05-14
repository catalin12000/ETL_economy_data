import pandas as pd

# Mapping of all 65 pipelines with their metadata - CORRECTED
data = [
    # 10 Automated Pipelines (Urgent)
    ["ed_consumer_price_index", "Automated", "Excel", "elstat_consumer_price_index.xls"],
    ["ed_employment", "Automated", "Excel", "ed_employment.xls"],
    ["gdp_greece", "Automated", "Excel", "elstat_gdp_greece.xls"],
    ["ed_loan_interest_rates", "Automated", "Excel", "Rates_TABLE_1+1a.xls"],
    ["ed_loan_amounts_millions", "Automated", "Excel", "Rates_TABLE_1+1a.xls"],
    ["cy_13_new_loans_millions", "Automated", "Excel", "cbc_mfs_monetary_statistics.xls"],
    ["ed_eu_consumer_confidence_index", "Automated", "CSV", "eurostat_ei_bsco_m.csv"],
    ["ed_eu_gdp", "Automated", "CSV", "eurostat_namq_10_gdp.csv"],
    ["ed_eu_hicp", "Automated", "CSV", "eurostat_prc_hicp_manr.csv"],
    ["ed_eu_unemployment_rate", "Automated", "CSV", "eurostat_une_rt_m.csv"],

    # PDF Based (13 Pipelines)
    ["ed_apartments_price_index_table", "Not Automated", "PDF", "Neoi_Pinakes_Timon_Katoikion_full.pdf"],
    ["ed_geo_distribution_of_issued_and_pending_permits", "Not Automated", "PDF", "migration_appendix_b.pdf"],
    ["ed_residence_permits_aggregate", "Not Automated", "PDF", "migration_appendix_b.pdf (Shared)"],
    ["ed_residence_permits_application", "Not Automated", "PDF", "migration_appendix_b.pdf (Shared)"],
    ["ed_residence_permits_current", "Not Automated", "PDF", "migration_appendix_b.pdf (Shared)"],
    ["ed_residence_permits_golden_visa", "Not Automated", "PDF", "migration_appendix_b.pdf (Shared)"],
    ["ed_residence_permits_issued", "Not Automated", "PDF", "migration_appendix_b.pdf (Shared)"],
    ["ed_residence_permits_top10_countries", "Not Automated", "PDF", "migration_appendix_b.pdf (Shared)"],
    ["ed_residence_permits_top10_countries_golden_visa", "Not Automated", "PDF", "migration_appendix_b.pdf (Shared)"],
    ["ed_office_price_volume_index", "Not Automated", "PDF", "Office_Price_Index.pdf / Office_Rent_Index.pdf"],
    ["ed_retail_price_rental_index", "Not Automated", "PDF", "Retail_Price_Index.pdf / Retail_Rent_Index.pdf"],
    ["cy_10_lro_contracts_of_sale", "Not Automated", "PDF", "contracts_of_sale_latest.pdf"],
    ["cy_11_lro_transfers", "Not Automated", "PDF", "lro_transfers_latest.pdf"],

    # HTML Based (2 Pipelines)
    ["ed_economic_forecast", "Not Automated", "HTML", "economic_forecast_greece.html"],
    ["cy_06_economic_forecast_cy", "Not Automated", "HTML", "economic_forecast_cyprus.html"],

    # Excel Based (30 Remaining + 10 Automated = 40 Total)
    ["ed_industrial_production_index", "Not Automated", "Excel", "industrial_production_index.xls"],
    ["ed_retail_trade_turnover_index", "Not Automated", "Excel", "elstat_retail_turnover.xls"],
    ["ed_retail_trade_volume_index", "Not Automated", "Excel", "elstat_retail_volume.xls"],
    ["ed_wholesale_trade_turnover_index", "Not Automated", "Excel", "elstat_wholesale_turnover.xls"],
    ["ed_wholesale_trade_volume_index", "Not Automated", "Excel", "elstat_wholesale_volume.xls"],
    ["ed_motor_trade_turnover_index", "Not Automated", "Excel", "elstat_motor_trade_turnover.xls"],
    ["ed_motor_trade_volume_index", "Not Automated", "Excel", "elstat_motor_trade_volume.xls"],
    ["ed_services_sector_turnover_monthly_index", "Not Automated", "Excel", "elstat_services_turnover.xls"],
    ["ed_building_permits_table", "Not Automated", "Excel", "elstat_building_permits.xls"],
    ["ed_building_permits_by_no_of_rooms", "Not Automated", "Excel", "elstat_building_permits_rooms.xls"],
    ["ed_construction_index", "Not Automated", "Excel", "elstat_construction_index.xls"],
    ["ed_fdi_activity", "Not Automated", "Excel", "BPM6_FDI_HOME_BY_ACTIVITY.xls"],
    ["ed_fdi_country", "Not Automated", "Excel", "BPM6_FDI_HOME_BY_COUNTRY.xls"],
    ["ed_fdi_real_estate", "Not Automated", "Excel", "Direct_Investment_in_Real_Estate.xls"],
    ["ed_gross_fixed_capital_formation", "Not Automated", "Excel", "elstat_gross_fixed_capital.xls"],
    ["ed_gva_by_sector", "Not Automated", "Excel", "ed_gva_by_sector.xls"],
    ["ed_household_income_allocation", "Not Automated", "Excel", "elstat_household_income.xls"],
    ["ed_housing_finances", "Not Automated", "Excel", "elstat_housing_finances.xls"],
    ["ed_imports_exports_millions", "Not Automated", "Excel", "elstat_imports_exports.xls"],
    ["ed_key_partners_primary_goods", "Not Automated", "Excel", "elstat_trade_balance.xls"],
    ["ed_new_built_properties_per_region", "Not Automated", "Excel", "elstat_new_built_properties.xls"],
    ["ed_new_establishments_building_permits", "Not Automated", "Excel", "elstat_new_establishments.xls"],
    ["ed_new_residential_building_cost_index", "Not Automated", "Excel", "elstat_residential_build_cost.xls"],
    ["ed_new_residential_buildings_work_categories", "Not Automated", "Excel", "elstat_residential_work_categories.xls"],
    ["ed_residents_di_activity", "Not Automated", "Excel", "BPM6_FDI_ABROAD_BY_ACTIVITY.xls"],
    ["ed_residents_di_country", "Not Automated", "Excel", "BPM6_FDI_ABROAD_BY_COUNTRY.xls"],
    ["ed_tourists_arrivals_revenue", "Not Automated", "Excel", "Receipts_By_Country_Of_Residence.xls"],
    ["ed_wage_growth_index", "Not Automated", "Excel", "elstat_wage_growth.xls"],
    ["cy_15_residential_price_indices", "Not Automated", "Excel", "cbc_rppi_data_series.xls"],
    ["cy_16_total_households_loans_millions", "Not Automated", "Excel", "cbc_aggregate_npls.xlsx"],
    ["cy_17_tourist_arrivals_country", "Not Automated", "Excel", "cystat_tourist_arrivals.xlsx"],

    # CSV Based (6 Remaining + 4 Automated = 10 Total)
    ["cy_01_average_monthly_earnings", "Not Automated", "CSV", "cystat_earnings.csv"],
    ["cy_02_building_permits_by_district", "Not Automated", "CSV", "cystat_building_permits_district.csv"],
    ["cy_03_building_permits_by_property_type", "Not Automated", "CSV", "cystat_building_permits_type.csv"],
    ["cy_04_construction_index_cy", "Not Automated", "CSV", "cystat_construction_materials.csv"],
    ["cy_05_consumer_price_index", "Not Automated", "CSV", "cystat_cpi.csv"],
    ["cy_09_gross_value_added_sector", "Not Automated", "CSV", "cystat_gva_sector_annual.csv"],
    ["cy_12_monthly_gross_earnings_distribution", "Not Automated", "CSV", "cystat_earnings_distribution.csv"],
    ["cy_14_per_day_expenditure_of_tourists", "Not Automated", "CSV", "cystat_tourist_expenditure.csv"],
    ["cy_18_tourist_arrivals_revenue", "Not Automated", "CSV", "cystat_tourist_revenue.csv"]
]

df = pd.DataFrame(data, columns=["Pipeline Name", "Automated Flag", "File Type", "Filename for Extraction"])

output_file = "PIPELINE_FILE_MAPPING.xlsx"

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df.to_excel(writer, index=False, sheet_name='File Mapping')
    ws = writer.sheets['File Mapping']
    ws.column_dimensions['A'].width = 50
    ws.column_dimensions['B'].width = 20
    ws.column_dimensions['C'].width = 15
    ws.column_dimensions['D'].width = 60
    
    from openpyxl.styles import Font, PatternFill
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill

print(f"Updated {output_file} with HTML types and final counts.")
