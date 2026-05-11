# Downloader Inventory Report (Technical Detail)

This report provides the exact download location and discovery logic for each pipeline.

## 1. Cyprus (zeus) Pipelines

| Pipeline ID | Country | Method | Discovery and Download Logic |
| :--- | :--- | :--- | :--- |
| `cy_01_average_monthly_earnings` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/el/8.CYSTAT-DB/Labour%20Cost%20and%20Earnings/Earnings/1110010G.px |
| `cy_02_building_permits_by_district` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Building%20Permits/1440010E.px |
| `cy_03_building_permits_by_property_type` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Building%20Permits/1440005E.px |
| `cy_04_construction_index_cy` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Construction/Price%20Index%20of%20Construction%20Materials/1420013E.px |
| `cy_05_consumer_price_index` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Price%20Indices/Consumer%20Price%20Index/0410055E.px |
| `cy_06_economic_forecast_cy` | Cyprus | API | GET request to Eurostat SDMX API: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ec_ef_m |
| `cy_09_gross_value_added_sector` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/National%20Accounts/Annual%20National%20Accounts/0610020E.px |
| `cy_10_lro_contracts_of_sale` | Cyprus | Web Scraper | Scrapes https://portal.dls.moi.gov.cy/stats_category/enimerosi/statistika/politirion-engrafon/ to find the latest monthly page, then searches for PDF links containing 'politirion-engrafon'. |
| `cy_11_lro_transfers` | Cyprus | Web Scraper | Scrapes https://portal.dls.moi.gov.cy/stats_category/enimerosi/statistika/ to find the latest monthly page, then searches for PDF links containing 'metavivaseis'. |
| `cy_12_monthly_gross_earnings_distribution` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Labour%20Cost%20and%20Earnings/Earnings/1110060E.px |
| `cy_13_new_loans_millions` | Cyprus | Web Scraper | Navigates CBC Root -> Year Page (latest) -> Month Page (latest) at https://www.centralbank.cy/en/publications/monetary-and-financial-statistics/ to find the 'MFS' Excel file. |
| `cy_14_per_day_expenditure_of_tourists` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Tourism/Revenue%20from%20Tourism/Monthly/2031024E.px |
| `cy_15_residential_price_indices` | Cyprus | Web Scraper | Scrapes https://www.centralbank.cy/en/publications/residential-property-price-indices to find the latest 'Data series' Excel link. |
| `cy_16_total_households_loans_millions` | Cyprus | Web Scraper | Scrapes https://www.centralbank.cy/en/licensing-supervision/banks/aggregate-cyprus-banking-sector-data to find the latest Excel link containing 'non-performing'. |
| `cy_17_tourist_arrivals_country` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Tourism/Tourists/Monthly/2021010E.px |
| `cy_18_tourist_arrivals_revenue` | Cyprus | API | POST request to CYSTAT PxWeb API: https://cystatdb.cystat.gov.cy/api/v1/en/8.CYSTAT-DB/Tourism/Revenue%20from%20Tourism/Monthly/2031010E.px |

## 2. Greece and EU (athena) Pipelines

| Pipeline ID | Country | Method | Discovery and Download Logic |
| :--- | :--- | :--- | :--- |
| `ed_apartments_price_index_table` | Greece | Web Scraper | Scrapes https://www.bankofgreece.gr/en/statistics/real-estate-market/residential-property-price-indices to find the latest Excel file named 'Indices of residential property prices'. |
| `ed_building_permits_by_no_of_rooms` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SBC03' (Statistics.gr), then finds the Excel file by title matching 'rooms'. |
| `ed_building_permits_table` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SBC03' (Statistics.gr), then finds the primary Excel data series. |
| `ed_construction_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT66' (Statistics.gr), then downloads the main Excel link. |
| `ed_consumer_price_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT87' (Statistics.gr), then finds the Excel file by title matching 'Comparisons of General CPI'. |
| `ed_economic_forecast` | Greece | API | GET request to EU Dissemination API for economic forecast datasets. |
| `ed_employment` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SJO02' (Statistics.gr), then downloads the main monthly Excel file. |
| `ed_eu_consumer_confidence_index` | EU | API | GET request to Eurostat SDMX API: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/ei_bsco_m |
| `ed_eu_gdp` | EU | API | GET request to Eurostat SDMX API: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/namq_10_gdp |
| `ed_eu_hicp` | EU | API | GET request to Eurostat SDMX API: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hicp_manr |
| `ed_eu_unemployment_rate` | EU | API | GET request to Eurostat SDMX API: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/une_rt_m |
| `ed_fdi_activity` | Greece | Direct File | Downloads static Excel file: https://www.bankofgreece.gr/RelatedDocuments/BPM6_FDI_HOME_BY_ACTIVITY.xls |
| `ed_fdi_country` | Greece | Direct File | Downloads static Excel file: https://www.bankofgreece.gr/RelatedDocuments/BPM6_FDI_HOME_BY_COUNTRY.xls |
| `ed_fdi_real_estate` | Greece | Direct File | Downloads static Excel file: https://www.bankofgreece.gr/RelatedDocuments/EN_Direct_Investment_in_Greece_in_Real_Estate.xlsx |
| `ed_geo_distribution_of_issued_and_pending_permits` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest PDF publication containing geographic distribution data. |
| `ed_gross_fixed_capital_formation` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SEL15' (Statistics.gr), then downloads the Excel file. |
| `ed_gva_by_sector` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SEL15' (Statistics.gr), then downloads the Excel file. |
| `ed_household_income_allocation` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SEL15' (Statistics.gr), then downloads the Excel file. |
| `ed_housing_finances` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SEL15' (Statistics.gr), then downloads the Excel file. |
| `ed_imports_exports_millions` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SFA01' (Statistics.gr), then downloads the main Excel file. |
| `ed_industrial_production_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT21' (Statistics.gr), then downloads the main Excel file. |
| `ed_key_partners_primary_goods` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SFA01' (Statistics.gr), then downloads the Excel file. |
| `ed_loan_amounts_millions` | Greece | Web Scraper | Scrapes https://www.bankofgreece.gr/en/statistics/financial-markets-and-interest-rates/bank-deposit-and-loan-interest-rates to find the latest 'Table 1' or 'Table 6.3' Excel file. |
| `ed_loan_interest_rates` | Greece | Direct File | Downloads static Excel file: https://www.bankofgreece.gr/RelatedDocuments/Rates_TABLE_1+1a.xls |
| `ed_motor_trade_turnover_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT42' (Statistics.gr), then downloads the main Excel file. |
| `ed_motor_trade_volume_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT42' (Statistics.gr), then downloads the main Excel file. |
| `ed_new_built_properties_per_region` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SBC03' (Statistics.gr), then downloads the Excel file. |
| `ed_new_establishments_building_permits` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SBC03' (Statistics.gr), then downloads the Excel file. |
| `ed_new_residential_building_cost_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT60' (Statistics.gr), then downloads the main Excel file. |
| `ed_new_residential_buildings_work_categories` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT60' (Statistics.gr), then downloads the main Excel file. |
| `ed_office_price_volume_index` | Greece | Web Scraper | Scrapes https://www.bankofgreece.gr/en/statistics/real-estate-market/commercial-property-indices to find the latest PDF publication. |
| `ed_residence_permits_aggregate` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest monthly PDF publication. |
| `ed_residence_permits_application` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest monthly PDF publication. |
| `ed_residence_permits_current` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest monthly PDF publication. |
| `ed_residence_permits_golden_visa` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest monthly PDF publication. |
| `ed_residence_permits_issued` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest monthly PDF publication. |
| `ed_residence_permits_top10_countries` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest monthly PDF publication. |
| `ed_residence_permits_top10_countries_golden_visa` | Greece | Web Scraper | Scrapes https://migration.gov.gr/statistika/ to find the latest monthly PDF publication. |
| `ed_residents_di_activity` | Greece | Direct File | Downloads static Excel file: https://www.bankofgreece.gr/RelatedDocuments/BPM6_FDI_ABROAD_BY_ACTIVITY.xls |
| `ed_residents_di_country` | Greece | Direct File | Downloads static Excel file: https://www.bankofgreece.gr/RelatedDocuments/BPM6_FDI_ABROAD_BY_COUNTRY.xls |
| `ed_retail_price_rental_index` | Greece | Web Scraper | Scrapes https://www.bankofgreece.gr/en/statistics/real-estate-market/commercial-property-indices to find the latest PDF publication. |
| `ed_retail_trade_turnover_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT39' (Statistics.gr), then downloads the main Excel file. |
| `ed_retail_trade_volume_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT39' (Statistics.gr), then downloads the main Excel file. |
| `ed_services_sector_turnover_monthly_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT48' (Statistics.gr), then downloads the main Excel file. |
| `ed_tourists_arrivals_revenue` | Greece | Web Scraper | Scrapes https://www.bankofgreece.gr/en/statistics/external-sector/balance-of-payments/travel-services to find the latest monthly Excel link. |
| `ed_wage_growth_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT12' (Statistics.gr), then downloads the main Excel file. |
| `ed_wholesale_trade_turnover_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT45' (Statistics.gr), then downloads the main Excel file. |
| `ed_wholesale_trade_volume_index` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'DKT45' (Statistics.gr), then downloads the main Excel file. |
| `gdp_greece` | Greece | Web Scraper | Uses ELSTAT automation to find the latest publication under code 'SEL84' (Statistics.gr), then downloads the quarterly Excel data. |

▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
 > write it exaclty as written here  1. ELSTAT (statistics.gr) - Dynamic Scraper
     The ELSTAT downloader is the most complex because the website changes its URL structure for every new publication.
      * Step 1: Locate Publication: The script starts with a "Publication Code" (e.g., DKT87 for CPI). It hits a search results page on statistics.gr and uses 
   a custom parser to find the latest "Publication Date."
      * Step 2: Enter Publication Page: It follows the link to the specific page for that month's release (e.g., "Consumer Price Index - October 2025").       
      * Step 3: Title Search: On that specific page, it scans all download links for a specific substring (e.g., "Comparisons of General CPI").
      * Step 4: Download: Once the matching .xls or .xlsx link is found, it performs a standard GET request to download the file.
                                                                                                                                                               
                                                                                                                                                               
     2. migration.gov.gr - PDF Scraper
     The migration downloader focuses on extracting official reports published as PDFs.
      * Step 1: Landing Page: It hits the central statistics page: https://migration.gov.gr/statistika/.
      * Step 2: Text Matching: It uses BeautifulSoup to scan the page for anchor tags (<a>). It looks for specific Greek or English keywords in the link text  
   (e.g., "Γεωγραφική Διάρθρωση" or "Monthly Reports").
      * Step 3: Extension Check: It ensures the link ends in .pdf.
      * Step 4: Download: It follows the direct link to the Greek government's content delivery network (CDN) to fetch the file.
                                                                                                                                                               
                                                                                                                                                               
     3. Bank of Greece (bankofgreece.gr) - Static & Scraper Mix
     The Bank of Greece (BoG) uses two different methods depending on the data type:
      * Method A (Static): For many tables (like FDI), BoG uses permanent, fixed URLs (e.g., .../Rates_TABLE_1+1a.xls). In these cases, the downloader simply  
   performs a direct GET request.
      * Method B (Scraper): For Real Estate and Loans, it hits a landing page (e.g., the Real Estate Market page). It searches for a specific header or link   
   title (e.g., "Indices of residential property prices") and extracts the current URL for the Excel file, as they sometimes change the filename when updating 
   the data.
                                                                                                                                                               
                                                                                                                                                               
     4. API Requests (CYSTAT & Eurostat) - Direct Query
     This method is the most reliable because it does not rely on "finding" a link; it requests data directly from a server.
      * CYSTAT (Cyprus): Uses the PxWeb API. The script sends a POST request to a specific dataset endpoint (e.g., 0410055E.px). Inside the request, it sends  
   a JSON Query that tells the server exactly which indicators, years, and categories it wants. The server then streams back a CSV or JSON file.
      * Eurostat (EU): Uses the SDMX-REST API. The script constructs a long URL containing the dataset code and filters (e.g.,
   .../data/namq_10_gdp/Q.CLV10_MEUR.SCA.B1GQ.EL+CY...). It performs a GET request, and the Eurostat server returns the data immediately.
                                                                                                                                                               
                                                                                                                                                               
     5. CBC (centralbank.cy) - Multi-Level Scraper
     The Central Bank of Cyprus (CBC) downloader is a "walker" that navigates a folder-like web structure.
      * Step 1: Root Navigation: It starts at a category landing page (e.g., Monetary and Financial Statistics).
      * Step 2: Year/Month Traversal: It looks for links matching the latest Year (e.g., "Year 2026"). It then clicks into that year and looks for the latest  
   available Month (e.g., "January").
      * Step 3: Keyword Search: Once on the Month page, it looks for links containing keywords like "MFS" or "Non-performing loans" and ending in .xls.        
      * Step 4: Download: It prepends the base domain https://www.centralbank.cy to the relative path found and downloads the Excel file.
                                                                                                                                                               
     Summary Table of Methods
                                                                                                                                                               
                                                                                                                                                               
                                                                                                                                                               
     ┌───────────┬─────────────────────┬───────────────────────────────────────────┐
     │ Source    │ Method              │ Key Technology                            │
     ├───────────┼─────────────────────┼───────────────────────────────────────────┤
     │ ELSTAT    │ Dynamic Scraper     │ BeautifulSoup + Publication Code Search   │
     │ Migration │ PDF Scraper         │ Link Text Keyword Matching                │
     │ BoG       │ Static/Scraper      │ Direct URL or Title-based Link Extraction │
     │ CYSTAT    │ API                 │ JSON POST Requests (PxWeb)                │
     │ Eurostat  │ API                 │ Structured REST GET Requests (SDMX)       │
     │ CBC       │ Multi-Level Scraper │ Recursive Year/Month Page Navigation      │
     └───────────┴─────────────────────┴─────────────────────────────────────────── very straighforwards
