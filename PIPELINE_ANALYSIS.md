# Pipeline Architecture Analysis

This document details the implementation patterns for all data pipelines in the `ETL_economy_data` project. It serves as a guide for understanding existing logic and creating new pipelines.

## 1. Type 1: ELSTAT Dynamic Download
These pipelines handle datasets from the Hellenic Statistical Authority (ELSTAT). Since ELSTAT publishes data on new pages (e.g., a new URL for each month/quarter), these pipelines dynamically resolve the latest publication URL.

**Core Logic:**
1.  **Resolve Page:** Uses `get_latest_publication_url` (for monthly/quarterly) or `get_latest_publication_year_url` (for annual) to find the current period's page.
2.  **Find File:** Scrapes the resolved page for a specific file title (exact or substring match) using `get_download_url_by_title`.
3.  **Download:** Downloads the file to `data/downloads/<id>/`.
4.  **State:** Tracks `file_sha256` to detect changes.

**Implemented Pipelines:**
*   **`ed_consumer_price_index`** (Monthly) - *Inflation data.*
*   **`ed_building_permits_table`** (Monthly) - *Construction permits.*
*   **`ed_construction_index`** (Quarterly) - *Production index in construction.*
*   **`ed_employment`** (Monthly) - *Unemployment rates.*
*   **`ed_imports_exports_millions`** (Annual) - *Trade balance.*
*   **`ed_key_partners_primary_goods`** (Monthly) - *Trade partners (SFC02).*
*   **`gdp_greece`** (Quarterly) - *GDP growth (SEL84).*
*   **`ed_gva_by_sector`** (Annual) - *Gross Value Added (SEL12).*
*   **`ed_wage_growth_index`** (Quarterly) - *Wage indices (DKT03).*
*   **`ed_household_income_allocation`** (Quarterly/Annual) - *Gross Savings (SEL60).*
*   **`ed_housing_finances`** (Quarterly) - *Disposable Income (SEL95).*

## 2. Type 2: Static URL Download
These pipelines download files from stable, permanent URLs (permalinks). Commonly used for Bank of Greece (BoG) datasets or EU Commission pages.

**Core Logic:**
1.  **Define URL:** Uses a constant `FILE_URL`.
2.  **Download:** Downloads directly.
3.  **State:** Tracks `file_sha256`.

**Implemented Pipelines:**
*   **`ed_fdi_activity`** (BoG) - *Inward FDI by Activity.*
*   **`ed_fdi_country`** (BoG) - *Inward FDI by Country.*
*   **`ed_fdi_real_estate`** (BoG) - *Real Estate FDI.*
*   **`ed_residents_di_activity`** (BoG) - *Outward FDI by Activity.*
*   **`ed_residents_di_country`** (BoG) - *Outward FDI by Country.*
*   **`ed_loan_amounts_millions`** (BoG) - *Housing/Consumer Loans.*
*   **`ed_loan_interest_rates`** (BoG) - *Interest Rates.*
*   **`ed_eu_economic_forecast_greece`** (EU Commission) - *Downloads HTML page, extracts table to CSV.*

## 3. Type 3: Extraction & Sync (Complex)
These pipelines handle data locked in complex formats (like PDF tables) and require historical tracking. They don't just download; they extract, compare with a "database" file, and update it.

**Core Logic:**
1.  **Download:** Fetches the raw file (PDF/Excel).
2.  **Extract:** Custom Python logic extracts a clean DataFrame.
3.  **Hash Data:** Hashes the *content* of the DataFrame (`data_sha256`) to ignore file metadata changes.
4.  **Sync:** Compares the extracted data with a local Master Excel file (`data/db/`).
5.  **Report:** specific report on what rows were added or updated.

**Implemented Pipelines:**
*   **`ed_apartments_price_index_table`** (BoG) - *Extracts indices from PDF.*

## 4. Type 4: Dependent / Cluster Pipelines
These pipelines are part of a group that shares a single source file. One "Master" pipeline downloads the file, and "Satellite" pipelines extract different parts of it.

**Core Logic:**
1.  **Check Master:** Checks if the source pipeline (`SOURCE_PIPELINE_ID`) has a new file.
2.  **Reuse:** Reads the file downloaded by the master pipeline.
3.  **Extract:** Processes its specific table/indicator.

**Implemented Pipelines:**
*   **`ed_geo_distribution_of_issued_and_pending_permits`** (Master) - *Downloads the "Appendix B" PDF.*
*   **`ed_residence_permits_aggregate`** (Satellite) - *Extracts Table 1.*
*   *Other Residence Permit pipelines follow this pattern.*

---

## Shared Source Clusters (Master-Satellite Pattern)

Many indicators share the same raw source file. In these cases, we designate one pipeline as the "Master" (responsible for the download) and others as "Satellites" (responsible for extraction).

| Master Pipeline ID | Satellite Pipelines / Shared Indicators | Source / File |
| :--- | :--- | :--- |
| `ed_loan_interest_rates` | `ed_loan_amounts_millions` | BoG: `Rates_TABLE_1+1a.xls` |
| `ed_fdi_activity` | `ed_fdi_construction` (To be built) | BoG: `BPM6_FDI_HOME_BY_ACTIVITY.xls` |
| `ed_key_partners_primary_goods` | `Primary traded goods` | ELSTAT: `SFC02` (Trade Balance) |
| `ed_building_permits_table` | `Floor space and volume` | ELSTAT: `SOP03` (Building Activity) |
| `ed_geo_distribution_...` | Residence Permit Cluster (8 Pipelines) | Ministry of Migration: Appendix B PDF |

---

## Detailed Source Reference (How to Modify)

Use this table to find the specific URL or Code if a source changes.

### Bank of Greece (Type 2)
| Pipeline ID | Target File URL (Permalink) | Notes |
| :--- | :--- | :--- |
| `ed_residents_di_activity` | `.../RelatedDocuments/BPM6_FDI_ABROAD_BY_ACTIVITY.xls` | Outward FDI flows. |
| `ed_residents_di_country` | `.../RelatedDocuments/BPM6_FDI_ABROAD_BY_COUNTRY.xls` | Outward FDI flows. |
| `ed_loan_amounts_millions` | `.../RelatedDocuments/Rates_TABLE_1+1a.xls` | Sheet: `Loans_Amounts`. New Business Flows. |
| `ed_loan_interest_rates` | `.../RelatedDocuments/Rates_TABLE_1+1a.xls` | Sheet: `Loans_Interest rates`. New Business Rates. |

### ELSTAT (Type 1)
| Pipeline ID | Publication Code | Target Title Substring | Notes |
| :--- | :--- | :--- | :--- |
| `gdp_greece` | **SEL84** | `02. Quarterly GDP - Seasonally adjusted` | Quarterly. |
| `ed_gva_by_sector` | **SEL12** | `Ακαθάριστη προστιθέμενη αξία κατά κλάδο (A64)` | Annual. |
| `ed_wage_growth_index` | **DKT03** | `Evolution of Gross Wages and Salaries` | Quarterly. |
| `ed_household_income_allocation` | **SEL60** | `Households accounts` | Annual/Quarterly. |
| `ed_housing_finances` | **SEL95** | `Quarterly Non-Financial Sector Accounts` | Quarterly. |