# Gemini Operational Rules

## Database Connectivity
- **Privilege Level:** READ-ONLY.
- **Security:** 
  - The connection string MUST NOT be hardcoded in the codebase.
  - Set the `DATABASE_URL` environment variable with the base connection string (e.g., `postgresql://user:password@host:port/defaultdb?sslmode=require`).
- **Database Switching:**
  - The framework automatically switches between databases based on the pipeline's region:
    - **Greece/EU Data:** Uses `athena` database.
    - **Cyprus Data:** Uses `zeus` database.
- **Rules:** 
  - NEVER execute `INSERT`, `UPDATE`, `DELETE`, or any other data modification statements against the PostgreSQL database.
  - The database connection must only be used to fetch the current state for comparison purposes.
  - All synchronization logic must happen locally, and deliverables must contain the differences found, without pushing those differences back to the live database.
  - The core utility function is `compare_with_postgres` in `etl/core/database.py`.

## Deliverables
- **Content:** Deliverables must only contain the new rows or updated information found during the extraction process compared to the live database state.
- **Filtering:** Current batch deliverables are filtered to only include data from **2024 onwards** (Year >= 2024).
- **Sorting:** Deliverables are sorted chronologically by Year, then Month (or Quarter).
- **Naming:** Follow the pattern `deliverable_{pipeline_id}_{Month}_{Year}.csv`.
