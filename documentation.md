# 📘 CBU Coding Challenge – Data Pipeline & Modeling
**Team:** MAAB Academy  
**Architecture:** Medallion (Bronze → Silver → Gold)  
**Goal:** Build a clean production-style data pipeline and create an ML-ready dataset for loan default prediction.

This repository includes the full SQL implementation for the data model, ETL pipeline, and final ML feature view.

---

# 🚀 1. Project Overview
This project transforms multiple cleaned CSV datasets into a structured, analytical, and machine-learning–ready model.

We follow the Medallion architecture:

1. **Bronze (landing)** – receives cleaned data from Python  
2. **Silver (stg)** – normalized relational layer with PK/FK constraints  
3. **Gold (mart)** – unified ML feature view  

This ensures clarity, scalability, and professional-grade data usability.

---

# 🥉 2. Bronze Layer (landing)
**File:** `table_scrtips_landing.sql`

The Bronze layer stores cleaned data sent from Python.  
It acts as a landing zone — no strict constraints and no transformations in SQL.

### Bronze Tables
- `landing.application_metadata_clean`
- `landing.demographics_clean`
- `landing.financial_ratios_clean`
- `landing.loan_details_clean`
- `landing.credit_history_clean`
- `landing.geographic_data_clean`

These tables mirror the Python-cleaned CSVs:
- application_metadata.csv  
- demographics.csv  
- financial_ratios.csv  
- loan_details.csv  
- credit_history.csv  
- geographic_data.csv  

---

# 🥈 3. Silver Layer (stg)
**File:** `table_scripts_staging.sql`

Silver is a normalized, relational data model centered around **customer_id**.

### Core Table
- `stg.customer` – demographics

### Child Tables
- `stg.application`
- `stg.financial_profile`
- `stg.loan`
- `stg.credit_profile`
- `stg.geography`

### Key Features
- Primary keys for all tables  
- Foreign keys referencing `stg.customer`  
- Standardized text fields (lowercase, trim)  
- Clean naming conventions  

The Silver layer turns semi-clean Bronze data into a structured relational warehouse.

---

# 🔄 4. ETL: Bronze → Silver
**File:** `sp_loading_landing_to_stage.sql`

The stored procedure `sp_load_landing_to_stage`:

- Performs MERGE inserts/updates
- Normalizes values (lowercase, trimming)
- Converts 0/1 fields to BIT
- Maps loan types and codes
- Logs execution times for each step

### Mapping Summary

| Bronze Table                | Silver Table             |
|-----------------------------|--------------------------|
| demographics_clean          | stg.customer             |
| application_metadata_clean  | stg.application          |
| financial_ratios_clean      | stg.financial_profile    |
| loan_details_clean          | stg.loan                 |
| credit_history_clean        | stg.credit_profile       |
| geographic_data_clean       | stg.geography            |

This ensures the Silver layer stays synchronized with the Bronze clean data.

---

# 🥇 5. Gold Layer (mart)
**File:** `gold_layer_mart.sql`

The final ML-ready dataset is built in:

## `mart.vw_loan_default_features`

This view joins all Silver tables using `customer_id` and `application_id`, giving **one row per loan application**.

