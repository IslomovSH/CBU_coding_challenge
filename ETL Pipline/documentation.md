# CBU Coding Challenge
**Team:** MAAB Academy  
**Architecture:** Medallion (Bronze → Silver → Gold)  


This repository includes the Pyhton data cleaning and SQL implementation for the data model, ETL pipeline, and final ML feature view.

---

#Overview
This project transforms multiple cleaned CSV datasets into a structured, analytical, and machine-learning–ready model.

We follow the Medallion architecture:

1. **Bronze (landing)** – receives cleaned data from Python  
2. **Silver (stg)** – normalized relational layer with PK/FK constraints  
3. **Gold (mart)** – unified ML feature view  

This ensures clarity, scalability, and professional-grade data usability.

---

# 2. Bronze Layer (landing)
**File:** `table_scrtips_landing.sql` and `main.ipynb`

The Bronze layer stores cleaned data sent from Python.  
It acts as a landing zone — no strict constraints and no transformations in SQL.


# 0. Python Layer – Data Cleaning & Landing Upload
**File:** `main.ipynb`

Before data enters SQL, all raw CSVs are cleaned inside the `main.ipynb` notebook.  
This notebook performs:

###  Data loading  
Reads the 6 main CSV files:
- application_metadata.csv  
- demographics.csv  
- financial_ratios.csv  
- loan_details.csv  
- credit_history.csv  
- geographic_data.csv  

###  Cleaning & Standardization  
- Converts numerical fields to correct numeric types  
- Fixes inconsistent strings (upper/lowercase, trailing spaces)  
- Normalizes categories  
- Handles missing values where appropriate  
- Ensures IDs align across all datasets  
- Validates row counts before loading  

###  Renaming & Consistency Mapping  
Column names are aligned with the SQL landing tables created in  
`table_scrtips_landing.sql`.

Example mappings:
- `cust_id` → `customer_id`  
- `customer_ref` → `customer_id`  
- `monthly_payment` → `monthly_payment` (loan)  
- 0/1 flags remain integers (SQL converts them later)

###  Upload to SQL Landing Layer  
The notebook writes each cleaned dataframe to the corresponding SQL **landing** table:

| DataFrame                     | Landing Table                       |
|-------------------------------|--------------------------------------|
| df_application_metadata       | landing.application_metadata_clean   |
| df_demographics               | landing.demographics_clean           |
| df_financial_ratios           | landing.financial_ratios_clean       |
| df_loan_details               | landing.loan_details_clean           |
| df_credit_history             | landing.credit_history_clean         |
| df_geographic_data            | landing.geographic_data_clean        |

This step completes the Bronze ingestion process.

###  Purpose of the Python Layer
The Python notebook ensures that:
- SQL receives fully cleaned, validated data  
- SQL does not need to perform heavy transformations  
- The ETL procedure (`sp_load_landing_to_stage.sql`) can run safely  
- The final dataset is consistent and ML-ready  

The notebook is part of the pipeline design and should be kept alongside the SQL code for reproducibility and version control.


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

#  3. Silver Layer (stg)
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

#  4. ETL: Bronze → Silver
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

#  5. Gold Layer (mart)
**File:** `gold_layer_mart.sql`

The final ML-ready dataset is built in:

## `mart.vw_loan_default_features`

This view joins all Silver tables using `customer_id` and `application_id`, giving **one row per loan application**.


#  10. Modeling & AUC Evaluation

Once the `mart.vw_loan_default_features` view is created, we use it as the
single source of truth for machine learning.

- **Target column:** `target_default_flag`
- **Problem type:** Binary classification (default vs no default)
- **Main metric:** ROC AUC (Area Under the ROC Curve)

### Workflow

1. Export the view `mart.vw_loan_default_features` to a CSV  
   (e.g., `loan_default_features.csv`) or read it directly from SQL.
2. Drop ID columns (`customer_id`, `application_id`) from the feature set.
3. One-hot encode categorical columns.
4. Split into train/test sets (stratified by `target_default_flag`).
5. Train a baseline model (Logistic Regression or tree-based model).
6. Predict default probabilities.
7. Evaluate performance using `roc_auc_score` from scikit-learn.






