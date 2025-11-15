/*
===============================================================
  Project : CBU Code Challenge Hackathon 
  Layer   : LANDING (Bronze) – Cleaned Data Ingestion from Python
  Team    : MAAB Academy
  Date    : 2025-11-15

  Description:
    - Defines LANDING schema tables that receive cleaned data from Python
    - Python handles initial cleaning 
    - These tables act as the Bronze storage in SQL before Silver (stg) normalization
===============================================================
*/

------------------------------------------------------------
-- 1. Create database
------------------------------------------------------------
CREATE DATABASE coding_challenge;
GO

USE coding_challenge;
GO

------------------------------------------------------------
-- 2. Create schemas for Medallion architecture
--    landing = Bronze (from Python)
--    stg     = Silver (normalized, relational)
--    mart    = Gold (ML / analytics)
------------------------------------------------------------
CREATE SCHEMA landing;  
GO
CREATE SCHEMA stg;      
GO
CREATE SCHEMA mart;     
GO


/**************************************************************
 * LANDING TABLES (Bronze in SQL)
 * --------------------------------
 * These tables receive already-cleaned data from Python:
 *  - No strict naming conventions required here
 *  - Types should match what Python outputs
 *  - We will MERGE from landing.* → stg.* in later steps
 **************************************************************/


------------------------------------------------------------
-- LANDING: application_metadata (from Python-cleaned DF)
-- Assumption: Python already ensures correct types & basic cleaning
------------------------------------------------------------

CREATE TABLE landing.application_metadata_clean (
    customer_ref               INT,
    application_id             INT,
    application_hour           TINYINT,
    application_day_of_week    TINYINT,
    account_open_year          SMALLINT,
    preferred_contact          NVARCHAR(50),
    referral_code              NVARCHAR(50),
    account_status_code        NVARCHAR(20),
    random_noise_1             FLOAT,
    num_login_sessions         TINYINT,
    num_customer_service_calls TINYINT,
    has_mobile_app             TINYINT,   -- 0/1 as integer coming from Python
    paperless_billing          TINYINT,   -- 0/1 as integer
    [default]                  TINYINT    -- target label
);
GO


------------------------------------------------------------
-- LANDING: demographics (from Python-cleaned DF)
-- annual_income should already be numeric in Python, but we keep
-- column name same as source; we’ll rename in the stg layer.
------------------------------------------------------------
CREATE TABLE landing.demographics_clean (
    cust_id           INT,
    age               TINYINT,
    annual_income     DECIMAL(18,2),  -- cleaned in Python
    employment_length DECIMAL(5,2),   -- e.g. 5.20 years
    employment_type   NVARCHAR(50),
    education         NVARCHAR(50),
    marital_status    NVARCHAR(50),
    num_dependents    TINYINT
);
GO


------------------------------------------------------------
-- LANDING: financial_ratios (from Python-cleaned DF)
-- All ratios/amounts assumed already numeric in Python
------------------------------------------------------------
CREATE TABLE landing.financial_ratios_clean (
    cust_num                    INT,
    monthly_income              DECIMAL(18,2),
    existing_monthly_debt       DECIMAL(18,2),
    monthly_payment             DECIMAL(18,2),
    debt_to_income_ratio        DECIMAL(10,4),
    debt_service_ratio          DECIMAL(10,4),
    payment_to_income_ratio     DECIMAL(10,4),
    credit_utilization          DECIMAL(10,4),
    revolving_balance           DECIMAL(18,2),
    credit_usage_amount         DECIMAL(18,2),
    available_credit            DECIMAL(18,2),
    total_monthly_debt_payment  DECIMAL(18,2),
    annual_debt_payment         DECIMAL(18,2),
    loan_to_annual_income       DECIMAL(10,4),
    total_debt_amount           DECIMAL(18,2),
    monthly_free_cash_flow      DECIMAL(18,2)
);
GO


------------------------------------------------------------
-- LANDING: loan_details (from Python-cleaned DF)
-- loan_amount & ratios already numeric in Python
------------------------------------------------------------
CREATE TABLE landing.loan_details_clean (
    customer_id          INT,
    loan_type            NVARCHAR(50),
    loan_amount          DECIMAL(18,2),
    loan_term            INT,
    interest_rate        DECIMAL(6,3),
    loan_purpose         NVARCHAR(100),
    loan_to_value_ratio  DECIMAL(10,4),
    origination_channel  NVARCHAR(50),
    loan_officer_id      INT,
    marketing_campaign   NVARCHAR(50)
);
GO


------------------------------------------------------------
-- LANDING: credit_history (from Python-cleaned DF)
-- Numeric types already handled in Python if needed
------------------------------------------------------------
CREATE TABLE landing.credit_history_clean (
    customer_number            INT,
    credit_score               INT,
    num_credit_accounts        INT,
    oldest_credit_line_age     DECIMAL(6,2),
    oldest_account_age_months  DECIMAL(6,2),
    total_credit_limit         DECIMAL(18,2),
    num_delinquencies_2yrs     DECIMAL(6,2),
    num_inquiries_6mo          INT,
    recent_inquiry_count       INT,
    num_public_records         INT,
    num_collections            INT,
    account_diversity_index    DECIMAL(6,3)
);
GO


------------------------------------------------------------
-- LANDING: geographic_data (from Python-cleaned DF)
------------------------------------------------------------
CREATE TABLE landing.geographic_data_clean (
    id                         INT,
    state                      NVARCHAR(10),
    regional_unemployment_rate DECIMAL(5,2),
    regional_median_income     DECIMAL(18,2),
    regional_median_rent       DECIMAL(18,2),
    housing_price_index        DECIMAL(8,2),
    cost_of_living_index       DECIMAL(8,2),
    previous_zip_code          INT
);
GO
