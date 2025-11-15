/*===============================================================
  Project : CBU Code Challenge Hackathon 
  Layer   : SILVER (stg) – Normalized, Relational and corrected naming Layer
  Flow    : landing (Bronze) → stg (Silver)
  Team    : MAAB Academy
  Date    : 2025-11-15

  Description:
    - Creates stg tables with clean naming and normalized structure
    - Adds primary keys and foreign keys for relational integrity
    - MERGEs cleaned data from landing.*_clean into stg.*
    - Applies normalization (lowercasing, trimming, category mapping)
===============================================================*/

USE coding_challenge;
GO

/*===============================================================
  1. CREATE STG TABLES (if not existing)
===============================================================*/

------------------------------------------------------------
-- STG: Customer (demographics) – core entity
------------------------------------------------------------
IF OBJECT_ID('stg.customer', 'U') IS NULL
BEGIN
    CREATE TABLE stg.customer (
        customer_id            INT           NOT NULL,
        age                    TINYINT       NULL,
        annual_income_amt      DECIMAL(18,2) NULL,
        employment_length_yrs  DECIMAL(5,2)  NULL,
        employment_type        NVARCHAR(30)  NULL,
        education_level        NVARCHAR(30)  NULL,
        marital_status         NVARCHAR(20)  NULL,
        dependents_count       TINYINT       NULL
    );
END;
GO

------------------------------------------------------------
-- STG: Application
------------------------------------------------------------
IF OBJECT_ID('stg.application', 'U') IS NULL
BEGIN
    CREATE TABLE stg.application (
        customer_id                   INT           NOT NULL,
        application_id                INT           NOT NULL,
        application_hour              TINYINT       NULL,
        application_dow               TINYINT       NULL,
        account_open_year             SMALLINT      NULL,
        preferred_contact_method      NVARCHAR(50)  NULL,
        referral_code                 NVARCHAR(50)  NULL,
        account_status_code           NVARCHAR(20)  NULL,
        random_noise_1                FLOAT         NULL,
        login_sessions_count          TINYINT       NULL,
        customer_service_calls_count  TINYINT       NULL,
        has_mobile_app_flag           BIT           NULL,
        paperless_billing_flag        BIT           NULL,
        default_flag                  BIT           NULL
    );
END;
GO

------------------------------------------------------------
-- STG: Financial profile
------------------------------------------------------------
IF OBJECT_ID('stg.financial_profile', 'U') IS NULL
BEGIN
    CREATE TABLE stg.financial_profile (
        customer_id                     INT           NOT NULL,
        monthly_income_amt              DECIMAL(18,2) NULL,
        existing_monthly_debt_amt       DECIMAL(18,2) NULL,
        loan_monthly_payment_amt        DECIMAL(18,2) NULL,
        debt_to_income_ratio            DECIMAL(10,4) NULL,
        debt_service_ratio              DECIMAL(10,4) NULL,
        payment_to_income_ratio         DECIMAL(10,4) NULL,
        credit_utilization_ratio        DECIMAL(10,4) NULL,
        revolving_balance_amt           DECIMAL(18,2) NULL,
        credit_usage_amt                DECIMAL(18,2) NULL,
        available_credit_amt            DECIMAL(18,2) NULL,
        total_monthly_debt_payment_amt  DECIMAL(18,2) NULL,
        annual_debt_payment_amt         DECIMAL(18,2) NULL,
        loan_to_annual_income_ratio     DECIMAL(10,4) NULL,
        total_debt_amt                  DECIMAL(18,2) NULL,
        monthly_free_cash_flow_amt      DECIMAL(18,2) NULL
    );
END;
GO

------------------------------------------------------------
-- STG: Loan
------------------------------------------------------------
IF OBJECT_ID('stg.loan', 'U') IS NULL
BEGIN
    CREATE TABLE stg.loan (
        customer_id             INT           NOT NULL,
        loan_type               NVARCHAR(30)  NULL,
        loan_amount_amt         DECIMAL(18,2) NULL,
        loan_term_months        INT           NULL,
        interest_rate_pct       DECIMAL(6,3)  NULL,
        loan_purpose            NVARCHAR(100) NULL,
        loan_to_value_ratio     DECIMAL(10,4) NULL,
        origination_channel     NVARCHAR(30)  NULL,
        loan_officer_id         INT           NULL,
        marketing_campaign_code NVARCHAR(50)  NULL
    );
END;
GO

------------------------------------------------------------
-- STG: Credit profile
------------------------------------------------------------
IF OBJECT_ID('stg.credit_profile', 'U') IS NULL
BEGIN
    CREATE TABLE stg.credit_profile (
        customer_id                INT           NOT NULL,
        credit_score               INT           NULL,
        num_credit_accounts        INT           NULL,
        oldest_credit_line_age_yrs DECIMAL(6,2)  NULL,
        oldest_account_age_months  DECIMAL(6,2)  NULL,
        total_credit_limit_amt     DECIMAL(18,2) NULL,
        num_delinquencies_2yrs     DECIMAL(6,2)  NULL,
        num_inquiries_6mo          INT           NULL,
        recent_inquiry_count       INT           NULL,
        num_public_records         INT           NULL,
        num_collections            INT           NULL,
        account_diversity_index    DECIMAL(6,3)  NULL
    );
END;
GO

------------------------------------------------------------
-- STG: Geography
------------------------------------------------------------
IF OBJECT_ID('stg.geography', 'U') IS NULL
BEGIN
    CREATE TABLE stg.geography (
        customer_id                 INT           NOT NULL,
        state                       NVARCHAR(10)  NULL,
        regional_unemployment_rate  DECIMAL(5,2)  NULL,
        regional_median_income_amt  DECIMAL(18,2) NULL,
        regional_median_rent_amt    DECIMAL(18,2) NULL,
        housing_price_index         DECIMAL(8,2)  NULL,
        cost_of_living_index        DECIMAL(8,2)  NULL,
        previous_zip_code           INT           NULL
    );
END;
GO


/*===============================================================
  2. ADD PRIMARY KEYS & FOREIGN KEYS (Relational Layer)
===============================================================*/

------------------------------------------------------------
-- Primary keys
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_stg_customer')
    ALTER TABLE stg.customer
    ADD CONSTRAINT PK_stg_customer PRIMARY KEY CLUSTERED (customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_stg_application')
    ALTER TABLE stg.application
    ADD CONSTRAINT PK_stg_application PRIMARY KEY CLUSTERED (customer_id, application_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_stg_financial_profile')
    ALTER TABLE stg.financial_profile
    ADD CONSTRAINT PK_stg_financial_profile PRIMARY KEY CLUSTERED (customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_stg_loan')
    ALTER TABLE stg.loan
    ADD CONSTRAINT PK_stg_loan PRIMARY KEY CLUSTERED (customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_stg_credit_profile')
    ALTER TABLE stg.credit_profile
    ADD CONSTRAINT PK_stg_credit_profile PRIMARY KEY CLUSTERED (customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_stg_geography')
    ALTER TABLE stg.geography
    ADD CONSTRAINT PK_stg_geography PRIMARY KEY CLUSTERED (customer_id);
GO

------------------------------------------------------------
-- Foreign keys: all child tables reference stg.customer
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_stg_application_customer')
    ALTER TABLE stg.application
    ADD CONSTRAINT FK_stg_application_customer
        FOREIGN KEY (customer_id) REFERENCES stg.customer(customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_stg_financial_profile_customer')
    ALTER TABLE stg.financial_profile
    ADD CONSTRAINT FK_stg_financial_profile_customer
        FOREIGN KEY (customer_id) REFERENCES stg.customer(customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_stg_loan_customer')
    ALTER TABLE stg.loan
    ADD CONSTRAINT FK_stg_loan_customer
        FOREIGN KEY (customer_id) REFERENCES stg.customer(customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_stg_credit_profile_customer')
    ALTER TABLE stg.credit_profile
    ADD CONSTRAINT FK_stg_credit_profile_customer
        FOREIGN KEY (customer_id) REFERENCES stg.customer(customer_id);
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_stg_geography_customer')
    ALTER TABLE stg.geography
    ADD CONSTRAINT FK_stg_geography_customer
        FOREIGN KEY (customer_id) REFERENCES stg.customer(customer_id);
GO



