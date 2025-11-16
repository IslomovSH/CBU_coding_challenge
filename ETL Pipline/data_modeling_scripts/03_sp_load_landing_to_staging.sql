CREATE OR ALTER PROCEDURE sp_load_landing_to_stage
AS
/*======================================================================
  Procedure : dbo.sp_load_landing_to_stage
  Project   : CBU Code Challenge Hackathon
  Layer     : Silver (stg) – Customer / Application / Financial / Loan /
                          Credit / Geography
  Team      : MAAB Academy
  Date      : 2025-11-15

  Description:
    - MERGEs cleaned data from landing.*_clean into stg.* tables:
        * landing.demographics_clean           → stg.customer
        * landing.application_metadata_clean   → stg.application
        * landing.financial_ratios_clean       → stg.financial_profile
        * landing.loan_details_clean           → stg.loan
        * landing.credit_history_clean         → stg.credit_profile
        * landing.geographic_data_clean        → stg.geography
======================================================================*/
BEGIN

    DECLARE 
        @run_start  DATETIME = GETDATE(),
        @run_end    DATETIME,
        @step_start DATETIME,
        @step_end   DATETIME;


    /*===============================================================
      1) MERGE landing.demographics_clean → stg.customer
    ===============================================================*/
    PRINT '======================================================';
    PRINT '1) MERGE landing.demographics_clean - stg.customer';
    PRINT '======================================================';
    PRINT ' ';

    SET @step_start = GETDATE();

    MERGE stg.customer AS tgt
    USING (
        SELECT
            cust_id           AS customer_id,
            age,
            annual_income     AS annual_income_amt,
            employment_length AS employment_length_yrs,
            LOWER(LTRIM(RTRIM(
                REPLACE(REPLACE(employment_type, '_', ' '), '-', ' ')
            )))               AS employment_type,
            education         AS education_level,
            LOWER(LTRIM(RTRIM(marital_status))) AS marital_status,
            num_dependents    AS dependents_count
        FROM coding_challenge.landing.demographics_clean
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.age                   = src.age,
            tgt.annual_income_amt     = src.annual_income_amt,
            tgt.employment_length_yrs = src.employment_length_yrs,
            tgt.employment_type       = src.employment_type,
            tgt.education_level       = src.education_level,
            tgt.marital_status        = src.marital_status,
            tgt.dependents_count      = src.dependents_count
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            customer_id,
            age,
            annual_income_amt,
            employment_length_yrs,
            employment_type,
            education_level,
            marital_status,
            dependents_count
        )
        VALUES (
            src.customer_id,
            src.age,
            src.annual_income_amt,
            src.employment_length_yrs,
            src.employment_type,
            src.education_level,
            src.marital_status,
            src.dependents_count
        );

    SET @step_end = GETDATE();
    PRINT 'Execution Time (ms): ' 
          + CAST(DATEDIFF(MILLISECOND, @step_start, @step_end) AS VARCHAR(20));
    PRINT ' ';


    /*===============================================================
      2) MERGE landing.application_metadata_clean → stg.application
    ===============================================================*/
    PRINT '======================================================';
    PRINT '2) MERGE landing.application_metadata_clean - stg.application';
    PRINT '======================================================';
    PRINT ' ';

    SET @step_start = GETDATE();

    MERGE stg.application AS tgt
    USING (
        SELECT
            customer_ref                            AS customer_id,
            application_id,
            application_hour,
            application_day_of_week                 AS application_dow,
            account_open_year,
            LOWER(LTRIM(RTRIM(preferred_contact)))  AS preferred_contact_method,
            LTRIM(RTRIM(referral_code))             AS referral_code,
            account_status_code,
            random_noise_1,
            num_login_sessions                      AS login_sessions_count,
            num_customer_service_calls              AS customer_service_calls_count,
            CASE WHEN has_mobile_app = 1 THEN 1 ELSE 0 END        AS has_mobile_app_flag,
            CASE WHEN paperless_billing = 1 THEN 1 ELSE 0 END     AS paperless_billing_flag,
            CASE WHEN [default] = 1 THEN 1 ELSE 0 END             AS default_flag
        FROM coding_challenge.landing.application_metadata_clean
    ) AS src
    ON  tgt.customer_id    = src.customer_id
    AND tgt.application_id = src.application_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.application_hour             = src.application_hour,
            tgt.application_dow              = src.application_dow,
            tgt.account_open_year            = src.account_open_year,
            tgt.preferred_contact_method     = src.preferred_contact_method,
            tgt.referral_code                = src.referral_code,
            tgt.account_status_code          = src.account_status_code,
            tgt.random_noise_1               = src.random_noise_1,
            tgt.login_sessions_count         = src.login_sessions_count,
            tgt.customer_service_calls_count = src.customer_service_calls_count,
            tgt.has_mobile_app_flag          = src.has_mobile_app_flag,
            tgt.paperless_billing_flag       = src.paperless_billing_flag,
            tgt.default_flag                 = src.default_flag
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            customer_id,
            application_id,
            application_hour,
            application_dow,
            account_open_year,
            preferred_contact_method,
            referral_code,
            account_status_code,
            random_noise_1,
            login_sessions_count,
            customer_service_calls_count,
            has_mobile_app_flag,
            paperless_billing_flag,
            default_flag
        )
        VALUES (
            src.customer_id,
            src.application_id,
            src.application_hour,
            src.application_dow,
            src.account_open_year,
            src.preferred_contact_method,
            src.referral_code,
            src.account_status_code,
            src.random_noise_1,
            src.login_sessions_count,
            src.customer_service_calls_count,
            src.has_mobile_app_flag,
            src.paperless_billing_flag,
            src.default_flag
        );

    SET @step_end = GETDATE();
    PRINT 'Execution Time (ms): ' 
          + CAST(DATEDIFF(MILLISECOND, @step_start, @step_end) AS VARCHAR(20));
    PRINT ' ';


    /*===============================================================
      3) MERGE landing.financial_ratios_clean → stg.financial_profile
    ===============================================================*/
    PRINT '======================================================';
    PRINT '3) MERGE landing.financial_ratios_clean - stg.financial_profile';
    PRINT '======================================================';
    PRINT ' ';

    SET @step_start = GETDATE();

    MERGE stg.financial_profile AS tgt
    USING (
        SELECT
            cust_num                   AS customer_id,
            monthly_income             AS monthly_income_amt,
            existing_monthly_debt      AS existing_monthly_debt_amt,
            monthly_payment            AS loan_monthly_payment_amt,
            debt_to_income_ratio,
            debt_service_ratio,
            payment_to_income_ratio,
            credit_utilization         AS credit_utilization_ratio,
            revolving_balance          AS revolving_balance_amt,
            credit_usage_amount        AS credit_usage_amt,
            available_credit           AS available_credit_amt,
            total_monthly_debt_payment AS total_monthly_debt_payment_amt,
            annual_debt_payment        AS annual_debt_payment_amt,
            loan_to_annual_income      AS loan_to_annual_income_ratio,
            total_debt_amount          AS total_debt_amt,
            monthly_free_cash_flow     AS monthly_free_cash_flow_amt
        FROM coding_challenge.landing.financial_ratios_clean
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.monthly_income_amt             = src.monthly_income_amt,
            tgt.existing_monthly_debt_amt      = src.existing_monthly_debt_amt,
            tgt.loan_monthly_payment_amt       = src.loan_monthly_payment_amt,
            tgt.debt_to_income_ratio           = src.debt_to_income_ratio,
            tgt.debt_service_ratio             = src.debt_service_ratio,
            tgt.payment_to_income_ratio        = src.payment_to_income_ratio,
            tgt.credit_utilization_ratio       = src.credit_utilization_ratio,
            tgt.revolving_balance_amt          = src.revolving_balance_amt,
            tgt.credit_usage_amt               = src.credit_usage_amt,
            tgt.available_credit_amt           = src.available_credit_amt,
            tgt.total_monthly_debt_payment_amt = src.total_monthly_debt_payment_amt,
            tgt.annual_debt_payment_amt        = src.annual_debt_payment_amt,
            tgt.loan_to_annual_income_ratio    = src.loan_to_annual_income_ratio,
            tgt.total_debt_amt                 = src.total_debt_amt,
            tgt.monthly_free_cash_flow_amt     = src.monthly_free_cash_flow_amt
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            customer_id,
            monthly_income_amt,
            existing_monthly_debt_amt,
            loan_monthly_payment_amt,
            debt_to_income_ratio,
            debt_service_ratio,
            payment_to_income_ratio,
            credit_utilization_ratio,
            revolving_balance_amt,
            credit_usage_amt,
            available_credit_amt,
            total_monthly_debt_payment_amt,
            annual_debt_payment_amt,
            loan_to_annual_income_ratio,
            total_debt_amt,
            monthly_free_cash_flow_amt
        )
        VALUES (
            src.customer_id,
            src.monthly_income_amt,
            src.existing_monthly_debt_amt,
            src.loan_monthly_payment_amt,
            src.debt_to_income_ratio,
            src.debt_service_ratio,
            src.payment_to_income_ratio,
            src.credit_utilization_ratio,
            src.revolving_balance_amt,
            src.credit_usage_amt,
            src.available_credit_amt,
            src.total_monthly_debt_payment_amt,
            src.annual_debt_payment_amt,
            src.loan_to_annual_income_ratio,
            src.total_debt_amt,
            src.monthly_free_cash_flow_amt
        );

    SET @step_end = GETDATE();
    PRINT 'Execution Time (ms): ' 
          + CAST(DATEDIFF(MILLISECOND, @step_start, @step_end) AS VARCHAR(20));
    PRINT ' ';


    /*===============================================================
      4) MERGE landing.loan_details_clean → stg.loan
    ===============================================================*/
    PRINT '======================================================';
    PRINT '4) MERGE landing.loan_details_clean - stg.loan';
    PRINT '======================================================';
    PRINT ' ';

    SET @step_start = GETDATE();

    MERGE stg.loan AS tgt
    USING (
        SELECT
            customer_id,
            CASE 
                WHEN LOWER(loan_type) LIKE '%personal%' THEN 'personal'
                WHEN LOWER(loan_type) LIKE '%mortgage%' THEN 'mortgage'
                WHEN LOWER(loan_type) LIKE '%auto%'     THEN 'auto'
                ELSE LOWER(LTRIM(RTRIM(loan_type)))
            END                                   AS loan_type,
            loan_amount                           AS loan_amount_amt,
            loan_term                             AS loan_term_months,
            interest_rate                         AS interest_rate_pct,
            loan_purpose,
            loan_to_value_ratio,
            LOWER(LTRIM(RTRIM(origination_channel))) AS origination_channel,
            loan_officer_id,
            marketing_campaign                    AS marketing_campaign_code
        FROM coding_challenge.landing.loan_details_clean
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.loan_type               = src.loan_type,
            tgt.loan_amount_amt         = src.loan_amount_amt,
            tgt.loan_term_months        = src.loan_term_months,
            tgt.interest_rate_pct       = src.interest_rate_pct,
            tgt.loan_purpose            = src.loan_purpose,
            tgt.loan_to_value_ratio     = src.loan_to_value_ratio,
            tgt.origination_channel     = src.origination_channel,
            tgt.loan_officer_id         = src.loan_officer_id,
            tgt.marketing_campaign_code = src.marketing_campaign_code
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            customer_id,
            loan_type,
            loan_amount_amt,
            loan_term_months,
            interest_rate_pct,
            loan_purpose,
            loan_to_value_ratio,
            origination_channel,
            loan_officer_id,
            marketing_campaign_code
        )
        VALUES (
            src.customer_id,
            src.loan_type,
            src.loan_amount_amt,
            src.loan_term_months,
            src.interest_rate_pct,
            src.loan_purpose,
            src.loan_to_value_ratio,
            src.origination_channel,
            src.loan_officer_id,
            src.marketing_campaign_code
        );

    SET @step_end = GETDATE();
    PRINT 'Execution Time (ms): ' 
          + CAST(DATEDIFF(MILLISECOND, @step_start, @step_end) AS VARCHAR(20));
    PRINT ' ';


    /*===============================================================
      5) MERGE landing.credit_history_clean → stg.credit_profile
    ===============================================================*/
    PRINT '======================================================';
    PRINT '5) MERGE landing.credit_history_clean - stg.credit_profile';
    PRINT '======================================================';
    PRINT ' ';

    SET @step_start = GETDATE();

    MERGE stg.credit_profile AS tgt
    USING (
        SELECT
            customer_number            AS customer_id,
            credit_score,
            num_credit_accounts,
            oldest_credit_line_age     AS oldest_credit_line_age_yrs,
            oldest_account_age_months,
            total_credit_limit         AS total_credit_limit_amt,
            num_delinquencies_2yrs,
            num_inquiries_6mo,
            recent_inquiry_count,
            num_public_records,
            num_collections,
            account_diversity_index
        FROM coding_challenge.landing.credit_history_clean
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.credit_score               = src.credit_score,
            tgt.num_credit_accounts        = src.num_credit_accounts,
            tgt.oldest_credit_line_age_yrs = src.oldest_credit_line_age_yrs,
            tgt.oldest_account_age_months  = src.oldest_account_age_months,
            tgt.total_credit_limit_amt     = src.total_credit_limit_amt,
            tgt.num_delinquencies_2yrs     = src.num_delinquencies_2yrs,
            tgt.num_inquiries_6mo          = src.num_inquiries_6mo,
            tgt.recent_inquiry_count       = src.recent_inquiry_count,
            tgt.num_public_records         = src.num_public_records,
            tgt.num_collections            = src.num_collections,
            tgt.account_diversity_index    = src.account_diversity_index
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            customer_id,
            credit_score,
            num_credit_accounts,
            oldest_credit_line_age_yrs,
            oldest_account_age_months,
            total_credit_limit_amt,
            num_delinquencies_2yrs,
            num_inquiries_6mo,
            recent_inquiry_count,
            num_public_records,
            num_collections,
            account_diversity_index
        )
        VALUES (
            src.customer_id,
            src.credit_score,
            src.num_credit_accounts,
            src.oldest_credit_line_age_yrs,
            src.oldest_account_age_months,
            src.total_credit_limit_amt,
            src.num_delinquencies_2yrs,
            src.num_inquiries_6mo,
            src.recent_inquiry_count,
            src.num_public_records,
            src.num_collections,
            src.account_diversity_index
        );

    SET @step_end = GETDATE();
    PRINT 'Execution Time (ms): ' 
          + CAST(DATEDIFF(MILLISECOND, @step_start, @step_end) AS VARCHAR(20));
    PRINT ' ';


    /*===============================================================
      6) MERGE landing.geographic_data_clean → stg.geography
    ===============================================================*/
    PRINT '======================================================';
    PRINT '6) MERGE landing.geographic_data_clean - stg.geography';
    PRINT '======================================================';
    PRINT ' ';

    SET @step_start = GETDATE();

    MERGE stg.geography AS tgt
    USING (
        SELECT
            id                            AS customer_id,
            state,
            regional_unemployment_rate,
            regional_median_income        AS regional_median_income_amt,
            regional_median_rent          AS regional_median_rent_amt,
            housing_price_index,
            cost_of_living_index,
            previous_zip_code
        FROM coding_challenge.landing.geographic_data_clean
    ) AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            tgt.state                      = src.state,
            tgt.regional_unemployment_rate = src.regional_unemployment_rate,
            tgt.regional_median_income_amt = src.regional_median_income_amt,
            tgt.regional_median_rent_amt   = src.regional_median_rent_amt,
            tgt.housing_price_index        = src.housing_price_index,
            tgt.cost_of_living_index       = src.cost_of_living_index,
            tgt.previous_zip_code          = src.previous_zip_code
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            customer_id,
            state,
            regional_unemployment_rate,
            regional_median_income_amt,
            regional_median_rent_amt,
            housing_price_index,
            cost_of_living_index,
            previous_zip_code
        )
        VALUES (
            src.customer_id,
            src.state,
            src.regional_unemployment_rate,
            src.regional_median_income_amt,
            src.regional_median_rent_amt,
            src.housing_price_index,
            src.cost_of_living_index,
            src.previous_zip_code
        );

    SET @step_end = GETDATE();
    PRINT 'Execution Time (ms): ' 
          + CAST(DATEDIFF(MILLISECOND, @step_start, @step_end) AS VARCHAR(20));
    PRINT ' ';


    ------------------------------------------------------------
    -- Total procedure time
    ------------------------------------------------------------
    SET @run_end = GETDATE();
    PRINT '======================================================';
    PRINT 'Total procedure execution time (ms): ' 
          + CAST(DATEDIFF(MILLISECOND, @run_start, @run_end) AS VARCHAR(20));
    PRINT '======================================================';

END;
GO

EXEC sp_load_landing_to_stage;
