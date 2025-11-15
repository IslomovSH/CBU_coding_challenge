/*===============================================================
  Object  : mart.loan_default_features
  Project : CBU Code Challenge Hackathon
  Layer   : Gold (mart) – ML Training Dataset
  Team    : MAAB Academy
  Date    : 2025-11-15

  Grain   : 1 row = 1 application (customer_id + application_id)

  Description:
    - Joins all Silver (stg) tables:
        * stg.application        (target + app behaviour)
        * stg.customer           (demographics)
        * stg.financial_profile  (income, debt, ratios)
        * stg.loan               (loan details)
        * stg.credit_profile     (credit behaviour)
        * stg.geography          (regional risk)
    - Exposes target column: target_default_flag
    - Adds a few simple engineered features:
        * debt_burden_ratio  (total_debt_amt / annual_income_amt)
        * income_per_dependent
===============================================================*/

USE coding_challenge;
GO

CREATE OR ALTER VIEW mart.vw_loan_default_features
AS
SELECT
    -- Keys / IDs
    a.customer_id,
    a.application_id,

    -- Target
    a.default_flag AS target_default_flag,

    /* =========================
       Application behaviour
       ========================= */
    a.application_hour,
    a.application_dow,
    a.account_open_year,
    a.preferred_contact_method,
    a.account_status_code,
    a.login_sessions_count,
    a.customer_service_calls_count,
    a.has_mobile_app_flag,
    a.paperless_billing_flag,

    /* =========================
       Customer demographics
       ========================= */
    c.age,
    c.annual_income_amt,
    c.employment_length_yrs,
    c.employment_type,
    c.education_level,
    c.marital_status,
    c.dependents_count,

    /* =========================
       Financial profile
       ========================= */
    f.monthly_income_amt,
    f.existing_monthly_debt_amt,
    f.loan_monthly_payment_amt,
    f.debt_to_income_ratio,
    f.debt_service_ratio,
    f.payment_to_income_ratio,
    f.credit_utilization_ratio,
    f.revolving_balance_amt,
    f.credit_usage_amt,
    f.available_credit_amt,
    f.total_monthly_debt_payment_amt,
    f.annual_debt_payment_amt,
    f.loan_to_annual_income_ratio,
    f.total_debt_amt,
    f.monthly_free_cash_flow_amt,

    /* =========================
       Loan details
       ========================= */
    l.loan_type,
    l.loan_amount_amt,
    l.loan_term_months,
    l.interest_rate_pct,
    l.loan_purpose,
    l.loan_to_value_ratio,
    l.origination_channel,
    l.marketing_campaign_code,

    /* =========================
       Credit profile
       ========================= */
    cp.credit_score,
    cp.num_credit_accounts,
    cp.oldest_credit_line_age_yrs,
    cp.oldest_account_age_months,
    cp.total_credit_limit_amt,
    cp.num_delinquencies_2yrs,
    cp.num_inquiries_6mo,
    cp.recent_inquiry_count,
    cp.num_public_records,
    cp.num_collections,
    cp.account_diversity_index,

    /* =========================
       Geography
       ========================= */
    g.state,
    g.regional_unemployment_rate,
    g.regional_median_income_amt,
    g.regional_median_rent_amt,
    g.housing_price_index,
    g.cost_of_living_index,

    /* =========================
       Simple engineered features
       ========================= */
    CASE 
        WHEN c.annual_income_amt IS NOT NULL AND c.annual_income_amt > 0
             THEN f.total_debt_amt / c.annual_income_amt
        ELSE NULL
    END AS debt_burden_ratio,         -- overall debt vs income

    CASE 
        WHEN c.dependents_count IS NOT NULL AND c.dependents_count > 0
             THEN c.annual_income_amt / c.dependents_count
        ELSE NULL
    END AS income_per_dependent       -- income pressure per dependent

FROM stg.application        AS a
LEFT JOIN stg.customer           AS c   ON a.customer_id = c.customer_id
LEFT JOIN stg.financial_profile  AS f   ON a.customer_id = f.customer_id
LEFT JOIN stg.loan               AS l   ON a.customer_id = l.customer_id
LEFT JOIN stg.credit_profile     AS cp  ON a.customer_id = cp.customer_id
LEFT JOIN stg.geography          AS g   ON a.customer_id = g.customer_id;
GO



