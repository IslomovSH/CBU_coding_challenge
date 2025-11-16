/*===============================================================
  Object  : mart.vw_loan_default_features
  Project : CBU Code Challenge Hackathon
  Layer   : Gold (mart) – ML Training Dataset
  Team    : MAAB Academy
  Date    : 2025-11-15

  Grain   : 1 row = 1 application (customer_id + application_id)

  Description:
    - Joins all Gold (mart) tables:
        * mart.application        (target + app behaviour)
        * mart.customer           (demographics)
        * mart.financial_profile  (income, debt, ratios)
        * mart.loan               (loan details)
        * mart.credit_profile     (credit behaviour)
        * mart.geography          (regional risk)
    - Exposes target column: target_default_flag
    - Adds engineered features (ratios, bands, frequencies)
===============================================================*/
USE coding_challenge;
GO

CREATE OR ALTER VIEW mart.vw_loan_default_features
AS
SELECT
    /* =========================
       Keys / Target
       ========================= */
    a.customer_id,
    a.application_id,
    a.default_flag AS target_default_flag,

    /* =========================
       Application behaviour
       ========================= */
    a.application_hour,
    a.application_dow,
    a.account_open_year,
    a.preferred_contact_method,
    a.referral_code,
    a.account_status_code,
    a.random_noise_1,
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
    l.loan_officer_id,
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
    g.previous_zip_code,

    /* ==========================================================
       Engineered ratios (safe, no leakage)
       ========================================================== */

    -- Overall debt vs annual income
    CASE 
        WHEN c.annual_income_amt IS NOT NULL AND c.annual_income_amt > 0
             THEN f.total_debt_amt / c.annual_income_amt
        ELSE NULL
    END AS debt_burden_ratio,

    -- Income pressure per dependent
    CASE 
        WHEN c.dependents_count IS NOT NULL AND c.dependents_count > 0
             THEN c.annual_income_amt / c.dependents_count
        ELSE NULL
    END AS income_per_dependent,

    -- Existing monthly debt vs monthly income (alternative DTI)
    CASE 
        WHEN f.monthly_income_amt IS NOT NULL AND f.monthly_income_amt > 0
             THEN f.existing_monthly_debt_amt / f.monthly_income_amt
        ELSE NULL
    END AS existing_debt_to_income_ratio_alt,

    -- Total credit limit vs annual income
    CASE 
        WHEN c.annual_income_amt IS NOT NULL AND c.annual_income_amt > 0
             THEN cp.total_credit_limit_amt / c.annual_income_amt
        ELSE NULL
    END AS credit_limit_to_income_ratio,

    -- Effective credit usage vs total limit
    CASE 
        WHEN cp.total_credit_limit_amt IS NOT NULL AND cp.total_credit_limit_amt > 0
             THEN f.credit_usage_amt / cp.total_credit_limit_amt
        ELSE NULL
    END AS credit_usage_to_limit_ratio,

    -- Customer income vs region median income
    CASE 
        WHEN g.regional_median_income_amt IS NOT NULL AND g.regional_median_income_amt > 0
             THEN c.annual_income_amt / g.regional_median_income_amt
        ELSE NULL
    END AS income_vs_region_median_ratio,

    -- Region rent burden (median rent vs median income)
    CASE 
        WHEN g.regional_median_income_amt IS NOT NULL AND g.regional_median_income_amt > 0
             THEN g.regional_median_rent_amt / g.regional_median_income_amt
        ELSE NULL
    END AS region_rent_to_income_ratio,

    /* ==========================================================
       Binned features (age, income, credit score)
       ========================================================== */

    CASE 
        WHEN c.age IS NULL THEN 'unknown'
        WHEN c.age < 25 THEN 'below 25'
        WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN c.age BETWEEN 35 AND 49 THEN '35-49'
        ELSE 'above 50'
    END AS age_band,

    CASE 
        WHEN cp.credit_score IS NULL THEN 'unknown'
        WHEN cp.credit_score < 580 THEN 'very_poor'
        WHEN cp.credit_score < 670 THEN 'fair'
        WHEN cp.credit_score < 740 THEN 'good'
        WHEN cp.credit_score < 800 THEN 'very_good'
        ELSE 'excellent'
    END AS credit_score_band,

    -- Income quartiles (relative, no hard thresholds)
    NTILE(4) OVER (ORDER BY c.annual_income_amt) AS income_quartile,

    /* ==========================================================
       Frequency encodings (safe: just counts, no target)
       ========================================================== */

    COUNT(*) OVER (PARTITION BY g.state)               AS state_customer_count,
    COUNT(*) OVER (PARTITION BY c.employment_type)     AS employment_type_freq,
    COUNT(*) OVER (PARTITION BY l.loan_purpose)        AS loan_purpose_freq,
    COUNT(*) OVER (PARTITION BY l.origination_channel) AS origination_channel_freq

FROM mart.application       AS a
LEFT JOIN mart.customer          AS c   ON a.customer_id = c.customer_id
LEFT JOIN mart.financial_profile AS f   ON a.customer_id = f.customer_id
LEFT JOIN mart.loan              AS l   ON a.customer_id = l.customer_id
LEFT JOIN mart.credit_profile    AS cp  ON a.customer_id = cp.customer_id
LEFT JOIN mart.geography         AS g   ON a.customer_id = g.customer_id;
GO
