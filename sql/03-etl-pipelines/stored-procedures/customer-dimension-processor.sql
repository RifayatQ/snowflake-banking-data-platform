-- ETL Stored Procedure for Customer Dimension (SCD Type 2)

USE DATABASE BANKING_MARTS;
USE SCHEMA DIM_CUSTOMER;

CREATE OR REPLACE PROCEDURE PROCESS_CUSTOMER_DIMENSION()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    processed_count INTEGER;
    new_records INTEGER;
    updated_records INTEGER;
    current_date DATE DEFAULT CURRENT_DATE();
BEGIN
    
    -- Temporary staging table for processing
    CREATE OR REPLACE TEMPORARY TABLE temp_customer_updates AS
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        CONCAT(c.first_name, ' ', c.last_name) as full_name,
        c.email as email_masked,  -- Already masked by policy
        c.phone as phone_masked,  -- Already masked by policy
        c.date_of_birth,
        CASE 
            WHEN c.age < 25 THEN '18-24'
            WHEN c.age < 35 THEN '25-34'
            WHEN c.age < 45 THEN '35-44'
            WHEN c.age < 55 THEN '45-54'
            WHEN c.age < 65 THEN '55-64'
            ELSE '65+'
        END as age_group,
        c.age as customer_age,
        c.address,
        c.city,
        c.province,
        c.postal_code,
        CASE 
            WHEN c.province IN ('ON', 'QC') THEN 'Central'
            WHEN c.province IN ('BC', 'AB', 'SK', 'MB') THEN 'Western'
            WHEN c.province IN ('NB', 'NS', 'PE', 'NL') THEN 'Atlantic'
            ELSE 'Northern'
        END as region,
        c.customer_since,
        c.tenure_years as customer_tenure_years,
        CASE 
            WHEN c.tenure_years >= 10 THEN 'Long-term (10+ years)'
            WHEN c.tenure_years >= 5 THEN 'Established (5-9 years)'
            WHEN c.tenure_years >= 2 THEN 'Developing (2-4 years)'
            ELSE 'New (0-1 years)'
        END as tenure_category,
        c.customer_segment,
        c.account_status,
        cs.credit_score,
        cs.risk_category,
        CASE 
            WHEN cs.credit_score >= 750 THEN 1
            WHEN cs.credit_score >= 650 THEN 2
            WHEN cs.credit_score >= 550 THEN 3
            ELSE 4
        END as risk_tier
    FROM BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG c
    LEFT JOIN BANKING_RAW.EXTERNAL_DATA.CREDIT_SCORES cs
        ON c.customer_id = cs.customer_id 
        AND cs.score_type = 'Current'
        AND cs.bureau_name = 'Equifax'
    WHERE c.data_quality_flag = 'VALID';

    -- Expire changed records in dimension table
    UPDATE DIM_CUSTOMER 
    SET expiration_date = :current_date,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP()
    WHERE is_current = TRUE
    AND customer_id IN (
        SELECT t.customer_id 
        FROM temp_customer_updates t
        JOIN DIM_CUSTOMER d ON t.customer_id = d.customer_id AND d.is_current = TRUE
        WHERE (
            NVL(t.first_name, '') != NVL(d.first_name, '') OR
            NVL(t.last_name, '') != NVL(d.last_name, '') OR
            NVL(t.customer_segment, '') != NVL(d.customer_segment, '') OR
            NVL(t.account_status, '') != NVL(d.account_status, '') OR
            NVL(t.credit_score, 0) != NVL(d.credit_score, 0) OR
            NVL(t.risk_category, '') != NVL(d.risk_category, '')
        )
    );

    GET DIAGNOSTICS updated_records = ROW_COUNT;

    -- Insert new and changed records
    INSERT INTO DIM_CUSTOMER (
        customer_id, first_name, last_name, full_name, email_masked, phone_masked,
        date_of_birth, age_group, customer_age, address, city, province, postal_code, region,
        customer_since, customer_tenure_years, tenure_category, customer_segment, account_status,
        credit_score, risk_category, risk_tier, effective_date, expiration_date, is_current
    )
    SELECT 
        t.customer_id, t.first_name, t.last_name, t.full_name, t.email_masked, t.phone_masked,
        t.date_of_birth, t.age_group, t.customer_age, t.address, t.city, t.province, t.postal_code, t.region,
        t.customer_since, t.customer_tenure_years, t.tenure_category, t.customer_segment, t.account_status,
        t.credit_score, t.risk_category, t.risk_tier, :current_date, '2099-12-31', TRUE
    FROM temp_customer_updates t
    LEFT JOIN DIM_CUSTOMER d ON t.customer_id = d.customer_id AND d.is_current = TRUE
    WHERE d.customer_id IS NULL  -- New records
    OR (  -- Changed records
        NVL(t.first_name, '') != NVL(d.first_name, '') OR
        NVL(t.last_name, '') != NVL(d.last_name, '') OR
        NVL(t.customer_segment, '') != NVL(d.customer_segment, '') OR
        NVL(t.account_status, '') != NVL(d.account_status, '') OR
        NVL(t.credit_score, 0) != NVL(d.credit_score, 0) OR
        NVL(t.risk_category, '') != NVL(d.risk_category, '')
    );

    GET DIAGNOSTICS new_records = ROW_COUNT;
    LET processed_count := new_records + updated_records;

    RETURN 'Customer dimension processing complete. New/Updated records: ' || new_records || ', Expired records: ' || updated_records;
END;
$$;
