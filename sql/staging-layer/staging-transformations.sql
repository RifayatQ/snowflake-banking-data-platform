CREATE TABLE BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG AS
SELECT 
    $1::string as customer_id,
    UPPER(TRIM($2)) as first_name,
    UPPER(TRIM($3)) as last_name,
    LOWER(TRIM($4)) as email,
    $5::string as phone,
    $6::date as date_of_birth,
    $7::string as address,
    $8::string as city,
    $9::string as province,
    $10::string as postal_code,
    $11::string as sin,
    $12::date as customer_since,
    $13::string as account_status,
    CURRENT_TIMESTAMP() as processed_at,
    CASE 
        WHEN $4 NOT LIKE '%@%.%' THEN 'INVALID_EMAIL'
        WHEN $6::date > CURRENT_DATE() THEN 'INVALID_DOB'
        WHEN $12::date > CURRENT_DATE() THEN 'INVALID_START_DATE'
        ELSE 'VALID'
    END as data_quality_flag
FROM @CUSTOMER_STAGE/customers.csv (file_format => csv_format);