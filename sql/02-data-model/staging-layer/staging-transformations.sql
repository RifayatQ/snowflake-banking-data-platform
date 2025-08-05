-- create customer staging table with data quality flag
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

-- load customers data from staging to banking_raw
insert into banking_raw.customer_data.customer_profiles
select  customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    address,
    city,
    province,
    postal_code,
    sin,
    customer_since,
    account_status,
    current_timestamp() as loaded_at
    from banking_staging.stg_customer.customer_profiles_stg;

-- create transaction staging table 
COPY INTO BANKING_RAW.transaction_data.transactions_staging
FROM @transaction_STAGE/transactions.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format');

-- load transactions data from staging to banking_raw
INSERT INTO BANKING_RAW.transaction_data.transactions (
    transaction_id,
    customer_id,
    account_id,
    transaction_date,
    transaction_type,
    amount,
    merchant_category,
    description,
    channel,
    loaded_at
)
SELECT 
    transaction_id,
    customer_id,
    account_id,
    transaction_date,
    transaction_type,
    amount,
    merchant_category,
    description,
    channel,
    CURRENT_TIMESTAMP()
FROM BANKING_staging.stg_transactions.transactions_staging;