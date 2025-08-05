-- Customer profiles (simulating CRM data)
CREATE TABLE BANKING_RAW.CUSTOMER_DATA.CUSTOMER_PROFILES (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    date_of_birth DATE,
    address STRING,
    city STRING,
    province STRING,
    postal_code STRING,
    sin STRING, -- Will be masked
    customer_since DATE,
    account_status STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);