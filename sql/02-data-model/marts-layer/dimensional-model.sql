USE DATABASE BANKING_MARTS;

-- Create schemas for different business domains
CREATE SCHEMA IF NOT EXISTS DIM_CUSTOMER 
    COMMENT = 'Customer dimension tables and related dimensions';

CREATE SCHEMA IF NOT EXISTS FACT_TRANSACTIONS 
    COMMENT = 'Transaction fact tables and related metrics';

CREATE SCHEMA IF NOT EXISTS DIM_TIME 
    COMMENT = 'Time dimension and date-related lookups';

CREATE SCHEMA IF NOT EXISTS DIM_PRODUCT 
    COMMENT = 'Banking product dimensions and hierarchies';

-- DIMENSION TABLES

-- Customer Dimension (SCD Type 2 for tracking changes)
USE SCHEMA DIM_CUSTOMER;

CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
    customer_key INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    email_masked STRING,
    phone_masked STRING,
     date_of_birth DATE,
    age_group STRING,
    customer_age INTEGER,
    address STRING,
    city STRING,
    province STRING,
    postal_code STRING,
    region STRING,
    customer_since DATE,
    customer_tenure_years INTEGER,
    tenure_category STRING,
    customer_segment STRING,
    account_status STRING,
    credit_score INTEGER,
    risk_category STRING,
    risk_tier INTEGER,,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), --audit field
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), --audit field
    CONSTRAINT uk_customer_effective UNIQUE (customer_id, effective_date)
)
CLUSTER BY (customer_id, effective_date)
COMMENT = 'Customer dimension with SCD Type 2 for historical tracking';

-- Account dimension
CREATE TABLE IF NOT EXISTS DIM_ACCOUNT (
    account_key INTEGER AUTOINCREMENT PRIMARY KEY,
    account_id STRING NOT NULL UNIQUE,
    customer_id STRING NOT NULL,
    account_type STRING,
    account_status STRING,
    account_opened_date DATE,
    account_closed_date DATE,
    product_category STRING,
    product_subcategory STRING,
    is_primary_account BOOLEAN,
    credit_limit DECIMAL(15,2),
    interest_rate DECIMAL(5,4),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (customer_id)
COMMENT = 'Account dimension for banking products';

--Time dimension
USE SCHEMA DIM_TIME;

CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_of_week_name STRING,
    day_of_year INTEGER,
    week_of_year INTEGER,
    week_start_date DATE,
    week_end_date DATE,
    month_number INTEGER,
    month_name STRING,
    month_year STRING,
    first_day_of_month DATE,
    last_day_of_month DATE,
    quarter_number INTEGER,
    quarter_name STRING,
    quarter_year STRING,
    first_day_of_quarter DATE,
    last_day_of_quarter DATE,
    year_number INTEGER,
    first_day_of_year DATE,
    last_day_of_year DATE,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    is_business_day BOOLEAN,
    holiday_name STRING,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER
)
COMMENT = 'Date dimension for time-based analysis';

--Time of day dimension
CREATE TABLE IF NOT EXISTS DIM_TIME_OF_DAY (
    time_key INTEGER PRIMARY KEY,
    hour_24 INTEGER,
    hour_12 INTEGER,
    minute INTEGER,
    am_pm STRING,
    time_period STRING, -- Morning, Afternoon, Evening, Night
    business_hours STRING, -- Business Hours, After Hours
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Time of day dimension for hourly analysis';

-- Merchant Category Dimension
USE SCHEMA DIM_PRODUCT;

CREATE TABLE IF NOT EXISTS DIM_MERCHANT (
    merchant_key INTEGER AUTOINCREMENT PRIMARY KEY,
    merchant_category STRING NOT NULL,
    category_group STRING,
    category_type STRING,
    is_high_risk BOOLEAN DEFAULT FALSE,
    category_description STRING,
    typical_transaction_range STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Merchant category dimension for transaction analysis';

-- Channel Dimension
CREATE TABLE IF NOT EXISTS DIM_CHANNEL (
    channel_key INTEGER AUTOINCREMENT PRIMARY KEY,
    channel_name STRING NOT NULL UNIQUE,
    channel_type STRING, -- Digital, Physical, Phone
    is_self_service BOOLEAN,
    is_assisted BOOLEAN,
    availability_hours STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Transaction channel dimension';