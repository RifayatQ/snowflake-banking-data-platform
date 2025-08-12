
USE DATABASE BANKING_RAW;
USE SCHEMA EXTERNAL_DATA;

-- Credit scores (simulating external bureau data)
CREATE TABLE IF NOT EXISTS CREDIT_SCORES (
    customer_id STRING,
    bureau_name STRING,
    credit_score INTEGER,
    score_date DATE,
    risk_category STRING,
    score_type STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    data_source STRING,
    score_model STRING
);