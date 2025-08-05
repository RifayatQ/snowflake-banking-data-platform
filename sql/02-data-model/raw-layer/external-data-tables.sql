-- Credit scores (simulating external bureau data)
CREATE TABLE BANKING_RAW.EXTERNAL_DATA.CREDIT_SCORES (
    customer_id STRING,
    bureau_name STRING,
    credit_score INTEGER,
    score_date DATE,
    risk_category STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);