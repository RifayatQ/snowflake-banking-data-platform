-- Create clustered table for transactions
CREATE TABLE BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_date DATE,
    transaction_month DATE, -- For monthly partitioning
    transaction_type STRING,
    amount DECIMAL(15,2),
    merchant_category STRING,
    description STRING,
    channel STRING,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (transaction_date, customer_id);

-- Populate with derived date fields
INSERT INTO BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED
SELECT 
    transaction_id,
    customer_id,
    account_id,
    DATE(transaction_date) as transaction_date,
    DATE_TRUNC('MONTH', transaction_date) as transaction_month,
    transaction_type,
    amount,
    merchant_category,
    description,
    channel,
    CURRENT_TIMESTAMP()
FROM BANKING_RAW.TRANSACTION_DATA.TRANSACTIONS;