-- Transaction data (simulating core banking)
CREATE TABLE BANKING_RAW.TRANSACTION_DATA.TRANSACTIONS (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_date TIMESTAMP_NTZ,
    transaction_type STRING,
    amount DECIMAL(15,2),
    merchant_category STRING,
    description STRING,
    channel STRING, -- ATM, Online, Branch, Mobile
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);