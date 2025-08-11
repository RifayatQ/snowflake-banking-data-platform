USE SCHEMA FACT_TRANSACTIONS;

-- Main Transaction Fact Table
CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS_DAILY (
    -- Surrogate keys
    transaction_key INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    account_key INTEGER,
    date_key INTEGER NOT NULL,
    time_key INTEGER,
    merchant_key INTEGER,
    channel_key INTEGER,
     -- Natural keys
    transaction_id STRING NOT NULL UNIQUE,
    customer_id STRING NOT NULL,
    account_id STRING,
    -- Measures (additive)
    transaction_amount DECIMAL(15,2) NOT NULL,
    transaction_fee DECIMAL(10,2) DEFAULT 0,
    transaction_count INTEGER DEFAULT 1,
    -- Calculated measures
    abs_transaction_amount DECIMAL(15,2),
    credit_amount DECIMAL(15,2),
    debit_amount DECIMAL(15,2),
    -- Flags and categories
    is_credit BOOLEAN,
    is_debit BOOLEAN,
    is_large_transaction BOOLEAN,
    is_international BOOLEAN DEFAULT FALSE,
    -- Risk and fraud indicators
    risk_score INTEGER,
    fraud_score DECIMAL(5,2),
    is_flagged_transaction BOOLEAN DEFAULT FALSE,
    -- Descriptive attributes
    description STRING,
    merchant_name STRING,
    location_city STRING,
    location_country STRING DEFAULT 'Canada',
    -- Audit fields
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
CLUSTER BY (transaction_date, customer_id)
COMMENT = 'Daily transaction fact table with full transaction details';

-- Customer Daily Summary Fact
CREATE TABLE IF NOT EXISTS FACT_CUSTOMER_DAILY (
    customer_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    customer_id STRING NOT NULL,
    summary_date DATE NOT NULL,
    transaction_count INTEGER DEFAULT 0,
    total_credit_amount DECIMAL(15,2) DEFAULT 0,
    total_debit_amount DECIMAL(15,2) DEFAULT 0,
    net_transaction_amount DECIMAL(15,2) DEFAULT 0,
    atm_transactions INTEGER DEFAULT 0,
    online_transactions INTEGER DEFAULT 0,
    mobile_transactions INTEGER DEFAULT 0,
    branch_transactions INTEGER DEFAULT 0,
    high_risk_transactions INTEGER DEFAULT 0,
    flagged_transactions INTEGER DEFAULT 0,
    max_transaction_amount DECIMAL(15,2) DEFAULT 0,
    channels_used INTEGER DEFAULT 0,
    merchant_categories_used INTEGER DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_key, date_key)
)
CLUSTER BY (summary_date, customer_id)
COMMENT = 'Daily customer transaction summary for performance';

-- Account Balance Fact (Snapshot)
CREATE TABLE IF NOT EXISTS FACT_ACCOUNT_BALANCE_DAILY (
    account_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    account_id STRING NOT NULL,
    balance_date DATE NOT NULL,
    opening_balance DECIMAL(15,2),
    closing_balance DECIMAL(15,2),
    available_balance DECIMAL(15,2),
    daily_credits DECIMAL(15,2) DEFAULT 0,
    daily_debits DECIMAL(15,2) DEFAULT 0,
    daily_transaction_count INTEGER DEFAULT 0,
    credit_limit DECIMAL(15,2),
    credit_utilization_pct DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (account_key, date_key)
)
CLUSTER BY (balance_date)
COMMENT = 'Daily account balance snapshots';
