-- Create Iceberg table for long-term transaction storage
CREATE OR REPLACE ICEBERG TABLE BANKING_ARCHIVE.HISTORICAL.TRANSACTIONS_ICEBERG (
    transaction_id STRING,
    customer_id STRING,
    account_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount DECIMAL(15,2),
    transaction_type STRING,
    merchant_category STRING,
    channel STRING,
    description STRING,
    location_country STRING,
    location_city STRING,
    risk_score INTEGER,
    processed_date DATE,
    archive_year INTEGER,
    archive_month INTEGER
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'banking_external_volume'
BASE_LOCATION = 'transactions/'
PARTITION BY (archive_year, archive_month);

-- Data migration to Iceberg
CREATE OR REPLACE PROCEDURE MIGRATE_TO_ICEBERG(START_DATE DATE, END_DATE DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    migration_count INTEGER;
BEGIN
    -- Insert historical data into Iceberg table
    INSERT INTO BANKING_ARCHIVE.HISTORICAL.TRANSACTIONS_ICEBERG
    SELECT 
        t.transaction_id,
        t.customer_id,
        t.account_id,
        t.transaction_date,
        t.amount,
        t.transaction_type,
        t.merchant_category,
        t.channel,
        t.description,
        'Canada' as location_country,
        c.city as location_city,
        COALESCE(r.composite_risk_score, 0) as risk_score,
        CURRENT_DATE() as processed_date,
        YEAR(t.transaction_date) as archive_year,
        MONTH(t.transaction_date) as archive_month
    FROM BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED t
    LEFT JOIN BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG c
        ON t.customer_id = c.customer_id
    LEFT JOIN BANKING_ANALYTICS.RISK_ANALYTICS.CUSTOMER_RISK_PROFILE r
        ON t.customer_id = r.customer_id
    WHERE DATE(t.transaction_date) BETWEEN :START_DATE AND :END_DATE;
    
    GET DIAGNOSTICS migration_count = ROW_COUNT;
    
    RETURN 'Migrated ' || migration_count || ' transactions to Iceberg archive';
END;
$$;

-- Time-travel analytics on Iceberg data
CREATE OR REPLACE VIEW BANKING_ANALYTICS.HISTORICAL.TRANSACTION_TRENDS AS
SELECT 
    archive_year,
    archive_month,
    transaction_type,
    merchant_category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM BANKING_ARCHIVE.HISTORICAL.TRANSACTIONS_ICEBERG
WHERE archive_year >= YEAR(CURRENT_DATE()) - 5  -- Last 5 years
GROUP BY archive_year, archive_month, transaction_type, merchant_category
ORDER BY archive_year DESC, archive_month DESC;