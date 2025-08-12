-- Initial bootstrap script to populate marts layer with existing data

CREATE OR REPLACE PROCEDURE BOOTSTRAP_MARTS_LAYER()
RETURNS STRING
LANGUAGE SQL
AS
$
DECLARE
    start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    end_time TIMESTAMP_NTZ;
    total_customers INTEGER;
    total_transactions INTEGER;
BEGIN
    
    -- 1: Populate all dimension tables
    CALL BANKING_MARTS.DIM_TIME.POPULATE_DATE_DIMENSION();
    CALL BANKING_MARTS.DIM_TIME.POPULATE_TIME_OF_DAY_DIMENSION();
    CALL BANKING_MARTS.DIM_PRODUCT.POPULATE_MERCHANT_DIMENSION();
    CALL BANKING_MARTS.DIM_PRODUCT.POPULATE_CHANNEL_DIMENSION();
    
    -- 2: Initial customer dimension load
    CALL BANKING_MARTS.DIM_CUSTOMER.PROCESS_CUSTOMER_DIMENSION();
    LET total_customers := (SELECT COUNT(*) FROM BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER WHERE is_current = TRUE);
    
    -- 3: Process all historical transactions
    -- Get date range of existing data
    LET min_date := (SELECT MIN(DATE(transaction_date)) FROM BANKING_RAW.TRANSACTION_DATA.TRANSACTIONS);
    LET max_date := (SELECT MAX(DATE(transaction_date)) FROM BANKING_RAW.TRANSACTION_DATA.TRANSACTIONS);
    
    -- Process transactions in chunks (monthly)
    LET current_date := min_date;
    WHILE (current_date <= max_date) DO
        LET month_end := LAST_DAY(current_date);
        CALL BANKING_MARTS.FACT_TRANSACTIONS.PROCESS_TRANSACTION_FACTS(:current_date, :month_end);
        LET current_date := DATEADD(month, 1, current_date);
    END WHILE;
    
    LET total_transactions := (SELECT COUNT(*) FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_TRANSACTIONS_DAILY);
    
    -- 4: Generate daily summaries for all dates
    LET current_date := min_date;
    WHILE (current_date <= max_date) DO
        CALL BANKING_MARTS.FACT_TRANSACTIONS.PROCESS_DAILY_CUSTOMER_SUMMARY(:current_date);
        LET current_date := DATEADD(day, 1, current_date);
    END WHILE;
    
    LET end_time := CURRENT_TIMESTAMP();
    
    RETURN 'Bootstrap completed in ' || DATEDIFF(second, :start_time, :end_time) || ' seconds. ' ||
           'Processed ' || :total_customers || ' customers and ' || :total_transactions || ' transactions.';
END;
$;

-- Quick verification queries
CREATE OR REPLACE VIEW BANKING_MARTS.MONITORING.MARTS_SUMMARY AS
SELECT 
    'Customer Dimension' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT customer_id) as unique_keys,
    MAX(updated_at) as last_updated
FROM BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER
WHERE is_current = TRUE

UNION ALL

SELECT 
    'Transaction Facts',
    COUNT(*),
    COUNT(DISTINCT customer_id),
    MAX(created_at)
FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_TRANSACTIONS_DAILY

UNION ALL

SELECT 
    'Daily Summaries',
    COUNT(*),
    COUNT(DISTINCT customer_id),
    MAX(created_at)
FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_CUSTOMER_DAILY

UNION ALL

SELECT 
    'Date Dimension',
    COUNT(*),
    COUNT(DISTINCT date_actual),
    MAX(date_actual)::TIMESTAMP_NTZ
FROM BANKING_MARTS.DIM_TIME.DIM_DATE;