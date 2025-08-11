-- ETL Stored Procedure for Transaction Fact Table

USE DATABASE BANKING_MARTS;
USE SCHEMA FACT_TRANSACTIONS;

CREATE OR REPLACE PROCEDURE PROCESS_TRANSACTION_FACTS(START_DATE DATE, END_DATE DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    processed_count INTEGER;
    error_count INTEGER;
BEGIN
    
    -- Process transactions for the specified date range
    INSERT INTO FACT_TRANSACTIONS_DAILY (
        customer_key, account_key, date_key, time_key, merchant_key, channel_key,
        transaction_id, customer_id, account_id, transaction_date, transaction_timestamp,
        transaction_type, transaction_amount, transaction_fee, transaction_count,
        abs_transaction_amount, credit_amount, debit_amount, is_credit, is_debit,
        is_large_transaction, risk_score, fraud_score, is_flagged_transaction,
        description, merchant_name, location_city, location_country
    )
    SELECT 
        -- Dimension keys (lookup from dimension tables)
        COALESCE(dc.customer_key, -1) as customer_key,
        COALESCE(da.account_key, -1) as account_key,
        COALESCE(dd.date_key, TO_NUMBER(TO_CHAR(t.transaction_date, 'YYYYMMDD'))) as date_key,
        COALESCE(dt.time_key, EXTRACT(hour FROM t.transaction_date) * 100 + EXTRACT(minute FROM t.transaction_date)) as time_key,
        COALESCE(dm.merchant_key, -1) as merchant_key,
        COALESCE(dch.channel_key, -1) as channel_key,
        
        -- Natural keys and transaction details
        t.transaction_id,
        t.customer_id,
        t.account_id,
        DATE(t.transaction_date) as transaction_date,
        t.transaction_date as transaction_timestamp,
        t.transaction_type,
        t.amount as transaction_amount,
        0 as transaction_fee,  
        1 as transaction_count,
        
        -- Calculated measures
        ABS(t.amount) as abs_transaction_amount,
        CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END as credit_amount,
        CASE WHEN t.amount < 0 THEN ABS(t.amount) ELSE 0 END as debit_amount,
        t.amount > 0 as is_credit,
        t.amount < 0 as is_debit,
        ABS(t.amount) > 10000 as is_large_transaction,
        
        -- Risk scoring (from analytics if available)
        COALESCE(ta.composite_risk_score, 0) as risk_score,
        COALESCE(ta.composite_anomaly_score, 0) as fraud_score,
        ta.fraud_risk_level IN ('HIGH_RISK', 'MEDIUM_RISK') as is_flagged_transaction,
        
        -- Descriptive attributes
        t.description,
        t.description as merchant_name, 
        'Calgary' as location_city,      -- Default city
        'Canada' as location_country
        
    FROM BANKING_RAW.TRANSACTION_DATA.TRANSACTIONS t
    
    -- Dimension lookups
    LEFT JOIN BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER dc
        ON t.customer_id = dc.customer_id AND dc.is_current = TRUE
    
    LEFT JOIN BANKING_MARTS.DIM_PRODUCT.DIM_ACCOUNT da
        ON t.account_id = da.account_id
        
    LEFT JOIN BANKING_MARTS.DIM_TIME.DIM_DATE dd
        ON DATE(t.transaction_date) = dd.date_actual
        
    LEFT JOIN BANKING_MARTS.DIM_TIME.DIM_TIME_OF_DAY dt
        ON EXTRACT(hour FROM t.transaction_date) = dt.hour_24
        
    LEFT JOIN BANKING_MARTS.DIM_PRODUCT.DIM_MERCHANT dm
        ON t.merchant_category = dm.merchant_category
        
    LEFT JOIN BANKING_MARTS.DIM_PRODUCT.DIM_CHANNEL dch
        ON t.channel = dch.channel_name
        
    -- Join with analytics for risk scores
    LEFT JOIN BANKING_ANALYTICS.RISK_ANALYTICS.TRANSACTION_ANOMALIES ta
        ON t.transaction_id = ta.transaction_id
    
    WHERE DATE(t.transaction_date) BETWEEN :START_DATE AND :END_DATE
    AND t.transaction_id NOT IN (
        SELECT transaction_id FROM FACT_TRANSACTIONS_DAILY 
        WHERE transaction_date BETWEEN :START_DATE AND :END_DATE
    );

    GET DIAGNOSTICS processed_count = ROW_COUNT;

    RETURN 'Transaction fact processing complete. Processed ' || processed_count || ' transactions for date range ' || :START_DATE || ' to ' || :END_DATE;
END;
$$;

