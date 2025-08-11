-- ETL Stored Procedure for Daily Customer Summaries

USE DATABASE BANKING_MARTS;
USE SCHEMA FACT_TRANSACTIONS;

CREATE OR REPLACE PROCEDURE PROCESS_DAILY_CUSTOMER_SUMMARY(SUMMARY_DATE DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    processed_count INTEGER;
BEGIN
    
    -- Delete existing summary for the date (if reprocessing)
    DELETE FROM FACT_CUSTOMER_DAILY WHERE summary_date = :SUMMARY_DATE;

    -- Insert daily customer summaries
    INSERT INTO FACT_CUSTOMER_DAILY (
        customer_key, date_key, customer_id, summary_date,
        transaction_count, total_credit_amount, total_debit_amount, net_transaction_amount,
        atm_transactions, online_transactions, mobile_transactions, branch_transactions,
        high_risk_transactions, flagged_transactions, max_transaction_amount,
        channels_used, merchant_categories_used
    )
    WITH daily_summary AS (
        SELECT 
            ft.customer_key,
            ft.date_key,
            ft.customer_id,
            :SUMMARY_DATE as summary_date,
            
            -- Basic transaction metrics
            COUNT(*) as transaction_count,
            SUM(ft.credit_amount) as total_credit_amount,
            SUM(ft.debit_amount) as total_debit_amount,
            SUM(ft.transaction_amount) as net_transaction_amount,
            
            -- Channel breakdown
            COUNT(CASE WHEN dch.channel_name = 'ATM' THEN 1 END) as atm_transactions,
            COUNT(CASE WHEN dch.channel_name = 'Online' THEN 1 END) as online_transactions,
            COUNT(CASE WHEN dch.channel_name = 'Mobile' THEN 1 END) as mobile_transactions,
            COUNT(CASE WHEN dch.channel_name = 'Branch' THEN 1 END) as branch_transactions,
            
            -- Risk metrics
            COUNT(CASE WHEN ft.risk_score > 70 THEN 1 END) as high_risk_transactions,
            COUNT(CASE WHEN ft.is_flagged_transaction THEN 1 END) as flagged_transactions,
            MAX(ft.abs_transaction_amount) as max_transaction_amount,
            
            -- Diversity metrics
            COUNT(DISTINCT dch.channel_name) as channels_used,
            COUNT(DISTINCT dm.merchant_category) as merchant_categories_used
            
        FROM FACT_TRANSACTIONS_DAILY ft
        LEFT JOIN BANKING_MARTS.DIM_PRODUCT.DIM_CHANNEL dch
            ON ft.channel_key = dch.channel_key
        LEFT JOIN BANKING_MARTS.DIM_PRODUCT.DIM_MERCHANT dm
            ON ft.merchant_key = dm.merchant_key
            
        WHERE ft.transaction_date = :SUMMARY_DATE
        GROUP BY ft.customer_key, ft.date_key, ft.customer_id
    )
    SELECT * FROM daily_summary;

    GET DIAGNOSTICS processed_count = ROW_COUNT;

    RETURN 'Daily customer summary processing complete. Processed ' || processed_count || ' customer summaries for ' || :SUMMARY_DATE;
END;
$$;