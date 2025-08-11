-- Stream to capture transaction changes for real-time processing

USE DATABASE BANKING_MARTS;

-- Create stream on raw transactions table
CREATE OR REPLACE STREAM TRANSACTION_CHANGES_STREAM 
ON TABLE BANKING_RAW.TRANSACTION_DATA.TRANSACTIONS
COMMENT = 'Stream to capture new transactions for real-time fact table updates';

-- Create procedure to process stream data
CREATE OR REPLACE PROCEDURE PROCESS_TRANSACTION_STREAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    stream_count INTEGER;
BEGIN
    
    -- Check if stream has data
    LET stream_count := (SELECT COUNT(*) FROM TRANSACTION_CHANGES_STREAM);
    
    IF (stream_count > 0) THEN
        -- Process new transactions from stream
        INSERT INTO BANKING_MARTS.FACT_TRANSACTIONS.FACT_TRANSACTIONS_DAILY (
            customer_key, transaction_id, customer_id, transaction_date, 
            transaction_timestamp, transaction_type, transaction_amount,
            abs_transaction_amount, credit_amount, debit_amount, 
            is_credit, is_debit, is_large_transaction, description
        )
        SELECT 
            COALESCE(dc.customer_key, -1),
            s.transaction_id,
            s.customer_id,
            DATE(s.transaction_date),
            s.transaction_date,
            s.transaction_type,
            s.amount,
            ABS(s.amount),
            CASE WHEN s.amount > 0 THEN s.amount ELSE 0 END,
            CASE WHEN s.amount < 0 THEN ABS(s.amount) ELSE 0 END,
            s.amount > 0,
            s.amount < 0,
            ABS(s.amount) > 10000,
            s.description
            
        FROM TRANSACTION_CHANGES_STREAM s
        LEFT JOIN BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER dc
            ON s.customer_id = dc.customer_id AND dc.is_current = TRUE
        WHERE s.METADATA$ACTION = 'INSERT';
        
        RETURN 'Processed ' || stream_count || ' transactions from stream';
    ELSE
        RETURN 'No new transactions to process';
    END IF;
END;
$$;

-- Task to process stream every 5 minutes
CREATE OR REPLACE TASK REAL_TIME_TRANSACTION_TASK
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'Process transaction stream every 5 minutes'
    WHEN SYSTEM$STREAM_HAS_DATA('TRANSACTION_CHANGES_STREAM')
AS
    CALL PROCESS_TRANSACTION_STREAM();

ALTER TASK REAL_TIME_TRANSACTION_TASK RESUME;