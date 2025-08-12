-- Data quality validation procedures for ETL pipelines

USE DATABASE BANKING_MARTS;

CREATE OR REPLACE PROCEDURE VALIDATE_DIMENSION_QUALITY()
RETURNS TABLE (
    table_name STRING, 
    check_type STRING, 
    check_result STRING, 
    record_count INTEGER, 
    issue_description STRING
)
LANGUAGE SQL
AS
$
BEGIN
    LET result_set RESULTSET := (
        -- Customer Dimension Checks
        SELECT 
            'DIM_CUSTOMER' as table_name,
            'Duplicate Check' as check_type,
            CASE WHEN duplicate_count > 0 THEN 'FAIL' ELSE 'PASS' END as check_result,
            duplicate_count as record_count,
            CASE WHEN duplicate_count > 0 THEN 'Found duplicate customer records' ELSE 'No duplicates found' END as issue_description
        FROM (
            SELECT COUNT(*) - COUNT(DISTINCT customer_id || effective_date) as duplicate_count
            FROM BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER
        )
        
        UNION ALL
        
        SELECT 
            'DIM_CUSTOMER',
            'SCD Integrity',
            CASE WHEN overlapping_count > 0 THEN 'FAIL' ELSE 'PASS' END,
            overlapping_count,
            CASE WHEN overlapping_count > 0 THEN 'Found overlapping SCD records' ELSE 'SCD integrity maintained' END
        FROM (
            SELECT COUNT(*) as overlapping_count
            FROM BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER d1
            JOIN BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER d2 
                ON d1.customer_id = d2.customer_id 
                AND d1.customer_key != d2.customer_key
            WHERE d1.effective_date <= d2.expiration_date
                AND d2.effective_date <= d1.expiration_date
                AND d1.is_current = TRUE AND d2.is_current = TRUE
        )
        
        UNION ALL
        
        -- Transaction Fact Checks
        SELECT 
            'FACT_TRANSACTIONS_DAILY',
            'Orphan Records',
            CASE WHEN orphan_count > 0 THEN 'FAIL' ELSE 'PASS' END,
            orphan_count,
            CASE WHEN orphan_count > 0 THEN 'Found transactions without customer dimension' ELSE 'All transactions have valid customers' END
        FROM (
            SELECT COUNT(*) as orphan_count
            FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_TRANSACTIONS_DAILY f
            LEFT JOIN BANKING_MARTS.DIM_CUSTOMER.DIM_CUSTOMER d 
                ON f.customer_key = d.customer_key
            WHERE d.customer_key IS NULL AND f.customer_key != -1
        )
        
        UNION ALL
        
        SELECT 
            'FACT_TRANSACTIONS_DAILY',
            'Amount Validation',
            CASE WHEN invalid_amounts > 0 THEN 'FAIL' ELSE 'PASS' END,
            invalid_amounts,
            CASE WHEN invalid_amounts > 0 THEN 'Found transactions with invalid amounts' ELSE 'All amounts are valid' END
        FROM (
            SELECT COUNT(*) as invalid_amounts
            FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_TRANSACTIONS_DAILY
            WHERE transaction_amount IS NULL 
                OR ABS(transaction_amount) > 1000000  -- Unrealistic amounts
                OR (credit_amount > 0 AND debit_amount > 0)  -- Should be one or the other
        )
    );
    
    RETURN TABLE(result_set);
END;
$;

-- Data reconciliation between raw and marts
CREATE OR REPLACE PROCEDURE RECONCILE_TRANSACTION_COUNTS(CHECK_DATE DATE)
RETURNS TABLE (
    source_layer STRING,
    transaction_count INTEGER,
    total_amount DECIMAL(15,2),
    variance_count INTEGER,
    variance_amount DECIMAL(15,2)
)
LANGUAGE SQL
AS
$
BEGIN
    LET result_set RESULTSET := (
        WITH raw_summary AS (
            SELECT 
                COUNT(*) as raw_count,
                SUM(amount) as raw_amount
            FROM BANKING_RAW.TRANSACTION_DATA.TRANSACTIONS
            WHERE DATE(transaction_date) = :CHECK_DATE
        ),
        marts_summary AS (
            SELECT 
                COUNT(*) as marts_count,
                SUM(transaction_amount) as marts_amount
            FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_TRANSACTIONS_DAILY
            WHERE transaction_date = :CHECK_DATE
        )
        SELECT 
            'RAW_LAYER' as source_layer,
            rs.raw_count as transaction_count,
            rs.raw_amount as total_amount,
            rs.raw_count - ms.marts_count as variance_count,
            rs.raw_amount - ms.marts_amount as variance_amount
        FROM raw_summary rs
        CROSS JOIN marts_summary ms
        
        UNION ALL
        
        SELECT 
            'MARTS_LAYER',
            ms.marts_count,
            ms.marts_amount,
            rs.raw_count - ms.marts_count,
            rs.raw_amount - ms.marts_amount
        FROM raw_summary rs
        CROSS JOIN marts_summary ms
    );
    
    RETURN TABLE(result_set);
END;
$;