-- Master ETL orchestration and monitoring

USE DATABASE BANKING_MARTS;

CREATE OR REPLACE PROCEDURE RUN_DAILY_ETL_PIPELINE(PROCESS_DATE DATE)
RETURNS TABLE (
    step_name STRING,
    status STRING,
    duration_seconds INTEGER,
    records_processed INTEGER,
    error_message STRING
)
LANGUAGE SQL
AS
$
DECLARE
    step_start TIMESTAMP_NTZ;
    step_end TIMESTAMP_NTZ;
    step_duration INTEGER;
    step_result STRING;
    records_count INTEGER;
    error_msg STRING DEFAULT NULL;
BEGIN
    
    -- Create results table
    CREATE OR REPLACE TEMPORARY TABLE etl_results (
        step_name STRING,
        status STRING,
        duration_seconds INTEGER,
        records_processed INTEGER,
        error_message STRING
    );
    
    -- 1: Populate Dimensions
    LET step_start := CURRENT_TIMESTAMP();
    BEGIN
        CALL BANKING_MARTS.DIM_TIME.POPULATE_DATE_DIMENSION();
        CALL BANKING_MARTS.DIM_TIME.POPULATE_TIME_OF_DAY_DIMENSION();
        CALL BANKING_MARTS.DIM_PRODUCT.POPULATE_MERCHANT_DIMENSION();
        CALL BANKING_MARTS.DIM_PRODUCT.POPULATE_CHANNEL_DIMENSION();
        
        LET step_end := CURRENT_TIMESTAMP();
        LET step_duration := DATEDIFF(second, :step_start, :step_end);
        
        INSERT INTO etl_results VALUES (
            'Populate Dimensions', 'SUCCESS', :step_duration, 0, NULL
        );
    EXCEPTION
        WHEN OTHER THEN
            LET error_msg := SQLERRM;
            LET step_end := CURRENT_TIMESTAMP();
            LET step_duration := DATEDIFF(second, :step_start, :step_end);
            
            INSERT INTO etl_results VALUES (
                'Populate Dimensions', 'FAILED', :step_duration, 0, :error_msg
            );
    END;
    
    -- 2: Process Customer Dimension
    LET step_start := CURRENT_TIMESTAMP();
    BEGIN
        LET step_result := (CALL BANKING_MARTS.DIM_CUSTOMER.PROCESS_CUSTOMER_DIMENSION());
        
        LET step_end := CURRENT_TIMESTAMP();
        LET step_duration := DATEDIFF(second, :step_start, :step_end);
        
        INSERT INTO etl_results VALUES (
            'Customer Dimension', 'SUCCESS', :step_duration, 0, :step_result
        );
    EXCEPTION
        WHEN OTHER THEN
            LET error_msg := SQLERRM;
            LET step_end := CURRENT_TIMESTAMP();
            LET step_duration := DATEDIFF(second, :step_start, :step_end);
            
            INSERT INTO etl_results VALUES (
                'Customer Dimension', 'FAILED', :step_duration, 0, :error_msg
            );
    END;
    
    -- 3: Process Transaction Facts
    LET step_start := CURRENT_TIMESTAMP();
    BEGIN
        LET step_result := (CALL BANKING_MARTS.FACT_TRANSACTIONS.PROCESS_TRANSACTION_FACTS(:PROCESS_DATE, :PROCESS_DATE));
        
        LET step_end := CURRENT_TIMESTAMP();
        LET step_duration := DATEDIFF(second, :step_start, :step_end);
        LET records_count := (SELECT COUNT(*) FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_TRANSACTIONS_DAILY WHERE transaction_date = :PROCESS_DATE);
        
        INSERT INTO etl_results VALUES (
            'Transaction Facts', 'SUCCESS', :step_duration, :records_count, :step_result
        );
    EXCEPTION
        WHEN OTHER THEN
            LET error_msg := SQLERRM;
            LET step_end := CURRENT_TIMESTAMP();
            LET step_duration := DATEDIFF(second, :step_start, :step_end);
            
            INSERT INTO etl_results VALUES (
                'Transaction Facts', 'FAILED', :step_duration, 0, :error_msg
            );
    END;
    
    -- 4: Process Daily Summaries
    LET step_start := CURRENT_TIMESTAMP();
    BEGIN
        LET step_result := (CALL BANKING_MARTS.FACT_TRANSACTIONS.PROCESS_DAILY_CUSTOMER_SUMMARY(:PROCESS_DATE));
        
        LET step_end := CURRENT_TIMESTAMP();
        LET step_duration := DATEDIFF(second, :step_start, :step_end);
        LET records_count := (SELECT COUNT(*) FROM BANKING_MARTS.FACT_TRANSACTIONS.FACT_CUSTOMER_DAILY WHERE summary_date = :PROCESS_DATE);
        
        INSERT INTO etl_results VALUES (
            'Daily Summaries', 'SUCCESS', :step_duration, :records_count, :step_result
        );
    EXCEPTION
        WHEN OTHER THEN
            LET error_msg := SQLERRM;
            LET step_end := CURRENT_TIMESTAMP();
            LET step_duration := DATEDIFF(second, :step_start, :step_end);
            
            INSERT INTO etl_results VALUES (
                'Daily Summaries', 'FAILED', :step_duration, 0, :error_msg
            );
    END;
    
    -- 5: Data Quality Validation
    LET step_start := CURRENT_TIMESTAMP();
    BEGIN
        -- Run quality checks
        CALL VALIDATE_DIMENSION_QUALITY();
        CALL RECONCILE_TRANSACTION_COUNTS(:PROCESS_DATE);
        
        LET step_end := CURRENT_TIMESTAMP();
        LET step_duration := DATEDIFF(second, :step_start, :step_end);
        
        INSERT INTO etl_results VALUES (
            'Data Quality Validation', 'SUCCESS', :step_duration, 0, 'Quality checks completed'
        );
    EXCEPTION
        WHEN OTHER THEN
            LET error_msg := SQLERRM;
            LET step_end := CURRENT_TIMESTAMP();
            LET step_duration := DATEDIFF(second, :step_start, :step_end);
            
            INSERT INTO etl_results VALUES (
                'Data Quality Validation', 'FAILED', :step_duration, 0, :error_msg
            );
    END;
    
    -- Return results
    LET result_set RESULTSET := (SELECT * FROM etl_results ORDER BY step_name);
    RETURN TABLE(result_set);
END;
$;