CREATE VIEW reports.PERFORMANCE_METRICS AS
SELECT 
    query_id,
    query_text,
    user_name,
    warehouse_name,
    execution_time,
    bytes_scanned,
    rows_produced,
    start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
AND warehouse_name IS NOT NULL
ORDER BY execution_time DESC;
