-- Operational monitoring and KPI dashboard

USE DATABASE BANKING_ANALYTICS;

-- Operational metrics dashboard view
CREATE OR REPLACE VIEW OPERATIONAL_DASHBOARD AS

-- Data Quality Metrics
SELECT 
    'Data Quality' as metric_category,
    'Customer Data Quality %' as metric_name,
    ROUND(
        COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) * 100.0 / COUNT(*), 2
    ) as metric_value,
    '%' as unit,
    COUNT(*) as total_records,
    CURRENT_TIMESTAMP() as last_updated
FROM BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG

UNION ALL

-- ETL Performance Metrics
SELECT 
    'ETL Performance' as metric_category,
    'Average ETL Runtime (min)' as metric_name,
    ROUND(AVG(execution_time)/1000/60, 2) as metric_value,
    'minutes' as unit,
    COUNT(*) as total_executions,
    MAX(start_time) as last_updated
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text LIKE '%PROCESS_CUSTOMER_DATA%'
AND start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())

UNION ALL

-- System Performance
SELECT 
    'System Performance' as metric_category,
    'Average Query Time (sec)' as metric_name,
    ROUND(AVG(execution_time)/1000, 2) as metric_value,
    'seconds' as unit,
    COUNT(*) as total_queries,
    MAX(start_time) as last_updated
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
AND warehouse_name IS NOT NULL

UNION ALL

-- Cost Metrics
SELECT 
    'Cost Management' as metric_category,
    'Daily Credits Used' as metric_name,
    ROUND(SUM(credits_used), 2) as metric_value,
    'credits' as unit,
    COUNT(DISTINCT warehouse_name) as active_warehouses,
    MAX(start_time) as last_updated
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())

UNION ALL

-- Business Metrics
SELECT 
    'Business Metrics' as metric_category,
    'High Risk Customers' as metric_name,
    COUNT(*) as metric_value,
    'count' as unit,
    (SELECT COUNT(*) FROM BANKING_ANALYTICS.RISK_ANALYTICS.CUSTOMER_RISK_PROFILE) as total_customers,
    CURRENT_TIMESTAMP() as last_updated
FROM BANKING_ANALYTICS.RISK_ANALYTICS.CUSTOMER_RISK_PROFILE
WHERE final_risk_category IN ('HIGH', 'VERY_HIGH')

UNION ALL

-- Fraud Detection
SELECT 
    'Security' as metric_category,
    'Fraud Alerts (24h)' as metric_name,
    COUNT(*) as metric_value,
    'alerts' as unit,
    COUNT(DISTINCT customer_id) as affected_customers,
    MAX(analyzed_at) as last_updated
FROM BANKING_ANALYTICS.RISK_ANALYTICS.TRANSACTION_ANOMALIES
WHERE fraud_risk_level IN ('HIGH_RISK', 'MEDIUM_RISK')
AND analyzed_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Create summary performance view for quick health checks
CREATE OR REPLACE VIEW PLATFORM_HEALTH_CHECK AS
WITH health_metrics AS (
    SELECT 
        -- Data freshness
        CASE WHEN MAX(loaded_at) >= DATEADD(hour, -2, CURRENT_TIMESTAMP()) 
             THEN 'HEALTHY' ELSE 'STALE' END as data_freshness_status,
        
        -- Data quality
        CASE WHEN (COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) * 100.0 / COUNT(*)) >= 95
             THEN 'HEALTHY' ELSE 'DEGRADED' END as data_quality_status,
             
        COUNT(*) as total_customer_records,
        MAX(loaded_at) as last_data_load
        
    FROM BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG
),
performance_metrics AS (
    SELECT 
        -- Query performance
        CASE WHEN AVG(execution_time/1000) <= 30 
             THEN 'HEALTHY' ELSE 'SLOW' END as query_performance_status,
        ROUND(AVG(execution_time/1000), 2) as avg_query_time_seconds,
        COUNT(*) as queries_last_hour
        
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    AND warehouse_name IS NOT NULL
)

SELECT 
    hm.data_freshness_status,
    hm.data_quality_status,
    pm.query_performance_status,
    
    -- Overall health
    CASE 
        WHEN hm.data_freshness_status = 'HEALTHY' 
             AND hm.data_quality_status = 'HEALTHY'
             AND pm.query_performance_status = 'HEALTHY' THEN 'HEALTHY'
        WHEN hm.data_freshness_status = 'STALE' 
             OR hm.data_quality_status = 'DEGRADED' THEN 'DEGRADED'
        ELSE 'WARNING'
    END as overall_platform_health,
    
    hm.total_customer_records,
    hm.last_data_load,
    pm.avg_query_time_seconds,
    pm.queries_last_hour,
    CURRENT_TIMESTAMP() as health_check_time
    
FROM health_metrics hm
CROSS JOIN performance_metrics pm;