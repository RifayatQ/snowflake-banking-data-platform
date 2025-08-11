CREATE VIEW BANKING_ANALYTICS.DATA_LINEAGE AS
SELECT 
    'BANKING_RAW.CUSTOMER_DATA.CUSTOMER_PROFILES' as source_table,
    'BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG' as target_table,
    'Data Quality Validation & Standardization' as transformation_type,
    'CUSTOMER_ETL_TASK' as process_name,
    'Hourly' as frequency
    
UNION ALL

SELECT 
    'BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG' as source_table,
    'BANKING_ANALYTICS.CUSTOMER_RISK_PROFILE' as target_table,
    'Risk Scoring & Analytics' as transformation_type,
    'Real-time View' as process_name,
    'On-demand' as frequency;