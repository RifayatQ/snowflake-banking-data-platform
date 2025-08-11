-- Key metrics

SELECT 
    'Total Customers Processed' as metric,
    COUNT(*) as value
FROM BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG
WHERE data_quality_flag = 'VALID'

UNION ALL

SELECT 
    'Total Transactions Analyzed' as metric,
    COUNT(*) as value
FROM BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED

UNION ALL

SELECT 
    'High Risk Customers Identified' as metric,
    COUNT(*) as value
FROM BANKING_ANALYTICS.CUSTOMER_RISK_PROFILE
WHERE final_risk_category = 'HIGH'

UNION ALL

SELECT 
    'Anomalous Transactions Detected' as metric,
    COUNT(*) as value
FROM BANKING_ANALYTICS.TRANSACTION_ANOMALIES
WHERE anomaly_type != 'NORMAL';