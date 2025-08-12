-- Replace the traditional ETL task with a dynamic table
CREATE OR REPLACE DYNAMIC TABLE BANKING_ANALYTICS.REAL_TIME.CUSTOMER_RISK_LIVE (
    customer_id,
    risk_score,
    risk_category,
    last_transaction_date,
    suspicious_activity_count,
    credit_utilization,
    risk_factors,
    scored_at
)
TARGET_LAG = '5 minutes'  -- Refresh every 5 minutes
WAREHOUSE = ANALYTICS_WH
AS
WITH recent_activity AS (
    SELECT 
        customer_id,
        COUNT(*) as transaction_count_24h,
        SUM(CASE WHEN ABS(amount) > 5000 THEN 1 ELSE 0 END) as large_transactions,
        MAX(transaction_date) as last_transaction_date,
        COUNT(DISTINCT merchant_category) as merchant_diversity,
        STDDEV(amount) as amount_volatility
    FROM BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED
    WHERE transaction_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    GROUP BY customer_id
),
risk_calculation AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        cs.credit_score,
        ra.transaction_count_24h,
        ra.large_transactions,
        ra.last_transaction_date,
        ra.amount_volatility,
        
        -- Real-time risk scoring
        CASE 
            WHEN cs.credit_score < 600 THEN 40
            WHEN cs.credit_score < 700 THEN 20
            ELSE 0
        END +
        CASE 
            WHEN ra.large_transactions > 3 THEN 30
            WHEN ra.large_transactions > 1 THEN 15
            ELSE 0
        END +
        CASE 
            WHEN ra.amount_volatility > 2000 THEN 20
            WHEN ra.amount_volatility > 1000 THEN 10
            ELSE 0
        END +
        CASE 
            WHEN ra.transaction_count_24h > 20 THEN 10
            WHEN ra.transaction_count_24h = 0 THEN 15
            ELSE 0
        END as risk_score
        
    FROM BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG c
    LEFT JOIN BANKING_RAW.EXTERNAL_DATA.CREDIT_SCORES cs ON c.customer_id = cs.customer_id
    LEFT JOIN recent_activity ra ON c.customer_id = ra.customer_id
    WHERE c.account_status = 'Active'
)
SELECT 
    customer_id,
    risk_score,
    CASE 
        WHEN risk_score >= 70 THEN 'CRITICAL'
        WHEN risk_score >= 50 THEN 'HIGH'
        WHEN risk_score >= 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END as risk_category,
    last_transaction_date,
    large_transactions as suspicious_activity_count,
    CASE 
        WHEN credit_score > 0 THEN ROUND(amount_volatility / credit_score * 100, 2)
        ELSE NULL
    END as credit_utilization,
    ARRAY_CONSTRUCT(
        CASE WHEN risk_score >= 40 THEN 'LOW_CREDIT_SCORE' END,
        CASE WHEN large_transactions > 1 THEN 'LARGE_TRANSACTIONS' END,
        CASE WHEN amount_volatility > 1000 THEN 'HIGH_VOLATILITY' END,
        CASE WHEN transaction_count_24h > 15 THEN 'HIGH_FREQUENCY' END
    ) as risk_factors,
    CURRENT_TIMESTAMP() as scored_at
FROM risk_calculation;

-- Real-time fraud alerts using dynamic table
CREATE OR REPLACE DYNAMIC TABLE BANKING_ANALYTICS.REAL_TIME.FRAUD_ALERTS (
    alert_id,
    customer_id,
    transaction_id,
    alert_type,
    severity,
    alert_details,
    created_at
)
TARGET_LAG = '1 minute'  -- Very frequent refresh for fraud detection
WAREHOUSE = ANALYTICS_WH
AS
SELECT 
    UUID_STRING() as alert_id,
    t.customer_id,
    t.transaction_id,
    'FRAUD_SUSPICION' as alert_type,
    CASE 
        WHEN anomaly_score >= 8 THEN 'CRITICAL'
        WHEN anomaly_score >= 5 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as severity,
    OBJECT_CONSTRUCT(
        'transaction_amount', t.amount,
        'anomaly_score', ta.composite_anomaly_score,
        'risk_factors', ta.alert_flags,
        'transaction_time', t.transaction_date,
        'merchant', t.description
    ) as alert_details,
    CURRENT_TIMESTAMP() as created_at
FROM BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED t
JOIN BANKING_ANALYTICS.RISK_ANALYTICS.TRANSACTION_ANOMALIES ta 
    ON t.transaction_id = ta.transaction_id
WHERE ta.fraud_risk_level IN ('HIGH_RISK', 'MEDIUM_RISK')
AND t.transaction_date >= DATEADD(hour, -1, CURRENT_TIMESTAMP());