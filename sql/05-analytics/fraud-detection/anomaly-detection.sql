-- Real-time fraud detection and anomaly identification

USE DATABASE BANKING_ANALYTICS;
USE SCHEMA RISK_ANALYTICS;

-- Transaction anomaly detection view
CREATE OR REPLACE VIEW TRANSACTION_ANOMALIES AS
WITH customer_baselines AS (
    -- Calculate customer spending baselines over last 90 days
    SELECT 
        customer_id,
        AVG(amount) as avg_amount,
        STDDEV(amount) as stddev_amount,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) as pct_95_amount,
        COUNT(*) as baseline_transaction_count,
        AVG(CASE WHEN amount > 0 THEN amount END) as avg_credit_amount,
        AVG(CASE WHEN amount < 0 THEN ABS(amount) END) as avg_debit_amount,
        
        -- Time pattern baselines
        MODE() WITHIN GROUP (ORDER BY EXTRACT(hour FROM transaction_date)) as typical_hour,
        COUNT(DISTINCT merchant_category) as typical_merchant_diversity,
        COUNT(DISTINCT channel) as typical_channel_diversity
        
    FROM BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED
    WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE())
    GROUP BY customer_id
    HAVING COUNT(*) >= 5  -- Need minimum transaction history
),

recent_transactions AS (
    -- Analyze recent transactions (last 7 days)
    SELECT 
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.amount,
        t.merchant_category,
        t.channel,
        t.description,
        
        -- Calculate transaction features
        ABS(t.amount) as transaction_amount,
        EXTRACT(hour FROM t.transaction_date) as transaction_hour,
        EXTRACT(dow FROM t.transaction_date) as day_of_week,
        
        -- Velocity features - transactions in time windows
        COUNT(*) OVER (
            PARTITION BY t.customer_id 
            ORDER BY t.transaction_date 
            RANGE BETWEEN INTERVAL '1 HOUR' PRECEDING AND CURRENT ROW
        ) as transactions_last_hour,
        
        COUNT(*) OVER (
            PARTITION BY t.customer_id 
            ORDER BY t.transaction_date 
            RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
        ) as transactions_last_24h,
        
        SUM(ABS(t.amount)) OVER (
            PARTITION BY t.customer_id 
            ORDER BY t.transaction_date 
            RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
        ) as amount_last_24h,
        
        -- Geographic/location anomalies (based on merchant patterns)
        COUNT(DISTINCT t.merchant_category) OVER (
            PARTITION BY t.customer_id 
            ORDER BY t.transaction_date 
            RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
        ) as merchant_diversity_24h
        
    FROM BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED t
    WHERE t.transaction_date >= DATEADD(day, -7, CURRENT_DATE())
),

anomaly_scoring AS (
    SELECT 
        rt.*,
        cb.avg_amount,
        cb.stddev_amount,
        cb.pct_95_amount,
        cb.typical_hour,
        cb.avg_credit_amount,
        cb.avg_debit_amount,
        
        -- Amount anomaly scoring
        CASE 
            WHEN cb.stddev_amount > 0 THEN 
                ABS(rt.amount - cb.avg_amount) / cb.stddev_amount
            ELSE 0
        END as amount_z_score,
        
        -- Large transaction flag
        CASE 
            WHEN ABS(rt.amount) > cb.pct_95_amount * 2 THEN 1
            ELSE 0
        END as large_transaction_flag,
        
        -- Time anomaly scoring
        CASE 
            WHEN rt.transaction_hour BETWEEN 2 AND 5 THEN 3  -- High risk hours
            WHEN ABS(rt.transaction_hour - cb.typical_hour) > 6 THEN 2  -- Unusual time
            ELSE 0
        END as time_anomaly_score,
        
        -- Velocity anomaly scoring
        CASE 
            WHEN rt.transactions_last_hour >= 5 THEN 3
            WHEN rt.transactions_last_hour >= 3 THEN 2
            WHEN rt.transactions_last_24h >= 20 THEN 2
            WHEN rt.transactions_last_24h >= 15 THEN 1
            ELSE 0
        END as velocity_anomaly_score,
        
        -- Weekend anomaly (unusual weekend activity)
        CASE 
            WHEN rt.day_of_week IN (0, 6) AND ABS(rt.amount) > cb.avg_amount * 2 THEN 1
            ELSE 0
        END as weekend_anomaly_flag
        
    FROM recent_transactions rt
    JOIN customer_baselines cb ON rt.customer_id = cb.customer_id
)

SELECT 
    transaction_id,
    customer_id,
    transaction_date,
    amount,
    merchant_category,
    channel,
    description,
    transaction_hour,
    
    -- Anomaly scores
    ROUND(amount_z_score, 2) as amount_z_score,
    time_anomaly_score,
    velocity_anomaly_score,
    large_transaction_flag,
    weekend_anomaly_flag,
    
    -- Composite anomaly score
    (CASE WHEN amount_z_score > 3 THEN 3 
          WHEN amount_z_score > 2 THEN 2 
          WHEN amount_z_score > 1.5 THEN 1 
          ELSE 0 END +
     time_anomaly_score +
     velocity_anomaly_score +
     large_transaction_flag +
     weekend_anomaly_flag) as composite_anomaly_score,
    
    -- Risk classification
    CASE 
        WHEN (CASE WHEN amount_z_score > 3 THEN 3 
                   WHEN amount_z_score > 2 THEN 2 
                   WHEN amount_z_score > 1.5 THEN 1 
                   ELSE 0 END +
              time_anomaly_score +
              velocity_anomaly_score +
              large_transaction_flag +
              weekend_anomaly_flag) >= 5 THEN 'HIGH_RISK'
        WHEN (CASE WHEN amount_z_score > 3 THEN 3 
                   WHEN amount_z_score > 2 THEN 2 
                   WHEN amount_z_score > 1.5 THEN 1 
                   ELSE 0 END +
              time_anomaly_score +
              velocity_anomaly_score +
              large_transaction_flag +
              weekend_anomaly_flag) >= 3 THEN 'MEDIUM_RISK'
        WHEN (CASE WHEN amount_z_score > 3 THEN 3 
                   WHEN amount_z_score > 2 THEN 2 
                   WHEN amount_z_score > 1.5 THEN 1 
                   ELSE 0 END +
              time_anomaly_score +
              velocity_anomaly_score +
              large_transaction_flag +
              weekend_anomaly_flag) >= 1 THEN 'LOW_RISK'
        ELSE 'NORMAL'
    END as fraud_risk_level,
    
    -- Alert flags
    ARRAY_CONSTRUCT(
        CASE WHEN amount_z_score > 3 THEN 'AMOUNT_OUTLIER' END,
        CASE WHEN time_anomaly_score >= 2 THEN 'TIME_ANOMALY' END,
        CASE WHEN velocity_anomaly_score >= 2 THEN 'VELOCITY_ALERT' END,
        CASE WHEN large_transaction_flag = 1 THEN 'LARGE_TRANSACTION' END,
        CASE WHEN weekend_anomaly_flag = 1 THEN 'WEEKEND_ANOMALY' END
    ) as alert_flags,
    
    -- Context for investigation
    transactions_last_hour,
    transactions_last_24h,
    amount_last_24h,
    
    CURRENT_TIMESTAMP() as analyzed_at
    
FROM anomaly_scoring
WHERE composite_anomaly_score > 0  -- Only return transactions with anomalies
ORDER BY composite_anomaly_score DESC, transaction_date DESC;