USE DATABASE BANKING_ANALYTICS;
USE SCHEMA RISK_ANALYTICS;

-- Customer risk scoring view
CREATE OR REPLACE VIEW CUSTOMER_RISK_PROFILE AS
WITH customer_base AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.customer_since,
        c.account_status,
        c.customer_segment,
        DATEDIFF('year', c.customer_since, CURRENT_DATE()) as customer_tenure_years,
        DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) as customer_age,
        cs.credit_score,
        cs.risk_category as bureau_risk_category
    FROM BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG c
    LEFT JOIN BANKING_RAW.EXTERNAL_DATA.CREDIT_SCORES cs
        ON c.customer_id = cs.customer_id
    WHERE c.data_quality_flag = 'VALID'
        AND c.account_status = 'Active'
),

transaction_behavior AS (
    SELECT 
        customer_id,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_credits,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as total_debits,
        AVG(amount) as avg_transaction_amount,
        STDDEV(amount) as transaction_volatility,
        
        -- Channel usage patterns
        COUNT(CASE WHEN channel = 'ATM' THEN 1 END) as atm_usage,
        COUNT(CASE WHEN channel = 'Online' THEN 1 END) as online_usage,
        COUNT(CASE WHEN channel = 'Mobile' THEN 1 END) as mobile_usage,
        COUNT(CASE WHEN channel = 'Branch' THEN 1 END) as branch_usage,
        
        -- Time-based patterns
        COUNT(CASE WHEN EXTRACT(hour FROM transaction_date) BETWEEN 22 AND 5 THEN 1 END) as after_hours_transactions,
        
        -- Recent activity (last 30 days)
        COUNT(CASE WHEN transaction_date >= DATEADD(day, -30, CURRENT_DATE()) THEN 1 END) as recent_transactions,
        SUM(CASE WHEN transaction_date >= DATEADD(day, -30, CURRENT_DATE()) AND amount > 0 
                 THEN amount ELSE 0 END) as recent_credits,
        
        -- Large transaction flags
        COUNT(CASE WHEN ABS(amount) > 10000 THEN 1 END) as large_transactions,
        MAX(ABS(amount)) as largest_transaction
        
    FROM BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED
    WHERE transaction_date >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY customer_id
),

risk_scoring AS (
    SELECT 
        cb.*,
        tb.*,
        
        -- Credit risk tier
        CASE 
            WHEN cb.credit_score >= 800 THEN 1
            WHEN cb.credit_score >= 750 THEN 2
            WHEN cb.credit_score >= 700 THEN 3
            WHEN cb.credit_score >= 650 THEN 4
            WHEN cb.credit_score >= 600 THEN 5
            WHEN cb.credit_score >= 550 THEN 6
            ELSE 7
        END as credit_risk_tier,
        
        -- Tenure risk tier
        CASE 
            WHEN cb.customer_tenure_years >= 10 THEN 1
            WHEN cb.customer_tenure_years >= 5 THEN 2
            WHEN cb.customer_tenure_years >= 2 THEN 3
            WHEN cb.customer_tenure_years >= 1 THEN 4
            ELSE 5
        END as tenure_risk_tier,
        
        -- Behavior risk tier
        CASE 
            WHEN tb.transaction_volatility IS NULL THEN 5
            WHEN tb.transaction_volatility <= 100 THEN 1
            WHEN tb.transaction_volatility <= 500 THEN 2
            WHEN tb.transaction_volatility <= 1000 THEN 3
            WHEN tb.transaction_volatility <= 2000 THEN 4
            ELSE 5
        END as behavior_risk_tier,
        
        -- Activity risk tier
        CASE 
            WHEN tb.recent_transactions IS NULL OR tb.recent_transactions = 0 THEN 5
            WHEN tb.recent_transactions >= 20 THEN 1
            WHEN tb.recent_transactions >= 10 THEN 2
            WHEN tb.recent_transactions >= 5 THEN 3
            WHEN tb.recent_transactions >= 1 THEN 4
            ELSE 5
        END as activity_risk_tier,

        -- Channel risk tier (diversified usage is lower risk)
        CASE 
            WHEN (CASE WHEN tb.online_usage > 0 THEN 1 ELSE 0 END +
                  CASE WHEN tb.mobile_usage > 0 THEN 1 ELSE 0 END +
                  CASE WHEN tb.atm_usage > 0 THEN 1 ELSE 0 END +
                  CASE WHEN tb.branch_usage > 0 THEN 1 ELSE 0 END) >= 3 THEN 1
            WHEN (CASE WHEN tb.online_usage > 0 THEN 1 ELSE 0 END +
                  CASE WHEN tb.mobile_usage > 0 THEN 1 ELSE 0 END +
                  CASE WHEN tb.atm_usage > 0 THEN 1 ELSE 0 END +
                  CASE WHEN tb.branch_usage > 0 THEN 1 ELSE 0 END) >= 2 THEN 2
            WHEN tb.total_transactions > 0 THEN 3
            ELSE 5
        END as channel_risk_tier
        
    FROM customer_base cb
    LEFT JOIN transaction_behavior tb ON cb.customer_id = tb.customer_id
)

SELECT 
    customer_id,
    first_name,
    last_name,
    customer_age,
    customer_tenure_years,
    customer_segment,
    credit_score,
    bureau_risk_category,
    total_transactions,
    avg_transaction_amount,
    transaction_volatility,
    recent_transactions,
    large_transactions,
    
    -- Individual risk tiers
    credit_risk_tier,
    tenure_risk_tier,
    behavior_risk_tier,
    activity_risk_tier,
    channel_risk_tier,
    
    -- Composite risk score (lower is better)
    (credit_risk_tier + tenure_risk_tier + behavior_risk_tier + 
     activity_risk_tier + channel_risk_tier) as composite_risk_score,
    
    -- Final risk category
    CASE 
        WHEN (credit_risk_tier + tenure_risk_tier + behavior_risk_tier + 
              activity_risk_tier + channel_risk_tier) <= 8 THEN 'LOW'
        WHEN (credit_risk_tier + tenure_risk_tier + behavior_risk_tier + 
              activity_risk_tier + channel_risk_tier) <= 15 THEN 'MEDIUM'
        WHEN (credit_risk_tier + tenure_risk_tier + behavior_risk_tier + 
              activity_risk_tier + channel_risk_tier) <= 22 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END as final_risk_category,
    
    -- Risk factors explanation
    ARRAY_CONSTRUCT(
        CASE WHEN credit_risk_tier >= 5 THEN 'LOW_CREDIT_SCORE' END,
        CASE WHEN tenure_risk_tier >= 4 THEN 'NEW_CUSTOMER' END,
        CASE WHEN behavior_risk_tier >= 4 THEN 'VOLATILE_TRANSACTIONS' END,
        CASE WHEN activity_risk_tier >= 4 THEN 'LOW_ACTIVITY' END,
        CASE WHEN channel_risk_tier >= 4 THEN 'LIMITED_CHANNEL_USAGE' END
    ) as risk_factors,
    
    CURRENT_TIMESTAMP() as scored_at
    
FROM risk_scoring
WHERE customer_id IS NOT NULL;