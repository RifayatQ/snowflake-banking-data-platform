-- Create semantic view for business user self-service
CREATE OR REPLACE VIEW BANKING_ANALYTICS.SEMANTIC.CUSTOMER_INSIGHTS AS
SELECT 
    -- Customer Identifiers (hidden from business users)
    customer_id as "Customer ID",
    
    -- Customer Demographics
    CONCAT(first_name, ' ', last_name) as "Customer Name",
    customer_age as "Age",
    customer_segment as "Banking Segment",
    DATEDIFF('year', customer_since, CURRENT_DATE()) as "Years as Customer",
    
    -- Account Information  
    account_status as "Account Status",
    CASE 
        WHEN account_status = 'Active' THEN '🟢 Active'
        WHEN account_status = 'Inactive' THEN '🟡 Inactive'
        ELSE '🔴 ' || account_status
    END as "Status Indicator",
    
    -- Transaction Behavior 
    COALESCE(total_transactions, 0) as "Total Transactions",
    ROUND(COALESCE(avg_transaction_amount, 0), 2) as "Average Transaction ($)",
    COALESCE(recent_transactions, 0) as "Transactions (Last 30 Days)",
    
    -- Risk Assessment 
    CASE 
        WHEN final_risk_category = 'LOW' THEN '🟢 Low Risk'
        WHEN final_risk_category = 'MEDIUM' THEN '🟡 Medium Risk'
        WHEN final_risk_category = 'HIGH' THEN '🟠 High Risk'
        WHEN final_risk_category = 'VERY_HIGH' THEN '🔴 Very High Risk'
        ELSE '❓ Not Assessed'
    END as "Risk Level",
    
    composite_risk_score as "Risk Score (0-100)",
    
    CASE 
        WHEN total_transactions > 50 THEN 'Heavy User'
        WHEN total_transactions > 20 THEN 'Regular User'
        WHEN total_transactions > 5 THEN 'Light User'
        ELSE 'Minimal User'
    END as "Usage Category",
    
    -- Credit Information
    CASE 
        WHEN credit_score >= 800 THEN '⭐ Excellent (800+)'
        WHEN credit_score >= 750 THEN '🟢 Very Good (750-799)'
        WHEN credit_score >= 700 THEN '🟡 Good (700-749)'
        WHEN credit_score >= 650 THEN '🟠 Fair (650-699)'
        WHEN credit_score >= 600 THEN '🔴 Poor (600-649)'
        ELSE '❌ Very Poor (<600)'
    END as "Credit Rating",
    
    -- Engagement Metrics
    ROUND(
        CASE 
            WHEN customer_tenure_years > 0 
            THEN COALESCE(total_transactions, 0) / customer_tenure_years 
            ELSE 0 
        END, 1
    ) as "Avg Transactions per Year",
    
    -- Last Activity
    CASE 
        WHEN DATEDIFF('day', last_transaction_date, CURRENT_DATE()) <= 7 THEN '🟢 This Week'
        WHEN DATEDIFF('day', last_transaction_date, CURRENT_DATE()) <= 30 THEN '🟡 This Month'
        WHEN DATEDIFF('day', last_transaction_date, CURRENT_DATE()) <= 90 THEN '🟠 Last 3 Months'
        ELSE '🔴 Over 3 Months Ago'
    END as "Last Activity",
    
    scored_at as "Data Last Updated"
    
FROM BANKING_ANALYTICS.RISK_ANALYTICS.CUSTOMER_RISK_PROFILE
WHERE customer_id IS NOT NULL;

-- Semantic view for fraud monitoring
CREATE OR REPLACE VIEW BANKING_ANALYTICS.SEMANTIC.FRAUD_DASHBOARD AS
SELECT 
    -- Time Information
    DATE(analyzed_at) as "Date",
    HOUR(analyzed_at) as "Hour",
    
    -- Customer Information (Masked for Privacy)
    LEFT(customer_id, 8) || '***' as "Customer ID (Masked)",
    
    -- Transaction Details
    transaction_id as "Transaction ID",
    ROUND(amount, 2) as "Amount ($)",
    merchant_category as "Merchant Type",
    channel as "Transaction Channel",
    
    -- Risk Assessment
    CASE 
        WHEN fraud_risk_level = 'HIGH_RISK' THEN '🔴 High Risk'
        WHEN fraud_risk_level = 'MEDIUM_RISK' THEN '🟡 Medium Risk'
        WHEN fraud_risk_level = 'LOW_RISK' THEN '🟠 Low Risk'
        ELSE '🟢 Normal'
    END as "Fraud Risk",
    
    composite_anomaly_score as "Anomaly Score",
    
    -- Alert Details (User Friendly)
    CASE 
        WHEN ARRAY_CONTAINS('AMOUNT_OUTLIER'::VARIANT, alert_flags) THEN '💰 Unusual Amount'
        WHEN ARRAY_CONTAINS('TIME_ANOMALY'::VARIANT, alert_flags) THEN '⏰ Unusual Time'
        WHEN ARRAY_CONTAINS('VELOCITY_ALERT'::VARIANT, alert_flags) THEN '⚡ High Frequency'
        WHEN ARRAY_CONTAINS('LARGE_TRANSACTION'::VARIANT, alert_flags) THEN '📈 Large Transaction'
        ELSE 'Multiple Factors'
    END as "Primary Alert Reason",
    
    -- Context for Investigation
    transactions_last_hour as "Transactions (Last Hour)",
    ROUND(amount_last_24h, 2) as "Total Amount (24h)",
    
    analyzed_at as "Alert Time"
    
FROM BANKING_ANALYTICS.RISK_ANALYTICS.TRANSACTION_ANOMALIES
WHERE fraud_risk_level IN ('HIGH_RISK', 'MEDIUM_RISK', 'LOW_RISK')
ORDER BY analyzed_at DESC;