USE DATABASE BANKING_MARTS;
USE SCHEMA DIM_TIME;

-- Populate Date Dimension for 10 years (5 years back, 5 years forward)
CREATE OR REPLACE PROCEDURE POPULATE_DATE_DIMENSION()
RETURNS STRING
LANGUAGE SQL
AS
$
BEGIN
    -- Clear existing data
    DELETE FROM DIM_DATE;
    
    -- Generate date records using a more efficient approach
    INSERT INTO DIM_DATE (
        date_key, date_actual, day_of_month, day_of_week, day_of_week_name, day_of_year,
        week_of_year, week_start_date, week_end_date,
        month_number, month_name, month_year, first_day_of_month, last_day_of_month,
        quarter_number, quarter_name, quarter_year, first_day_of_quarter, last_day_of_quarter,
        year_number, first_day_of_year, last_day_of_year,
        is_weekend, is_holiday, is_business_day, holiday_name,
        fiscal_year, fiscal_quarter, fiscal_month
    )
    WITH date_series AS (
        SELECT DATEADD(day, SEQ4(), '2019-01-01') as date_actual
        FROM TABLE(GENERATOR(ROWCOUNT => 4018))  -- ~11 years of dates
        WHERE date_actual <= '2029-12-31'
    )
    SELECT 
        TO_NUMBER(TO_CHAR(date_actual, 'YYYYMMDD')) as date_key,
        date_actual,
        EXTRACT(day FROM date_actual) as day_of_month,
        EXTRACT(dayofweek FROM date_actual) as day_of_week,
        DAYNAME(date_actual) as day_of_week_name,
        EXTRACT(dayofyear FROM date_actual) as day_of_year,
        
        -- Week information
        EXTRACT(week FROM date_actual) as week_of_year,
        DATE_TRUNC('week', date_actual) as week_start_date,
        DATEADD(day, 6, DATE_TRUNC('week', date_actual)) as week_end_date,
        
        -- Month information
        EXTRACT(month FROM date_actual) as month_number,
        MONTHNAME(date_actual) as month_name,
        TO_CHAR(date_actual, 'YYYY-MM') as month_year,
        DATE_TRUNC('month', date_actual) as first_day_of_month,
        LAST_DAY(date_actual) as last_day_of_month,
        
        -- Quarter information
        EXTRACT(quarter FROM date_actual) as quarter_number,
        'Q' || EXTRACT(quarter FROM date_actual) as quarter_name,
        EXTRACT(year FROM date_actual) || '-Q' || EXTRACT(quarter FROM date_actual) as quarter_year,
        DATE_TRUNC('quarter', date_actual) as first_day_of_quarter,
        LAST_DAY(DATE_TRUNC('quarter', date_actual), 'quarter') as last_day_of_quarter,
        
        -- Year information
        EXTRACT(year FROM date_actual) as year_number,
        DATE_TRUNC('year', date_actual) as first_day_of_year,
        LAST_DAY(date_actual, 'year') as last_day_of_year,
        
        -- Business calendar
        EXTRACT(dayofweek FROM date_actual) IN (0, 6) as is_weekend,
        
        -- Canadian holidays (simplified)
        CASE 
            WHEN TO_CHAR(date_actual, 'MM-DD') = '01-01' THEN TRUE  -- New Year's Day
            WHEN TO_CHAR(date_actual, 'MM-DD') = '07-01' THEN TRUE  -- Canada Day
            WHEN TO_CHAR(date_actual, 'MM-DD') = '12-25' THEN TRUE  -- Christmas
            WHEN TO_CHAR(date_actual, 'MM-DD') = '12-26' THEN TRUE  -- Boxing Day
            ELSE FALSE
        END as is_holiday,
        
        -- Business day (not weekend and not holiday)
        NOT (EXTRACT(dayofweek FROM date_actual) IN (0, 6)) AND NOT (
            CASE 
                WHEN TO_CHAR(date_actual, 'MM-DD') = '01-01' THEN TRUE
                WHEN TO_CHAR(date_actual, 'MM-DD') = '07-01' THEN TRUE
                WHEN TO_CHAR(date_actual, 'MM-DD') = '12-25' THEN TRUE
                WHEN TO_CHAR(date_actual, 'MM-DD') = '12-26' THEN TRUE
                ELSE FALSE
            END
        ) as is_business_day,
        
        -- Holiday names
        CASE 
            WHEN TO_CHAR(date_actual, 'MM-DD') = '01-01' THEN 'New Year''s Day'
            WHEN TO_CHAR(date_actual, 'MM-DD') = '07-01' THEN 'Canada Day'
            WHEN TO_CHAR(date_actual, 'MM-DD') = '12-25' THEN 'Christmas Day'
            WHEN TO_CHAR(date_actual, 'MM-DD') = '12-26' THEN 'Boxing Day'
            ELSE NULL
        END as holiday_name,
        
        -- Fiscal year (April to March)
        CASE 
            WHEN EXTRACT(month FROM date_actual) >= 4 THEN EXTRACT(year FROM date_actual) + 1
            ELSE EXTRACT(year FROM date_actual)
        END as fiscal_year,
        
        CASE 
            WHEN EXTRACT(month FROM date_actual) IN (4,5,6) THEN 1
            WHEN EXTRACT(month FROM date_actual) IN (7,8,9) THEN 2
            WHEN EXTRACT(month FROM date_actual) IN (10,11,12) THEN 3
            ELSE 4
        END as fiscal_quarter,
        
        CASE 
            WHEN EXTRACT(month FROM date_actual) >= 4 THEN EXTRACT(month FROM date_actual) - 3
            ELSE EXTRACT(month FROM date_actual) + 9
        END as fiscal_month
        
    FROM date_series;
    
    RETURN 'Date dimension populated with ' || (SELECT COUNT(*) FROM DIM_DATE) || ' records';
END;
$;

-- Populate Time of Day Dimension
CREATE OR REPLACE PROCEDURE POPULATE_TIME_OF_DAY_DIMENSION()
RETURNS STRING
LANGUAGE SQL
AS
$
BEGIN
    DELETE FROM DIM_TIME_OF_DAY;
    
    INSERT INTO DIM_TIME_OF_DAY (
        time_key, hour_24, hour_12, minute, am_pm, time_period, business_hours
    )
    WITH time_series AS (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY h.hour, m.minute) - 1 as time_key,
            h.hour as hour_24,
            m.minute
        FROM (SELECT SEQ4() as hour FROM TABLE(GENERATOR(ROWCOUNT => 24))) h
        CROSS JOIN (SELECT SEQ4() * 15 as minute FROM TABLE(GENERATOR(ROWCOUNT => 4))) m
        WHERE m.minute < 60
    )
    SELECT 
        time_key,
        hour_24,
        CASE WHEN hour_24 = 0 THEN 12 
             WHEN hour_24 <= 12 THEN hour_24 
             ELSE hour_24 - 12 END as hour_12,
        minute,
        CASE WHEN hour_24 < 12 THEN 'AM' ELSE 'PM' END as am_pm,
        
        -- Time periods
        CASE 
            WHEN hour_24 BETWEEN 6 AND 11 THEN 'Morning'
            WHEN hour_24 BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN hour_24 BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END as time_period,
        
        -- Business hours (9 AM to 5 PM)
        CASE 
            WHEN hour_24 BETWEEN 9 AND 17 THEN 'Business Hours'
            ELSE 'After Hours'
        END as business_hours
        
    FROM time_series;
    
    RETURN 'Time of day dimension populated with ' || (SELECT COUNT(*) FROM DIM_TIME_OF_DAY) || ' records';
END;
$;

-- Populate Merchant Category Dimension
USE SCHEMA DIM_PRODUCT;

CREATE OR REPLACE PROCEDURE POPULATE_MERCHANT_DIMENSION()
RETURNS STRING
LANGUAGE SQL
AS
$
BEGIN
    DELETE FROM DIM_MERCHANT;
    
    INSERT INTO DIM_MERCHANT (
        merchant_category, category_group, category_type, is_high_risk, 
        category_description, typical_transaction_range
    )
    VALUES
    ('Grocery', 'Retail', 'Essential', FALSE, 'Food and grocery stores', '$20-200'),
    ('Gas', 'Automotive', 'Essential', FALSE, 'Gas stations and fuel', '$30-150'),
    ('Restaurant', 'Food Service', 'Discretionary', FALSE, 'Restaurants and dining', '$15-100'),
    ('Retail', 'Shopping', 'Discretionary', FALSE, 'General retail and shopping', '$25-500'),
    ('Healthcare', 'Medical', 'Essential', FALSE, 'Medical and healthcare services', '$50-1000'),
    ('Entertainment', 'Leisure', 'Discretionary', FALSE, 'Entertainment and recreation', '$20-300'),
    ('ATM', 'Banking', 'Service', FALSE, 'ATM withdrawals and deposits', '$20-500'),
    ('Online', 'E-commerce', 'Mixed', FALSE, 'Online purchases', '$10-1000'),
    ('Transfer', 'Banking', 'Service', FALSE, 'Money transfers', '$50-5000'),
    ('Payment', 'Financial', 'Service', FALSE, 'Bill payments and services', '$25-2000'),
    ('Cash Advance', 'Financial', 'Credit', TRUE, 'Cash advances', '$100-2000'),
    ('Gaming', 'Entertainment', 'High Risk', TRUE, 'Gaming and gambling', '$25-10000'),
    ('Crypto', 'Financial', 'High Risk', TRUE, 'Cryptocurrency transactions', '$100-50000');
    
    RETURN 'Merchant dimension populated with ' || (SELECT COUNT(*) FROM DIM_MERCHANT) || ' categories';
END;
$;

-- Populate Channel Dimension
CREATE OR REPLACE PROCEDURE POPULATE_CHANNEL_DIMENSION()
RETURNS STRING
LANGUAGE SQL
AS
$
BEGIN
    DELETE FROM DIM_CHANNEL;
    
    INSERT INTO DIM_CHANNEL (
        channel_name, channel_type, is_self_service, is_assisted, availability_hours
    )
    VALUES
    ('ATM', 'Physical', TRUE, FALSE, '24/7'),
    ('Online', 'Digital', TRUE, FALSE, '24/7'),
    ('Mobile', 'Digital', TRUE, FALSE, '24/7'),
    ('Branch', 'Physical', FALSE, TRUE, 'Business Hours'),
    ('Phone', 'Voice', FALSE, TRUE, 'Extended Hours'),
    ('Call Center', 'Voice', FALSE, TRUE, '24/7');
    
    RETURN 'Channel dimension populated with ' || (SELECT COUNT(*) FROM DIM_CHANNEL) || ' channels';
END;
$;