-- Create stream to track changes
CREATE STREAM CUSTOMER_STREAM ON TABLE BANKING_RAW.CUSTOMER_DATA.CUSTOMER_PROFILES;

-- Create stored procedure for ETL
CREATE OR REPLACE PROCEDURE PROCESS_CUSTOMER_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO BANKING_STAGING.STG_CUSTOMER.CUSTOMER_PROFILES_STG AS target
    USING (
        SELECT 
            customer_id,
            UPPER(TRIM(first_name)) as first_name,
            UPPER(TRIM(last_name)) as last_name,
            LOWER(TRIM(email)) as email,
            phone,
            date_of_birth,
            address,
            city,
            province,
            postal_code,
            sin,
            customer_since,
            account_status,
            CURRENT_TIMESTAMP() as processed_at,
            CASE 
                WHEN email NOT LIKE '%@%.%' THEN 'INVALID_EMAIL'
                WHEN date_of_birth > CURRENT_DATE() THEN 'INVALID_DOB'
                ELSE 'VALID'
            END as data_quality_flag
        FROM CUSTOMER_STREAM
        WHERE METADATA$ACTION = 'INSERT'
    ) AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN UPDATE SET
        first_name = source.first_name,
        last_name = source.last_name,
        email = source.email,
        processed_at = source.processed_at
    WHEN NOT MATCHED THEN INSERT (
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        date_of_birth,
        address,
        city,
        province,
        postal_code,
        sin,
        customer_since,
        account_status,
        processed_at,
        data_quality_flag
    ) VALUES (
        source.customer_id,
        source.first_name,
        source.last_name,
        source.email,
        source.phone,
        source.date_of_birth,
        source.address,
        source.city,
        source.province,
        source.postal_code,
        source.sin,
        source.customer_since,
        source.account_status,
        source.processed_at,
        source.data_quality_flag
    );

    RETURN 'Merge executed successfully';
END;
$$;

-- Create task to run ETL every hour (optional)
CREATE TASK CUSTOMER_ETL_TASK
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '60 MINUTE'
AS
    CALL PROCESS_CUSTOMER_DATA();

-- Start the task
ALTER TASK CUSTOMER_ETL_TASK RESUME;