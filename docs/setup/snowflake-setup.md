### Snowflake Account
- Snowflake account with appropriate privileges
- ACCOUNTADMIN or SYSADMIN role access
- Compute warehouse with sufficient credits

### Development Environment
- Python 3.8 or higher
- Git for version control
- Text editor or IDE (VS Code recommended)

### Required Packages
```bash
pip install snowflake-connector-python pandas pyyaml
```

## Step 1: Snowflake Account Configuration

### 1.1 Create Warehouses
```sql
-- ETL warehouse for data processing
CREATE WAREHOUSE ETL_WH 
  WITH WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Analytics warehouse for queries
CREATE WAREHOUSE ANALYTICS_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Development warehouse
CREATE WAREHOUSE DEV_WH
  WITH WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```
### 1.2 Configure Resource Monitors
```sql
-- Create resource monitor for cost control
CREATE RESOURCE MONITOR BANKING_PLATFORM_MONITOR
  WITH CREDIT_QUOTA = 100
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply to warehouses
ALTER WAREHOUSE ETL_WH SET RESOURCE_MONITOR = BANKING_PLATFORM_MONITOR;
ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = BANKING_PLATFORM_MONITOR;
```

### 2.1 Clone Repository
```bash
git clone https://github.com/rifayatq/snowflake-banking-data-platform.git
cd snowflake-banking-data-platform
```

### Enable Query Optimization
```sql
-- Enable query acceleration (if available)
ALTER WAREHOUSE ANALYTICS_WH 
SET ENABLE_QUERY_ACCELERATION = TRUE
MAX_QUERY_ACCELERATION_SCALE_FACTOR = 8;

-- Set up automatic clustering
ALTER TABLE BANKING_MARTS.FACT_TRANSACTIONS.TRANSACTIONS_CLUSTERED
RESUME RECLUSTER;
```