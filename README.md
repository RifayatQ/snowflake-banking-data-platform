# snowflake-banking-data-platform
Enterprise-grade banking data platform built on Snowflake, demonstrating data engineering best practices, security implementation, and real-time analytics for customer risk assessment and fraud detection.

Project Structure
```text
snowflake-banking-data-platform/
├── 📁 docs/
│ └── setup/
│ └── snowflake-setup.md
├── 📁 python/
│ └── data-generation/
│ ├── customer-data_generator.py
│ └── generate-credit-scores.py
├── 📁 sql/
│ ├── 📁 01-database-setup/
│ │ ├── create-databases.sql
│ │ └── create-schemas.sql
│ ├── 📁 02-data-model/
│ │ ├── 📁 marts-layer/
│ │ │ ├── dimensional-model.sql
│ │ │ └── fact-tables.sql
│ │ ├── 📁 raw-layer/
│ │ │ ├── customer-tables.sql
│ │ │ ├── external-data-tables.sql
│ │ │ └── transaction-tables.sql
│ │ └── 📁 staging-layer/
│ │ ├── data-quality-checks.sql
│ │ ├── etl-with-tasks-and-streams.sql
│ │ └── staging-transformations.sql
│ ├── 📁 03-etl-pipelines/
│ │ ├── 📁 dimension-population/
│ │ │ └── populate-time-dimensions.sql
│ │ ├── 📁 orchestration/
│ │ │ └── master-etl-controller.sql
│ │ ├── 📁 stored-procedures/
│ │ │ ├── customer-dimension-processor.sql
│ │ │ ├── daily-aggregation-processor.sql
│ │ │ └── transaction-fact-processor.sql
│ │ ├── 📁 streams/
│ │ │ └── transaction-change-stream.sql
│ │ ├── 📁 tasks/
│ │ │ └── daily-etl-orchestration.sql
│ │ └── 📁 utilities/
│ │ └── data-quality-checks.sql
│ ├── 📁 04-security/
│ │ ├── masking-policies.sql
│ │ └── roles-and-grants.sql
│ ├── 📁 05-analytics/
│ │ ├── 📁 customer-analytic/
│ │ ├── 📁 fraud-detection/
│ │ │ └── anomaly-detection.sql
│ │ └── 📁 risk-models/
│ │ └── customer-risk-scoring.sql
│ ├── 📁 06-monitoring/
│ │ ├── data-quality-monitoring.sql
│ │ ├── operational-dashboard.sql
│ │ └── performance-monitoring.sql
│ ├── 📁 07-optimization/
│ │ ├── clustering-optimization.sql
│ │ ├── 📁 dynamic-tables/
│ │ │ └── real-time-risk-scoring.sql
│ │ ├── 📁 iceberg-tables/
│ │ │ └── transaction-archive.sql
│ │ └── 📁 semantic-views/
│ │ │ └── customer-analytics-semantic.sql
│ └── 📁 08-documentation/
│ ├── data-architecture.md
│ ├── data-lineage.sql
│ └── summary-report.sql
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
``` 
