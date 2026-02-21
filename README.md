# Financial Analytics Data Platform

**Snowflake Medallion Warehouse + Prefect Orchestrated Ingestion**

A production-ready financial analytics platform built on Snowflake with ingestion orchestrated using Prefect.

## Architecture Overview

Local CSV Files\
↓\
Prefect Flow (Ingestion & Orchestration)\
↓\
Snowflake Internal Stages\
↓\
Raw Schema (Landing Layer)\
↓\
Streams (CDC)\
↓\
Curated Schema (Dimensions & Facts)\
↓\
Dynamic Data Quality Checks

------------------------------------------------------------------------

## Medallion Architecture

### Raw Layer (`raw` schema)

-   Landing tables for source data
-   Untransformed operational datasets
-   Ingested via Prefect + Snowflake PUT/COPY

### Curated Layer (`curated` schema)

-   Dimension tables: `dim_customer`, `dim_product`
-   Fact table: `fact_revenue`
-   Populated using MERGE for idempotent transformations

### Streams (CDC)

-   Capture incremental changes from raw tables

### Tasks (Orchestration)

-   Scheduled transformations with dependency chaining
-   CRON scheduling (Europe/Dublin)

### Data Quality Framework

-   Dynamic stored procedure (`curated.run_dq_checks`)
-   Rules stored in `curated.dq_rules`
-   Results logged in `curated.dq_logs`

Supported checks: - NULL_CHECK - RANGE_CHECK - FORMAT_CHECK

------------------------------------------------------------------------

## Key Features

### Ingestion (Prefect)

-   Automatic CSV discovery
-   Stage validation/creation
-   Parallel uploads (PUT) and COPY operations
-   Retry logic
-   Structured JSON logging

### Warehouse (Snowflake)

-   Medallion architecture
-   MERGE-based idempotent loads
-   Change Data Capture (Streams)
-   Task dependency orchestration
-   Auto-suspend warehouse (cost optimization)
-   Dynamic DQ framework

------------------------------------------------------------------------

## Setup

### Prerequisites

-   Python 3.8+
-   Snowflake account
-   Prefect 3+
-   snowflake-connector-python

Install dependencies:

pip install prefect snowflake-connector-python python-dotenv

Create a .env file with Snowflake credentials and configuration.

------------------------------------------------------------------------

## Running the Pipeline

1.  Execute warehouse and schema SQL setup scripts.
2.  Run ingestion:

python -m ingestion_flow

## 👤 Author

Thushan Withanage

Last Updated: February 2026