# Financial Analytics Data Warehouse

A production-ready Snowflake data warehouse implementing the medallion architecture (raw → curated layers) for comprehensive financial data analytics and reporting. This project also includes a **dynamic, automated Data Quality (DQ) framework** that allows checks to be added, modified, or updated without any changes to the core procedure logic, ensuring reliable and trusted analytics.

## Overview

This project delivers a modern, scalable data warehouse solution on Snowflake designed to process and analyze financial data including customer information, product catalogs, invoices, payments, and operational expenses. The architecture leverages Snowflake's native capabilities including Streams for real-time CDC, scheduled Tasks for orchestration, MERGE operations for reliable data transformations, and a flexible DQ framework for automated data validation.

## Architecture

### Medallion Pattern

The project follows the **Medallion Architecture** for clean data governance:

```
Raw Layer (Landing) → Curated Layer (Analytics Ready)
        ↓                          ↓
  Operational Data        Dimension & Fact Tables
```

### Layers

* **Raw Layer** (`raw` schema): Source tables ingested from operational systems, untransformed
* **Curated Layer** (`curated` schema): Transformed dimension and fact tables optimized for analytics and reporting
* **Streams**: Snowflake Streams for Change Data Capture (CDC) on raw tables to capture data changes
* **Tasks**: Scheduled automated orchestration of data transformations with dependency management
* **Data Quality**: Dynamic DQ procedure that reads rules from `curated.dq_rules` for NULL, RANGE, and FORMAT checks, logging any issues automatically

## Prerequisites

* Snowflake account with ACCOUNTADMIN or equivalent permissions
* SQL IDE (Snowflake Web UI, DBeaver, VS Code with Snowflake extension, etc.)
* Sample data files available in the `data/` folder
* Ability to create databases, schemas, warehouses, and tasks

## Installation & Setup

Execute the setup scripts in the following order:

### 1. Create Warehouse

```sql
@include warehouse_design.sql
```

**Warehouse Configuration:**

* Name: `FINANCE_ANALYTICS_WH`
* Size: XSMALL (scalable as needed)
* Auto-Suspend: 60 minutes
* Auto-Resume: Enabled

### 2. Create Raw Layer

```sql
@include raw_schema_design.sql
```

Creates:

* Database: `finance_analytics`
* Schema: `raw`
* Tables: customers, products, invoices, payments, expenses

### 3. Create Curated Layer

```sql
@include curated_schema_design.sql
```

Creates:

* Schema: `curated`
* Dimension tables: dim_customer, dim_product
* Fact table: fact_revenue

### 4. Create Streams (CDC)

```sql
@include streams/customers_stream.sql
@include streams/invoices_stream.sql
@include streams/products_stream.sql
```

### 5. Create & Resume Tasks

```sql
@include tasks/dim_customer_task.sql
@include tasks/dim_product_task.sql
@include tasks/fact_revenue_task.sql
```

## Data Pipeline

### Task Orchestration & Scheduling

The pipeline uses Snowflake Tasks with explicit scheduling and dependencies:

```
dim_customer_task (Scheduled: 6 AM Europe/Dublin)
    ↓
dim_product_task (Depends on dim_customer_task)
    ↓
fact_revenue_task (Depends on dim_customer_task & dim_product_task)
    ↓
dq_task (Runs automated dynamic DQ checks daily at 8 AM)
```

## Data Quality Checks

Automated **Data Quality (DQ) checks** ensure reliable and trusted analytics for all dimension and fact tables. The DQ procedure is dynamic, reading rules from the `curated.dq_rules` table so checks can be added, removed, or updated **without modifying the core logic**.

### Key Features

* **Automated DQ Checks** – The stored procedure (`curated.run_dq_checks`) executes rules defined in `curated.dq_rules`.
* **Flexible Rule Management** – New rules can be added or existing rules modified in `dq_rules` **without changing the core procedure logic**.
* **Check Types**:

  * `NULL_CHECK` – Identifies missing values in critical columns
  * `RANGE_CHECK` – Ensures numeric values are within expected bounds
  * `FORMAT_CHECK` – Validates text/ID formats
* **Logging & Monitoring** – Issues are logged into `curated.dq_logs` with severity levels for review.
* **Scheduled Execution** – A Snowflake Task (`curated.dq_task`) runs daily at 8 AM (Europe/Dublin).

### Benefits

* Detects anomalies before downstream reporting
* Improves confidence in dashboards and KPIs
* Dynamic and extensible, rules can be added or updated without changing core procedure logic

## Key Features

✅ **Medallion Architecture** - Clear separation of raw and curated data layers
✅ **Real-time CDC** - Snowflake Streams capture data changes automatically
✅ **Scheduled Tasks** - Orchestrated data pipeline with CRON scheduling
✅ **Task Dependencies** - Ensure data consistency with explicit sequencing
✅ **Idempotent Loads** - MERGE operations for safe, repeatable transformations
✅ **Cost Optimization** - Auto-suspend warehouse for unused time
✅ **Scalable Design** - Easily extend with new dimensions, facts, or business logic
✅ **Dynamic Data Quality Monitoring** - Flexible, rule-driven DQ checks that can be updated without changing pipeline code

## Troubleshooting

### Tasks Not Executing

**Issue**: Tasks are suspended or not running

**Solutions**:

1. Check task is resumed: `ALTER TASK <task_name> RESUME;`
2. Verify warehouse exists and is accessible: `SHOW WAREHOUSES;`
3. Check warehouse is not suspended: `ALTER WAREHOUSE FINANCE_ANALYTICS_WH RESUME;`
4. Verify sufficient quota and permissions: `SHOW GRANTS ON WAREHOUSE FINANCE_ANALYTICS_WH;`

### No Data in Curated Tables

**Issue**: Dimension/fact tables are empty after tasks run

**Solutions**:

1. Verify raw data loaded: `SELECT COUNT(*) FROM raw.customers;`
2. Check streams have data: `SELECT COUNT(*) FROM raw.customers_stream;`
3. Review task execution errors: Query `TASK_HISTORY` for error messages
4. Manually trigger task: `EXECUTE TASK curated.dim_customer_task;`