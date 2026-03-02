# Financial Analytics Data Platform

**Snowflake Medallion Warehouse + Prefect Orchestrated Ingestion**

A production-ready financial analytics platform built on Snowflake using a Medallion architecture, with ingestion orchestrated via Prefect.

The platform delivers business-ready financial KPIs through a dedicated `analytics` schema built on top of curated fact and dimension tables.

------------------------------------------------------------------------

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
Analytics Schema (Business KPI Views)\
↓\
BI / Dashboard Layer

------------------------------------------------------------------------

## Medallion Architecture

### Raw Layer (`raw` schema)

-   Landing tables for source data\
-   Untransformed operational datasets\
-   Ingested via Prefect + Snowflake `PUT` / `COPY`

------------------------------------------------------------------------

### Curated Layer (`curated` schema)

Business-modeled warehouse layer:

**Dimensions** - `dim_customer` - `dim_product`

**Fact** - `fact_revenue`

Features: - MERGE-based idempotent transformations\
- Change Data Capture using Streams\
- Task-based orchestration\
- Data Quality stored procedure framework

------------------------------------------------------------------------

### Analytics Layer (`analytics` schema)

This layer exposes business-ready KPI views designed for reporting, dashboards, and financial analysis.

------------------------------------------------------------------------

## Analytics Views

### 1. Monthly Revenue Summary

`analytics.monthly_revenue_summary`

Provides revenue metrics grouped by: - Year - Month - Customer Segment - Country

Metrics: - `total_revenue` - `total_invoices` - `avg_invoice_value`

------------------------------------------------------------------------

### 2. Customer Lifetime Value (LTV)

`analytics.customer_ltv`

Customer-level financial metrics: - Total invoices - Total revenue - Average invoice value - Customer lifespan (days between first and last invoice)

------------------------------------------------------------------------

### 3. Product Performance

`analytics.product_performance`

Product-level performance metrics: - Revenue per product - Number of invoices sold - Average selling price - Category-level aggregation

------------------------------------------------------------------------

### 4. Accounts Receivable Aging

`analytics.ar_aging`

Invoice-level aging analysis: - Days outstanding - Aging buckets (0--30, 31--60, 61--90, 90+)

------------------------------------------------------------------------

### 5. Revenue Growth Analysis

`analytics.revenue_growth`

Monthly revenue with: - Month-over-Month (MoM) growth % - Year-over-Year (YoY) growth %

------------------------------------------------------------------------

### 6. Rolling 12-Month Revenue

`analytics.rolling_12m_revenue`

Trailing 12-month revenue trend using window functions.

------------------------------------------------------------------------

## Data Quality Framework

Implemented in `curated` schema:

-   Stored procedure: `run_dq_checks`
-   Rules table: `dq_rules`
-   Log table: `dq_logs`

Supported checks: - NULL_CHECK\
- RANGE_CHECK\
- FORMAT_CHECK

------------------------------------------------------------------------

## Setup

### Prerequisites

-   Python 3.8+\
-   Snowflake account\
-   Prefect 3+\
-   snowflake-connector-python

Install dependencies:

    pip install prefect snowflake-connector-python python-dotenv

------------------------------------------------------------------------

## Running the Pipeline

1.  Execute warehouse and schema SQL setup scripts.\
2.  Run ingestion:

```{=html}
```
    python -m ingestion_flow

3.  Query analytics views:

``` sql
SELECT * FROM analytics.monthly_revenue_summary;
SELECT * FROM analytics.revenue_growth;
```

------------------------------------------------------------------------

## Business Value

-   End-to-end data engineering\
-   Financial KPI modeling\
-   Advanced SQL (window functions, growth rates, rolling metrics)\
-   CDC-based incremental processing\
-   Production-grade orchestration\
-   Data quality automation

------------------------------------------------------------------------

## Author

Thushan Withanage

Last Updated: 2nd March 2026