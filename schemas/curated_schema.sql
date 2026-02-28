CREATE DATABASE finance_analytics;

USE DATABASE finance_analytics;

CREATE SCHEMA curated;

CREATE OR REPLACE TABLE curated.dim_customer (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id STRING,
    company_name STRING,
    industry STRING,
    country STRING,
    segment STRING,
    signup_date DATE
);

CREATE OR REPLACE TABLE curated.dim_product (
    product_sk NUMBER AUTOINCREMENT,
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2)
);

CREATE OR REPLACE TABLE curated.fact_revenue (
    invoice_id STRING,
    customer_sk NUMBER,
    product_sk NUMBER,
    invoice_date DATE,
    amount NUMBER(10,2)
);

CREATE OR REPLACE TABLE curated.dq_logs (
    log_id STRING DEFAULT UUID_STRING(),
    table_name STRING,
    column_name STRING,
    check_type STRING,
    issue_description STRING,
    row_count NUMBER,
    severity STRING,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE curated.dq_rules (
    rule_id NUMBER AUTOINCREMENT,
    table_name STRING,
    column_name STRING,
    check_type STRING,
    check_params STRING,
    severity STRING DEFAULT 'WARN',
    active BOOLEAN DEFAULT TRUE
);