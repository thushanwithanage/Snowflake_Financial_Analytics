CREATE DATABASE finance_analytics;

USE DATABASE finance_analytics;

CREATE SCHEMA raw;

CREATE OR REPLACE TABLE raw.customers (
    customer_id STRING,
    company_name STRING,
    industry STRING,
    country STRING,
    signup_date DATE,
    segment STRING
);

CREATE OR REPLACE TABLE raw.products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2)
);

CREATE OR REPLACE TABLE raw.invoices (
    invoice_id STRING,
    customer_id STRING,
    product_id STRING,
    invoice_date DATE,
    due_date DATE,
    amount NUMBER(10,2),
    status STRING
);

CREATE OR REPLACE TABLE raw.payments (
    payment_id STRING,
    invoice_id STRING,
    payment_date DATE,
    payment_amount NUMBER(10,2)
);

CREATE OR REPLACE TABLE raw.expenses (
    expense_id STRING,
    expense_date DATE,
    department STRING,
    expense_type STRING,
    amount NUMBER(10,2)
);