CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.monthly_revenue_summary AS
SELECT
    DATE_TRUNC('month', f.invoice_date) AS imonth,
    c.segment,
    c.country,
    SUM(f.amount) AS total_revenue,
    COUNT(DISTINCT f.invoice_id) AS total_invoices,
    AVG(f.amount) AS avg_invoice_value
FROM curated.fact_revenue f
JOIN curated.dim_customer c
    ON f.customer_sk = c.customer_sk
JOIN curated.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY imonth, c.segment, c.country
ORDER BY imonth, c.segment, c.country;

CREATE OR REPLACE VIEW analytics.customer_ltv AS
SELECT
    c.customer_id,
    c.company_name,
    c.segment,
    COUNT(DISTINCT f.invoice_id) AS total_invoices,
    SUM(f.amount) AS total_revenue,
    AVG(f.amount) AS avg_invoice_value,
    MAX(f.invoice_date) - MIN(f.invoice_date) AS customer_lifespan_days
FROM curated.fact_revenue f
JOIN curated.dim_customer c
    ON f.customer_sk = c.customer_sk
GROUP BY c.customer_id, c.company_name, c.segment;

CREATE OR REPLACE VIEW analytics.product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.amount) AS revenue,
    COUNT(DISTINCT f.invoice_id) AS invoices_sold,
    AVG(f.amount) AS avg_price
FROM curated.fact_revenue f
JOIN curated.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY p.product_id, p.product_name, p.category;

CREATE OR REPLACE VIEW analytics.ar_aging AS
SELECT
    f.invoice_id,
    c.customer_id,
    f.amount,
    DATEDIFF('day', f.invoice_date, CURRENT_DATE) AS days_outstanding,
    CASE 
        WHEN DATEDIFF('day', f.invoice_date, CURRENT_DATE) <= 30 THEN '0-30'
        WHEN DATEDIFF('day', f.invoice_date, CURRENT_DATE) <= 60 THEN '31-60'
        WHEN DATEDIFF('day', f.invoice_date, CURRENT_DATE) <= 90 THEN '61-90'
        ELSE '90+' 
    END AS aging_bucket
FROM curated.fact_revenue f
JOIN curated.dim_customer c
    ON f.customer_sk = c.customer_sk
WHERE f.invoice_date <= CURRENT_DATE;