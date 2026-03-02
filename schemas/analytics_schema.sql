CREATE SCHEMA IF NOT EXISTS analytics;

-- Monthly Revenue Summary by Customer Segment and Country
CREATE OR REPLACE VIEW analytics.monthly_revenue_summary AS
SELECT
    DATE_PART('year', f.invoice_date) AS iyear,
    DATE_PART('month', f.invoice_date) AS imonth,
    c.segment,
    c.country,
    SUM(f.amount) AS total_revenue,
    COUNT(DISTINCT f.invoice_id) AS total_invoices,
    AVG(f.amount) AS avg_invoice_value
FROM curated.fact_revenue f
INNER JOIN curated.dim_customer c
    ON f.customer_sk = c.customer_sk
GROUP BY iyear, imonth, c.segment, c.country;

-- Customer Lifetime Value Analysis
CREATE OR REPLACE VIEW analytics.customer_ltv AS
SELECT
    c.customer_id,
    c.company_name,
    c.segment,
    COUNT(DISTINCT f.invoice_id) AS total_invoices,
    COALESCE(SUM(f.amount), 0) AS total_revenue,
    COALESCE(AVG(f.amount), 0) AS avg_invoice_value,
    CASE
        WHEN COUNT(f.invoice_id) > 0 THEN MAX(f.invoice_date) - MIN(f.invoice_date)
        ELSE 0
    END AS customer_lifespan_days,
FROM curated.fact_revenue f
INNER JOIN curated.dim_customer c
    ON f.customer_sk = c.customer_sk
GROUP BY c.customer_id, c.company_name, c.segment;

-- Product Performance Metrics
CREATE OR REPLACE VIEW analytics.product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(SUM(f.amount), 0) AS revenue,
    COUNT(DISTINCT f.invoice_id) AS invoices_sold,
    COALESCE(AVG(f.amount), 0) AS avg_price
FROM curated.fact_revenue f
INNER JOIN curated.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY p.product_id, p.product_name, p.category

-- Accounts Receivable Aging Analysis
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
INNER JOIN curated.dim_customer c
    ON f.customer_sk = c.customer_sk
WHERE f.invoice_date <= CURRENT_DATE;

-- Sales Growth Analysis (MoM and YoY)
CREATE OR REPLACE VIEW analytics.revenue_growth AS
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', invoice_date) AS imonth,
        SUM(amount) AS revenue
    FROM curated.fact_revenue
    GROUP BY DATE_TRUNC('month', invoice_date)
)
SELECT
    imonth,
    revenue,
    (revenue - LAG(revenue) OVER (ORDER BY imonth))
        / NULLIF(LAG(revenue) OVER (ORDER BY imonth),0) AS mom_growth_pct,
    (revenue - LAG(revenue,12) OVER (ORDER BY imonth))
        / NULLIF(LAG(revenue,12) OVER (ORDER BY imonth),0) AS yoy_growth_pct
FROM monthly_sales;

-- Rolling 12-Month Revenue Trend
CREATE OR REPLACE VIEW analytics.rolling_12m_revenue AS
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', invoice_date) AS imonth,
        SUM(amount) AS revenue
    FROM curated.fact_revenue
    GROUP BY DATE_TRUNC('month', invoice_date)
)
SELECT
    imonth,
    SUM(revenue) OVER (
        ORDER BY imonth
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_12m_revenue
FROM monthly_sales;