CREATE OR REPLACE TASK curated.dim_customer_task
WAREHOUSE = 'finance_analytics_wh'
SCHEDULE = 'USING CRON 0 6 * * * Europe/Dublin'
AS
MERGE INTO curated.dim_customer t
USING raw.customers_stream s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
    company_name = s.company_name,
    industry = s.industry,
    country = s.country,
    segment = s.segment,
    signup_date = s.signup_date
WHEN NOT MATCHED THEN INSERT (
    customer_id, company_name, industry, country, segment, signup_date
)
VALUES (
    s.customer_id, s.company_name, s.industry, s.country, s.segment, s.signup_date
);

ALTER TASK curated.dim_customer_task RESUME;