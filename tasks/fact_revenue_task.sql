CREATE OR REPLACE TASK curated.fact_revenue_task
AFTER curated.dim_customer_task, curated.dim_product_task
AS
MERGE INTO curated.fact_revenue f
USING (
    SELECT
        i.invoice_id,
        c.customer_sk,
        p.product_sk,
        i.invoice_date,
        i.amount
    FROM raw.invoices_stream i
    JOIN curated.dim_customer c
        ON i.customer_id = c.customer_id
    JOIN curated.dim_product p
        ON i.product_id = p.product_id
) s
ON f.invoice_id = s.invoice_id
WHEN MATCHED THEN UPDATE SET
    customer_sk = s.customer_sk,
    product_sk = s.product_sk,
    invoice_date = s.invoice_date,
    amount = s.amount
WHEN NOT MATCHED THEN INSERT (
    invoice_id, customer_sk, product_sk, invoice_date, amount
)
VALUES (
    s.invoice_id, s.customer_sk, s.product_sk, s.invoice_date, s.amount
);

ALTER TASK curated.fact_revenue_task RESUME;