CREATE OR REPLACE TASK curated.dim_product_task
AFTER curated.dim_customer_task
AS
MERGE INTO curated.dim_product t
USING raw.products_stream s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
    product_name = s.product_name,
    category = s.category,
    price = s.price
WHEN NOT MATCHED THEN INSERT (
    product_id, product_name, category, price
)
VALUES (
    s.product_id, s.product_name, s.category, s.price
);

ALTER TASK curated.dim_product_task RESUME;