CREATE OR REPLACE STREAM raw.customers_stream
ON TABLE raw.customers
SHOW_INITIAL_ROWS = TRUE;