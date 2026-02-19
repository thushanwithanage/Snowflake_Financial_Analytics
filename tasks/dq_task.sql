CREATE OR REPLACE TASK curated.dq_task
WAREHOUSE = 'finance_analytics_wh'
SCHEDULE = 'USING CRON 0 8 * * * Europe/Dublin'
AS
CALL curated.run_dq_checks();