INSERT `{{ params.dwh_dataset }}.D_STOCK_TA`
SELECT
Date,
Ticker as Stock_id,
Stock as Name_id,
TA as TA_id,
TA_2 as TA_description,
Value,
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.TA_STAGING`
ORDER BY
DATE,
STOCK_ID;
