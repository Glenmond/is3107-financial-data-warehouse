INSERT `{{ params.dwh_dataset }}.D_US_YIELDS`
SELECT
Date,
Ticker as US_yields_id,
Treasury_Yield as Name_id,
High,
Low,
Open,
Close,
Adj_Close,
Volume
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.US_YIELDS_STAGING`
ORDER BY
Date,
US_yields_id;