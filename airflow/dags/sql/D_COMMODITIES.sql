INSERT `{{ params.dwh_dataset }}.D_COMMODITIES`
SELECT
Date,
Ticker as Commodities_id,
Commodities as Name_id,
High,
Low,
Open,
Close,
Adj_Close,
Volume
FROM `{{ params.project_id }}.{{ params.staging_dataset }}.COMMODITIES_STAGING`
ORDER BY
Date,
Commodities_id;