CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_EXCHANGE_RATE` AS
SELECT
Date,
Ticker as Exchange_rate_id,
Commodities as Name_id,
High,
Low,
Open,
Close,
Adj_Close,
Volume
FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.EXCHANGE_RATE_STAGING`
ORDER BY
Date,
Exchange_rate_id;