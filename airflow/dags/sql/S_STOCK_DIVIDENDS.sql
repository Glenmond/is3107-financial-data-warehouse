CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_STOCK_DIVIDENDS` AS
SELECT
Date,
Stock,
Ticker,
Dividends
FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.STOCK_DIVIDENDS_STAGING`;