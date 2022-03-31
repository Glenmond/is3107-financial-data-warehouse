CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_ALL_PRICE` AS
SELECT
price.Date AS Date,
price.Ticker AS Ticker_id,
price.Name AS Name_id,
price.High * er.Adj_Close AS High,
price.Low * er.Adj_Close AS Low,
price.Open * er.Adj_Close AS Open,
price.Close * er.Adj_Close AS Close,
price.Adj_Close * er.Adj_Close AS Adj_close,
price.Volume AS Volume,
er.Adj_Close AS Conversion_factor,
er.Exchange_rate AS Exchange_rate_ticker
FROM
`{{ params.project_id }}.{{ params.staging_source_dataset }}.PRICE_STAGING` price
LEFT JOIN
`{{ params.project_id }}.{{ params.staging_source_dataset }}.EXCHANGE_RATE_STAGING` er
ON
er.Date = price.Date
UNION ALL
SELECT
price.Date AS Date,
price.Ticker AS Ticker_id,
price.Name AS Name_id,
price.High AS High,
price.Low AS Low,
price.Open AS Open,
price.Close AS Close,
price.Adj_Close AS Adj_close,
price.Volume AS Volume,
1 AS Conversion_factor,
"SGD" AS Exchange_rate_ticker
FROM
`{{ params.project_id }}.{{ params.staging_source_dataset }}.PRICE_STAGING` price
ORDER BY
Date,
Ticker_id DESC