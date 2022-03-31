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
er.Commodities AS Exchange_rate_ticker
FROM
`{{ params.project_id }}.{{ params.staging_source_dataset }}.PRICE_STAGING` price
LEFT JOIN
`{{ params.project_id }}.{{ params.staging_source_dataset }}.EXCHANGE_RATE_STAGING` er
ON
er.Date = price.Date
ORDER BY
DATE,
Ticker_id;