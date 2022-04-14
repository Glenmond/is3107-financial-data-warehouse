DROP TABLE IF EXISTS T_PG_ALL_PRICES;

CREATE TABLE IF NOT EXISTS T_PG_ALL_PRICES AS
SELECT
price.Date_ AS Date_,
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
staging.S_PG_ALL_PRICES price
LEFT JOIN
staging.S_PG_EXCHANGE_RATE er
ON
er.Date_ = price.Date_
UNION ALL
SELECT
price.Date_ AS Date_,
price.Ticker AS Ticker_id,
price.Name AS Name_id,
price.High AS High,
price.Low AS Low,
price.Open AS Open,
price.Close AS Close,
price.Adj_Close AS Adj_close,
price.Volume AS Volume,
1 AS Conversion_factor,
'SGD' AS Exchange_rate_ticker
FROM
staging.S_PG_ALL_PRICES price
ORDER BY
Date_,
Ticker_id DESC;