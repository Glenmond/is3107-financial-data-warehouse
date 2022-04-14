DROP TABLE IF EXISTS T_PG_EXCHANGE_RATE;

CREATE TABLE IF NOT EXISTS T_PG_EXCHANGE_RATE AS
SELECT
Date_,
Ticker as Exchange_rate_id,
Exchange_rate as Name_id,
High,
Low,
Open,
Close,
Adj_Close,
Volume
FROM staging.S_PG_EXCHANGE_RATE
ORDER BY
Date_,
Exchange_rate_id;