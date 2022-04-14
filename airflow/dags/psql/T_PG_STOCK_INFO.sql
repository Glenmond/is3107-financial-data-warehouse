DROP TABLE IF EXISTS T_PG_STOCK_INFO;

CREATE TABLE IF NOT EXISTS T_PG_STOCK_INFO AS
SELECT
Stock as Stock,
Ticker as Ticker_id,
Industry as Stock_industry,
Summary as Stock_summary
FROM staging.S_PG_STOCK_INFO;