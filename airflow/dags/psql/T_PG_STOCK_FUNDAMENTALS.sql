DROP TABLE IF EXISTS T_PG_STOCK_FUNDAMENTALS;

CREATE TABLE IF NOT EXISTS T_PG_STOCK_FUNDAMENTALS AS
SELECT
fun.Date_ AS Date_,
fun.Ticker as Ticker_id,
fun.Total_Revenue/fun.Total_Assets AS Total_asset_turnover,
fun.Cash/fun.Total_Current_Liabilities AS Cash_ratio,
fun.Total_Liab/fun.Total_Assets AS Debt_ratio,
fun.Net_Income/fun.Total_Stockholder_Equity AS Return_on_equity,
price.Adj_Close/(fun.Net_Income/(fun.Common_Stock + fun.Treasury_Stock)) AS Price_earning_ratio
FROM
staging.S_PG_STOCK_FUNDAMENTALS fun
LEFT JOIN
staging.S_PG_ALL_PRICES price
ON
fun.Date_ = price.Date_
AND fun.Ticker = price.Ticker;


