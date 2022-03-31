CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_STOCK_FUNDAMENTALS` AS
SELECT
fun.Date AS Date,
fun.Ticker as Ticker_id,
fun.Total_Revenue/fun.Total_Assets AS Total_asset_turnover,
fun.Cash/fun.Total_Current_Liabilities AS Cash_ratio,
fun.Total_Liab/fun.Total_Assets AS Debt_ratio,
fun.Net_Income/fun.Total_Stockholder_Equity AS Return_on_equity,
price.Adj_Close/(fun.Net_Income/(fun.Common_Stock + fun.Treasury_Stock)) AS Price_earning_ratio
FROM
`{{ params.project_id }}.{{ params.staging_source_dataset }}.STOCK_FUNDAMENTALS_STAGING` fun
LEFT JOIN
`{{ params.project_id }}.{{ params.staging_source_dataset }}.PRICE_STAGING` price
ON
fun.Date = price.Date
AND fun.Ticker = price.Ticker;
