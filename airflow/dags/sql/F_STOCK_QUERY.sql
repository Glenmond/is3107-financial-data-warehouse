INSERT `{{ params.dwh_dataset }}.F_STOCK_QUERY` 
SELECT 
ps.Date as DATE,
ps.Ticker_id as Ticker,
ps.Name_id as Name,
ps.Adj_Close as Adj_Close,
ps.Exchange_rate_ticker as Exchange_rate,
ps.Conversion_factor as FX_rate,
si.Stock_industry as Industry,
si.Stock_summary as Description,
sf.Return_on_equity as ROE,
sf.Price_earning_ratio as PE_RATIO,
sgir.sora as Sora,
div.Dividends as Dividends,
fgi.FG_Textvalue as Fear_Greed_Value,
esg.Total_ESG as ESG_Score,
ns.News_spikes_w as News_spikes_w,
ns.News_spikes_m as News_spikes_m,
ns.Avg_ess as Avg_ess,
ns.Avg_ess_w as Avg_ess_w,
ns.Avg_ess_m as Avg_ess_m,
ns.Avg_str_ess_w as Avg_str_ess_w,
ns.Avg_str_ess_m as Avg_str_ess_m,
fomc.Score_Statement_ML as Score_Statement_ML,
fomc.Score_Statement_DB as Score_Statement_DB,
fomc.Score_Minutes_ML as Score_Minutes_ML,
fomc.Score_Minutes_DB as Score_Minutes_DB
FROM `{{ params.project_id }}.{{ params.dwh_dataset }}.D_ALL_PRICE` ps 
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_STOCK_INFO` si 
ON ps.Ticker_id = si.Ticker_id
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_STOCK_FUNDAMENTALS` sf
ON ps.Ticker_id = sf.Ticker_id and ps.Date = sf.Date
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_SG_IR` sgir
ON ps.Date = sgir.Date 
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_STOCK_DIVIDENDS` div
ON ps.Date = div.Date and ps.Ticker_id = div.Ticker
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_FEAR_GREED_INDEX` fgi
ON ps.Date = fgi.Date
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_ESG_SCORE` esg 
ON ps.Ticker_id = esg.Ticker_id and ps.Date = esg.Date
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_NEWS_VOL_SPIKES` ns 
ON ps.Ticker_id = ns.Ticker_id and ps.Date = ns.Date
LEFT JOIN `{{ params.project_id }}.{{ params.dwh_dataset }}.D_FOMC` fomc 
ON ps.Date = fomc.Date
ORDER BY 
Date DESC, Ticker ASC