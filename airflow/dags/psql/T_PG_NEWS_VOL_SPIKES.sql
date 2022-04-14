DROP TABLE IF EXISTS T_PG_NEWS_VOL_SPIKES;

CREATE TABLE IF NOT EXISTS T_PG_NEWS_VOL_SPIKES AS
SELECT 
timestamp_utc as Date_,
entity_name as Ticker_id,
news_spikes_w as News_spikes_w,
news_spikes_m as News_spikes_m,
avg_ess as Avg_ess,
avg_ess_w as Avg_ess_w,
avg_ess_m as Avg_ess_m,
avg_str_ess_w as Avg_str_ess_w,
avg_str_ess_m as Avg_str_ess_m
FROM staging.S_PG_NEWS_VOL_SPIKES
ORDER BY Date_ DESC;