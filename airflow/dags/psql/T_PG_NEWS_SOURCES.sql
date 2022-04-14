DROP TABLE IF EXISTS T_PG_NEWS_SOURCES;

CREATE TABLE IF NOT EXISTS T_PG_NEWS_SOURCES AS
SELECT DISTINCT
timestamp_tz as Date_,
entity_name as Ticker_id,
country_code as Country_code,
event_sentiment_score as Event_sentiment_score,
topic as Topic,
type_ as Event_type,
category as Category,
event_text as Event_text,
news_type as News_type,
source_name as Source_name,
headline as Headline
FROM staging.S_PG_NEWS_SOURCES
ORDER BY Date_ DESC;

