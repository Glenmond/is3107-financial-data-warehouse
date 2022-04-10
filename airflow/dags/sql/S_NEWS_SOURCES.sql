CREATE OR REPLACE TABLE `{{ params.staging_destination_dataset }}.S_NEWS_SOURCES` AS
SELECT
timestamp_tz as Date,
entity_name as Ticker_id,
country_code as Country_code,
event_sentiment_score as Event_sentiment_score,
topic as Topic,
type as Event_type,
category as Category,
event_text as Event_text,
news_type as News_type,
source_name as Source_name,
headline as Headline
FROM `{{ params.project_id }}.{{ params.staging_source_dataset }}.NEWS_SOURCES_STAGING`
ORDER BY Date DESC;