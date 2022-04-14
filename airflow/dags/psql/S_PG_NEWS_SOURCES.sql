CREATE TABLE IF NOT EXISTS staging.S_PG_NEWS_SOURCES(
    timestamp_tz TEXT NOT NULL,
    entity_name TEXT NULL,
    country_code TEXT NULL,
    event_sentiment_score DECIMAL NULL,
    topic TEXT NULL,
    group_ TEXT NULL,
    type_ TEXT NULL,
    category TEXT NULL,
    event_text TEXT NULL,
    news_type TEXT NULL,
    source_name TEXT NULL,
    headline TEXT NULL,
    PRIMARY KEY (timestamp_tz,entity_name, event_text, group_, type_, news_type, headline)
);
