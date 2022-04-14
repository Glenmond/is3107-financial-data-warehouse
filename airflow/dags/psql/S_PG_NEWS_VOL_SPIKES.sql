CREATE TABLE IF NOT EXISTS staging.S_PG_NEWS_VOL_SPIKES(
    timestamp_utc TEXT NOT NULL,
    rp_entity_id DECIMAL NULL,
    entity_name DECIMAL NULL,
    avg_ess DECIMAL NULL,
    news_spikes_w DECIMAL NULL,
    avg_ess_w DECIMAL NULL,
    avg_ess_m DECIMAL NULL,
    news_spikes_m DECIMAL NULL,
    avg_str_ess_w DECIMAL NULL,
    avg_str_ess_m DECIMAL NULL,
    PRIMARY KEY (timestamp_utc)
);
