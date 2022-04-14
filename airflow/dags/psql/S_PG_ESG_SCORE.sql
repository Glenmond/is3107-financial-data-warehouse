CREATE TABLE IF NOT EXISTS staging.S_PG_ESG_SCORE(
    Date_ TEXT NOT NULL,
    Ticker_id TEXT NULL,
    Social_score DECIMAL NULL,
    Governance_score DECIMAL NULL,
    Environment_score DECIMAL NULL,
    Total_ESG DECIMAL NULL,
    PRIMARY KEY (Date_)
);