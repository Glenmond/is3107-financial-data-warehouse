-- Create Exchange Rate Table

CREATE TABLE IF NOT EXISTS staging.S_PG_EXCHANGE_RATE(
    Date_ TEXT NOT NULL,
    Ticker TEXT NULL,
    High decimal NULL,
    Low decimal NULL,
    Open decimal NULL,
    Close decimal NULL,
    Volume decimal NULL,
    Adj_Close decimal NULL,
    Exchange_rate TEXT NULL,
    PRIMARY KEY(Date_, Ticker, Exchange_rate)
    );