-- Create prices.csv table

CREATE TABLE IF NOT EXISTS staging.S_PG_ALL_PRICES(
    Date_ TEXT NOT NULL,
    Ticker TEXT NOT NULL,
    High decimal NULL,
    Low decimal NULL,
    Open decimal NULL,
    Close decimal NULL,
    Volume decimal NULL,
    Adj_Close decimal NULL,
    Name TEXT NOT NULL,
    PRIMARY KEY (Date_, Ticker)
);

