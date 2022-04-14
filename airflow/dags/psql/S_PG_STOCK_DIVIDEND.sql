-- Create Stock_Dividends Table

CREATE TABLE IF NOT EXISTS staging.S_PG_STOCK_DIVIDEND(
    Date_ TEXT NOT NULL,
    Dividends decimal NOT NULL,
    Ticker TEXT NOT NULL,
    Stock TEXT NOT NULL,
    PRIMARY KEY (Date_, Ticker)
);
